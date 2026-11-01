use std::cell::Cell;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use dolos_cardano::consensus::{ChainFragment, RollbackResult};
use dolos_core::config::{RetryConfig, SyncConfig};
use dolos_core::{BlockSlot, ChainPoint};
use gasket::framework::*;
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{HeaderContent, NextResponse, Tip};
use pallas::network::miniprotocols::Point;
use tracing::{debug, error, info, warn};

use super::{SyncLimit, SyncProgress};
use crate::adapters::WalAdapter;
use crate::prelude::*;

fn to_traverse(header: &HeaderContent) -> Result<MultiEraHeader<'_>, WorkerError> {
    let out = match header.byron_prefix {
        Some((subtag, _)) => MultiEraHeader::decode(header.variant, Some(subtag), &header.cbor),
        None => MultiEraHeader::decode(header.variant, None, &header.cbor),
    };

    out.or_panic()
}

// ============================================================================
// Pull stage
// ============================================================================

pub type DownstreamPort = gasket::messaging::OutputPort<PullEvent>;

enum PullResult {
    Blocks(Vec<ChainPoint>),
    Rollback(ChainPoint),
    Empty,
}

pub enum WorkUnit {
    Pull,
    Await,
}

pub struct Worker {
    peer_session: PeerClient,
    chain: ChainFragment,
    /// The tip the peer reported last.
    tip: ChainPoint,
    /// Whether this session delivered something new: a block, the tip, or a
    /// header past every one an earlier session received.
    progressed: bool,
    /// Why this session failed, once it has.
    lost: Option<String>,
}

impl Worker {
    /// Note why the session failed, for the stage to reconnect or give up.
    fn lose(&mut self, what: &str, error: impl Display) -> WorkerError {
        warn!(%error, "{what}");
        self.lost = Some(format!("{what}: {error}"));
        WorkerError::Restart
    }

    /// Receive the next chainsync response, using the appropriate method
    /// depending on whether we have agency (catching up) or not (at the tip).
    async fn recv_next_header(&mut self) -> Result<NextResponse<HeaderContent>, WorkerError> {
        let client = self.peer_session.chainsync();

        let next = if client.has_agency() {
            client.request_next().await
        } else {
            client.recv_while_must_reply().await
        };

        next.map_err(|error| self.lose("chain-sync failed", error))
    }

    /// Gather up to `max_headers` headers from the upstream peer.
    ///
    /// For each chainsync response:
    /// - RollForward: validate chain continuity, track in fragment
    /// - RollBackward: update fragment; if out of scope, return as rollback
    /// - Await: stop gathering (peer has no more blocks)
    ///
    /// Returns the gathered points to fetch, a rollback to propagate, or empty.
    async fn pull_headers(
        &mut self,
        max_headers: usize,
        stage: &mut Stage,
    ) -> Result<PullResult, WorkerError> {
        let mut gathered = 0;

        while gathered < max_headers {
            let next = self.recv_next_header().await?;

            match next {
                NextResponse::RollForward(header, tip) => {
                    let header = to_traverse(&header).or_panic()?;
                    let point = ChainPoint::Specific(header.slot(), header.hash());
                    let prev_hash = header.previous_hash();

                    if let Err(error) = self.chain.roll_forward(point.clone(), prev_hash) {
                        return Err(
                            self.lose("the upstream sent a header that does not chain", error)
                        );
                    }

                    debug!(%point, "header received from upstream peer");
                    gathered += 1;

                    if stage
                        .furthest_header
                        .is_none_or(|furthest| point.slot() > furthest)
                    {
                        stage.furthest_header = Some(point.slot());
                        self.progressed = true;
                    }

                    self.track_tip(&tip, stage);
                }
                NextResponse::RollBackward(point, tip) => {
                    debug!(?point, "rollback sent by upstream peer");

                    self.track_tip(&tip, stage);

                    let chain_point = ChainPoint::from(point);

                    match self.chain.roll_back(&chain_point) {
                        RollbackResult::OutOfScope(point) => {
                            return Ok(PullResult::Rollback(point))
                        }
                        RollbackResult::Handled => (),
                    }
                }
                NextResponse::Await => break,
            }
        }

        let points = self.chain.take_pending();

        if points.is_empty() {
            Ok(PullResult::Empty)
        } else {
            Ok(PullResult::Blocks(points))
        }
    }

    /// Fetch block bodies for the given points and flush them downstream.
    async fn fetch_and_flush(
        &mut self,
        points: &[ChainPoint],
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        let to_pallas = |cp: &ChainPoint| -> Point {
            Point::try_from(cp.clone()).expect("pending points are always Specific")
        };

        let fetched = match points {
            [single] => self
                .peer_session
                .blockfetch()
                .fetch_single(to_pallas(single))
                .await
                .map(|block| vec![block]),
            [first, .., last] => {
                self.peer_session
                    .blockfetch()
                    .fetch_range((to_pallas(first), to_pallas(last)))
                    .await
            }
            [] => return Ok(()),
        };

        let blocks = fetched.map_err(|error| self.lose("block-fetch failed", error))?;

        debug!(len = blocks.len(), "block batch pulled from peer");

        stage.flush_blocks(blocks).await?;
        self.progressed = true;

        Ok(())
    }

    async fn pull(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        let max_headers = match unit {
            WorkUnit::Pull => stage.block_fetch_batch_size,
            WorkUnit::Await => 1,
        };

        match self.pull_headers(max_headers, stage).await? {
            PullResult::Blocks(points) => self.fetch_and_flush(&points, stage).await?,
            PullResult::Rollback(point) => stage.flush_rollback(point).await?,
            PullResult::Empty => (),
        }

        if !self.peer_session.chainsync().has_agency() {
            self.progressed = true;

            if stage.progress.fix_target(self.tip.clone()) {
                info!(tip = %self.tip, "reached the upstream tip, the catch-up target");
            }
        }

        Ok(())
    }

    fn track_tip(&mut self, tip: &Tip, stage: &Stage) {
        self.tip = ChainPoint::from(tip.0.clone());
        stage.chain_tip.set(tip.0.slot_or_default() as i64);
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("finding intersection candidates");

        let mut candidates = stage
            .wal
            .intersect_candidates(5)
            .or_panic()?
            .into_iter()
            .map(TryFrom::try_from)
            .filter_map(|x| x.ok())
            .collect_vec();

        if candidates.is_empty() {
            candidates.push(Point::Origin);
        }

        debug!("connecting to peer");

        let mut peer_session =
            match PeerClient::connect(&stage.peer_address, stage.network_magic).await {
                Ok(session) => session,
                Err(error) => return Err(stage.attempt_failed(format!("connecting: {error}"))),
            };

        info!(
            address = stage.peer_address,
            magic = stage.network_magic,
            "connected to peer"
        );

        debug!("finding intersect");

        let (point, tip) = match peer_session.chainsync().find_intersect(candidates).await {
            Ok(found) => found,
            Err(error) => {
                peer_session.abort().await;
                return Err(stage.attempt_failed(format!("finding the intersection: {error}")));
            }
        };

        let intersection = point
            .ok_or(Error::message("couldn't find intersect"))
            .or_panic()?;

        info!(?intersection, "found intersection");

        let worker = Self {
            peer_session,
            chain: ChainFragment::start(ChainPoint::from(intersection)),
            tip: ChainPoint::from(tip.0),
            progressed: false,
            lost: None,
        };

        Ok(worker)
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        if stage.limit == SyncLimit::CatchUp && stage.progress.target().is_some() {
            // Nothing past the target is pulled. The apply stage ends the
            // pipeline once it commits the target; until then this stage
            // waits, so that its end is never taken for the pipeline's.
            std::future::pending::<()>().await;
        }

        let client = self.peer_session.chainsync();

        if client.has_agency() {
            debug!("should request next batch of blocks");
            Ok(WorkSchedule::Unit(WorkUnit::Pull))
        } else {
            debug!("should await next block");
            Ok(WorkSchedule::Unit(WorkUnit::Await))
        }
    }

    async fn execute(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        let result = self.pull(unit, stage).await;

        let Err(WorkerError::Restart) = result else {
            return result;
        };

        // A session that delivered something reconnects at once; one that
        // delivered nothing counts as a failed attempt.
        if self.progressed {
            stage.failures.set(0);
            return Err(WorkerError::Restart);
        }

        let cause = self
            .lost
            .take()
            .unwrap_or_else(|| "the session failed".into());

        match stage.count_failure(cause) {
            Some(backoff) => {
                tokio::time::sleep(backoff).await;
                Err(WorkerError::Restart)
            }
            None => Err(WorkerError::Panic),
        }
    }
}

#[derive(Stage)]
#[stage(name = "pull", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    peer_address: String,
    network_magic: u64,
    block_fetch_batch_size: usize,
    wal: WalAdapter,
    limit: SyncLimit,
    progress: Arc<SyncProgress>,
    retry: RetryConfig,

    /// Attempts in a row that failed before their session delivered
    /// anything new: a connection, an intersection, or a session.
    failures: Cell<usize>,
    /// The slot of the furthest header any session received.
    furthest_header: Option<BlockSlot>,

    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

impl Stage {
    pub fn new(
        config: &SyncConfig,
        peer_address: String,
        network_magic: u64,
        limit: SyncLimit,
        progress: Arc<SyncProgress>,
        wal: WalAdapter,
    ) -> Self {
        Self {
            peer_address,
            network_magic,
            limit,
            progress,
            retry: config.retry.clone(),
            failures: Cell::new(0),
            furthest_header: None,
            block_fetch_batch_size: config.pull_batch_size,
            wal,
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }

    /// Count a failed attempt, and return the backoff before the next one,
    /// or none once `[sync.retry]` allows no more, recording why.
    fn count_failure(&self, cause: String) -> Option<Duration> {
        let failures = self.failures.get() + 1;
        self.failures.set(failures);

        if failures <= self.retry.max_retries {
            info!(
                failures,
                max_retries = self.retry.max_retries,
                "retrying the upstream peer"
            );
            return Some(backoff(&self.retry, failures));
        }

        error!(failures, %cause, "giving up on the upstream peer");

        let address = &self.peer_address;
        self.progress.record_upstream_failure(if failures == 1 {
            format!("gave up on the upstream peer {address}: {cause}")
        } else {
            format!(
                "gave up on the upstream peer {address} after {failures} failed attempts in a row; the last: {cause}"
            )
        });

        None
    }

    /// A failed connection or intersection: retried by gasket, which backs
    /// off, until the count runs out.
    fn attempt_failed(&self, cause: String) -> WorkerError {
        warn!(%cause, "upstream attempt failed");

        match self.count_failure(cause) {
            Some(_) => WorkerError::Retry,
            None => WorkerError::Panic,
        }
    }

    async fn flush_blocks(&mut self, blocks: Vec<BlockBody>) -> Result<(), WorkerError> {
        for cbor in blocks {
            self.downstream
                .send(PullEvent::RollForward(Arc::new(cbor)).into())
                .await
                .or_panic()?;
        }

        Ok(())
    }

    async fn flush_rollback(&mut self, point: ChainPoint) -> Result<(), WorkerError> {
        debug!(slot = point.slot(), "rollback");

        self.downstream
            .send(PullEvent::Rollback(point).into())
            .await
            .or_panic()?;

        Ok(())
    }
}

/// Retry `n` waits `backoff_unit_seconds × backoff_factor^n`, at most
/// `max_backoff_seconds`, as gasket's own retries do.
pub(super) fn backoff(retry: &RetryConfig, n: usize) -> Duration {
    let units = u64::from(retry.backoff_factor).saturating_pow(n as u32);
    let backoff = retry.backoff_unit_seconds.saturating_mul(units);

    Duration::from_secs(backoff.min(retry.max_backoff_seconds))
}
