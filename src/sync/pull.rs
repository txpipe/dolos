use std::sync::Arc;

use dolos_cardano::consensus::{ChainFragment, RollbackResult};
use dolos_core::config::SyncConfig;
use dolos_core::ChainPoint;
use gasket::framework::*;
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{HeaderContent, NextResponse, Tip};
use pallas::network::miniprotocols::Point;
use tracing::{debug, info, warn};

use super::retry::{Attempts, Failure};
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
    /// Whether this session delivered something: a block, or the tip.
    /// Headers alone are not enough: a peer whose chain grows sends new ones
    /// to every session, and may never serve their blocks.
    delivered: bool,
    /// Why this session failed, once it has.
    lost: Option<Failure>,
}

impl Worker {
    /// Note why the session failed, for the stage to reconnect or give up.
    fn lose(&mut self, what: &str, error: &(dyn std::error::Error + 'static)) -> WorkerError {
        self.lost = Some(Failure::new(what, error));
        WorkerError::Restart
    }

    /// The session delivered something: the failed attempts start again.
    fn deliver(&mut self, stage: &Stage) {
        self.delivered = true;
        stage.attempts.reset();
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

        next.map_err(|error| self.lose("chain-sync failed", &error))
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
                            self.lose("the upstream sent a header that does not chain", &error)
                        );
                    }

                    debug!(%point, "header received from upstream peer");
                    gathered += 1;

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

        let blocks = fetched.map_err(|error| self.lose("block-fetch failed", &error))?;

        debug!(len = blocks.len(), "block batch pulled from peer");

        stage.flush_blocks(blocks).await?;
        self.deliver(stage);

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
            self.deliver(stage);

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

    /// Connect to the peer and find the intersection: one attempt.
    async fn open(stage: &Stage, candidates: Vec<Point>) -> Result<Self, Failure> {
        debug!("connecting to peer");

        let mut peer_session = PeerClient::connect(&stage.peer_address, stage.network_magic)
            .await
            .map_err(|error| Failure::connecting(&error))?;

        info!(
            address = stage.peer_address,
            magic = stage.network_magic,
            "connected to peer"
        );

        debug!("finding intersect");

        let found = peer_session.chainsync().find_intersect(candidates).await;

        let (point, tip) = match found {
            Ok(found) => found,
            Err(error) => {
                peer_session.abort().await;
                return Err(Failure::new("finding the intersection", &error));
            }
        };

        let tip = ChainPoint::from(tip.0);

        let Some(intersection) = point else {
            peer_session.abort().await;

            return Err(Failure::message(format!(
                "finding the intersection: the peer, at {tip}, holds none of the WAL's recent points"
            )));
        };

        info!(?intersection, "found intersection");

        Ok(Self {
            peer_session,
            chain: ChainFragment::start(ChainPoint::from(intersection)),
            tip,
            delivered: false,
            lost: None,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    /// Retried here rather than by gasket, so that a failed connection or
    /// intersection waits the backoff of its place in the count.
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        loop {
            let candidates = stage.intersect_candidates()?;

            match Self::open(stage, candidates).await {
                Ok(worker) => return Ok(worker),
                Err(failure) => stage.attempts.failed(failure).await?,
            }
        }
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

        let failure = self
            .lost
            .take()
            .unwrap_or_else(|| Failure::message("the session failed".into()));

        // A session that delivered something reconnects at once; one that
        // delivered nothing is a failed attempt.
        if self.delivered {
            warn!(cause = %failure, "the upstream session ended, reconnecting");
        } else {
            stage.attempts.failed(failure).await?;
        }

        Err(WorkerError::Restart)
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

    /// Connections, intersections and sessions that failed in a row before
    /// their session delivered anything.
    attempts: Attempts,

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
            attempts: Attempts::new(
                "pull",
                peer_address.clone(),
                config.retry.clone(),
                progress.clone(),
            ),
            peer_address,
            network_magic,
            limit,
            progress,
            block_fetch_batch_size: config.pull_batch_size,
            wal,
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }

    /// The points the WAL offers for an intersection, Origin when it holds
    /// none.
    fn intersect_candidates(&self) -> Result<Vec<Point>, WorkerError> {
        let mut candidates = self
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

        Ok(candidates)
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
