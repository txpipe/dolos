use std::sync::Arc;

use dolos_core::config::{PeerConfig, SyncConfig, SyncLimit};
use gasket::framework::*;
use itertools::Itertools;
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{
    HeaderContent, NextResponse, RollbackBuffer, RollbackEffect, Tip,
};
use pallas::network::miniprotocols::Point;
use tracing::{debug, info, warn};

use crate::adapters::WalAdapter;
use crate::prelude::*;

fn to_traverse(header: &HeaderContent) -> Result<MultiEraHeader<'_>, WorkerError> {
    let out = match header.byron_prefix {
        Some((subtag, _)) => MultiEraHeader::decode(header.variant, Some(subtag), &header.cbor),
        None => MultiEraHeader::decode(header.variant, None, &header.cbor),
    };

    out.or_panic()
}

/// Check that a new header's previous hash matches the tip of the chain buffer.
/// Returns Err(Restart) if the chain is discontinuous, signaling that
/// we should reconnect and find a new intersection.
fn check_chain_continuity(
    header: &MultiEraHeader,
    chain_buffer: &RollbackBuffer,
) -> Result<(), WorkerError> {
    let prev_hash = header.previous_hash();

    let known = chain_buffer.latest().and_then(|p| match p {
        Point::Specific(_, hash) => <[u8; 32]>::try_from(hash.as_slice()).ok().map(Hash::new),
        Point::Origin => None,
    });

    if let (Some(prev), Some(known)) = (prev_hash, known) {
        if prev != known {
            warn!(
                slot = header.slot(),
                expected = %known,
                got = %prev,
                "block parent hash mismatch, upstream peer may have switched forks — reconnecting"
            );
            return Err(WorkerError::Restart);
        }
    }

    Ok(())
}

pub type DownstreamPort = gasket::messaging::OutputPort<PullEvent>;

enum PullResult {
    Blocks(Vec<Point>),
    Rollback(Point),
    Empty,
}

pub enum WorkUnit {
    Pull,
    Await,
}

pub enum PullQuota {
    WaitingTip,
    Unlimited,
    BlockQuota(u64),
    Reached,
}

impl PullQuota {
    fn should_quit(&self) -> bool {
        matches!(self, Self::Reached)
    }

    fn on_tip(&mut self) {
        if let Self::WaitingTip = self {
            *self = Self::Reached;
        }
    }

    fn consume_blocks(&mut self, count: u64) {
        if let Self::BlockQuota(x) = self {
            let new = x.saturating_sub(count);

            if new == 0 {
                *self = Self::Reached;
            } else {
                *self = Self::BlockQuota(new);
            }
        }
    }
}

impl From<SyncLimit> for PullQuota {
    fn from(limit: SyncLimit) -> Self {
        match limit {
            SyncLimit::UntilTip => Self::WaitingTip,
            SyncLimit::NoLimit => Self::Unlimited,
            SyncLimit::MaxBlocks(blocks) => Self::BlockQuota(blocks),
        }
    }
}

pub struct Worker {
    peer_session: PeerClient,
    chain_buffer: RollbackBuffer,
}

impl Worker {
    /// Receive the next chainsync response, using the appropriate method
    /// depending on whether we have agency (catching up) or not (at the tip).
    async fn recv_next_header(&mut self) -> Result<NextResponse<HeaderContent>, WorkerError> {
        let client = self.peer_session.chainsync();

        if client.has_agency() {
            client.request_next().await.or_restart()
        } else {
            client.recv_while_must_reply().await.or_restart()
        }
    }

    /// Gather up to `max_headers` headers from the upstream peer.
    ///
    /// For each chainsync response:
    /// - RollForward: validate chain continuity, track in buffer
    /// - RollBackward: update buffer; if out of scope, return as rollback
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

                    check_chain_continuity(&header, &self.chain_buffer)?;

                    let point = Point::Specific(header.slot(), header.hash().to_vec());
                    debug!(?point, "header received from upstream peer");
                    self.chain_buffer.roll_forward(point);
                    gathered += 1;

                    stage.track_tip(&tip);
                }
                NextResponse::RollBackward(point, tip) => {
                    debug!(?point, "rollback sent by upstream peer");

                    match self.chain_buffer.roll_back(&point) {
                        RollbackEffect::OutOfScope => return Ok(PullResult::Rollback(point)),
                        RollbackEffect::Handled => (),
                    }

                    stage.track_tip(&tip);
                }
                NextResponse::Await => break,
            }
        }

        let points = self.chain_buffer.pop_with_depth(0);

        if points.is_empty() {
            Ok(PullResult::Empty)
        } else {
            Ok(PullResult::Blocks(points))
        }
    }

    /// Fetch block bodies for the given points and flush them downstream.
    async fn fetch_and_flush(
        &mut self,
        points: &[Point],
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        let blocks = match points {
            [single] => {
                let block = self
                    .peer_session
                    .blockfetch()
                    .fetch_single(single.clone())
                    .await
                    .or_restart()?;

                vec![block]
            }
            [first, .., last] => {
                self.peer_session
                    .blockfetch()
                    .fetch_range((first.clone(), last.clone()))
                    .await
                    .or_restart()?
            }
            [] => return Ok(()),
        };

        debug!(len = blocks.len(), "block batch pulled from peer");

        stage.quota.consume_blocks(blocks.len() as u64);
        stage.flush_blocks(blocks).await?;

        Ok(())
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

        let mut peer_session = PeerClient::connect(&stage.peer_address, stage.network_magic)
            .await
            .or_retry()?;

        info!(
            address = stage.peer_address,
            magic = stage.network_magic,
            "connected to peer"
        );

        debug!("finding intersect");

        let (point, _) = peer_session
            .chainsync()
            .find_intersect(candidates)
            .await
            .or_restart()?;

        let intersection = point
            .ok_or(Error::message("couldn't find intersect"))
            .or_panic()?;

        info!(?intersection, "found intersection");

        let mut chain_buffer = RollbackBuffer::new();
        chain_buffer.roll_forward(intersection);

        let worker = Self {
            peer_session,
            chain_buffer,
        };

        Ok(worker)
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        if stage.quota.should_quit() {
            warn!("quota reached, stopping sync");
            return Ok(WorkSchedule::Done);
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
            stage.quota.on_tip();
        }

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "pull", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    peer_address: String,
    network_magic: u64,
    block_fetch_batch_size: usize,
    wal: WalAdapter,
    quota: PullQuota,

    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

impl Stage {
    pub fn new(
        config: &SyncConfig,
        upstream: &PeerConfig,
        network_magic: u64,
        wal: WalAdapter,
    ) -> Self {
        Self {
            peer_address: upstream.peer_address.clone(),
            network_magic,
            quota: config.sync_limit.clone().into(),
            block_fetch_batch_size: config.pull_batch_size(),
            wal,
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
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

    async fn flush_rollback(&mut self, point: Point) -> Result<(), WorkerError> {
        match &point {
            Point::Origin => debug!("rollback to origin"),
            Point::Specific(slot, _) => debug!(slot, "rollback"),
        };

        self.downstream
            .send(PullEvent::Rollback(point.into()).into())
            .await
            .or_panic()?;

        Ok(())
    }

    fn track_tip(&self, tip: &Tip) {
        self.chain_tip.set(tip.0.slot_or_default() as i64);
    }
}
