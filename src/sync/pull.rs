use gasket::framework::*;
use itertools::Itertools;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{
    HeaderContent, NextResponse, RollbackBuffer, RollbackEffect, Tip,
};
use pallas::network::miniprotocols::Point;
use tracing::{debug, info};

use crate::prelude::*;
use crate::wal::redb::WalStore;
use crate::wal::WalReader;

fn to_traverse(header: &HeaderContent) -> Result<MultiEraHeader<'_>, WorkerError> {
    let out = match header.byron_prefix {
        Some((subtag, _)) => MultiEraHeader::decode(header.variant, Some(subtag), &header.cbor),
        None => MultiEraHeader::decode(header.variant, None, &header.cbor),
    };

    out.or_panic()
}

pub type DownstreamPort = gasket::messaging::OutputPort<PullEvent>;

enum PullBatch {
    BlockRange(Point, Point),
    OutOfScopeRollback(Point),
    Empty,
}

pub enum WorkUnit {
    Pull,
    Await,
}

pub struct Worker {
    peer_session: PeerClient,
}

impl Worker {
    async fn gather_pull_batch(&mut self, stage: &mut Stage) -> Result<PullBatch, WorkerError> {
        let client = self.peer_session.chainsync();
        let mut buffer = RollbackBuffer::new();

        while buffer.size() < stage.block_fetch_batch_size {
            let next = client.request_next().await.or_restart()?;

            match next {
                NextResponse::RollForward(header, tip) => {
                    let header = to_traverse(&header).or_panic()?;
                    let point = Point::Specific(header.slot(), header.hash().to_vec());
                    buffer.roll_forward(point);

                    stage.track_tip(&tip);
                }
                NextResponse::RollBackward(point, _) => match buffer.roll_back(&point) {
                    RollbackEffect::OutOfScope => return Ok(PullBatch::OutOfScopeRollback(point)),
                    RollbackEffect::Handled => (),
                },
                NextResponse::Await => break,
            }
        }

        let range = match (buffer.oldest(), buffer.latest()) {
            (Some(a), Some(b)) => PullBatch::BlockRange(a.clone(), b.clone()),
            _ => PullBatch::Empty,
        };

        Ok(range)
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("finding intersection candidates");

        let candidates = stage
            .wal
            .intersect_candidates(5)
            .or_panic()?
            .into_iter()
            .map(From::from)
            .collect_vec();

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

        let worker = Self { peer_session };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        _stage: &mut Stage,
    ) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
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
        match unit {
            WorkUnit::Pull => {
                let batch = self.gather_pull_batch(stage).await?;

                match batch {
                    PullBatch::BlockRange(start, end) => {
                        let blocks = self
                            .peer_session
                            .blockfetch()
                            .fetch_range((start, end))
                            .await
                            .or_restart()?;

                        stage.flush_blocks(blocks).await?;
                    }
                    PullBatch::OutOfScopeRollback(point) => {
                        stage.flush_rollback(point).await?;
                    }
                    PullBatch::Empty => (),
                };
            }
            WorkUnit::Await => {
                let next = self
                    .peer_session
                    .chainsync()
                    .recv_while_must_reply()
                    .await
                    .or_restart()?;

                match next {
                    NextResponse::RollForward(header, tip) => {
                        let header = to_traverse(&header).or_panic()?;
                        let point = Point::Specific(header.slot(), header.hash().to_vec());

                        let block = self
                            .peer_session
                            .blockfetch()
                            .fetch_single(point)
                            .await
                            .or_restart()?;

                        stage.flush_blocks(vec![block]).await?;
                        stage.track_tip(&tip);
                    }
                    NextResponse::RollBackward(point, tip) => {
                        stage.flush_rollback(point).await?;
                        stage.track_tip(&tip);
                    }
                    NextResponse::Await => (),
                }
            }
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
    wal: WalStore,

    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

impl Stage {
    pub fn new(
        peer_address: String,
        network_magic: u64,
        block_fetch_batch_size: usize,
        wal: WalStore,
    ) -> Self {
        Self {
            peer_address,
            network_magic,
            wal,
            block_fetch_batch_size,
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }

    async fn flush_blocks(&mut self, blocks: Vec<BlockBody>) -> Result<(), WorkerError> {
        for cbor in blocks {
            // TODO: can we avoid decoding in this stage?

            let payload = {
                let decoded = MultiEraBlock::decode(&cbor).or_panic()?;

                RawBlock {
                    slot: decoded.slot(),
                    hash: decoded.hash(),
                    era: decoded.era(),
                    body: cbor,
                }
            };

            self.downstream
                .send(PullEvent::RollForward(payload).into())
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
            .send(PullEvent::Rollback(point.clone()).into())
            .await
            .or_panic()?;

        Ok(())
    }

    fn track_tip(&self, tip: &Tip) {
        self.chain_tip.set(tip.0.slot_or_default() as i64);
    }
}
