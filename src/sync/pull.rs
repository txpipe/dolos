use std::collections::VecDeque;

use gasket::framework::*;
use tracing::{debug, info};

use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{
    HeaderContent, NextResponse, RollbackBuffer, RollbackEffect, Tip,
};
use pallas::network::miniprotocols::Point;

use crate::prelude::*;

const HARDCODED_BREADCRUMBS: usize = 20;

#[derive(Clone)]
pub enum Intersection {
    Tip,
    Origin,
    Breadcrumbs(VecDeque<Point>),
}

impl Intersection {
    pub fn add_breadcrumb(&mut self, slot: u64, hash: &[u8]) {
        let point = Point::Specific(slot, Vec::from(hash));

        match self {
            Intersection::Tip => {
                *self = Intersection::Breadcrumbs(VecDeque::from(vec![point]));
            }
            Intersection::Origin => {
                *self = Intersection::Breadcrumbs(VecDeque::from(vec![point]));
            }
            Intersection::Breadcrumbs(x) => {
                x.push_front(point);

                if x.len() > HARDCODED_BREADCRUMBS {
                    x.pop_back();
                }
            }
        }
    }
}

impl FromIterator<(u64, Hash<32>)> for Intersection {
    fn from_iter<T: IntoIterator<Item = (u64, Hash<32>)>>(iter: T) -> Self {
        let points: VecDeque<_> = iter
            .into_iter()
            .map(|(slot, hash)| Point::Specific(slot, hash.to_vec()))
            .collect();

        if points.is_empty() {
            Intersection::Origin
        } else {
            Intersection::Breadcrumbs(points)
        }
    }
}

fn to_traverse(header: &HeaderContent) -> Result<MultiEraHeader<'_>, WorkerError> {
    let out = match header.byron_prefix {
        Some((subtag, _)) => MultiEraHeader::decode(header.variant, Some(subtag), &header.cbor),
        None => MultiEraHeader::decode(header.variant, None, &header.cbor),
    };

    out.or_panic()
}

pub type DownstreamPort = gasket::messaging::tokio::OutputPort<PullEvent>;

async fn intersect(peer: &mut PeerClient, intersection: &Intersection) -> Result<(), WorkerError> {
    let chainsync = peer.chainsync();

    let intersect = match intersection {
        Intersection::Origin => {
            info!("intersecting origin");
            chainsync.intersect_origin().await.or_restart()?.into()
        }
        Intersection::Tip => {
            info!("intersecting tip");
            chainsync.intersect_tip().await.or_restart()?.into()
        }
        Intersection::Breadcrumbs(points) => {
            info!("intersecting breadcrumbs");
            let (point, _) = chainsync
                .find_intersect(points.clone().into())
                .await
                .or_restart()?;
            point
        }
    };

    info!(?intersect, "intersected");

    Ok(())
}

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
    async fn define_pull_batch(&mut self, stage: &mut Stage) -> Result<PullBatch, WorkerError> {
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
        debug!("connecting");

        let mut peer_session = PeerClient::connect(&stage.peer_address, stage.network_magic)
            .await
            .or_retry()?;

        info!(
            address = stage.peer_address,
            magic = stage.network_magic,
            "connected to upstream node"
        );

        intersect(&mut peer_session, &stage.intersection).await?;

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
                let batch = self.define_pull_batch(stage).await?;

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
    intersection: Intersection,

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
        intersection: Intersection,
    ) -> Self {
        Self {
            peer_address,
            network_magic,
            intersection,
            block_fetch_batch_size,
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }

    async fn flush_blocks(&mut self, blocks: Vec<RawBlock>) -> Result<(), WorkerError> {
        for cbor in blocks {
            // TODO: can we avoid decoding in this stage?
            let block = MultiEraBlock::decode(&cbor).or_panic()?;
            let slot = block.slot();
            let hash = block.hash();

            self.downstream
                .send(PullEvent::RollForward(slot, hash, cbor).into())
                .await
                .or_panic()?;

            self.intersection.add_breadcrumb(slot, hash.as_ref());
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

        if let Point::Specific(slot, hash) = &point {
            self.intersection.add_breadcrumb(*slot, hash);
        }

        Ok(())
    }

    fn track_tip(&self, tip: &Tip) {
        self.chain_tip.set(tip.0.slot_or_default() as i64);
    }
}
