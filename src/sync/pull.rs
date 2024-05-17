use std::collections::VecDeque;

use gasket::framework::*;
use tracing::{debug, info};

use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{self, HeaderContent, NextResponse};
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

pub struct Worker {
    peer_session: PeerClient,
}

impl Worker {
    async fn process_next(
        &mut self,
        stage: &mut Stage,
        next: &NextResponse<HeaderContent>,
    ) -> Result<(), WorkerError> {
        match next {
            NextResponse::RollForward(header, tip) => {
                let header = to_traverse(header).or_panic()?;
                let slot = header.slot();
                let hash = header.hash();

                debug!(slot, %hash, "chain sync roll forward");

                let block = self
                    .peer_session
                    .blockfetch()
                    .fetch_single(Point::Specific(slot, hash.to_vec()))
                    .await
                    .or_retry()?;

                stage
                    .downstream
                    .send(PullEvent::RollForward(slot, hash, block).into())
                    .await
                    .or_panic()?;

                stage.intersection.add_breadcrumb(slot, hash.as_ref());

                stage.chain_tip.set(tip.0.slot_or_default() as i64);

                Ok(())
            }
            chainsync::NextResponse::RollBackward(point, tip) => {
                match &point {
                    Point::Origin => debug!("rollback to origin"),
                    Point::Specific(slot, _) => debug!(slot, "rollback"),
                };

                stage
                    .downstream
                    .send(PullEvent::Rollback(point.clone()).into())
                    .await
                    .or_panic()?;

                if let Point::Specific(slot, hash) = &point {
                    stage.intersection.add_breadcrumb(*slot, hash);
                }

                stage.chain_tip.set(tip.0.slot_or_default() as i64);

                Ok(())
            }
            chainsync::NextResponse::Await => {
                info!("chain-sync reached the tip of the chain");
                Ok(())
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("connecting");

        let mut peer_session = PeerClient::connect(&stage.peer_address, stage.network_magic)
            .await
            .or_retry()?;

        intersect(&mut peer_session, &stage.intersection).await?;

        let worker = Self { peer_session };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        _stage: &mut Stage,
    ) -> Result<WorkSchedule<NextResponse<HeaderContent>>, WorkerError> {
        let client = self.peer_session.chainsync();

        let next = match client.has_agency() {
            true => {
                info!("requesting next block");
                client.request_next().await.or_restart()?
            }
            false => {
                info!("awaiting next block (blocking)");
                client.recv_while_must_reply().await.or_restart()?
            }
        };

        Ok(WorkSchedule::Unit(next))
    }

    async fn execute(
        &mut self,
        unit: &NextResponse<HeaderContent>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        self.process_next(stage, unit).await
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        // TODO: doesn't work because teardown takes mutable ref but abort takes
        // self.peer_session.abort();

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "pull", unit = "NextResponse<HeaderContent>", worker = "Worker")]
pub struct Stage {
    peer_address: String,
    network_magic: u64,
    intersection: Intersection,

    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

impl Stage {
    pub fn new(peer_address: String, network_magic: u64, intersection: Intersection) -> Self {
        Self {
            peer_address,
            network_magic,
            intersection,
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }
}
