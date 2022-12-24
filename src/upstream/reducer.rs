use gasket::error::AsWorkError;
use tracing::{debug, error, instrument};

use crate::prelude::*;
use crate::rolldb::RollDB;

pub type UpstreamPort = gasket::messaging::TwoPhaseInputPort<BlockFetchEvent>;

pub struct Worker {
    upstream: UpstreamPort,
    db: RollDB,
    block_count: gasket::metrics::Counter,
    wal_count: gasket::metrics::Counter,
}

impl Worker {
    pub fn new(upstream: UpstreamPort, db: RollDB) -> Self {
        Self {
            upstream,
            db,
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }

    fn apply(&mut self, event: BlockFetchEvent) -> Result<(), gasket::error::Error> {
        match event {
            BlockFetchEvent::RollForward(slot, hash, body) => {
                self.db.roll_forward(slot, hash, body).or_panic()?;
            }
            BlockFetchEvent::Rollback(point) => match point {
                pallas::network::miniprotocols::Point::Specific(slot, _) => {
                    self.db.roll_back(slot).or_panic()?;
                }
                pallas::network::miniprotocols::Point::Origin => {
                    todo!();
                }
            },
        }

        //self.db.compact();

        Ok(())
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("saved_blocks", &self.block_count)
            .with_counter("wal_commands", &self.wal_count)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.upstream.recv_or_idle()?;
        self.apply(msg.payload)?;

        // remove the processed event from the queue
        self.upstream.commit();

        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
