use gasket::error::AsWorkError;
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::miniprotocols::chainsync::{HeaderContent, N2NClient, NextResponse};
use pallas::network::miniprotocols::{chainsync, handshake};
use pallas::network::multiplexer::{self, StdChannel};

use crate::prelude::*;

fn to_traverse<'b>(header: &'b chainsync::HeaderContent) -> Result<MultiEraHeader<'b>, Error> {
    let out = match header.byron_prefix {
        Some((subtag, _)) => MultiEraHeader::decode(header.variant, Some(subtag), &header.cbor),
        None => MultiEraHeader::decode(header.variant, None, &header.cbor),
    };

    out.map_err(Error::parse)
}

type OuroborosClient = N2NClient<StdChannel>;

type OutputPort = gasket::messaging::OutputPort<ChainSyncEvent>;

pub struct Worker {
    peer_address: String,
    network_magic: u64,
    chain_cursor: Cursor,
    ouroboros_client: Option<OuroborosClient>,
    output: OutputPort,
    block_count: gasket::metrics::Counter,
    chain_tip: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(
        peer_address: String,
        network_magic: u64,
        chain_cursor: Cursor,
        output: OutputPort,
    ) -> Self {
        Self {
            peer_address,
            network_magic,
            chain_cursor,
            output,
            ouroboros_client: None,
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }

    pub fn connect(&self) -> Result<OuroborosClient, Error> {
        log::debug!("connecting muxer");

        let bearer = multiplexer::bearers::Bearer::connect_unix(&self.peer_address)
            .map_err(Error::client)?;
        let mut plexer = multiplexer::StdPlexer::new(bearer);

        let channel0 = plexer.use_channel(0);
        let channel5 = plexer.use_channel(5);

        plexer.muxer.spawn();
        plexer.demuxer.spawn();

        log::debug!("doing handshake");

        let versions = handshake::n2n::VersionTable::v7_and_above(self.network_magic);
        let mut client = handshake::Client::new(channel0);

        let output = client.handshake(versions).map_err(Error::client)?;

        log::info!("handshake output: {:?}", output);

        match output {
            handshake::Confirmation::Accepted(version, _) => {
                log::info!("connected to upstream peer using version {}", version);
                Ok(OuroborosClient::new(channel5))
            }
            _ => Err(Error::client("couldn't agree on handshake version")),
        }
    }

    fn process_next(
        &mut self,
        next: NextResponse<HeaderContent>,
    ) -> Result<(), gasket::error::Error> {
        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                let h = to_traverse(&h).or_panic()?;
                self.output
                    .send(ChainSyncEvent::RollForward(h.slot(), h.hash()).into())?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.output.send(ChainSyncEvent::Rollback(p).into())?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::Await => {
                log::info!("chain-sync reached the tip of the chain");
                Ok(())
            }
        }
    }

    fn request_next(&mut self) -> Result<(), gasket::error::Error> {
        log::info!("requesting next block");

        let next = self
            .ouroboros_client
            .as_mut()
            .unwrap()
            .request_next()
            .or_restart()?;

        self.process_next(next)
    }

    fn await_next(&mut self) -> Result<(), gasket::error::Error> {
        log::info!("awaiting next block (blocking)");

        let next = self
            .ouroboros_client
            .as_mut()
            .unwrap()
            .recv_while_must_reply()
            .or_restart()?;

        self.process_next(next)
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("received_blocks", &self.block_count)
            .with_gauge("chain_tip", &self.chain_tip)
            .build()
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let mut client = self.connect().or_retry()?;

        let point = self.chain_cursor.last_point().or_panic()?;

        log::info!("intersecting chain at point: {:?}", point);

        let (point, _) = client
            .find_intersect(vec![point])
            .map_err(Error::client)
            .or_restart()?;

        log::info!("chain-sync intersection is {:?}", point);

        self.ouroboros_client = Some(client);
        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        match self.ouroboros_client.as_ref().unwrap().has_agency() {
            true => self.request_next()?,
            false => self.await_next()?,
        };

        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
