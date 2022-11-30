use gasket::error::AsWorkError;
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::miniprotocols::chainsync;
use pallas::network::miniprotocols::chainsync::{HeaderContent, N2NClient, NextResponse};
use pallas::network::multiplexer;

use crate::prelude::*;

fn to_traverse<'b>(header: &'b chainsync::HeaderContent) -> Result<MultiEraHeader<'b>, Error> {
    let out = match header.byron_prefix {
        Some((subtag, _)) => MultiEraHeader::decode(header.variant, Some(subtag), &header.cbor),
        None => MultiEraHeader::decode(header.variant, None, &header.cbor),
    };

    out.map_err(Error::parse)
}

type MuxerPort = gasket::messaging::OutputPort<(u16, multiplexer::Payload)>;
type DemuxerPort = gasket::messaging::InputPort<multiplexer::Payload>;

type DownstreamPort = gasket::messaging::OutputPort<ChainSyncEvent>;

pub struct GasketChannel(u16, MuxerPort, DemuxerPort);

impl multiplexer::agents::Channel for GasketChannel {
    fn enqueue_chunk(
        &mut self,
        payload: multiplexer::Payload,
    ) -> Result<(), multiplexer::agents::ChannelError> {
        match self
            .1
            .send(gasket::messaging::Message::from((self.0, payload)))
        {
            Ok(_) => Ok(()),
            Err(err) => Err(multiplexer::agents::ChannelError::NotConnected(None)),
        }
    }

    fn dequeue_chunk(&mut self) -> Result<multiplexer::Payload, multiplexer::agents::ChannelError> {
        match self.2.recv() {
            Ok(msg) => Ok(msg.payload),
            Err(_) => Err(multiplexer::agents::ChannelError::NotConnected(None)),
        }
    }
}

type OuroborosClient = N2NClient<GasketChannel>;

pub struct Worker {
    chain_cursor: Cursor,
    client: OuroborosClient,
    downstream: DownstreamPort,
    block_count: gasket::metrics::Counter,
    chain_tip: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(
        chain_cursor: Cursor,
        muxer: MuxerPort,
        demuxer: DemuxerPort,
        downstream: DownstreamPort,
    ) -> Self {
        let channel = GasketChannel(5, muxer, demuxer);
        let client = OuroborosClient::new(channel);

        Self {
            chain_cursor,
            client,
            downstream,
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }

    fn process_next(
        &mut self,
        next: NextResponse<HeaderContent>,
    ) -> Result<(), gasket::error::Error> {
        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                let h = to_traverse(&h).or_panic()?;
                self.downstream
                    .send(ChainSyncEvent::RollForward(h.slot(), h.hash()).into())?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.downstream.send(ChainSyncEvent::Rollback(p).into())?;
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
        let next = self.client.request_next().or_restart()?;
        self.process_next(next)
    }

    fn await_next(&mut self) -> Result<(), gasket::error::Error> {
        log::info!("awaiting next block (blocking)");
        let next = self.client.recv_while_must_reply().or_restart()?;
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
        let point = self.chain_cursor.last_point().or_panic()?;

        log::info!("intersecting chain at point: {:?}", point);

        let (point, _) = self
            .client
            .find_intersect(vec![point])
            .map_err(Error::client)
            .or_restart()?;

        log::info!("chain-sync intersection is {:?}", point);

        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        match self.client.has_agency() {
            true => self.request_next()?,
            false => self.await_next()?,
        };

        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
