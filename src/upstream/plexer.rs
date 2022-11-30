use gasket::error::AsWorkError;
use pallas::network::miniprotocols::handshake;
use pallas::network::multiplexer;
use pallas::network::multiplexer::bearers::Bearer;
use pallas::network::multiplexer::demux::{Demuxer, Egress};
use pallas::network::multiplexer::mux::{Ingress, Muxer};
use pallas::network::multiplexer::sync::SyncPlexer;

type InputPort = gasket::messaging::InputPort<(u16, multiplexer::Payload)>;
type OutputPort = gasket::messaging::OutputPort<multiplexer::Payload>;

struct GasketEgress(OutputPort);

impl Egress for GasketEgress {
    fn send(
        &mut self,
        payload: multiplexer::Payload,
    ) -> Result<(), multiplexer::demux::EgressError> {
        self.0
            .send(gasket::messaging::Message::from(payload))
            .map_err(|_| multiplexer::demux::EgressError(vec![]))
    }
}

struct GasketIngress(InputPort);

impl Ingress for GasketIngress {
    fn recv_timeout(
        &mut self,
        duration: std::time::Duration,
    ) -> Result<multiplexer::Message, multiplexer::mux::IngressError> {
        self.0
            .recv_timeout(duration)
            .map(|msg| msg.payload)
            .map_err(|err| match err {
                gasket::error::Error::RecvIdle => multiplexer::mux::IngressError::Empty,
                _ => multiplexer::mux::IngressError::Disconnected,
            })
    }
}

struct Session {
    demuxer: Demuxer<GasketEgress>,
    muxer: Muxer<GasketIngress>,
}

type IsBusy = bool;

impl Session {
    fn demux_tick(&mut self) -> Result<IsBusy, gasket::error::Error> {
        match self.demuxer.tick() {
            Ok(x) => match x {
                multiplexer::demux::TickOutcome::Busy => Ok(true),
                multiplexer::demux::TickOutcome::Idle => Ok(false),
            },
            Err(err) => match err {
                multiplexer::demux::DemuxError::BearerError(err) => {
                    Err(gasket::error::Error::ShouldRestart(err.to_string()))
                }
                multiplexer::demux::DemuxError::EgressDisconnected(x, _) => Err(
                    gasket::error::Error::WorkPanic(format!("egress disconnected {}", x)),
                ),
                multiplexer::demux::DemuxError::EgressUnknown(x, _) => Err(
                    gasket::error::Error::WorkPanic(format!("unknown egress {}", x)),
                ),
            },
        }
    }

    fn mux_tick(&mut self) -> Result<IsBusy, gasket::error::Error> {
        match self.muxer.tick() {
            multiplexer::mux::TickOutcome::Busy => Ok(true),
            multiplexer::mux::TickOutcome::Idle => Ok(false),
            multiplexer::mux::TickOutcome::BearerError(err) => {
                Err(gasket::error::Error::ShouldRestart(err.to_string()))
            }
            multiplexer::mux::TickOutcome::IngressDisconnected => Err(
                gasket::error::Error::WorkPanic("ingress disconnected".into()),
            ),
        }
    }
}

pub struct Worker {
    peer_address: String,
    network_magic: u64,
    input: InputPort,
    channel5_out: OutputPort,
    session: Option<Session>,
}

impl Worker {
    pub fn new(
        peer_address: String,
        network_magic: u64,
        input: InputPort,
        channel5_out: OutputPort,
    ) -> Self {
        Self {
            peer_address,
            network_magic,
            input,
            channel5_out,
            session: None,
        }
    }

    fn handshake(&self, bearer: Bearer) -> Result<Bearer, gasket::error::Error> {
        log::debug!("doing handshake");

        let plexer = SyncPlexer::new(bearer, 0);
        let versions = handshake::n2n::VersionTable::v7_and_above(self.network_magic);
        let mut client = handshake::Client::new(plexer);

        let output = client.handshake(versions).or_panic()?;
        log::info!("handshake output: {:?}", output);

        let bearer = client.unwrap().unwrap();

        match output {
            handshake::Confirmation::Accepted(version, _) => {
                log::info!("connected to upstream peer using version {}", version);
                Ok(bearer)
            }
            _ => Err(gasket::error::Error::WorkPanic(
                "couldn't agree on handshake version".into(),
            )),
        }
    }

    fn connect(&self) -> Result<Session, gasket::error::Error> {
        log::debug!("connecting muxer");

        let bearer = multiplexer::bearers::Bearer::connect_tcp(&self.peer_address).or_restart()?;

        let bearer = self.handshake(bearer)?;

        let mut demuxer = Demuxer::new(bearer.clone());
        demuxer.register(5, GasketEgress(self.channel5_out.clone()));

        let muxer = Muxer::new(bearer, GasketIngress(self.input.clone()));

        Ok(Session { demuxer, muxer })
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        // TODO: define networking metrics (bytes in / out, etc)
        gasket::metrics::Builder::new().build()
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let session = self.connect()?;
        self.session = Some(session);

        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let session = self.session.as_mut().unwrap();

        session.demux_tick()?;
        session.mux_tick()?;

        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
