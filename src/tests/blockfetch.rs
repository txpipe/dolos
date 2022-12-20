use std::ops::DerefMut;
use std::str::FromStr;
use std::time::Duration;

use pallas::crypto::hash::Hash;
use tracing::info;

use crate::upstream::blockfetch;
use crate::upstream::plexer;
use crate::upstream::prelude::*;

#[test]
fn connect_to_real_relay() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let mut input = MuxInputPort::default();
    let mut output = DemuxOutputPort::default();

    let plexer_3_chainsync = protocol_channel(3, &mut input, &mut output);

    let plexer = plexer::Worker::new(
        "preview-node.world.dev.cardano.org:30002".into(),
        2,
        input,
        None,
        Some(output),
    );

    let mut source = gasket::messaging::OutputPort::default();
    let mut upstream = blockfetch::UpstreamPort::default();
    gasket::messaging::connect_ports(&mut source, &mut upstream, 20);

    let mut downstream = blockfetch::DownstreamPort::default();
    let mut sink = gasket::messaging::SinkPort::default();
    gasket::messaging::connect_ports(&mut downstream, sink.deref_mut(), 20);

    let blockfetch = blockfetch::Worker::new(plexer_3_chainsync, upstream, downstream);

    let plexer =
        gasket::runtime::spawn_stage(plexer, gasket::runtime::Policy::default(), Some("plexer"));

    let blockfetch = gasket::runtime::spawn_stage(
        blockfetch,
        gasket::runtime::Policy::default(),
        Some("blockfetch"),
    );

    source
        .send(
            crate::model::ChainSyncEvent::RollForward(
                200,
                Hash::<32>::from_str(
                    &"222b288a0d52fb6a3a35ab80d41082de6bae1c693d9c0451ba9b8cba2ec6badc",
                )
                .unwrap(),
            )
            .into(),
        )
        .unwrap();

    let results = sink.drain_at_least::<1>(Duration::from_secs(60)).unwrap();

    for res in results {
        match res.payload {
            crate::prelude::BlockFetchEvent::RollForward(slot, hash, _) => {
                info!(slot, %hash, "roll forward received");
            }
            crate::prelude::BlockFetchEvent::Rollback(_) => {
                panic!("rollback not expected for known point");
            }
        }
    }

    plexer.dismiss_stage().unwrap();
    blockfetch.dismiss_stage().unwrap();
}
