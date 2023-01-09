use std::ops::DerefMut;
use std::time::Duration;

use pallas::network::miniprotocols::Point;

use crate::prelude::Cursor;
use crate::upstream::chainsync;
use crate::upstream::plexer;
use crate::upstream::prelude::*;

#[test]
fn connect_to_real_relay() {
    let mut input = MuxInputPort::default();
    let mut output = DemuxOutputPort::default();

    let plexer_2_chainsync = protocol_channel(2, &mut input, &mut output);

    let plexer = plexer::Worker::new(
        "preview-node.world.dev.cardano.org:30002".into(),
        2,
        input,
        Some(output),
        None,
    );

    let mut downstream = chainsync::DownstreamPort::default();
    let mut sink = gasket::messaging::SinkPort::default();
    gasket::messaging::connect_ports(&mut downstream, sink.deref_mut(), 20);

    let cursor = Cursor::StaticCursor(vec![]);
    let chainsync = chainsync::Worker::new(cursor, plexer_2_chainsync, downstream);

    let plexer =
        gasket::runtime::spawn_stage(plexer, gasket::runtime::Policy::default(), Some("plexer"));

    let chainsync = gasket::runtime::spawn_stage(
        chainsync,
        gasket::runtime::Policy::default(),
        Some("chainsync"),
    );

    let results = sink.drain_at_least::<20>(Duration::from_secs(60)).unwrap();

    for res in results {
        match res.payload {
            crate::prelude::ChainSyncEvent::Rollback(x) => {
                assert!(matches!(x, Point::Origin));
            }
            _ => (),
        }
    }

    plexer.dismiss_stage().unwrap();
    chainsync.dismiss_stage().unwrap();
}
