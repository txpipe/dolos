use crate::upstream::chainsync;
use crate::upstream::plexer;
use crate::upstream::prelude::*;
use pallas::network::miniprotocols::Point;

#[test]
fn connect_to_real_relay() {
    let known_point = Point::Specific(
        3866155,
        hex::decode("9a5446c4178c708706f8218ee05cec7674396e5f044f911eacc1ad1147cc353e").unwrap(),
    );

    let mut input = MuxInputPort::default();
    let mut output = DemuxOutputPort::default();

    let client_channel = protocol_channel(2, &mut input, &mut output);

    let worker = plexer::Worker::new(
        "preview-node.world.dev.cardano.org:30002".into(),
        2,
        input,
        Some(output),
        None,
    );

    let tether = gasket::runtime::spawn_stage(worker, gasket::runtime::Policy::default(), None);

    let mut client = chainsync::OuroborosClient::new(client_channel);
    let (point, _) = client.find_intersect(vec![known_point.clone()]).unwrap();

    assert_eq!(point.unwrap(), known_point);

    tether.dismiss_stage().unwrap();
    tether.join_stage();
}
