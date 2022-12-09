use pallas::network::miniprotocols::Point;
use tracing::info;

use crate::upstream::plexer;
use crate::upstream::prelude::*;

#[test]
fn connect_to_real_relay() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish(),
    )
    .unwrap();

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
        output,
    );

    let tether = gasket::runtime::spawn_stage(worker, gasket::runtime::Policy::default(), None);

    let mut client = OuroborosClient::new(client_channel);
    let (point, _) = client.find_intersect(vec![known_point.clone()]).unwrap();

    assert_eq!(point.unwrap(), known_point);

    info!("dismissing");
    tether.dismiss_stage().unwrap();
    tether.join_stage();
}
