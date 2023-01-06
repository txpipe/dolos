use std::thread::sleep;

use dolos::{
    prelude::*,
    rolldb::RollDB,
    upstream::{blockfetch, chainsync, plexer, reducer},
};

#[derive(clap::Args)]
pub struct Args {
    #[clap(long, value_parser)]
    //#[clap(description = "config file to load by the daemon")]
    config: Option<std::path::PathBuf>,
}

pub fn run(args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let mut mux_input = MuxInputPort::default();

    let mut demux2_out = DemuxOutputPort::default();
    let mut demux2_in = DemuxInputPort::default();
    gasket::messaging::connect_ports(&mut demux2_out, &mut demux2_in, 1000);

    let mut demux3_out = DemuxOutputPort::default();
    let mut demux3_in = DemuxInputPort::default();
    gasket::messaging::connect_ports(&mut demux3_out, &mut demux3_in, 1000);

    let mut mux2_out = MuxOutputPort::default();
    let mut mux3_out = MuxOutputPort::default();
    gasket::messaging::funnel_ports(vec![&mut mux2_out, &mut mux3_out], &mut mux_input, 1000);

    let mut chainsync_downstream = chainsync::DownstreamPort::default();
    let mut blockfetch_upstream = blockfetch::UpstreamPort::default();
    gasket::messaging::connect_ports(&mut chainsync_downstream, &mut blockfetch_upstream, 20);

    let mut blockfetch_downstream = blockfetch::DownstreamPort::default();
    let mut reducer_upstream = reducer::UpstreamPort::default();
    gasket::messaging::connect_ports(&mut blockfetch_downstream, &mut reducer_upstream, 20);

    let cursor = Cursor::StaticCursor(vec![]);

    let plexer = gasket::runtime::spawn_stage(
        plexer::Worker::new(
            "preview-node.world.dev.cardano.org:30002".into(),
            2,
            mux_input,
            Some(demux2_out),
            Some(demux3_out),
        ),
        gasket::runtime::Policy::default(),
        Some("plexer"),
    );

    let channel2 = ProtocolChannel(2, mux2_out, demux2_in);

    let chainsync = gasket::runtime::spawn_stage(
        chainsync::Worker::new(cursor, channel2, chainsync_downstream),
        gasket::runtime::Policy::default(),
        Some("chainsync"),
    );

    let channel3 = ProtocolChannel(3, mux3_out, demux3_in);

    let blockfetch = gasket::runtime::spawn_stage(
        blockfetch::Worker::new(channel3, blockfetch_upstream, blockfetch_downstream),
        gasket::runtime::Policy::default(),
        Some("blockfetch"),
    );

    let db = RollDB::open("./tmp").unwrap();

    let reducer = gasket::runtime::spawn_stage(
        reducer::Worker::new(reducer_upstream, db),
        gasket::runtime::Policy::default(),
        Some("reducer"),
    );

    gasket::daemon::Daemon(vec![plexer, chainsync, blockfetch, reducer]).block();

    Ok(())
}
