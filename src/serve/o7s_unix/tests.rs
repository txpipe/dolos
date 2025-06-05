use pallas::network::{
    facades::NodeClient,
    miniprotocols::{chainsync::NextResponse, Point, MAINNET_MAGIC},
};
use std::{path::Path, time::Duration};
use tokio_util::sync::CancellationToken;

use crate::adapters::{DomainAdapter, StateAdapter, WalAdapter};
use crate::prelude::*;

type ServerHandle = tokio::task::JoinHandle<Result<(), crate::prelude::Error>>;

async fn setup_server_client_pair(port: u32, wal: WalAdapter) -> (ServerHandle, NodeClient) {
    let cancel = CancellationToken::new();

    let server = tokio::spawn(super::serve(
        super::Config {
            listen_path: format!("dolos{port}.socket").into(),
            magic: MAINNET_MAGIC,
        },
        wal,
        cancel,
    ));

    tokio::time::sleep(Duration::from_secs(3)).await;

    let client = NodeClient::connect(Path::new(&format!("dolos{port}.socket")), MAINNET_MAGIC)
        .await
        .unwrap();

    (server, client)
}

#[tokio::test]
async fn test_chainsync_happy_path() {
    // let _ = tracing::subscriber::set_global_default(
    //     tracing_subscriber::FmtSubscriber::builder()
    //         .with_max_level(tracing::Level::DEBUG)
    //         .finish(),
    // );

    let mut wal = WalAdapter::Redb(dolos_redb::testing::wal_with_dummy_blocks(300));

    // use servers in different ports until we implement some sort of test harness
    let (server, mut client) = setup_server_client_pair(30032, wal.clone()).await;

    let known_points = vec![Point::Specific(
        20,
        dolos_redb::testing::slot_to_hash(20).to_vec(),
    )];

    let (point, _) = client
        .chainsync()
        .find_intersect(known_points.clone())
        .await
        .unwrap();

    assert_eq!(point.unwrap(), known_points[0]);

    let next = client.chainsync().request_next().await.unwrap();

    match next {
        NextResponse::RollBackward(Point::Specific(slot, _), _) => assert_eq!(slot, 20),
        _ => panic!("expected rollback to point"),
    }

    for _ in 21..300 {
        let next = client.chainsync().request_next().await.unwrap();

        match next {
            NextResponse::RollForward(_, _) => (),
            _ => panic!("expected rollforward"),
        }
    }

    let next = client.chainsync().request_next().await.unwrap();

    match next {
        NextResponse::Await => (),
        _ => panic!("expected await"),
    }

    for slot in 301..320 {
        wal.roll_forward(std::iter::once(dolos_redb::testing::dummy_block_from_slot(
            slot,
        )))
        .unwrap();

        let next = client.chainsync().recv_while_must_reply().await.unwrap();

        match next {
            NextResponse::RollForward(_, _) => (),
            _ => panic!("expected rollforward"),
        }

        let next = client.chainsync().request_next().await.unwrap();

        match next {
            NextResponse::Await => (),
            _ => panic!("expected await"),
        }
    }

    wal.roll_back(&ChainPoint::Specific(
        310,
        dolos_redb::testing::slot_to_hash(310),
    ))
    .unwrap();

    let next = client.chainsync().recv_while_must_reply().await.unwrap();

    match next {
        NextResponse::RollBackward(Point::Specific(slot, _), _) => assert_eq!(slot, 310),
        _ => panic!("expected rollback to point"),
    }

    server.abort();
}
