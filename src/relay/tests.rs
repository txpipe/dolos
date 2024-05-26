use pallas::network::{
    facades::PeerClient,
    miniprotocols::{chainsync::NextResponse, Point, MAINNET_MAGIC},
};

use crate::wal::{self, redb::WalStore, WalWriter};

type ServerHandle = tokio::task::JoinHandle<Result<(), crate::prelude::Error>>;

async fn setup_server_client_pair(port: u32, wal: WalStore) -> (ServerHandle, PeerClient) {
    let server = tokio::spawn(super::serve(
        super::Config {
            listen_address: format!("[::]:{port}"),
            magic: MAINNET_MAGIC,
        },
        wal,
    ));

    let client = PeerClient::connect(&format!("localhost:{port}"), MAINNET_MAGIC)
        .await
        .unwrap();

    (server, client)
}

#[tokio::test]
async fn test_blockfetch_happy_path() {
    // let _ = tracing::subscriber::set_global_default(
    //     tracing_subscriber::FmtSubscriber::builder()
    //         .with_max_level(tracing::Level::DEBUG)
    //         .finish(),
    // );

    let wal = wal::testing::db_with_dummy_blocks(300);

    // use servers in different ports until we implement some sort of test harness
    let (server, mut client) = setup_server_client_pair(30031, wal.clone()).await;

    let range = (
        Point::Specific(20, wal::testing::slot_to_hash(20).to_vec()),
        Point::Specific(60, wal::testing::slot_to_hash(60).to_vec()),
    );

    let blocks = client.blockfetch().fetch_range(range).await.unwrap();

    assert_eq!(blocks.len(), 41);

    for block in blocks {
        // TODO: check block content
        println!("received block of size: {}", block.len());
    }

    server.abort();
}

#[tokio::test]
async fn test_chainsync_happy_path() {
    // let _ = tracing::subscriber::set_global_default(
    //     tracing_subscriber::FmtSubscriber::builder()
    //         .with_max_level(tracing::Level::DEBUG)
    //         .finish(),
    // );

    let mut wal = wal::testing::db_with_dummy_blocks(300);

    // use servers in different ports until we implement some sort of test harness
    let (server, mut client) = setup_server_client_pair(30032, wal.clone()).await;

    let known_points = vec![Point::Specific(20, wal::testing::slot_to_hash(20).to_vec())];

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
        wal.roll_forward(std::iter::once(wal::testing::dummy_block_from_slot(slot)))
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

    wal.roll_back(&wal::ChainPoint::Specific(
        310,
        wal::testing::slot_to_hash(310),
    ))
    .unwrap();

    let next = client.chainsync().recv_while_must_reply().await.unwrap();

    match next {
        NextResponse::RollBackward(Point::Specific(slot, _), _) => assert_eq!(slot, 310),
        _ => panic!("expected rollback to point"),
    }

    server.abort();
}
