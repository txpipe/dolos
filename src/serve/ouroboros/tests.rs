use std::str::FromStr;

use pallas::{
    crypto::hash::Hash,
    network::{
        facades::PeerClient,
        miniprotocols::{chainsync, Point, MAINNET_MAGIC},
    },
};

const dummy_blocks: [(u64, &str); 5] = [
    (
        0,
        "15b9eeee849dd6386d3770b0745e0450190f7560e5159b1b3ab13b14b2684a45",
    ),
    (
        20,
        "ff8d558a3d5a0e058beb3d94d26a567f75cd7d09ff5485aa0d0ebc38b61378d4",
    ),
    (
        40,
        "a6874c7ada973c7887b46d89a8fb26afc862649fe76146837fbb04b4c5a9001c",
    ),
    (
        60,
        "104e3e8f30e80eb2d5ff54c0a9bd0c933f1dc428d0b20e8d504b92b93014eb30",
    ),
    (
        80,
        "5b8f5bb0ab220385f3dbe3707bee92a8024f1e46b4843347eceae4061931c18e",
    ),
];

type ServerHandle = tokio::task::JoinHandle<Result<(), crate::prelude::Error>>;

async fn setup_server_client_pair(port: u32) -> (ServerHandle, PeerClient) {
    let mut db = crate::storage::rolldb::RollDB::open_tmp(100).unwrap();

    for (slot, hash) in dummy_blocks.iter() {
        db.roll_forward(*slot, Hash::<32>::from_str(hash).unwrap(), vec![1u8; 200])
            .unwrap();
    }

    let server = tokio::spawn(super::serve(
        super::Config {
            listen_address: format!("[::]:{port}"),
            magic: MAINNET_MAGIC,
        },
        db,
    ));

    let client = PeerClient::connect(&format!("localhost:{port}"), MAINNET_MAGIC)
        .await
        .unwrap();

    (server, client)
}

#[tokio::test]
async fn test_blockfetch() {
    // use servers in different ports until we implement some sort of test harness
    let (server, mut client) = setup_server_client_pair(30031).await;

    let range = (
        Point::Specific(
            20,
            hex::decode("ff8d558a3d5a0e058beb3d94d26a567f75cd7d09ff5485aa0d0ebc38b61378d4")
                .unwrap(),
        ),
        Point::Specific(
            60,
            hex::decode("104e3e8f30e80eb2d5ff54c0a9bd0c933f1dc428d0b20e8d504b92b93014eb30")
                .unwrap(),
        ),
    );

    let blocks = client.blockfetch().fetch_range(range).await.unwrap();

    assert_eq!(blocks.len(), 3);

    for block in blocks {
        // TODO: check block content
        println!("received block of size: {}", block.len());
    }

    server.abort();
}

//#[tokio::test]
async fn do_chainsync() {
    // use servers in different ports until we implement some sort of test harness
    let (server, mut client) = setup_server_client_pair(30032).await;

    let known_points = vec![Point::Specific(
        20,
        hex::decode("ff8d558a3d5a0e058beb3d94d26a567f75cd7d09ff5485aa0d0ebc38b61378d4").unwrap(),
    )];

    let (point, _) = client
        .chainsync()
        .find_intersect(known_points)
        .await
        .unwrap();

    // assert point matches

    for _ in 0..3 {
        let next = client.chainsync().request_next().await.unwrap();

        match next {
            chainsync::NextResponse::RollForward(h, _) => {}
            _ => unreachable!(),
        };
    }

    server.abort();
}
