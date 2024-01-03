use std::str::FromStr;

use pallas::{
    crypto::hash::Hash,
    network::{
        facades::PeerClient,
        miniprotocols::{chainsync, Point, MAINNET_MAGIC},
    },
};

const DUMMY_BLOCKS: [(u64, &str); 5] = [
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

const DUMMY_BLOCK_BYTES: &str = "820183851a2d964a09582089d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df484830058200e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a85820afc0da64183bf2664f3d4eec7238d524ba607faeeab24fc100eb861dba69971b8300582025777aca9e4a73d48fc73b4f961d345b06d4a6f349cb7916570d35537d53479f5820d36a2619a672494604e11bb447cbcf5231e9f2ba25c2169177edc941bd50ad6c5820afc0da64183bf2664f3d4eec7238d524ba607faeeab24fc100eb861dba69971b58204e66280cd94d591072349bec0a3090a53aa945562efb6d08d56e53654b0e40988482000058401bc97a2fe02c297880ce8ecfd997fe4c1ec09ee10feeee9f686760166b05281d6283468ffd93becb0c956ccddd642df9b1244c915911185fa49355f6f22bfab98101820282840058401bc97a2fe02c297880ce8ecfd997fe4c1ec09ee10feeee9f686760166b05281d6283468ffd93becb0c956ccddd642df9b1244c915911185fa49355f6f22bfab9584061261a95b7613ee6bf2067dad77b70349729b0c50d57bc1cf30de0db4a1e73a885d0054af7c23fc6c37919dba41c602a57e2d0f9329a7954b867338d6fb2c9455840e03e62f083df5576360e60a32e22bbb07b3c8df4fcab8079f1d6f61af3954d242ba8a06516c395939f24096f3df14e103a7d9c2b80a68a9363cf1f27c7a4e307584044f18ef23db7d2813415cb1b62e8f3ead497f238edf46bb7a97fd8e9105ed9775e8421d18d47e05a2f602b700d932c181e8007bbfb231d6f1a050da4ebeeba048483000000826a63617264616e6f2d736c00a058204ba92aa320c60acc9ad7b9a64f2eda55c4d2ec28e604faf186708b4f0c4e8edf849fff8300d9010280d90102809fff82809fff81a0";

type ServerHandle = tokio::task::JoinHandle<Result<(), crate::prelude::Error>>;

async fn setup_server_client_pair(port: u32) -> (ServerHandle, PeerClient) {
    let mut db = crate::storage::rolldb::RollDB::open_tmp(100).unwrap();

    for (slot, hash) in DUMMY_BLOCKS.iter() {
        db.roll_forward(
            *slot,
            Hash::<32>::from_str(hash).unwrap(),
            hex::decode(DUMMY_BLOCK_BYTES).unwrap(),
        )
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
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    );

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

#[tokio::test]
async fn test_chainsync() {
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    );

    // use servers in different ports until we implement some sort of test harness
    let (server, mut client) = setup_server_client_pair(30032).await;

    let known_points = vec![Point::Specific(
        20,
        hex::decode("ff8d558a3d5a0e058beb3d94d26a567f75cd7d09ff5485aa0d0ebc38b61378d4").unwrap(),
    )];

    let (point, _) = client
        .chainsync()
        .find_intersect(known_points.clone())
        .await
        .unwrap();

    assert_eq!(point.unwrap(), known_points[0]);

    let next = client.chainsync().request_next().await.unwrap();

    assert!(matches!(next, chainsync::NextResponse::RollBackward(x, _) if x == known_points[0]));

    for _ in 0..3 {
        let next = client.chainsync().request_next().await.unwrap();

        assert!(matches!(next, chainsync::NextResponse::RollForward(_, _)));
    }

    let next = client.chainsync().request_next().await.unwrap();

    assert!(matches!(next, chainsync::NextResponse::Await));

    server.abort();
}
