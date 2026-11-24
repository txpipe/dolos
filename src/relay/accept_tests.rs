//! The relay's accept loop when a peer connects and never sends its handshake.

use std::time::Duration;

use dolos_testing::toy_domain::ToyDomain;
use pallas::network::{
    facades::{PeerClient, DEFAULT_HANDSHAKE_TIMEOUT},
    miniprotocols::Point,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::task::TaskTracker;

use crate::{prelude::*, test_logs::Warnings};

const MAGIC: u64 = 2;

#[tokio::test(start_paused = true)]
async fn a_silent_peer_misses_the_deadline_and_the_next_peer_is_served() {
    let warnings = Warnings::capture("dolos::relay");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let domain = ToyDomain::new(None, None);

    let server = tokio::spawn(async move {
        let mut tasks = TaskTracker::new();
        super::serve_peers(
            domain,
            &listener,
            MAGIC,
            &mut tasks,
            CancelTokenImpl::default(),
        )
        .await
    });

    let _silent = TcpStream::connect(address).await.unwrap();

    tokio::time::sleep(DEFAULT_HANDSHAKE_TIMEOUT - Duration::from_secs(1)).await;
    assert_eq!(warnings.lines(), Vec::<String>::new());

    tokio::time::sleep(Duration::from_secs(2)).await;
    let logged = warnings.lines();
    assert_eq!(logged.len(), 1, "{logged:?}");
    assert!(
        logged[0].contains("error on incoming connection") && logged[0].contains("deadline"),
        "{logged:?}"
    );

    let mut client = PeerClient::connect(address, MAGIC).await.unwrap();
    client
        .blockfetch()
        .request_range((Point::Origin, Point::Origin))
        .await
        .expect("the second peer's request is answered");

    assert!(!server.is_finished());
}
