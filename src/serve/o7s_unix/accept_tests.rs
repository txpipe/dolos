//! The node-to-client accept loop when a client connects and never sends its
//! handshake.

use std::time::Duration;

use dolos_core::config::OuroborosConfig;
use dolos_testing::toy_domain::ToyDomain;
use pallas::network::{
    facades::{NodeClient, DEFAULT_HANDSHAKE_TIMEOUT},
    miniprotocols::Point,
};
use tokio::net::UnixStream;
use tokio_util::task::TaskTracker;

use super::DriverConfig;
use crate::{prelude::*, test_logs::Warnings};

const MAGIC: u64 = 2;

#[tokio::test(start_paused = true)]
async fn a_silent_client_misses_the_deadline_and_the_next_client_is_served() {
    let warnings = Warnings::capture("dolos::serve::o7s_unix");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.socket");
    let config = DriverConfig {
        service: OuroborosConfig {
            listen_path: path.clone(),
        },
        network_magic: MAGIC,
    };
    let domain = ToyDomain::new(None, None);

    let server = tokio::spawn(async move {
        let mut tasks = TaskTracker::new();
        super::accept_client_connections(domain, &config, &mut tasks, CancelTokenImpl::default())
            .await
    });

    while !path.exists() {
        tokio::task::yield_now().await;
    }

    let _silent = UnixStream::connect(&path).await.unwrap();

    tokio::time::sleep(DEFAULT_HANDSHAKE_TIMEOUT - Duration::from_secs(1)).await;
    assert_eq!(warnings.lines(), Vec::<String>::new());

    tokio::time::sleep(Duration::from_secs(2)).await;
    let logged = warnings.lines();
    assert_eq!(logged.len(), 1, "{logged:?}");
    assert!(
        logged[0].contains("error on incoming connection") && logged[0].contains("deadline"),
        "{logged:?}"
    );

    let mut client = NodeClient::connect(&path, MAGIC).await.unwrap();
    let (point, _) = client
        .chainsync()
        .find_intersect(vec![Point::Origin])
        .await
        .unwrap();

    assert_eq!(point, Some(Point::Origin));
    assert!(!server.is_finished());
}
