//! Chain-sync and state-query sessions as a client sees them, over a real Unix
//! socket.

use std::{collections::HashMap, future::Future, path::Path, sync::Arc, time::Duration};

use dolos_core::{
    config::RootConfig, ArchiveStore as _, ArchiveWriter as _, LogValue, StateStore as _,
    StateWriter as _, TipEvent, WalStore as _,
};
use dolos_testing::{blocks::make_conway_block, toy_domain::ToyDomain};
use pallas::codec::utils::AnyCbor;
use pallas::network::{
    facades::{NodeClient, NodeServer},
    miniprotocols::{
        chainsync::{BlockContent, NextResponse},
        localstate::{self, queries_v16 as q16},
        Point,
    },
};
use tempfile::TempDir;
use tokio::{
    net::UnixListener,
    task::{JoinError, JoinHandle},
    time::timeout,
};

use super::{chainsync, handle_session};
use crate::{adapters::DomainAdapter, engine::DomainBuilder, prelude::*, serve::CancelTokenImpl};

const MAGIC: u64 = 2;

type Blocks = Vec<(ChainPoint, RawBlock)>;

fn blocks(slots: &[u64]) -> Blocks {
    slots.iter().map(|slot| make_conway_block(*slot)).collect()
}

fn write_archive<D: Domain>(domain: &D, blocks: &[(ChainPoint, RawBlock)]) {
    let writer = domain.archive().start_writer().unwrap();
    for (point, block) in blocks {
        writer.apply(point, block).unwrap();
    }
    writer.commit().unwrap();
}

fn write_wal<D: Domain>(domain: &D, blocks: &[(ChainPoint, RawBlock)]) {
    let entries = blocks
        .iter()
        .map(|(point, block)| {
            let value = LogValue {
                block: block.to_vec(),
                delta: vec![],
                inputs: HashMap::new(),
            };
            (point.clone(), value)
        })
        .collect();

    domain.wal().append_entries(entries).unwrap();
}

/// A domain over the stores a real node opens. `ToyDomain` has its own copy of
/// the tip replay, so only these tests exercise the adapter's.
fn real_domain(dir: &Path) -> DomainAdapter {
    dolos_cardano::include::preview::save(dir).unwrap();

    let config = format!(
        r#"
        compatibility = "v2-alpha.2"

        [chain]
        protocol = "cardano"
        network = "preview"

        [upstream]
        peer_address = "unused.example:3001"

        [storage]
        path = {}
        "#,
        toml::Value::String(dir.join("data").display().to_string()),
    );
    let config: RootConfig = toml::from_str(&config).unwrap();

    crate::storage::ensure_storage_path(&config).unwrap();

    DomainBuilder::new(&config, Arc::new(dolos_cardano::include::preview::load()))
        .build()
        .unwrap()
}

/// Bind a socket, run `serve` on the accepted connection and connect to it.
async fn connect<F, Fut>(serve: F) -> (NodeClient, JoinHandle<Result<(), ServeError>>, TempDir)
where
    F: FnOnce(NodeServer) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), ServeError>> + Send,
{
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.socket");
    let listener = UnixListener::bind(&path).unwrap();

    let server = tokio::spawn(async move {
        let connection = NodeServer::accept(&listener, MAGIC).await.unwrap();

        serve(connection).await
    });

    let client = NodeClient::connect(&path, MAGIC).await.unwrap();

    (client, server, dir)
}

async fn connect_session<D: Domain>(
    domain: D,
) -> (NodeClient, JoinHandle<Result<(), ServeError>>, TempDir) {
    connect(|connection| handle_session(domain, connection, CancelTokenImpl::default())).await
}

/// The chain-sync protocol alone, so a test sees how it ends rather than what
/// the connection's task makes of it.
async fn connect_chainsync<D: Domain>(
    domain: D,
) -> (NodeClient, JoinHandle<Result<(), ServeError>>, TempDir) {
    connect(|connection| async move {
        let NodeServer {
            plexer, chainsync, ..
        } = connection;

        let result = chainsync::handle_session(domain, chainsync, CancelTokenImpl::default()).await;

        plexer.abort().await;

        result
    })
    .await
}

async fn within<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("the client got no reply and no EOF")
}

async fn intersect(client: &mut NodeClient, point: &ChainPoint) {
    let (found, _) = within(
        client
            .chainsync()
            .find_intersect(vec![Point::try_from(point.clone()).unwrap()]),
    )
    .await
    .unwrap();

    assert_eq!(found, Some(Point::try_from(point.clone()).unwrap()));
}

async fn expect_rollback(client: &mut NodeClient, point: &ChainPoint) {
    match within(client.chainsync().request_next()).await.unwrap() {
        NextResponse::RollBackward(got, _) => {
            assert_eq!(got, Point::try_from(point.clone()).unwrap())
        }
        NextResponse::RollForward(..) => panic!("expected a rollback, got a roll forward"),
        NextResponse::Await => panic!("expected a rollback, got an await"),
    }
}

async fn expect_forward(client: &mut NodeClient, block: &RawBlock) {
    match within(client.chainsync().request_next()).await.unwrap() {
        NextResponse::RollForward(BlockContent(got), _) => assert_eq!(&got, block.as_ref()),
        NextResponse::RollBackward(point, _) => {
            panic!("expected a roll forward, got a rollback to {point:?}")
        }
        NextResponse::Await => panic!("expected a roll forward, got an await"),
    }
}

/// Ask for the next block with none available.
async fn expect_await(client: &mut NodeClient) {
    within(client.chainsync().send_request_next())
        .await
        .unwrap();

    assert!(matches!(
        within(client.chainsync().recv_while_can_await())
            .await
            .unwrap(),
        NextResponse::Await
    ));
}

/// Ask for the next block with none available, then see `block` arrive at the
/// tip. Anything sent before it is a replay of a block the client already
/// holds.
async fn expect_await_then_forward<D: Domain>(
    client: &mut NodeClient,
    domain: &D,
    point: &ChainPoint,
    block: &RawBlock,
) {
    expect_await(client).await;
    expect_tip_forward(client, domain, point, block).await;
}

/// With the client waiting at the tip, see `block` arrive there.
async fn expect_tip_forward<D: Domain>(
    client: &mut NodeClient,
    domain: &D,
    point: &ChainPoint,
    block: &RawBlock,
) {
    domain.notify_tip(TipEvent::Apply(point.clone(), block.clone()));

    match within(client.chainsync().recv_while_must_reply())
        .await
        .unwrap()
    {
        NextResponse::RollForward(BlockContent(got), _) => assert_eq!(&got, block.as_ref()),
        other => panic!("expected the new block, got {:?}", discriminant(&other)),
    }
}

/// A point the archive doesn't hold.
fn missing_point() -> Option<Point> {
    Some(Point::Specific(999, vec![0; 32]))
}

fn expect_acquire_failure(result: Result<(), localstate::ClientError>) {
    assert!(
        matches!(result, Err(localstate::ClientError::AcquirePointNotFound)),
        "expected an acquire failure, got {result:?}"
    );
}

fn set_cursor<D: Domain>(domain: &D, point: ChainPoint) {
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(point).unwrap();
    writer.commit().unwrap();
}

/// What the tip-reading queries answer, which must agree. The session must be
/// acquired.
async fn expect_tip(client: &mut NodeClient, expected: Point) {
    let point = within(q16::get_chain_point(client.statequery()))
        .await
        .unwrap();
    assert_eq!(point, expected);

    let query = q16::Request::LedgerQuery(q16::LedgerQuery::BlockQuery(
        0,
        q16::BlockQuery::GetLedgerTip,
    ));
    let (point,): (Point,) = within(client.statequery().query(query)).await.unwrap();
    assert_eq!(point, expected);

    within(q16::get_block_epoch_number(client.statequery(), 0))
        .await
        .unwrap();
}

fn discriminant(response: &NextResponse<BlockContent>) -> &'static str {
    match response {
        NextResponse::RollForward(..) => "a roll forward",
        NextResponse::RollBackward(..) => "a rollback",
        NextResponse::Await => "an await",
    }
}

#[tokio::test]
async fn a_failed_intersect_ends_the_connection() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100]);
    write_archive(&domain, &chain);

    let (mut client, server, _dir) = connect_session(domain).await;

    let reply = within(
        client
            .chainsync()
            .find_intersect(vec![Point::try_from(chain[0].0.clone()).unwrap()]),
    )
    .await;

    assert!(reply.is_err(), "the client should see the connection end");
    within(server).await.unwrap().unwrap_err();
}

#[tokio::test]
async fn a_failed_next_ends_the_connection() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100, 110, 120]);
    write_archive(&domain, &chain);

    let (mut client, server, _dir) = connect_session(domain).await;

    intersect(&mut client, &chain[0].0).await;
    expect_rollback(&mut client, &chain[0].0).await;
    expect_forward(&mut client, &chain[1].1).await;
    expect_forward(&mut client, &chain[2].1).await;

    let reply = within(client.chainsync().request_next()).await;

    assert!(reply.is_err(), "the client should see the connection end");
    within(server).await.unwrap().unwrap_err();
}

#[tokio::test]
async fn a_failed_intersect_is_an_error_rather_than_a_panic() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100]);
    write_archive(&domain, &chain);

    let (mut client, server, _dir) = connect_chainsync(domain).await;

    let _ = within(
        client
            .chainsync()
            .find_intersect(vec![Point::try_from(chain[0].0.clone()).unwrap()]),
    )
    .await;

    let result = within(server).await.expect("the session must not panic");

    assert!(result.is_err());
}

#[tokio::test]
async fn a_client_finishing_the_protocol_ends_the_session_cleanly() {
    let domain = ToyDomain::new(None, None);

    let (mut client, server, _dir) = connect_chainsync(domain).await;

    within(client.chainsync().send_done()).await.unwrap();

    within(server).await.unwrap().unwrap();
}

#[tokio::test]
async fn an_archive_intersection_sends_every_block_after_it() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100, 110, 120, 130]);
    write_archive(&domain, &chain[..3]);
    write_wal(&domain, &chain[2..]);

    let (mut client, _server, _dir) = connect_session(domain).await;

    intersect(&mut client, &chain[0].0).await;
    expect_rollback(&mut client, &chain[0].0).await;
    expect_forward(&mut client, &chain[1].1).await;
    expect_forward(&mut client, &chain[2].1).await;
    expect_forward(&mut client, &chain[3].1).await;
}

#[tokio::test]
async fn a_page_intersection_on_a_real_domain_sends_every_block_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let domain = real_domain(dir.path());
    let chain = blocks(&[100, 110, 120, 130]);
    write_wal(&domain, &chain[..3]);

    let (mut client, _server, _socket) = connect_session(domain.clone()).await;

    intersect(&mut client, &chain[0].0).await;
    expect_rollback(&mut client, &chain[0].0).await;
    expect_forward(&mut client, &chain[1].1).await;
    expect_forward(&mut client, &chain[2].1).await;

    expect_await_then_forward(&mut client, &domain, &chain[3].0, &chain[3].1).await;
}

#[tokio::test]
async fn a_tip_intersection_on_a_real_domain_does_not_resend_the_tip() {
    let dir = tempfile::tempdir().unwrap();
    let domain = real_domain(dir.path());
    let chain = blocks(&[100, 110, 120]);
    write_wal(&domain, &chain[..2]);

    let (mut client, _server, _socket) = connect_session(domain.clone()).await;

    intersect(&mut client, &chain[1].0).await;
    expect_rollback(&mut client, &chain[1].0).await;

    expect_await_then_forward(&mut client, &domain, &chain[2].0, &chain[2].1).await;
}

#[tokio::test]
async fn a_seeded_wal_on_a_real_domain_replays_no_empty_block() {
    let dir = tempfile::tempdir().unwrap();
    let domain = real_domain(dir.path());
    let chain = blocks(&[100, 110]);
    domain.wal().reset_to(&chain[0].0).unwrap();

    let (mut client, _server, _socket) = connect_session(domain.clone()).await;

    intersect(&mut client, &chain[0].0).await;
    expect_rollback(&mut client, &chain[0].0).await;

    expect_await_then_forward(&mut client, &domain, &chain[1].0, &chain[1].1).await;
}

/// Asking for a block before any intersection panics in the chain-sync task, so
/// this needs another trigger once that panic is fixed.
#[tokio::test]
async fn a_panicking_protocol_ends_the_connection() {
    let domain = ToyDomain::new(None, None);

    let (mut client, server, _dir) = connect_session(domain).await;

    let reply = within(client.chainsync().request_next()).await;

    assert!(reply.is_err(), "the client should see the connection end");

    let err = within(server).await.unwrap().unwrap_err();
    assert!(
        matches!(
            &err,
            ServeError::Internal(e)
                if e.downcast_ref::<JoinError>().is_some_and(JoinError::is_panic)
        ),
        "expected the task's panic, got {err:?}"
    );
}

/// A corrupt archive body makes the acquire itself fail, rather than answer.
#[tokio::test]
async fn a_failed_state_query_ends_the_connection() {
    let domain = ToyDomain::new(None, None);
    let (point, _) = make_conway_block(100);
    write_archive(&domain, &[(point.clone(), Arc::new(vec![0u8; 4]))]);

    let (mut client, server, _dir) = connect_session(domain).await;

    let reply = within(
        client
            .statequery()
            .acquire(Some(Point::try_from(point).unwrap())),
    )
    .await;

    assert!(reply.is_err(), "the client should see the connection end");

    let err = within(server).await.unwrap().unwrap_err();
    assert!(
        !matches!(&err, ServeError::Internal(e) if e.downcast_ref::<JoinError>().is_some()),
        "expected the session's error, got the task's panic: {err:?}"
    );
}

#[tokio::test]
async fn a_failed_acquire_can_be_retried_with_chain_sync_still_serving() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100, 110]);
    write_wal(&domain, &chain[..1]);

    let (mut client, _server, _dir) = connect_session(domain.clone()).await;

    intersect(&mut client, &chain[0].0).await;
    expect_rollback(&mut client, &chain[0].0).await;
    expect_await(&mut client).await;

    expect_acquire_failure(within(client.statequery().acquire(missing_point())).await);
    within(client.statequery().acquire(Some(Point::Origin)))
        .await
        .unwrap();

    expect_tip_forward(&mut client, &domain, &chain[1].0, &chain[1].1).await;
}

#[tokio::test]
async fn a_failed_reacquire_can_be_followed_by_an_acquire() {
    let domain = ToyDomain::new(None, None);

    let (mut client, _server, _dir) = connect_session(domain).await;

    within(client.statequery().acquire(Some(Point::Origin)))
        .await
        .unwrap();

    within(client.statequery().send_reacquire(missing_point()))
        .await
        .unwrap();
    expect_acquire_failure(within(client.statequery().recv_while_acquiring()).await);

    within(client.statequery().acquire(Some(Point::Origin)))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_failed_acquire_then_done_ends_the_session_cleanly() {
    let domain = ToyDomain::new(None, None);

    let (mut client, server, _dir) = connect_session(domain).await;

    expect_acquire_failure(within(client.statequery().acquire(missing_point())).await);

    within(client.statequery().send_done()).await.unwrap();
    within(client.chainsync().send_done()).await.unwrap();

    within(server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_successful_reacquire_stays_acquired() {
    let domain = ToyDomain::new(None, None);

    let (mut client, _server, _dir) = connect_session(domain).await;

    within(client.statequery().acquire(Some(Point::Origin)))
        .await
        .unwrap();

    within(client.statequery().send_reacquire(Some(Point::Origin)))
        .await
        .unwrap();
    within(client.statequery().recv_while_acquiring())
        .await
        .unwrap();

    // Both of these are only legal while acquired.
    within(client.statequery().query_any(AnyCbor::from_encode(())))
        .await
        .unwrap();
    within(client.statequery().send_release()).await.unwrap();

    within(client.statequery().acquire(Some(Point::Origin)))
        .await
        .unwrap();
}

#[tokio::test]
async fn an_acquire_checks_its_point_against_the_archive() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100]);
    write_archive(&domain, &chain);

    let (mut client, _server, _dir) = connect_session(domain).await;

    let held = Point::try_from(chain[0].0.clone()).unwrap();
    within(client.statequery().acquire(Some(held)))
        .await
        .unwrap();
    within(client.statequery().send_release()).await.unwrap();

    let other_hash = Point::Specific(100, vec![0; 32]);
    expect_acquire_failure(within(client.statequery().acquire(Some(other_hash))).await);
}

#[tokio::test]
async fn an_acquire_with_no_point_takes_the_state_cursor() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100]);

    set_cursor(&domain, chain[0].0.clone());

    let (mut client, _server, _dir) = connect_session(domain.clone()).await;

    // The cursor's block isn't archived yet.
    expect_acquire_failure(within(client.statequery().acquire(None)).await);

    write_archive(&domain, &chain);
    within(client.statequery().acquire(None)).await.unwrap();

    expect_tip(&mut client, Point::try_from(chain[0].0.clone()).unwrap()).await;
}

#[tokio::test]
async fn a_store_with_no_cursor_is_at_origin() {
    let domain = ToyDomain::new(None, None);

    let (mut client, _server, _dir) = connect_session(domain).await;

    within(client.statequery().acquire(None)).await.unwrap();

    expect_tip(&mut client, Point::Origin).await;
}

/// The state between an epoch boundary's commit and its block's: the cursor
/// names a slot whose block isn't archived yet.
#[tokio::test]
async fn a_state_query_at_an_epoch_boundary_keeps_chain_sync_serving() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100, 110]);
    write_archive(&domain, &chain[..1]);
    write_wal(&domain, &chain[..1]);
    set_cursor(&domain, ChainPoint::Slot(110));

    let (mut client, _server, _dir) = connect_session(domain.clone()).await;

    intersect(&mut client, &chain[0].0).await;
    expect_rollback(&mut client, &chain[0].0).await;
    expect_await(&mut client).await;

    within(client.statequery().acquire(None)).await.unwrap();

    expect_tip(&mut client, Point::try_from(chain[0].0.clone()).unwrap()).await;

    expect_tip_forward(&mut client, &domain, &chain[1].0, &chain[1].1).await;
}

#[tokio::test]
async fn a_slot_cursor_with_nothing_archived_is_at_origin() {
    let domain = ToyDomain::new(None, None);
    set_cursor(&domain, ChainPoint::Slot(110));

    let (mut client, _server, _dir) = connect_session(domain).await;

    within(client.statequery().acquire(None)).await.unwrap();

    expect_tip(&mut client, Point::Origin).await;
}

/// The cursor's own block, not the archive tip past it.
#[tokio::test]
async fn a_slot_cursor_with_its_block_archived_takes_that_block() {
    let domain = ToyDomain::new(None, None);
    let chain = blocks(&[100, 110]);
    write_archive(&domain, &chain);
    set_cursor(&domain, ChainPoint::Slot(100));

    let (mut client, _server, _dir) = connect_session(domain).await;

    within(client.statequery().acquire(None)).await.unwrap();

    expect_tip(&mut client, Point::try_from(chain[0].0.clone()).unwrap()).await;
}
