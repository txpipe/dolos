//! A node-to-node peer over a synthetic chain, for a node under test to sync
//! from: chain-sync and block-fetch over TCP, a tip that can keep advancing,
//! a stop that closes every connection, and faults that close one session.
//!
//! Block `n` sits at slot `n`, chained from block `n - 1`; block 1 has no
//! parent. The chain never forks.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use dolos_core::{ChainPoint, RawBlock};
use pallas::ledger::traverse::{Era, MultiEraBlock};
use pallas::network::facades::{KeepAliveLoop, PeerServer};
use pallas::network::miniprotocols::{
    blockfetch,
    chainsync::{self, ClientRequest, HeaderContent, Tip},
    Point,
};
use tokio::sync::watch;

use crate::blocks::make_conway_block_with_prev;

/// How a [`PeerFixture`] serves its chain.
#[derive(Debug, Clone, Default)]
pub struct PeerOptions {
    /// The blocks the chain holds when the fixture starts.
    pub initial_blocks: u64,
    /// Add a block this often; never when `None`.
    pub produce_every: Option<Duration>,
    /// Stop, closing every connection, once this many headers were sent.
    pub stop_after_headers: Option<usize>,
    /// Close the session instead of answering these intersection requests,
    /// numbered from 1 across sessions.
    pub close_at_intersects: Vec<usize>,
    /// Close the first session that intersects once it sent this many
    /// headers.
    pub close_first_session_after_headers: Option<usize>,
    /// Close every session on its first request for the next update.
    pub close_at_first_request: bool,
    /// Close the session on any block-fetch request.
    pub refuse_block_fetch: bool,
    /// Serve this block's body as bytes that decode as no block.
    pub corrupt_body: Option<u64>,
}

struct Block {
    point: ChainPoint,
    header: HeaderContent,
    body: RawBlock,
}

impl Block {
    fn new(number: u64, parent: Option<&Block>) -> Self {
        let (point, body) =
            make_conway_block_with_prev(number, parent.and_then(|b| b.point.hash()), number);

        let block = MultiEraBlock::decode(&body).expect("a synthetic block decodes");

        let header = HeaderContent {
            variant: header_variant(block.era()),
            byron_prefix: None,
            cbor: block.header().cbor().to_vec(),
        };

        Self {
            point,
            header,
            body,
        }
    }

    fn wire_point(&self) -> Point {
        Point::try_from(self.point.clone()).expect("a block point is specific")
    }
}

/// The chain-sync header variant of a post-Byron era.
fn header_variant(era: Era) -> u8 {
    match era {
        Era::Shelley => 1,
        Era::Allegra => 2,
        Era::Mary => 3,
        Era::Alonzo => 4,
        Era::Babbage => 5,
        Era::Conway => 6,
        era => panic!("the fixture has no header variant for {era}"),
    }
}

struct Chain {
    blocks: Mutex<Vec<Arc<Block>>>,
    length: watch::Sender<usize>,
}

impl Chain {
    fn new(initial: u64) -> Self {
        let chain = Self {
            blocks: Mutex::new(Vec::new()),
            length: watch::Sender::new(0),
        };

        for _ in 0..initial {
            chain.extend();
        }

        chain
    }

    fn extend(&self) {
        let mut blocks = self.blocks.lock().unwrap();
        let block = Block::new(blocks.len() as u64 + 1, blocks.last().map(Arc::as_ref));
        blocks.push(Arc::new(block));
        self.length.send_replace(blocks.len());
    }

    fn get(&self, index: usize) -> Option<Arc<Block>> {
        self.blocks.lock().unwrap().get(index).cloned()
    }

    fn tip_point(&self) -> ChainPoint {
        self.blocks
            .lock()
            .unwrap()
            .last()
            .map_or(ChainPoint::Origin, |block| block.point.clone())
    }

    fn tip(&self) -> Tip {
        let blocks = self.blocks.lock().unwrap();

        match blocks.last() {
            Some(block) => Tip(block.wire_point(), blocks.len() as u64),
            None => Tip(Point::Origin, 0),
        }
    }

    /// The first of `points` on the chain, and the index of the block after
    /// it.
    fn intersect(&self, points: &[Point]) -> Option<(Point, usize)> {
        let blocks = self.blocks.lock().unwrap();

        points.iter().find_map(|point| match point {
            Point::Origin => Some((Point::Origin, 0)),
            Point::Specific(..) => blocks
                .iter()
                .position(|block| &block.wire_point() == point)
                .map(|index| (point.clone(), index + 1)),
        })
    }

    /// The bodies from `from` through `to`, when both are on the chain, with
    /// junk for block `corrupt`.
    fn range(&self, from: &Point, to: &Point, corrupt: Option<u64>) -> Vec<Vec<u8>> {
        let blocks = self.blocks.lock().unwrap();
        let index = |point: &Point| blocks.iter().position(|b| &b.wire_point() == point);

        match (index(from), index(to)) {
            (Some(from), Some(to)) if from <= to => blocks[from..=to]
                .iter()
                .map(|block| match corrupt {
                    Some(number) if block.point.slot() == number => vec![0xff; 8],
                    _ => block.body.to_vec(),
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    async fn wait_for_block(&self, index: usize) -> Arc<Block> {
        let mut length = self.length.subscribe();

        length
            .wait_for(|length| *length > index)
            .await
            .expect("the chain outlives its sessions");

        self.get(index).expect("the block was just added")
    }
}

struct Shared {
    chain: Chain,
    options: PeerOptions,
    next_requests: AtomicUsize,
    headers_sent: AtomicUsize,
    intersects: AtomicUsize,
    sessions: AtomicUsize,
    stop: watch::Sender<bool>,
}

impl Shared {
    fn header_sent(&self) {
        let sent = self.headers_sent.fetch_add(1, Ordering::SeqCst) + 1;

        if self
            .options
            .stop_after_headers
            .is_some_and(|limit| sent >= limit)
        {
            self.stop.send_replace(true);
        }
    }
}

/// A peer serving a synthetic chain on a local port, on its own thread.
/// Dropping it stops it.
pub struct PeerFixture {
    shared: Arc<Shared>,
    address: SocketAddr,
    thread: Option<JoinHandle<()>>,
}

impl PeerFixture {
    /// Listen on `address` (port 0 picks a free one) for peers handshaking
    /// with `magic`. The port is bound when this returns.
    pub fn start(address: &str, magic: u64, options: PeerOptions) -> std::io::Result<Self> {
        let listener = std::net::TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;

        let shared = Arc::new(Shared {
            chain: Chain::new(options.initial_blocks),
            options: options.clone(),
            next_requests: AtomicUsize::new(0),
            headers_sent: AtomicUsize::new(0),
            intersects: AtomicUsize::new(0),
            sessions: AtomicUsize::new(0),
            stop: watch::Sender::new(false),
        });

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;

        let serving = shared.clone();
        let thread = std::thread::spawn(move || {
            runtime.block_on(serve(listener, magic, options.produce_every, serving));
            // Dropping the runtime drops every session, and their sockets.
            drop(runtime);
        });

        Ok(Self {
            shared,
            address,
            thread: Some(thread),
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The chain's last block, or Origin.
    pub fn tip(&self) -> ChainPoint {
        self.shared.chain.tip_point()
    }

    /// The chain-sync requests for the next update received so far, across
    /// connections. After an intersection, the first is answered with the
    /// rollback to it and each later one with a block, or with an await
    /// when there is none yet.
    pub fn next_requests(&self) -> usize {
        self.shared.next_requests.load(Ordering::SeqCst)
    }

    /// The chain-sync headers sent so far, across connections, including one
    /// sent after an await to a request made before it.
    pub fn headers_sent(&self) -> usize {
        self.shared.headers_sent.load(Ordering::SeqCst)
    }

    /// The intersection requests received so far, across connections,
    /// including those answered by closing the session.
    pub fn intersects(&self) -> usize {
        self.shared.intersects.load(Ordering::SeqCst)
    }

    /// Whether the fixture has stopped, by [`Self::stop`] or after
    /// [`PeerOptions::stop_after_headers`].
    pub fn is_stopped(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Close the listener and every connection, and wait until they are.
    pub fn stop(&mut self) {
        self.shared.stop.send_replace(true);

        if let Some(thread) = self.thread.take() {
            thread.join().expect("the peer fixture thread panicked");
        }
    }
}

impl Drop for PeerFixture {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn serve(
    listener: std::net::TcpListener,
    magic: u64,
    produce_every: Option<Duration>,
    shared: Arc<Shared>,
) {
    let listener = tokio::net::TcpListener::from_std(listener).expect("a nonblocking listener");
    let mut stop = shared.stop.subscribe();

    let produce = async {
        let Some(every) = produce_every else {
            return std::future::pending().await;
        };

        let mut interval = tokio::time::interval(every);
        interval.tick().await;

        loop {
            interval.tick().await;
            shared.chain.extend();
        }
    };

    let accept = async {
        loop {
            match PeerServer::accept(&listener, magic).await {
                Ok(peer) => {
                    tokio::spawn(session(peer, shared.clone()));
                }
                Err(_) => continue,
            }
        }
    };

    tokio::select! {
        _ = stop.wait_for(|stopped| *stopped) => (),
        _ = produce => (),
        _ = accept => (),
    }
}

async fn session(peer: PeerServer, shared: Arc<Shared>) {
    // Every agent stays alive for the session: the multiplexer fails the
    // connection on a message for a protocol whose agent was dropped, and the
    // node's submit stage speaks tx-submission.
    let PeerServer {
        plexer,
        chainsync,
        blockfetch,
        keepalive,
        txsubmission: _txsubmission,
        peersharing: _peersharing,
        handshake: _handshake,
        ..
    } = peer;

    let keepalive = KeepAliveLoop::server(keepalive).spawn();

    // Either side ending closes the connection.
    tokio::select! {
        _ = serve_chainsync(chainsync, &shared) => (),
        _ = serve_blockfetch(blockfetch, &shared) => (),
    }

    keepalive.abort();
    plexer.abort().await;
}

async fn serve_chainsync(
    mut server: chainsync::N2NServer,
    shared: &Shared,
) -> Result<(), chainsync::ServerError> {
    let chain = &shared.chain;
    let options = &shared.options;

    // A client that asks for the next update without intersecting starts at
    // Origin, announced by a rollback as after any intersection.
    let mut next = 0;
    let mut rollback = Some(Point::Origin);
    let mut session = None;
    let mut requests = 0;
    let mut headers = 0;

    while let Some(request) = server.recv_while_idle().await? {
        match request {
            ClientRequest::Intersect(points) => {
                let number = shared.intersects.fetch_add(1, Ordering::SeqCst) + 1;

                if options.close_at_intersects.contains(&number) {
                    return Ok(());
                }

                match chain.intersect(&points) {
                    Some((point, after)) => {
                        next = after;
                        rollback = Some(point.clone());
                        session.get_or_insert_with(|| {
                            shared.sessions.fetch_add(1, Ordering::SeqCst) + 1
                        });
                        server.send_intersect_found(point, chain.tip()).await?;
                    }
                    None => server.send_intersect_not_found(chain.tip()).await?,
                }
            }
            ClientRequest::RequestNext => {
                shared.next_requests.fetch_add(1, Ordering::SeqCst);
                requests += 1;

                if options.close_at_first_request && requests == 1 {
                    return Ok(());
                }

                if let Some(point) = rollback.take() {
                    server.send_roll_backward(point, chain.tip()).await?;
                    continue;
                }

                let block = match chain.get(next) {
                    Some(block) => block,
                    None => {
                        server.send_await_reply().await?;
                        chain.wait_for_block(next).await
                    }
                };

                server
                    .send_roll_forward(block.header.clone(), chain.tip())
                    .await?;

                next += 1;
                headers += 1;
                shared.header_sent();

                if session == Some(1) && options.close_first_session_after_headers == Some(headers)
                {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

async fn serve_blockfetch(
    mut server: blockfetch::Server,
    shared: &Shared,
) -> Result<(), blockfetch::ServerError> {
    while let Some(blockfetch::BlockRequest((from, to))) = server.recv_while_idle().await? {
        if shared.options.refuse_block_fetch {
            return Ok(());
        }

        let bodies = shared.chain.range(&from, &to, shared.options.corrupt_body);
        server.send_block_range(bodies).await?;
    }

    Ok(())
}
