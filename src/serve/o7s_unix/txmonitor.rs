use futures_util::StreamExt as _;
use std::collections::BTreeSet;

use pallas::codec::utils::TagWrap;
use pallas::network::miniprotocols::txmonitor::{
    self, ClientQueryRequest, MempoolMeasures, MempoolSizeAndCapacity, SizeAndCapacity,
};
use tracing::{debug, info, warn};

use dolos_cardano::load_effective_pparams;
use dolos_core::{MempoolStore as _, StateStore as _, TipSubscription as _};

use crate::prelude::*;

// HACK: the tx era number differs from the block era number, we subtract 1 to
// make them match.
fn to_n2c_era(era: u16) -> u8 {
    (era - 1) as u8
}

struct Snapshot {
    slot: u64,
    txs: Vec<MempoolTx>,
    cursor: usize,
}

impl Snapshot {
    fn fingerprint(&self) -> (u64, BTreeSet<TxHash>) {
        (self.slot, self.txs.iter().map(|tx| tx.hash).collect())
    }

    fn size_in_bytes(&self) -> u64 {
        self.txs
            .iter()
            .map(|tx| tx.payload.cbor().len() as u64)
            .sum()
    }
}

pub struct Session<D: Domain> {
    domain: D,
    connection: txmonitor::Server,
    snapshot: Option<Snapshot>,
}

impl<D: Domain> Session<D> {
    fn take_snapshot(&self) -> Result<Snapshot, Error> {
        let point = self
            .domain
            .state()
            .read_cursor()
            .map_err(Error::server)?
            .unwrap_or(ChainPoint::Origin);

        let mempool = self.domain.mempool();

        // confirmed txs are already on-chain, a node mempool wouldn't hold them
        let txs = mempool
            .peek_pending()
            .into_iter()
            .chain(mempool.peek_inflight())
            .filter(|tx| !matches!(tx.stage, MempoolTxStage::Confirmed))
            .collect();

        Ok(Snapshot {
            slot: point.slot(),
            txs,
            cursor: 0,
        })
    }

    fn acquired(&self) -> &Snapshot {
        self.snapshot
            .as_ref()
            .expect("txmonitor request handled without an acquired snapshot")
    }

    async fn handle_acquire(&mut self) -> Result<(), Error> {
        let snapshot = self.take_snapshot()?;
        let slot = snapshot.slot;

        debug!(slot, txs = snapshot.txs.len(), "acquired mempool snapshot");

        self.snapshot = Some(snapshot);

        self.connection
            .send_acquired(slot)
            .await
            .map_err(Error::server)?;

        Ok(())
    }

    async fn handle_await_acquire(&mut self) -> Result<(), Error> {
        let previous = self.acquired().fingerprint();

        let mut mempool_updates = self.domain.mempool().subscribe();
        let mut mempool_open = true;

        let mut tip_updates = self
            .domain
            .watch_tip(None)
            .map_err(|e| Error::server(e.to_string()))?;

        loop {
            let snapshot = self.take_snapshot()?;

            if snapshot.fingerprint() != previous {
                let slot = snapshot.slot;

                debug!(slot, txs = snapshot.txs.len(), "acquired changed snapshot");

                self.snapshot = Some(snapshot);

                self.connection
                    .send_acquired(slot)
                    .await
                    .map_err(Error::server)?;

                return Ok(());
            }

            tokio::select! {
                update = mempool_updates.next(), if mempool_open => {
                    match update {
                        Some(Err(e)) => return Err(Error::server(e)),
                        Some(Ok(_)) => (),
                        // a closed stream must not busy-loop the select; keep
                        // waking on tip changes only
                        None => mempool_open = false,
                    }
                }
                _ = tip_updates.next_tip() => (),
            }
        }
    }

    async fn handle_next_tx(&mut self) -> Result<(), Error> {
        let snapshot = self
            .snapshot
            .as_mut()
            .expect("txmonitor request handled without an acquired snapshot");

        let tx = snapshot.txs.get(snapshot.cursor).map(|tx| {
            let EraCbor(era, cbor) = &tx.payload;
            (to_n2c_era(*era), TagWrap::new(cbor.clone().into()))
        });

        if tx.is_some() {
            snapshot.cursor += 1;
        }

        self.connection
            .send_next_tx(tx)
            .await
            .map_err(Error::server)?;

        Ok(())
    }

    async fn handle_has_tx(&mut self, id: txmonitor::TxId) -> Result<(), Error> {
        // peers wrap the same hash in every plausible era and combine the
        // answers, so we match on the hash alone and ignore the era tag
        let (_era, hash) = &id;

        let has = self
            .acquired()
            .txs
            .iter()
            .any(|tx| tx.hash.as_slice() == hash.as_slice());

        self.connection
            .send_has_tx(has)
            .await
            .map_err(Error::server)?;

        Ok(())
    }

    fn capacity_in_bytes(&self) -> Result<u64, Error> {
        // cardano-node sizes its mempool at twice the max block body by
        // default; report the same so clients get a familiar reference
        let pparams = load_effective_pparams::<D>(self.domain.state())
            .map_err(|e| Error::server(e.to_string()))?;

        Ok(pparams.max_block_body_size_or_default() * 2)
    }

    async fn handle_get_sizes(&mut self) -> Result<(), Error> {
        let capacity = self.capacity_in_bytes()?;
        let snapshot = self.acquired();

        let sizes = MempoolSizeAndCapacity {
            capacity_in_bytes: capacity as u32,
            size_in_bytes: snapshot.size_in_bytes() as u32,
            number_of_txs: snapshot.txs.len() as u32,
        };

        self.connection
            .send_size_and_capacity(sizes)
            .await
            .map_err(Error::server)?;

        Ok(())
    }

    async fn handle_get_measures(&mut self) -> Result<(), Error> {
        let capacity = self.capacity_in_bytes()?;
        let snapshot = self.acquired();

        let measures = MempoolMeasures {
            tx_count: snapshot.txs.len() as u32,
            measures: vec![(
                "transaction_bytes".to_string(),
                SizeAndCapacity {
                    size: snapshot.size_in_bytes(),
                    capacity,
                },
            )],
        };

        self.connection
            .send_measures(measures)
            .await
            .map_err(Error::server)?;

        Ok(())
    }

    async fn process_requests(&mut self) -> Result<(), Error> {
        loop {
            if self
                .connection
                .recv_while_idle()
                .await
                .map_err(Error::server)?
                .is_none()
            {
                break;
            }

            self.handle_acquire().await?;

            loop {
                match self
                    .connection
                    .recv_while_acquired()
                    .await
                    .map_err(Error::server)?
                {
                    ClientQueryRequest::AwaitAcquire => {
                        self.handle_await_acquire().await?;
                    }
                    ClientQueryRequest::NextTx => {
                        self.handle_next_tx().await?;
                    }
                    ClientQueryRequest::HasTx(id) => {
                        self.handle_has_tx(id).await?;
                    }
                    ClientQueryRequest::GetSizes => {
                        self.handle_get_sizes().await?;
                    }
                    ClientQueryRequest::GetMeasures => {
                        self.handle_get_measures().await?;
                    }
                    ClientQueryRequest::Release => {
                        self.snapshot = None;
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn handle_session<D: Domain, C: CancelToken>(
    domain: D,
    connection: txmonitor::Server,
    cancel: C,
) -> Result<(), ServeError> {
    let mut session = Session {
        domain,
        connection,
        snapshot: None,
    };

    info!("txmonitor session started");

    tokio::select! {
        result = session.process_requests() => {
            if let Err(e) = result {
                warn!(?e, "txmonitor session error");
                return Err(ServeError::Internal(e.into()));
            }
            info!("txmonitor client ended protocol");
        },
        _ = cancel.cancelled() => {
            info!("txmonitor protocol was cancelled");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio_util::sync::CancellationToken;

    use dolos_testing::mempool::make_test_mempool_tx;
    use dolos_testing::slot_to_chainpoint;
    use dolos_testing::toy_domain::ToyDomain;

    use pallas::network::facades::{NodeClient, NodeServer};

    use crate::serve::CancelTokenImpl;

    fn spawn_server(
        domain: ToyDomain,
        listener: tokio::net::UnixListener,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let connection = NodeServer::accept(&listener, 0).await.unwrap();

            let NodeServer {
                plexer, txmonitor, ..
            } = connection;

            let cancel = CancelTokenImpl(CancellationToken::new());

            handle_session(domain, txmonitor, cancel).await.unwrap();

            plexer.abort().await;
        })
    }

    #[tokio::test]
    async fn txmonitor_serves_snapshot_queries() {
        let domain = ToyDomain::new(None, None);

        let seeded_hash = TxHash::from([0xab; 32]);

        domain
            .mempool()
            .receive(make_test_mempool_tx(seeded_hash))
            .unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("node.socket");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();

        let server = spawn_server(domain, listener);

        let mut client = NodeClient::connect(&socket, 0).await.unwrap();

        client.monitor().acquire().await.unwrap();

        let sizes = client.monitor().query_size_and_capacity().await.unwrap();
        assert_eq!(sizes.number_of_txs, 1);
        assert_eq!(sizes.size_in_bytes, 1);
        assert!(sizes.capacity_in_bytes > 0);

        let (era, body) = client.monitor().query_next_tx().await.unwrap().unwrap();
        assert_eq!(era, 6);
        assert_eq!(body.0.len(), 1);

        let next = client.monitor().query_next_tx().await.unwrap();
        assert!(next.is_none());

        let has = client
            .monitor()
            .query_has_tx((6, seeded_hash.to_vec().into()))
            .await
            .unwrap();
        assert!(has);

        // the era wrapper is not part of the identity check
        let has = client
            .monitor()
            .query_has_tx((4, seeded_hash.to_vec().into()))
            .await
            .unwrap();
        assert!(has);

        let has = client
            .monitor()
            .query_has_tx((6, vec![0xcd; 32].into()))
            .await
            .unwrap();
        assert!(!has);

        let measures = client.monitor().query_measures().await.unwrap();
        assert_eq!(measures.tx_count, 1);

        client.monitor().release().await.unwrap();
        client.monitor().done().await.unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn txmonitor_await_acquire_wakes_on_change() {
        let domain = ToyDomain::new(None, None);

        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("node.socket");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();

        let server = spawn_server(domain.clone(), listener);

        let mut client = NodeClient::connect(&socket, 0).await.unwrap();

        client.monitor().acquire().await.unwrap();

        let new_hash = TxHash::from([0x11; 32]);

        let trigger = tokio::spawn({
            let domain = domain.clone();

            async move {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;

                domain
                    .mempool()
                    .receive(make_test_mempool_tx(new_hash))
                    .unwrap();

                // the toy mempool emits no events; a tip event provides the
                // wake-up, mirroring a new block reaching the node
                domain.notify_tip(TipEvent::Mark(slot_to_chainpoint(1)));
            }
        });

        client.monitor().await_acquire().await.unwrap();

        let has = client
            .monitor()
            .query_has_tx((6, new_hash.to_vec().into()))
            .await
            .unwrap();
        assert!(has);

        trigger.await.unwrap();

        client.monitor().release().await.unwrap();
        client.monitor().done().await.unwrap();

        server.await.unwrap();
    }
}
