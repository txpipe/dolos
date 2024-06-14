use crate::{
    ledger::{self, EraCbor, TxoRef},
    wal::{self, WalReader as _},
};
use futures_core::Stream;
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec as u5c;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraBlock};
use std::{collections::HashSet, future, pin::Pin};
use tonic::{Request, Response, Status};
use tracing::info;

fn block_to_txs(
    block: &wal::RawBlock,
    mapper: &interop::Mapper<ledger::store::LedgerStore>,
) -> Vec<u5c::watch::AnyChainTx> {
    let wal::RawBlock { body, .. } = block;
    let block = MultiEraBlock::decode(body).unwrap();
    let txs = block.txs();

    txs.iter()
        .map(|x| mapper.map_tx(x))
        .map(|x| u5c::watch::AnyChainTx {
            chain: Some(u5c::watch::any_chain_tx::Chain::Cardano(x)),
        })
        .collect()
}

fn roll_to_watch_response(
    mapper: &interop::Mapper<ledger::store::LedgerStore>,
    log: &wal::LogValue,
) -> impl Stream<Item = u5c::watch::WatchTxResponse> {
    let txs: Vec<_> = match log {
        wal::LogValue::Apply(block) => block_to_txs(block, mapper)
            .into_iter()
            .map(u5c::watch::watch_tx_response::Action::Apply)
            .map(|x| u5c::watch::WatchTxResponse { action: Some(x) })
            .collect(),
        wal::LogValue::Undo(block) => block_to_txs(block, mapper)
            .into_iter()
            .map(u5c::watch::watch_tx_response::Action::Undo)
            .map(|x| u5c::watch::WatchTxResponse { action: Some(x) })
            .collect(),
        // TODO: shouldn't we have a u5c event for origin?
        wal::LogValue::Mark(..) => vec![],
    };

    tokio_stream::iter(txs)
}

pub struct WatchServiceImpl {
    wal: wal::redb::WalStore,
    mapper: interop::Mapper<ledger::store::LedgerStore>,
}

impl WatchServiceImpl {
    pub fn new(wal: wal::redb::WalStore, ledger: ledger::store::LedgerStore) -> Self {
        Self {
            wal,
            mapper: interop::Mapper::new(ledger),
        }
    }
}

#[async_trait::async_trait]
impl u5c::watch::watch_service_server::WatchService for WatchServiceImpl {
    type WatchTxStream = Pin<
        Box<dyn Stream<Item = Result<u5c::watch::WatchTxResponse, tonic::Status>> + Send + 'static>,
    >;

    async fn watch_tx(
        &self,
        request: Request<u5c::watch::WatchTxRequest>,
    ) -> Result<Response<Self::WatchTxStream>, Status> {
        let request = request.into_inner();

        let from_seq = self
            .wal
            .find_tip()
            .map_err(|_err| Status::internal("can't read WAL"))?
            .map(|(x, _)| x)
            .unwrap_or_default();

        let mapper = self.mapper.clone();

        let stream = wal::WalStream::start(self.wal.clone(), from_seq)
            .flat_map(move |(_, log)| roll_to_watch_response(&mapper, &log))
            .map(|x| Ok(x));

        Ok(Response::new(Box::pin(stream)))
    }
}
