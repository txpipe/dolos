use crate::{
    ledger::{self, store::LedgerStore},
    wal::{self, LogValue, WalReader as _},
};
use futures_core::Stream;
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::ledger::{
    addresses::Address,
    traverse::{MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets},
};
use pallas::{interop::utxorpc::spec as u5c, ledger::traverse::MultiEraTx};
use pallas::{interop::utxorpc::Mapper, ledger::traverse::MultiEraAsset};
use std::pin::Pin;
use tonic::{Request, Response, Status};

fn tx_addresses(tx: &MultiEraTx) -> Vec<Address> {
    let a = tx
        .produces()
        .iter()
        .map(|(_, x)| x.address().ok())
        .flatten()
        .collect();

    // TODO: read consumed

    a
}

fn tx_moved_assets<'a>(tx: &'a MultiEraTx) -> Vec<MultiEraPolicyAssets<'a>> {
    let a = tx
        .produces()
        .iter()
        .flat_map(|(_, x)| x.non_ada_assets())
        .collect();

    // TODO: read consumed

    a
}

fn match_address_pattern(addr: &Address, pattern: &u5c::cardano::AddressPattern) -> Option<bool> {
    Some(true)
}

fn match_asset_pattern(
    asset: &MultiEraAsset,
    pattern: &u5c::cardano::AssetPattern,
) -> Option<bool> {
    Some(true)
}

fn match_output_pattern(
    txo: &MultiEraOutput,
    pattern: &u5c::cardano::TxOutputPattern,
) -> Option<bool> {
    let a = pattern
        .address
        .as_ref()
        .and_then(|x| match_address_pattern(tx, x));

    let b = pattern
        .asset
        .as_ref()
        .and_then(|x| match_asset_pattern(tx, x));

    a.and(b)
}

fn match_tx_pattern(tx: &MultiEraTx, pattern: &u5c::cardano::TxPattern) -> Option<bool> {
    let a = pattern
        .consumes
        .as_ref()
        .and_then(|x| match_output_pattern(tx, x));

    let b = pattern
        .has_address
        .iter()
        .cartesian_product(tx_addresses(tx))
        .map(|(p, subject)| match_address_pattern(&subject, p))
        .fold(Some(true), |acc, x| acc.or(x));

    let c = pattern
        .mints_asset
        .as_ref()
        .and_then(|x| match_asset_pattern(tx, x));

    let d = pattern
        .moves_asset
        .as_ref()
        .and_then(|x| match_asset_pattern(tx, x));

    let e = pattern
        .produces
        .iter()
        .cartesian_product(tx.produces().iter().map(|(_, x)| x))
        .map(|(p, subject)| match_output_pattern(subject, p))
        .fold(Some(true), |acc, x| acc.or(x));

    a.and(b).and(c).and(d).and(e)
}

fn match_predicate(tx: &MultiEraTx, predicate: &Option<u5c::watch::TxPredicate>) -> bool {
    match predicate {
        Some(x) => match &x.r#match {
            Some(x) => match &x.chain {
                Some(x) => match x {
                    u5c::watch::any_chain_tx_pattern::Chain::Cardano(x) => match_tx_pattern(x),
                },
                None => false,
            },
            None => todo!(),
        },
        None => true,
    }
}

fn yield_matching_txs(
    log: LogValue,
    ledger: &LedgerStore,
    predicate: &Option<u5c::watch::TxPredicate>,
) -> impl Stream<Item = u5c::watch::WatchTxResponse> {
    let (is_apply, block) = match log {
        wal::LogValue::Apply(x) => (true, x),
        wal::LogValue::Undo(x) => (false, x),
        wal::LogValue::Mark(_) => todo!(),
    };

    let blockd = MultiEraBlock::decode(&block.body).unwrap();
    let slice = crate::ledger::load_slice_for_block(&blockd, ledger, &[]).unwrap();

    let mapper = Mapper::new(slice);

    let into_action = move |x| {
        if is_apply {
            u5c::watch::watch_tx_response::Action::Apply(x)
        } else {
            u5c::watch::watch_tx_response::Action::Undo(x)
        }
    };

    let txs: Vec<_> = blockd
        .txs()
        .into_iter()
        .filter(|x| match_predicate(x, predicate))
        .map(|x| mapper.map_tx(&x))
        .map(|x| u5c::watch::AnyChainTx {
            chain: Some(u5c::watch::any_chain_tx::Chain::Cardano(x)),
        })
        .map(into_action)
        .map(|x| u5c::watch::WatchTxResponse { action: Some(x) })
        .collect();

    tokio_stream::iter(txs)
}

pub struct WatchServiceImpl {
    wal: wal::redb::WalStore,
    ledger: ledger::store::LedgerStore,
}

impl WatchServiceImpl {
    pub fn new(wal: wal::redb::WalStore, ledger: ledger::store::LedgerStore) -> Self {
        Self { wal, ledger }
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
        let req = request.into_inner();

        let from_seq = self
            .wal
            .find_tip()
            .map_err(|_err| Status::internal("can't read WAL"))?
            .map(|(x, _)| x)
            .unwrap_or_default();

        let ledger = self.ledger.clone();

        let stream = wal::WalStream::start(self.wal.clone(), from_seq)
            .flat_map(move |(_, log)| yield_matching_txs(log, &ledger, &req.predicate))
            .map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }
}
