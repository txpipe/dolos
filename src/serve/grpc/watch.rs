use crate::{
    state::LedgerStore,
    wal::{self, WalReader as _},
};
use futures_core::Stream;
use futures_util::StreamExt;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec as u5c;
use pallas::{
    interop::utxorpc::spec::watch::any_chain_tx_pattern::Chain,
    ledger::{addresses::Address, traverse::MultiEraBlock},
};
use std::pin::Pin;
use tonic::{Request, Response, Status};

fn outputs_match_address(
    pattern: &u5c::cardano::AddressPattern,
    outputs: &Vec<u5c::cardano::TxOutput>,
) -> bool {
    let exact_matches = pattern.exact_address.is_empty()
        || outputs.iter().any(|o| o.address == &pattern.exact_address);

    let delegation_matches = pattern.delegation_part.is_empty()
        || outputs.iter().any(|o| {
            let addr = Address::from_bytes(&o.address).unwrap();
            match addr {
                Address::Shelley(s) => s.delegation().to_vec().eq(&pattern.delegation_part),
                _ => false,
            }
        });
    let payment_matches = pattern.payment_part.is_empty()
        || outputs.iter().any(|o| {
            let addr = Address::from_bytes(&o.address).unwrap();
            match addr {
                Address::Shelley(s) => s.payment().to_vec().eq(&pattern.payment_part),
                _ => false,
            }
        });

    exact_matches && delegation_matches && payment_matches
}

fn outputs_match_asset(
    asset_pattern: &u5c::cardano::AssetPattern,
    outputs: &Vec<u5c::cardano::TxOutput>,
) -> bool {
    (asset_pattern.asset_name.is_empty() && asset_pattern.policy_id.is_empty())
        || outputs.iter().any(|o| {
            o.assets.iter().any(|ma| {
                ma.policy_id.eq(&asset_pattern.policy_id)
                    && ma
                        .assets
                        .iter()
                        .any(|a| a.name.eq(&asset_pattern.asset_name))
            })
        })
}

fn matches_output(
    pattern: &u5c::cardano::TxOutputPattern,
    outputs: &Vec<u5c::cardano::TxOutput>,
) -> bool {
    let mut matches = true;

    if let Some(addr_pattern) = &pattern.address {
        matches &= outputs_match_address(&addr_pattern, outputs);
    }
    if let Some(asset_pattern) = &pattern.asset {
        matches &= outputs_match_asset(&asset_pattern, outputs);
    }

    matches
}

fn matches_cardano_pattern(tx_pattern: &u5c::cardano::TxPattern, tx: &u5c::cardano::Tx) -> bool {
    let mut matches = true;
    if let Some(addr_pattern) = &tx_pattern.has_address {
        let outputs = tx.outputs.iter().map(|x| x.to_owned()).collect();
        let inputs = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().map(|output| output.to_owned()))
            .collect();

        matches &= outputs_match_address(addr_pattern, &inputs)
            || outputs_match_address(addr_pattern, &outputs);
    }
    if let Some(out_pattern) = &tx_pattern.consumes {
        let inputs = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().map(|output| output.to_owned()))
            .collect();
        matches &= matches_output(out_pattern, &inputs);
    }
    if let Some(asset_pattern) = &tx_pattern.mints_asset {
        matches &= (asset_pattern.asset_name.is_empty() && asset_pattern.policy_id.is_empty())
            || tx.mint.iter().any(|ma| {
                ma.policy_id.eq(&asset_pattern.policy_id)
                    && ma
                        .assets
                        .iter()
                        .any(|a| a.name.eq(&asset_pattern.asset_name))
            })
    }
    if let Some(asset_pattern) = &tx_pattern.moves_asset {
        let inputs = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().map(|output| output.to_owned()))
            .collect();
        matches &= outputs_match_asset(&asset_pattern, &inputs)
            || outputs_match_asset(&asset_pattern, &tx.outputs);
    }
    if let Some(out_pattern) = &tx_pattern.produces {
        matches &= matches_output(out_pattern, &tx.outputs);
    }

    matches
}

fn matches_chain(chain: &Chain, tx: &u5c::cardano::Tx) -> bool {
    match chain {
        Chain::Cardano(tx_pattern) => matches_cardano_pattern(tx_pattern, tx),
    }
}

fn apply_predicate(predicate: &u5c::watch::TxPredicate, tx: &u5c::cardano::Tx) -> bool {
    let mut tx_matches = true;
    if let Some(pattern) = &predicate.r#match {
        if let Some(chain) = &pattern.chain {
            tx_matches &= matches_chain(chain, tx);
        }
    }

    let not_clause = predicate
        .not
        .iter()
        .any(|p: &u5c::watch::TxPredicate| apply_predicate(p, tx));
    let and_clause = predicate.all_of.iter().all(|p| apply_predicate(p, tx));
    let or_clause =
        predicate.any_of.len() == 0 || predicate.any_of.iter().any(|p| apply_predicate(p, tx));

    tx_matches && !not_clause && and_clause && or_clause
}

fn block_to_txs(
    block: &wal::RawBlock,
    mapper: &interop::Mapper<LedgerStore>,
    request: &u5c::watch::WatchTxRequest,
) -> Vec<u5c::watch::AnyChainTx> {
    let wal::RawBlock { body, .. } = block;
    let block = MultiEraBlock::decode(body).unwrap();
    let txs = block.txs();

    txs.iter()
        .map(|x: &pallas::ledger::traverse::MultiEraTx<'_>| mapper.map_tx(x))
        .filter(|tx| {
            if let Some(predicate) = &request.predicate {
                return apply_predicate(predicate, tx);
            }
            true
        })
        .map(|x| u5c::watch::AnyChainTx {
            chain: Some(u5c::watch::any_chain_tx::Chain::Cardano(x)),
        })
        .collect()
}

fn roll_to_watch_response(
    mapper: &interop::Mapper<LedgerStore>,
    log: &wal::LogValue,
    request: &u5c::watch::WatchTxRequest,
) -> impl Stream<Item = u5c::watch::WatchTxResponse> {
    let txs: Vec<_> = match log {
        wal::LogValue::Apply(block) => block_to_txs(block, mapper, request)
            .into_iter()
            .map(u5c::watch::watch_tx_response::Action::Apply)
            .map(|x| u5c::watch::WatchTxResponse { action: Some(x) })
            .collect(),
        wal::LogValue::Undo(block) => block_to_txs(block, mapper, request)
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
    mapper: interop::Mapper<LedgerStore>,
}

impl WatchServiceImpl {
    pub fn new(wal: wal::redb::WalStore, ledger: LedgerStore) -> Self {
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
        let inner_req = request.into_inner();

        let from_seq = self
            .wal
            .find_tip()
            .map_err(|_err| Status::internal("can't read WAL"))?
            .map(|(x, _)| x)
            .unwrap_or_default();

        let mapper = self.mapper.clone();

        let stream = wal::WalStream::start(self.wal.clone(), from_seq)
            .flat_map(move |(_, log)| roll_to_watch_response(&mapper, &log, &inner_req))
            .map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }
}
