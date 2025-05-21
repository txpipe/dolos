use crate::{
    model::BlockBody,
    state::LedgerStore,
    wal::{self, ChainPoint, WalReader as _},
};
use futures_core::Stream;
use futures_util::StreamExt;
use pallas::interop::utxorpc::spec as u5c;
use pallas::interop::utxorpc::{self as interop, Mapper};
use pallas::{
    interop::utxorpc::spec::watch::any_chain_tx_pattern::Chain,
    ledger::{addresses::Address, traverse::MultiEraBlock},
};
use std::pin::Pin;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

fn raw_to_anychain(mapper: &Mapper<LedgerStore>, body: &BlockBody) -> u5c::watch::AnyChainBlock {
    let block = mapper.map_block_cbor(body);

    u5c::watch::AnyChainBlock {
        native_bytes: body.to_vec().into(),
        chain: u5c::watch::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn outputs_match_address(
    pattern: &u5c::cardano::AddressPattern,
    outputs: &[u5c::cardano::TxOutput],
) -> bool {
    let exact_matches = pattern.exact_address.is_empty()
        || outputs.iter().any(|o| o.address == pattern.exact_address);

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
    outputs: &[u5c::cardano::TxOutput],
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
    outputs: &[u5c::cardano::TxOutput],
) -> bool {
    let address_match = pattern
        .address
        .as_ref()
        .is_none_or(|addr_pattern| outputs_match_address(addr_pattern, outputs));

    let asset_match = pattern
        .asset
        .as_ref()
        .is_none_or(|asset_pattern| outputs_match_asset(asset_pattern, outputs));

    address_match && asset_match
}

fn matches_cardano_pattern(tx_pattern: &u5c::cardano::TxPattern, tx: &u5c::cardano::Tx) -> bool {
    let has_address_match = tx_pattern.has_address.as_ref().is_none_or(|addr_pattern| {
        let outputs: Vec<_> = tx.outputs.to_vec();
        let inputs: Vec<_> = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().cloned())
            .collect();

        outputs_match_address(addr_pattern, &inputs)
            || outputs_match_address(addr_pattern, &outputs)
    });

    let consumes_match = tx_pattern.consumes.as_ref().is_none_or(|out_pattern| {
        let inputs: Vec<_> = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().cloned())
            .collect();
        matches_output(out_pattern, &inputs)
    });

    let mints_asset_match = tx_pattern.mints_asset.as_ref().is_none_or(|asset_pattern| {
        (asset_pattern.asset_name.is_empty() && asset_pattern.policy_id.is_empty())
            || tx.mint.iter().any(|ma| {
                ma.policy_id.eq(&asset_pattern.policy_id)
                    && ma
                        .assets
                        .iter()
                        .any(|a| a.name.eq(&asset_pattern.asset_name))
            })
    });

    let moves_asset_match = tx_pattern.moves_asset.as_ref().is_none_or(|asset_pattern| {
        let inputs: Vec<_> = tx
            .inputs
            .iter()
            .filter_map(|x| x.as_output.as_ref().cloned())
            .collect();
        outputs_match_asset(asset_pattern, &inputs)
            || outputs_match_asset(asset_pattern, &tx.outputs)
    });

    let produces_match = tx_pattern
        .produces
        .as_ref()
        .is_none_or(|out_pattern| matches_output(out_pattern, &tx.outputs));

    has_address_match && consumes_match && mints_asset_match && moves_asset_match && produces_match
}

fn matches_chain(chain: &Chain, tx: &u5c::cardano::Tx) -> bool {
    match chain {
        Chain::Cardano(tx_pattern) => matches_cardano_pattern(tx_pattern, tx),
    }
}

fn apply_predicate(predicate: &u5c::watch::TxPredicate, tx: &u5c::cardano::Tx) -> bool {
    let tx_matches = predicate
        .r#match
        .as_ref()
        .and_then(|pattern| pattern.chain.as_ref())
        .is_none_or(|chain| matches_chain(chain, tx));

    let not_clause = predicate.not.iter().any(|p| apply_predicate(p, tx));

    let and_clause = predicate.all_of.iter().all(|p| apply_predicate(p, tx));

    let or_clause =
        predicate.any_of.is_empty() || predicate.any_of.iter().any(|p| apply_predicate(p, tx));

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
            request
                .predicate
                .as_ref()
                .is_none_or(|predicate| apply_predicate(predicate, tx))
        })
        .map(|x| u5c::watch::AnyChainTx {
            block: Some(raw_to_anychain(mapper, body)),
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
    cancellation_token: CancellationToken,
}

impl WatchServiceImpl {
    pub fn new(
        wal: wal::redb::WalStore,
        ledger: LedgerStore,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            wal,
            mapper: interop::Mapper::new(ledger),
            cancellation_token,
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

        let intersect = inner_req
            .intersect
            .iter()
            .map(|x| ChainPoint::Specific(x.slot, x.hash.to_vec().as_slice().into()))
            .collect::<Vec<ChainPoint>>();

        let from_seq = if intersect.is_empty() {
            self.wal
                .find_tip()
                .map_err(|_err| Status::internal("can't read WAL"))?
                .map(|(x, _)| x)
                .unwrap_or_default()
        } else {
            self.wal
                .find_intersect(&intersect)
                .map_err(|_err| Status::internal("can't read WAL"))?
                .map(|(x, _)| x)
                .unwrap_or_default()
        };

        let mapper = self.mapper.clone();

        let stream =
            wal::WalStream::start(self.wal.clone(), from_seq, self.cancellation_token.clone())
                .flat_map(move |(_, log)| roll_to_watch_response(&mapper, &log, &inner_req))
                .map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }
}
