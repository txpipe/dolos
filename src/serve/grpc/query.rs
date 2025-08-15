use itertools::Itertools as _;
use pallas::interop::utxorpc::{self as interop, spec::query::any_utxo_pattern::UtxoPattern};
use pallas::interop::utxorpc::{spec as u5c, LedgerContext};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::collections::HashSet;
use tonic::{Request, Response, Status};
use tracing::info;

use super::masking::apply_mask;
use crate::prelude::*;
use pallas::ledger::traverse::wellknown::{MAINNET_MAGIC, TESTNET_MAGIC, PREVIEW_MAGIC, PRE_PRODUCTION_MAGIC};

/// Get the CAIP-2 blockchain identifier for the given network magic
fn get_caip2_identifier(network_magic: u32) -> String {
    let network_magic = network_magic as u64;
    match network_magic {
        MAINNET_MAGIC => format!("cardano-mainnet:{}", network_magic),
        TESTNET_MAGIC => format!("cardano-testnet:{}", network_magic),
        PREVIEW_MAGIC => format!("cardano-preview:{}", network_magic),
        PRE_PRODUCTION_MAGIC => format!("cardano-preprod:{}", network_magic),
        _ => format!("cardano:{}", network_magic), // fallback for unknown networks
    }
}

pub struct QueryServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    domain: D,
    mapper: interop::Mapper<D>,
}

impl<D> QueryServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    pub fn new(domain: D) -> Self {
        let mapper = interop::Mapper::new(domain.clone());

        Self { domain, mapper }
    }

    fn point_to_u5c(&self, point: &ChainPoint) -> u5c::query::ChainPoint {
        match point {
            ChainPoint::Origin => u5c::query::ChainPoint {
                slot: 0,
                hash: vec![].into(),
                height: 0,
                timestamp: 0,
            },
            ChainPoint::Slot(slot) | ChainPoint::Specific(slot, _) => {
                // Calculate height by looking up block from slot
                let height = self.domain
                    .archive()
                    .get_block_by_slot(slot)
                    .map(|block| {
                        block.map(|body| {
                            // Parse the block to get the height
                            use pallas::ledger::traverse::MultiEraBlock;
                            if let Ok(parsed_block) = MultiEraBlock::decode(&body) {
                                parsed_block.number()
                            } else {
                                *slot  // Fallback to slot
                            }
                        }).unwrap_or(*slot)
                    })
                    .unwrap_or(*slot);

                // Calculate timestamp from slot using proper era handling
                // Get protocol parameter updates up to this slot
                let updates = self.domain.state()
                    .get_pparams(*slot)
                    .ok()
                    .and_then(|updates| {
                        updates.into_iter()
                            .map(TryInto::try_into)
                            .collect::<Result<Vec<_>, _>>()
                            .ok()
                    })
                    .unwrap_or_default();

                // Get chain summary with proper era handling
                let summary = pparams::fold_with_hacks(self.domain.genesis(), &updates, *slot);
                
                // Calculate timestamp using the canonical function
                let timestamp = dolos_cardano::slot_time(*slot, &summary) as u64;

                u5c::query::ChainPoint {
                    slot: *slot,
                    hash: point.hash().map(|h| h.to_vec()).unwrap_or_default().into(),
                    height,
                    timestamp,
                }
            }
        }
    }
}

fn into_status(err: impl std::error::Error) -> Status {
    Status::internal(err.to_string())
}

trait IntoSet {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status>;
}

fn intersect<S: StateStore>(
    ledger: &S,
    a: impl IntoSet,
    b: impl IntoSet,
) -> Result<HashSet<TxoRef>, Status> {
    let a = a.into_set(ledger)?;
    let b = b.into_set(ledger)?;

    Ok(a.intersection(&b).cloned().collect())
}

struct ByAddressQuery(bytes::Bytes);

impl ByAddressQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByAddressQuery {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        ledger.get_utxo_by_address(&self.0).map_err(into_status)
    }
}

struct ByPaymentQuery(bytes::Bytes);

impl ByPaymentQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByPaymentQuery {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        ledger.get_utxo_by_payment(&self.0).map_err(into_status)
    }
}

struct ByDelegationQuery(bytes::Bytes);

impl ByDelegationQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByDelegationQuery {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        ledger.get_utxo_by_stake(&self.0).map_err(into_status)
    }
}

impl IntoSet for u5c::cardano::AddressPattern {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        let exact = ByAddressQuery::maybe_from(self.exact_address);
        let payment = ByPaymentQuery::maybe_from(self.payment_part);
        let delegation = ByDelegationQuery::maybe_from(self.delegation_part);

        match (exact, payment, delegation) {
            (Some(x), None, None) => x.into_set(ledger),
            (None, Some(x), None) => x.into_set(ledger),
            (None, None, Some(x)) => x.into_set(ledger),
            (None, Some(a), Some(b)) => intersect(ledger, a, b),
            (None, None, None) => Ok(HashSet::default()),
            _ => Err(Status::invalid_argument("conflicting address criteria")),
        }
    }
}

struct ByPolicyQuery(bytes::Bytes);

impl ByPolicyQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByPolicyQuery {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        ledger.get_utxo_by_policy(&self.0).map_err(into_status)
    }
}

struct ByAssetQuery(bytes::Bytes);

impl ByAssetQuery {
    fn maybe_from(data: bytes::Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        Some(Self(data))
    }
}

impl IntoSet for ByAssetQuery {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        ledger.get_utxo_by_asset(&self.0).map_err(into_status)
    }
}

impl IntoSet for u5c::cardano::AssetPattern {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        let by_policy = ByPolicyQuery::maybe_from(self.policy_id);
        let by_asset = ByAssetQuery::maybe_from(self.asset_name);

        match (by_policy, by_asset) {
            (Some(x), None) => x.into_set(ledger),
            (None, Some(x)) => x.into_set(ledger),
            (None, None) => Ok(HashSet::default()),
            _ => Err(Status::invalid_argument("conflicting asset criteria")),
        }
    }
}

impl IntoSet for u5c::cardano::TxOutputPattern {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        match (self.address, self.asset) {
            (None, Some(x)) => x.into_set(ledger),
            (Some(x), None) => x.into_set(ledger),
            (Some(a), Some(b)) => intersect(ledger, a, b),
            (None, None) => Ok(HashSet::default()),
        }
    }
}

impl IntoSet for u5c::query::AnyUtxoPattern {
    fn into_set<S: StateStore>(self, ledger: &S) -> Result<HashSet<TxoRef>, Status> {
        match self.utxo_pattern {
            Some(UtxoPattern::Cardano(x)) => x.into_set(ledger),
            _ => Ok(HashSet::new()),
        }
    }
}

fn from_u5c_txoref(txo: u5c::query::TxoRef) -> Result<TxoRef, Status> {
    let hash = super::convert::bytes_to_hash32(&txo.hash)?;
    Ok(TxoRef(hash, txo.index))
}

fn into_u5c_utxo<S: Domain + LedgerContext>(
    txo: &TxoRef,
    body: &EraCbor,
    mapper: &interop::Mapper<S>,
) -> Result<u5c::query::AnyUtxoData, pallas::codec::minicbor::decode::Error> {
    let parsed = MultiEraOutput::try_from(body)?;
    let parsed = mapper.map_tx_output(&parsed, None);

    Ok(u5c::query::AnyUtxoData {
        txo_ref: Some(u5c::query::TxoRef {
            hash: txo.0.to_vec().into(),
            index: txo.1,
        }),
        native_bytes: body.1.clone().into(),
        parsed_state: Some(u5c::query::any_utxo_data::ParsedState::Cardano(parsed)),
    })
}

#[async_trait::async_trait]
impl<D> u5c::query::query_service_server::QueryService for QueryServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    async fn read_params(
        &self,
        request: Request<u5c::query::ReadParamsRequest>,
    ) -> Result<Response<u5c::query::ReadParamsResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let tip = self
            .domain
            .state()
            .read_cursor()
            .map_err(into_status)?
            .ok_or(Status::internal("Failed to find ledger tip"))?;

        let pparams = dolos_cardano::load_effective_pparams::<D>(self.domain.state())
            .map_err(|_| Status::internal("Failed to load current pparams"))?;

        let pparams = dolos_cardano::utils::pparams_to_pallas(&pparams);

        let mut response = u5c::query::ReadParamsResponse {
            values: Some(u5c::query::AnyChainParams {
                params: u5c::query::any_chain_params::Params::Cardano(
                    self.mapper.map_pparams(pparams),
                )
                .into(),
            }),
            ledger_tip: tip.as_ref().map(|p| self.point_to_u5c(p)),
        };

        if let Some(mask) = message.field_mask {
            response = apply_mask(response, mask.paths)
                .map_err(|_| Status::internal("Failed to apply field mask"))?
        }

        Ok(Response::new(response))
    }

    async fn read_data(
        &self,
        request: Request<u5c::query::ReadDataRequest>,
    ) -> Result<Response<u5c::query::ReadDataResponse>, Status> {
        let _message = request.into_inner();

        info!("received new grpc query");

        todo!()
    }

    async fn read_utxos(
        &self,
        request: Request<u5c::query::ReadUtxosRequest>,
    ) -> Result<Response<u5c::query::ReadUtxosResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let keys: Vec<_> = message
            .keys
            .into_iter()
            .map(from_u5c_txoref)
            .try_collect()?;

        let utxos = StateStore::get_utxos(self.domain.state(), keys)
            .map_err(|e| Status::internal(e.to_string()))?;

        let items: Vec<_> = utxos
            .iter()
            .map(|(k, v)| into_u5c_utxo(k, v, &self.mapper))
            .try_collect()
            .map_err(|e| Status::internal(e.to_string()))?;

        let cursor = self
            .domain
            .state()
            .read_cursor()
            .map_err(|e| Status::internal(e.to_string()))?
            .as_ref()
            .map(|p| self.point_to_u5c(p));

        Ok(Response::new(u5c::query::ReadUtxosResponse {
            items,
            ledger_tip: cursor,
        }))
    }

    async fn search_utxos(
        &self,
        request: Request<u5c::query::SearchUtxosRequest>,
    ) -> Result<Response<u5c::query::SearchUtxosResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let set = match message.predicate {
            Some(x) => match x.r#match {
                Some(x) => x.into_set(self.domain.state())?,
                _ => {
                    return Err(Status::invalid_argument(
                        "only 'match' predicate is supported by Dolos",
                    ));
                }
            },
            _ => {
                return Err(Status::invalid_argument(
                    "criteria too broad, narrow it down",
                ));
            }
        };

        let utxos = StateStore::get_utxos(self.domain.state(), set.into_iter().collect_vec())
            .map_err(|e| Status::internal(e.to_string()))?;

        let items: Vec<_> = utxos
            .iter()
            .map(|(k, v)| into_u5c_utxo(k, v, &self.mapper))
            .try_collect()
            .map_err(|e| Status::internal(e.to_string()))?;

        let cursor = self
            .domain
            .state()
            .read_cursor()
            .map_err(|e| Status::internal(e.to_string()))?
            .as_ref()
            .map(|p| self.point_to_u5c(p));

        Ok(Response::new(u5c::query::SearchUtxosResponse {
            items,
            ledger_tip: cursor,
            next_token: String::default(),
        }))
    }

    async fn read_tx(
        &self,
        request: Request<u5c::query::ReadTxRequest>,
    ) -> Result<Response<u5c::query::ReadTxResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query");

        let tx_hash = message.hash;

        let (block_bytes, tx_index) =
            ArchiveStore::get_block_with_tx(self.domain.archive(), &tx_hash)
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("tx hash not found"))?;

        let block = MultiEraBlock::decode(&block_bytes)
            .map_err(|e| Status::internal(format!("failed to decode block: {e}")))?;

        let tx = block
            .txs()
            .get(tx_index)
            .cloned()
            .ok_or_else(|| Status::not_found("tx hash not found"))?;

        let native_bytes = tx.encode().into();

        let cursor = self
            .domain
            .state()
            .read_cursor()
            .map_err(|e| Status::internal(e.to_string()))?
            .as_ref()
            .map(|p| self.point_to_u5c(p));

        let mut response = u5c::query::ReadTxResponse {
            tx: Some(u5c::query::AnyChainTx {
                native_bytes,
                block_ref: Some(u5c::query::ChainPoint {
                    slot: block.slot(),
                    hash: block.hash().to_vec().into(),
                    height: block.header().number(),
                    timestamp: self.domain.get_slot_timestamp(block.slot()).unwrap_or(0),
                }),
                chain: Some(u5c::query::any_chain_tx::Chain::Cardano(
                    self.mapper.map_tx(&tx),
                )),
            }),
            ledger_tip: cursor,
        };

        if let Some(mask) = message.field_mask {
            response = apply_mask(response, mask.paths)
                .map_err(|e| Status::internal(format!("failed to apply field mask: {e}")))?;
        }

        Ok(Response::new(response))
    }

    async fn read_genesis(
        &self,
        _request: Request<u5c::query::ReadGenesisRequest>,
    ) -> Result<Response<u5c::query::ReadGenesisResponse>, Status> {
        info!("received read_genesis grpc query");

        let genesis = self.domain.genesis();
        let tip = self.domain.state().cursor().map_err(into_status)?;

        // Get current protocol parameters if available
        let current_params = if let Some(tip_point) = tip.as_ref() {
            let updates = self
                .domain
                .state()
                .get_pparams(tip_point.slot())
                .map_err(into_status)?;

            let updates: Vec<_> = updates
                .into_iter()
                .map(TryInto::try_into)
                .try_collect::<_, _, pallas::codec::minicbor::decode::Error>()
                .map_err(|e| Status::internal(e.to_string()))?;

            let summary = pparams::fold_with_hacks(genesis, &updates, tip_point.slot());
            let era = summary.era_for_slot(tip_point.slot());
            Some(era.pparams.clone())
        } else {
            None
        };

        // Map genesis
        let unified_genesis = self.mapper.map_genesis(
            &genesis.byron,
            &genesis.shelley,
            &genesis.alonzo,
            &genesis.conway,
            current_params,
        );

        // Get genesis hash
        let genesis_hash = if let Ok(Some(point)) = self.domain.state().cursor() {
            match point {
                ChainPoint::Origin | ChainPoint::Slot(_) => vec![],
                ChainPoint::Specific(_, hash) => hash.to_vec(),
            }
        } else {
            vec![]
        };

        let response = u5c::query::ReadGenesisResponse {
            genesis: genesis_hash.into(),
            caip2: get_caip2_identifier(unified_genesis.network_magic),
            config: Some(u5c::query::read_genesis_response::Config::Cardano(unified_genesis)),
        };

        Ok(Response::new(response))
    }

    async fn read_era_summary(
        &self,
        _request: Request<u5c::query::ReadEraSummaryRequest>,
    ) -> Result<Response<u5c::query::ReadEraSummaryResponse>, Status> {
        info!("received read_era_summary grpc query");

        let genesis = self.domain.genesis();
        let tip = self.domain.state().cursor().map_err(into_status)?;

        // Get current protocol parameters if available
        let current_params = if let Some(tip_point) = tip.as_ref() {
            let updates = self
                .domain
                .state()
                .get_pparams(tip_point.slot())
                .map_err(into_status)?;

            let updates: Vec<_> = updates
                .into_iter()
                .map(TryInto::try_into)
                .try_collect::<_, _, pallas::codec::minicbor::decode::Error>()
                .map_err(|e| Status::internal(e.to_string()))?;

            let summary = pparams::fold_with_hacks(genesis, &updates, tip_point.slot());
            let era = summary.era_for_slot(tip_point.slot());
            Some(era.pparams.clone())
        } else {
            None
        };

        // Map era summaries using pallas
        let era_summaries = self.mapper.map_era_summaries(current_params);

        let response = u5c::query::ReadEraSummaryResponse {
            summary: Some(u5c::query::read_era_summary_response::Summary::Cardano(era_summaries)),
        };

        Ok(Response::new(response))
    }
}
