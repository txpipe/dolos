use dolos_cardano::{
    load_effective_pparams, load_epoch, load_era_summary, AccountState, EraProtocol,
    EraSummary as DolosEraSummary, FixedNamespace as _, PoolState,
};
use dolos_core::{IndexStore, StateStore};
use pallas::codec::minicbor::{self, Decode, Encode, Encoder};
use pallas::codec::utils::{AnyCbor, AnyUInt, Bytes, KeyValuePairs, Nullable, TagWrap};
use pallas::ledger::traverse::{MultiEraOutput, OriginalHash};
use pallas::network::miniprotocols::localstate::queries_v16 as q16;
use pallas::network::miniprotocols::localtxsubmission::SMaybe;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tracing::debug;

use crate::prelude::*;

#[derive(Debug, Encode, Decode, PartialEq, Clone)]
pub struct LocalPState {
    #[n(0)]
    stake_pool_params: BTreeMap<Bytes, q16::PoolParams>,
    #[n(1)]
    future_stake_pool_params: BTreeMap<Bytes, q16::PoolParams>,
    #[n(2)]
    retiring: BTreeMap<Bytes, u32>,
    #[n(3)]
    deposits: BTreeMap<Bytes, q16::Coin>,
}

pub struct EraHistoryResponse<'a> {
    pub eras: &'a [DolosEraSummary],
    pub system_start: u64,
    pub security_param: u64,
}

impl<'a, C> minicbor::Encode<C> for EraHistoryResponse<'a> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        encoder: &mut Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        encoder.array(self.eras.len() as u64)?;

        const PICOSECONDS_PER_SECOND: u128 = 1_000_000_000_000;

        for era in self.eras {
            encoder.array(3)?;

            // Start Bound
            encoder.array(3)?;
            let start_relative_time = era.start.timestamp.saturating_sub(self.system_start) as u128;
            let start_relative_picos = start_relative_time
                .saturating_mul(PICOSECONDS_PER_SECOND)
                .min(u64::MAX as u128) as u64;
            encoder.u64(start_relative_picos)?;
            encoder.u64(era.start.slot)?;
            encoder.u64(era.start.epoch)?;

            // EraEnd
            let era_is_open_ended = match &era.end {
                Some(end) => {
                    let end_relative_time = end.timestamp.saturating_sub(self.system_start) as u128;
                    let end_relative_picos =
                        end_relative_time.saturating_mul(PICOSECONDS_PER_SECOND);
                    // If the time would overflow u64, treat this era as open-ended
                    if end_relative_picos > u64::MAX as u128 {
                        // EraUnbounded: Null
                        encoder.null()?;
                        true
                    } else {
                        // EraEnd Bound: Bound
                        // Bound: [RelativeTime, Slot, Epoch]
                        // Note: we encode Bound directly, not wrapped in [1, Bound]
                        encoder.array(3)?;
                        encoder.u64(end_relative_picos as u64)?;
                        encoder.u64(end.slot)?;
                        encoder.u64(end.epoch)?;
                        false
                    }
                }
                None => {
                    // EraUnbounded: Null
                    encoder.null()?;
                    true
                }
            };

            let safe_from_tip = self.security_param * 2;
            let genesis_window = self.security_param * 2;

            // EraParams: [EpochSize, SlotLength, SafeZone, GenesisWindow]
            encoder.array(4)?;
            encoder.u64(era.epoch_length)?;
            let slot_length_picos = (era.slot_length as u128)
                .saturating_mul(PICOSECONDS_PER_SECOND)
                .min(u64::MAX as u128) as u64;
            encoder.u64(slot_length_picos)?;

            // SafeZone
            if era_is_open_ended {
                // UnsafeIndefiniteSafeZone: [1]
                encoder.array(1)?;
                encoder.u8(1)?;
            } else {
                // StandardSafeZone: [0, Word64, SafeBeforeEpoch]
                // SafeBeforeEpoch: [0] (Legacy/Backwards compatibility)
                encoder.array(3)?;
                encoder.u8(0)?;
                encoder.u64(safe_from_tip)?;

                // SafeBeforeEpoch: [0]
                encoder.array(1)?;
                encoder.u8(0)?;
            }

            // GenesisWindow
            encoder.u64(genesis_window)?;
        }
        Ok(())
    }
}

pub fn build_era_history_response(
    eras: &[DolosEraSummary],
    genesis: &Genesis,
) -> Result<AnyCbor, Error> {
    if eras.is_empty() {
        return Err(Error::server("era summary is empty"));
    }

    let system_start = genesis
        .shelley
        .system_start
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp() as u64)
        .ok_or_else(|| Error::server("invalid system start"))?;

    let security_param = genesis
        .shelley
        .security_param
        .ok_or_else(|| Error::server("missing security param"))?;

    let resp = EraHistoryResponse {
        eras,
        system_start,
        security_param: security_param.into(),
    };

    Ok(AnyCbor::from_encode(resp))
}

fn convert_output_to_q16(output: &MultiEraOutput) -> Result<q16::TransactionOutput, Error> {
    use pallas::codec::utils::NonEmptyKeyValuePairs;
    use pallas::ledger::primitives::conway::DatumOption;

    let address = output.address().map_err(Error::server)?.to_vec();

    let value_data = output.value();
    let lovelace = AnyUInt::U64(value_data.coin());

    let assets = value_data.assets();
    let has_assets = !assets.is_empty();

    let value = if has_assets {
        let mut policy_map: Vec<(
            pallas::crypto::hash::Hash<28>,
            NonEmptyKeyValuePairs<pallas::codec::utils::Bytes, AnyUInt>,
        )> = vec![];

        for policy_assets in assets {
            let policy_id = *policy_assets.policy();
            let mut asset_entries: Vec<(pallas::codec::utils::Bytes, AnyUInt)> = vec![];

            for asset in policy_assets.assets() {
                let name = asset.name();
                let amount = asset.output_coin().unwrap_or(0);
                asset_entries.push((name.to_vec().into(), AnyUInt::U64(amount)));
            }

            if !asset_entries.is_empty() {
                policy_map.push((policy_id, NonEmptyKeyValuePairs::Def(asset_entries)));
            }
        }

        if policy_map.is_empty() {
            q16::Value::Coin(lovelace)
        } else {
            q16::Value::Multiasset(lovelace, NonEmptyKeyValuePairs::Def(policy_map))
        }
    } else {
        q16::Value::Coin(lovelace)
    };

    let inline_datum = output.datum().map(|d| match d {
        DatumOption::Hash(h) => q16::DatumOption::Hash(h),
        DatumOption::Data(data) => {
            q16::DatumOption::Data(pallas::codec::utils::CborWrap(convert_plutus_data(&data.0)))
        }
    });

    let datum_hash = output.datum().map(|d| match d {
        DatumOption::Hash(h) => h,
        DatumOption::Data(data) => data.original_hash(),
    });

    if output.era() >= pallas::ledger::traverse::Era::Alonzo {
        Ok(q16::TransactionOutput::Current(
            q16::PostAlonsoTransactionOutput {
                address: address.into(),
                amount: value,
                inline_datum,
                script_ref: None,
            },
        ))
    } else {
        Ok(q16::TransactionOutput::Legacy(
            q16::LegacyTransactionOutput {
                address: address.into(),
                amount: value,
                datum_hash,
            },
        ))
    }
}

fn convert_plutus_data(data: &pallas::ledger::primitives::PlutusData) -> q16::PlutusData {
    match data {
        pallas::ledger::primitives::PlutusData::Constr(constr) => {
            let fields = constr
                .fields
                .iter()
                .map(convert_plutus_data)
                .collect::<Vec<_>>();
            q16::PlutusData::Constr(q16::Constr {
                tag: constr.tag,
                any_constructor: constr.any_constructor,
                fields: pallas::codec::utils::MaybeIndefArray::Indef(fields),
            })
        }
        pallas::ledger::primitives::PlutusData::Map(kvs) => {
            let mapped = kvs
                .iter()
                .map(|(k, v)| (convert_plutus_data(k), convert_plutus_data(v)))
                .collect::<Vec<_>>();
            q16::PlutusData::Map(KeyValuePairs::Def(mapped))
        }
        pallas::ledger::primitives::PlutusData::BigInt(bi) => match bi {
            pallas::ledger::primitives::BigInt::Int(i) => {
                q16::PlutusData::BigInt(q16::BigInt::Int(*i))
            }
            pallas::ledger::primitives::BigInt::BigUInt(bytes) => {
                let raw: Vec<u8> = bytes.clone().into();
                q16::PlutusData::BigInt(q16::BigInt::BigUInt(raw.into()))
            }
            pallas::ledger::primitives::BigInt::BigNInt(bytes) => {
                let raw: Vec<u8> = bytes.clone().into();
                q16::PlutusData::BigInt(q16::BigInt::BigNInt(raw.into()))
            }
        },
        pallas::ledger::primitives::PlutusData::BoundedBytes(bytes) => {
            let raw: Vec<u8> = bytes.clone().into();
            q16::PlutusData::BoundedBytes(raw.into())
        }
        pallas::ledger::primitives::PlutusData::Array(arr) => {
            let items = arr.iter().map(convert_plutus_data).collect::<Vec<_>>();
            q16::PlutusData::Array(pallas::codec::utils::MaybeIndefArray::Indef(items))
        }
    }
}

pub fn build_utxo_by_address_response<D: Domain>(
    domain: &D,
    addrs: &q16::Addrs,
) -> Result<AnyCbor, Error> {
    use pallas::ledger::addresses::Address;

    let mut utxo_pairs: Vec<(q16::UTxO, q16::TransactionOutput)> = Vec::new();

    let mut all_refs = std::collections::HashSet::new();
    for addr in addrs.iter() {
        let addr_bytes: &[u8] = addr.as_ref();
        debug!(addr_len = addr_bytes.len(), addr_hex = %hex::encode(addr_bytes), "looking up utxos for address");

        let mut refs = domain
            .indexes()
            .utxos_by_tag("address", addr_bytes)
            .map_err(|e| Error::server(format!("failed to get utxos by address: {}", e)))?;

        debug!(num_refs = refs.len(), "found utxo refs by full address");

        if refs.is_empty() {
            if let Ok(Address::Shelley(shelley_addr)) = Address::from_bytes(addr_bytes) {
                let payment_bytes = shelley_addr.payment().to_vec();
                debug!(payment_hex = %hex::encode(&payment_bytes), "trying payment credential lookup");
                refs = domain
                    .indexes()
                    .utxos_by_tag("payment", &payment_bytes)
                    .map_err(|e| Error::server(format!("failed to get utxos by payment: {}", e)))?;
                debug!(
                    num_refs = refs.len(),
                    "found utxo refs by payment credential"
                );
            }
        }

        all_refs.extend(refs);
    }

    debug!(
        total_refs = all_refs.len(),
        "total unique utxo refs to fetch"
    );

    let refs_vec: Vec<_> = all_refs.into_iter().collect();
    let utxos = domain
        .state()
        .get_utxos(refs_vec.clone())
        .map_err(|e| Error::server(format!("failed to get utxos: {}", e)))?;

    debug!(fetched_utxos = utxos.len(), "fetched utxo data");

    for utxo_ref in refs_vec {
        if let Some(era_cbor) = utxos.get(&utxo_ref) {
            let output = MultiEraOutput::try_from(era_cbor.as_ref())
                .map_err(|e| Error::server(format!("failed to decode utxo: {}", e)))?;
            let q16_utxo = q16::UTxO {
                transaction_id: utxo_ref.0,
                index: AnyUInt::U32(utxo_ref.1),
            };

            let q16_output = convert_output_to_q16(&output)?;
            utxo_pairs.push((q16_utxo, q16_output));
        }
    }

    debug!(num_utxos = utxo_pairs.len(), "returning utxos");

    let response: KeyValuePairs<q16::UTxO, q16::TransactionOutput> = KeyValuePairs::Def(utxo_pairs);

    Ok(AnyCbor::from_encode((response,)))
}

pub fn build_protocol_params<D: Domain>(domain: &D) -> Result<q16::ProtocolParam, Error> {
    let pparams = load_effective_pparams::<D>(domain.state())
        .map_err(|e| Error::server(format!("failed to load protocol params: {}", e)))?;
    fn to_q16_rational(r: &pallas::ledger::primitives::RationalNumber) -> q16::RationalNumber {
        q16::RationalNumber {
            numerator: r.numerator,
            denominator: r.denominator,
        }
    }
    fn to_q16_ex_units(e: &pallas::ledger::primitives::ExUnits) -> q16::ExUnits {
        q16::ExUnits {
            mem: e.mem,
            steps: e.steps,
        }
    }
    fn to_q16_ex_unit_prices(e: &pallas::ledger::primitives::ExUnitPrices) -> q16::ExUnitPrices {
        q16::ExUnitPrices {
            mem_price: to_q16_rational(&e.mem_price),
            step_price: to_q16_rational(&e.step_price),
        }
    }
    fn to_q16_cost_models(c: &pallas::ledger::primitives::conway::CostModels) -> q16::CostModels {
        q16::CostModels {
            plutus_v1: c.plutus_v1.clone(),
            plutus_v2: c.plutus_v2.clone(),
            plutus_v3: c.plutus_v3.clone(),
            unknown: KeyValuePairs::from(c.unknown.clone().into_iter().collect::<Vec<_>>()),
        }
    }
    fn to_q16_pool_voting_thresholds(
        p: &pallas::ledger::primitives::conway::PoolVotingThresholds,
    ) -> q16::PoolVotingThresholds {
        q16::PoolVotingThresholds {
            motion_no_confidence: to_q16_rational(&p.motion_no_confidence),
            committee_normal: to_q16_rational(&p.committee_normal),
            committee_no_confidence: to_q16_rational(&p.committee_no_confidence),
            hard_fork_initiation: to_q16_rational(&p.hard_fork_initiation),
            pp_security_group: to_q16_rational(&p.security_voting_threshold),
        }
    }
    fn to_q16_drep_voting_thresholds(
        d: &pallas::ledger::primitives::conway::DRepVotingThresholds,
    ) -> q16::DRepVotingThresholds {
        q16::DRepVotingThresholds {
            motion_no_confidence: to_q16_rational(&d.motion_no_confidence),
            committee_normal: to_q16_rational(&d.committee_normal),
            committee_no_confidence: to_q16_rational(&d.committee_no_confidence),
            update_to_constitution: to_q16_rational(&d.update_constitution),
            hard_fork_initiation: to_q16_rational(&d.hard_fork_initiation),
            pp_network_group: to_q16_rational(&d.pp_network_group),
            pp_economic_group: to_q16_rational(&d.pp_economic_group),
            pp_technical_group: to_q16_rational(&d.pp_technical_group),
            pp_gov_group: to_q16_rational(&d.pp_governance_group),
            treasury_withdrawal: to_q16_rational(&d.treasury_withdrawal),
        }
    }
    Ok(q16::ProtocolParam {
        minfee_a: pparams.min_fee_a(),
        minfee_b: pparams.min_fee_b(),
        max_block_body_size: pparams.max_block_body_size(),
        max_transaction_size: pparams.max_transaction_size(),
        max_block_header_size: pparams.max_block_header_size(),
        key_deposit: pparams.key_deposit().map(AnyUInt::U64),
        pool_deposit: pparams.pool_deposit().map(AnyUInt::U64),
        maximum_epoch: pparams.maximum_epoch(),
        desired_number_of_stake_pools: pparams.desired_number_of_stake_pools().map(|n| n as u64),
        pool_pledge_influence: pparams.pool_pledge_influence().map(|r| to_q16_rational(&r)),
        expansion_rate: pparams.expansion_rate().map(|r| to_q16_rational(&r)),
        treasury_growth_rate: pparams.treasury_growth_rate().map(|r| to_q16_rational(&r)),
        protocol_version: pparams.protocol_version().map(|v| (v.0, v.1)),
        min_pool_cost: pparams.min_pool_cost().map(AnyUInt::U64),
        ada_per_utxo_byte: pparams.ada_per_utxo_byte().map(AnyUInt::U64),
        cost_models_for_script_languages: Some(to_q16_cost_models(
            &pparams.cost_models_for_script_languages(),
        )),
        execution_costs: pparams.execution_costs().map(|e| to_q16_ex_unit_prices(&e)),
        max_tx_ex_units: pparams.max_tx_ex_units().map(|e| to_q16_ex_units(&e)),
        max_block_ex_units: pparams.max_block_ex_units().map(|e| to_q16_ex_units(&e)),
        max_value_size: pparams.max_value_size().map(|n| n as u64),
        collateral_percentage: pparams.collateral_percentage().map(|n| n as u64),
        max_collateral_inputs: pparams.max_collateral_inputs().map(|n| n as u64),
        pool_voting_thresholds: pparams
            .pool_voting_thresholds()
            .map(|p| to_q16_pool_voting_thresholds(&p)),
        drep_voting_thresholds: pparams
            .drep_voting_thresholds()
            .map(|d| to_q16_drep_voting_thresholds(&d)),
        min_committee_size: pparams.min_committee_size(),
        committee_term_limit: pparams.committee_term_limit(),
        governance_action_validity_period: pparams.governance_action_validity_period(),
        governance_action_deposit: pparams.governance_action_deposit().map(AnyUInt::U64),
        drep_deposit: pparams.drep_deposit().map(AnyUInt::U64),
        drep_inactivity_period: pparams.drep_inactivity_period(),
        minfee_refscript_cost_per_byte: pparams
            .min_fee_ref_script_cost_per_byte()
            .map(|r| to_q16_rational(&r)),
    })
}
fn convert_pool_params(operator: &[u8], params: &dolos_cardano::PoolParams) -> q16::PoolParams {
    let relays: Vec<q16::Relay> = params
        .relays
        .iter()
        .map(|r| match r {
            pallas::ledger::primitives::Relay::SingleHostAddr(port, ipv4, ipv6) => {
                q16::Relay::SingleHostAddr((*port).into(), ipv4.clone().into(), ipv6.clone().into())
            }
            pallas::ledger::primitives::Relay::SingleHostName(port, dns) => {
                q16::Relay::SingleHostName((*port).into(), dns.clone())
            }
            pallas::ledger::primitives::Relay::MultiHostName(dns) => {
                q16::Relay::MultiHostName(dns.clone())
            }
        })
        .collect();

    let pool_metadata: Nullable<q16::PoolMetadata> = match &params.pool_metadata {
        Some(metadata) => Nullable::Some(q16::PoolMetadata {
            url: metadata.url.clone(),
            hash: metadata.hash.to_vec().into(),
        }),
        None => Nullable::Null,
    };

    q16::PoolParams {
        operator: operator.to_vec().into(),
        vrf_keyhash: params.vrf_keyhash.to_vec().into(),
        pledge: AnyUInt::U64(params.pledge),
        cost: AnyUInt::U64(params.cost),
        margin: q16::UnitInterval {
            numerator: params.margin.numerator,
            denominator: params.margin.denominator,
        },
        reward_account: params.reward_account.to_vec().into(),
        pool_owners: BTreeSet::from_iter(
            params
                .pool_owners
                .iter()
                .map(|h| Bytes::from(h.to_vec()))
                .collect::<Vec<_>>(),
        )
        .into(),
        relays,
        pool_metadata,
    }
}

pub fn build_stake_pools_response<D: Domain>(domain: &D) -> Result<AnyCbor, Error> {
    let state = domain.state();
    let pools_iter = state
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .map_err(|e| Error::server(format!("failed to iterate pools: {}", e)))?;

    let mut pool_ids: BTreeSet<Bytes> = BTreeSet::new();

    for record in pools_iter {
        let (_, pool) = record.map_err(|e| Error::server(format!("failed to read pool: {}", e)))?;

        let live_snapshot_opt = pool.snapshot.live();
        let live_snapshot = match live_snapshot_opt {
            Some(ls) => ls,
            None => continue,
        };

        if live_snapshot.is_retired {
            continue;
        }

        let pool_id: Bytes = pool.operator.to_vec().into();
        pool_ids.insert(pool_id);
    }

    debug!(num_pools = pool_ids.len(), "returning stake pools");

    let pools_response: q16::Pools = TagWrap(pool_ids);
    Ok(AnyCbor::from_encode((pools_response,)))
}

pub fn build_pool_state_response<D: Domain>(
    domain: &D,
    pools_filter: &SMaybe<q16::Pools>,
) -> Result<AnyCbor, Error> {
    let state = domain.state();
    let pools_iter = state
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .map_err(|e| Error::server(format!("failed to iterate pools: {}", e)))?;

    // Extract the filter set if provided
    let filter_set: Option<BTreeSet<Vec<u8>>> = match pools_filter {
        SMaybe::Some(pools) => {
            let set: BTreeSet<Vec<u8>> = pools.0.iter().map(|p| p.to_vec()).collect();
            Some(set)
        }
        SMaybe::None => None,
    };

    let mut stake_pool_params: BTreeMap<Bytes, q16::PoolParams> = BTreeMap::new();
    let mut future_stake_pool_params: BTreeMap<Bytes, q16::PoolParams> = BTreeMap::new();
    let mut retiring: BTreeMap<Bytes, u32> = BTreeMap::new();
    let mut deposits: BTreeMap<Bytes, q16::Coin> = BTreeMap::new();

    for record in pools_iter {
        let (_, pool) = record.map_err(|e| Error::server(format!("failed to read pool: {}", e)))?;

        let pool_id_bytes = pool.operator.to_vec();
        if let Some(ref filter) = filter_set {
            if !filter.contains(&pool_id_bytes) {
                continue;
            }
        }

        let pool_id: Bytes = pool_id_bytes.into();

        let live_snapshot_opt = pool.snapshot.live();
        let live_snapshot = match live_snapshot_opt {
            Some(ls) => ls,
            None => continue,
        };

        if live_snapshot.is_retired {
            continue;
        }

        stake_pool_params.insert(
            pool_id.clone(),
            convert_pool_params(pool_id.as_ref(), &live_snapshot.params),
        );

        if let Some(next_snapshot) = pool.snapshot.next() {
            future_stake_pool_params.insert(
                pool_id.clone(),
                convert_pool_params(pool_id.as_ref(), &next_snapshot.params),
            );
        }

        if let Some(retiring_epoch) = pool.retiring_epoch {
            retiring.insert(pool_id.clone(), retiring_epoch as u32);
        }

        deposits.insert(pool_id, AnyUInt::U64(pool.deposit));
    }

    debug!(
        num_pools = stake_pool_params.len(),
        num_future = future_stake_pool_params.len(),
        num_retiring = retiring.len(),
        "returning pool state"
    );

    let pstate = LocalPState {
        stake_pool_params,
        future_stake_pool_params,
        retiring,
        deposits,
    };

    let encoded = minicbor::to_vec(pstate)
        .map_err(|e| Error::server(format!("failed to encode pool state: {e}")))?;

    let wrapped = TagWrap::<Bytes, 24>(encoded.into());
    Ok(AnyCbor::from_encode(vec![wrapped]))
}

pub fn build_stake_snapshots_response<D: Domain>(
    domain: &D,
    pools_filter: &SMaybe<q16::Pools>,
) -> Result<AnyCbor, Error> {
    let state = domain.state();

    let chain_summary = load_era_summary::<D>(state)
        .map_err(|e| Error::server(format!("failed to load era summary: {e}")))?;

    let epoch_state = load_epoch::<D>(state)
        .map_err(|e| Error::server(format!("failed to load epoch: {}", e)))?;
    let current_epoch = epoch_state.number;

    let filter_set: Option<BTreeSet<Vec<u8>>> = match pools_filter {
        SMaybe::Some(pools) => {
            let set: BTreeSet<Vec<u8>> = pools.0.iter().map(|p| p.to_vec()).collect();
            Some(set)
        }
        SMaybe::None => None,
    };

    let mut stake_snapshots: Vec<(Bytes, q16::Stakes)> = Vec::new();

    let protocol_for_epoch = |epoch: u64| -> EraProtocol {
        let era = chain_summary.era_for_epoch(epoch);
        EraProtocol::from(era.protocol)
    };

    type StakeSnapshotArg = (BTreeSet<Vec<u8>>, HashMap<Vec<u8>, u64>, u64);

    let gather_for_epoch = |stake_epoch: u64,
                            protocol: EraProtocol,
                            filter_set: &Option<BTreeSet<Vec<u8>>>,
                            state: &D::State|
     -> Result<StakeSnapshotArg, Error> {
        let mut active_pools: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut pool_stakes: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut total_active: u64 = 0;

        let pools_iter = state
            .iter_entities_typed::<PoolState>(PoolState::NS, None)
            .map_err(|e| Error::server(format!("failed to iterate pools: {e}")))?;

        let mut all_active_pools: BTreeSet<Vec<u8>> = BTreeSet::new();
        for record in pools_iter {
            let (_, pool) =
                record.map_err(|e| Error::server(format!("failed to read pool: {e}")))?;
            let pool_id = pool.operator.to_vec();

            if let Some(snapshot) = pool.snapshot.snapshot_at(stake_epoch) {
                if !snapshot.is_retired {
                    all_active_pools.insert(pool_id.clone());
                    // Only add to filtered active_pools if it passes the filter
                    if filter_set.is_none() || filter_set.as_ref().unwrap().contains(&pool_id) {
                        active_pools.insert(pool_id);
                    }
                }
            }
        }

        let accounts_iter = state
            .iter_entities_typed::<AccountState>(AccountState::NS, None)
            .map_err(|e| Error::server(format!("failed to iterate accounts: {e}")))?;

        for record in accounts_iter {
            let (_, account) =
                record.map_err(|e| Error::server(format!("failed to read account: {e}")))?;

            let Some(pool_hash) = account.delegated_pool_at(stake_epoch) else {
                continue;
            };

            let pool_id = pool_hash.to_vec();

            if !all_active_pools.contains(&pool_id) {
                continue;
            }

            let stake_amount = account
                .stake
                .snapshot_at(stake_epoch)
                .map(|x| x.total_for_era(protocol))
                .unwrap_or_default();

            if stake_amount == 0 {
                continue;
            }

            total_active = total_active.saturating_add(stake_amount);

            if active_pools.contains(&pool_id) {
                pool_stakes
                    .entry(pool_id.clone())
                    .and_modify(|x| *x = x.saturating_add(stake_amount))
                    .or_insert(stake_amount);
            }
        }

        Ok((active_pools, pool_stakes, total_active))
    };

    let mark_epoch = current_epoch.saturating_sub(1);
    let set_epoch = current_epoch.saturating_sub(2);
    let go_epoch = current_epoch.saturating_sub(3);

    let (mark_active_pools, mark_stakes, mark_total_active) = gather_for_epoch(
        mark_epoch,
        protocol_for_epoch(mark_epoch),
        &filter_set,
        state,
    )?;
    let (set_active_pools, set_stakes, set_total_active) =
        gather_for_epoch(set_epoch, protocol_for_epoch(set_epoch), &filter_set, state)?;
    let (go_active_pools, go_stakes, go_total_active) =
        gather_for_epoch(go_epoch, protocol_for_epoch(go_epoch), &filter_set, state)?;

    let mut all_pools: BTreeSet<Vec<u8>> = mark_active_pools
        .union(&set_active_pools)
        .cloned()
        .collect();
    all_pools.extend(go_active_pools);

    let mark_total = mark_total_active;
    let set_total = set_total_active;
    let go_total = go_total_active;

    for pool_id_bytes in all_pools {
        let mark_stake = *mark_stakes.get(&pool_id_bytes).unwrap_or(&0);
        let set_stake = *set_stakes.get(&pool_id_bytes).unwrap_or(&0);
        let go_stake = *go_stakes.get(&pool_id_bytes).unwrap_or(&0);

        stake_snapshots.push((
            pool_id_bytes.clone().into(),
            q16::Stakes {
                snapshot_mark_pool: mark_stake,
                snapshot_set_pool: set_stake,
                snapshot_go_pool: go_stake,
            },
        ));
    }

    debug!(
        num_pools = stake_snapshots.len(),
        mark_total, set_total, go_total, "returning stake snapshots"
    );

    let response = q16::StakeSnapshots {
        stake_snapshots: KeyValuePairs::Def(stake_snapshots),
        snapshot_stake_mark_total: mark_total,
        snapshot_stake_set_total: set_total,
        snapshot_stake_go_total: go_total,
    };

    let encoded = minicbor::to_vec(response)
        .map_err(|e| Error::server(format!("failed to encode stake snapshots: {e}")))?;

    let wrapped = TagWrap::<Bytes, 24>(encoded.into());
    Ok(AnyCbor::from_encode(vec![wrapped]))
}
