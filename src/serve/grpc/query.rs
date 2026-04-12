use dolos_cardano::indexes::CardanoIndexExt;
use itertools::Itertools as _;
use pallas::interop::utxorpc::{self as interop, spec::query::any_utxo_pattern::UtxoPattern};
use pallas::interop::utxorpc::{spec as u5c, LedgerContext};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::collections::HashSet;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use super::masking::apply_mask;
use crate::prelude::*;
use dolos_cardano::indexes::AsyncCardanoQueryExt;

pub fn point_to_u5c<T: LedgerContext>(_ledger: &T, point: &ChainPoint) -> u5c::query::ChainPoint {
    u5c::query::ChainPoint {
        slot: point.slot(),
        hash: point.hash().map(|h| h.to_vec()).unwrap_or_default().into(),
        ..Default::default()
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
}

fn into_status(err: impl std::error::Error) -> Status {
    Status::internal(err.to_string())
}

trait IntoSet {
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status>;
}

fn intersect<S: CardanoIndexExt>(
    indexes: &S,
    a: impl IntoSet,
    b: impl IntoSet,
) -> Result<HashSet<TxoRef>, Status> {
    let a = a.into_set(indexes)?;
    let b = b.into_set(indexes)?;

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
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        indexes.utxos_by_address(&self.0).map_err(into_status)
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
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        indexes.utxos_by_payment(&self.0).map_err(into_status)
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
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        indexes.utxos_by_stake(&self.0).map_err(into_status)
    }
}

impl IntoSet for u5c::cardano::AddressPattern {
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        let exact = ByAddressQuery::maybe_from(self.exact_address);
        let payment = ByPaymentQuery::maybe_from(self.payment_part);
        let delegation = ByDelegationQuery::maybe_from(self.delegation_part);

        match (exact, payment, delegation) {
            (Some(x), None, None) => x.into_set(indexes),
            (None, Some(x), None) => x.into_set(indexes),
            (None, None, Some(x)) => x.into_set(indexes),
            (None, Some(a), Some(b)) => intersect(indexes, a, b),
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
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        indexes.utxos_by_policy(&self.0).map_err(into_status)
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
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        indexes.utxos_by_asset(&self.0).map_err(into_status)
    }
}

impl IntoSet for u5c::cardano::AssetPattern {
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        let by_policy = ByPolicyQuery::maybe_from(self.policy_id);
        let by_asset = ByAssetQuery::maybe_from(self.asset_name);

        match (by_policy, by_asset) {
            (Some(x), None) => x.into_set(indexes),
            (None, Some(x)) => x.into_set(indexes),
            (None, None) => Ok(HashSet::default()),
            _ => Err(Status::invalid_argument("conflicting asset criteria")),
        }
    }
}

impl IntoSet for u5c::cardano::TxOutputPattern {
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        match (self.address, self.asset) {
            (None, Some(x)) => x.into_set(indexes),
            (Some(x), None) => x.into_set(indexes),
            (Some(a), Some(b)) => intersect(indexes, a, b),
            (None, None) => Ok(HashSet::default()),
        }
    }
}

impl IntoSet for u5c::query::AnyUtxoPattern {
    fn into_set<S: CardanoIndexExt>(self, indexes: &S) -> Result<HashSet<TxoRef>, Status> {
        match self.utxo_pattern {
            Some(UtxoPattern::Cardano(x)) => x.into_set(indexes),
            _ => Ok(HashSet::new()),
        }
    }
}

fn from_u5c_txoref(txo: u5c::query::TxoRef) -> Result<TxoRef, Status> {
    let hash = super::convert::bytes_to_hash32(&txo.hash)?;
    Ok(TxoRef(hash, txo.index))
}

fn u64_to_bigint(value: u64) -> Option<u5c::cardano::BigInt> {
    if value <= i64::MAX as u64 {
        Some(u5c::cardano::BigInt {
            big_int: Some(u5c::cardano::big_int::BigInt::Int(value as i64)),
        })
    } else {
        Some(u5c::cardano::BigInt {
            big_int: Some(u5c::cardano::big_int::BigInt::BigUInt(
                value.to_be_bytes().to_vec().into(),
            )),
        })
    }
}

fn rational_to_u5c(numerator: u64, denominator: u64) -> u5c::cardano::RationalNumber {
    u5c::cardano::RationalNumber {
        numerator: numerator as i32,
        denominator: denominator as u32,
    }
}

fn float_to_u5c_rational(value: f32) -> u5c::cardano::RationalNumber {
    let value = dolos_cardano::utils::float_to_rational(value);
    rational_to_u5c(value.numerator, value.denominator)
}

fn map_execution_prices(
    value: &pallas::interop::hardano::configs::alonzo::ExecutionPrices,
) -> u5c::cardano::ExPrices {
    let value: pallas::ledger::primitives::alonzo::ExUnitPrices = value.clone().into();

    u5c::cardano::ExPrices {
        steps: Some(rational_to_u5c(
            value.step_price.numerator,
            value.step_price.denominator,
        )),
        memory: Some(rational_to_u5c(
            value.mem_price.numerator,
            value.mem_price.denominator,
        )),
    }
}

fn map_execution_units(
    value: &pallas::interop::hardano::configs::alonzo::ExUnits,
) -> u5c::cardano::ExUnits {
    u5c::cardano::ExUnits {
        steps: value.ex_units_steps,
        memory: value.ex_units_mem,
    }
}

fn map_cost_models(
    genesis: &Genesis,
) -> (
    Option<u5c::cardano::CostModels>,
    Option<u5c::cardano::CostModelMap>,
) {
    use pallas::interop::hardano::configs::alonzo::Language;

    let plutus_v1 = genesis
        .alonzo
        .cost_models
        .get(&Language::PlutusV1)
        .cloned()
        .map(Vec::<i64>::from)
        .map(|values| u5c::cardano::CostModel { values });

    let plutus_v2 = genesis
        .alonzo
        .cost_models
        .get(&Language::PlutusV2)
        .cloned()
        .map(Vec::<i64>::from)
        .map(|values| u5c::cardano::CostModel { values });

    let plutus_v3 =
        (!genesis.conway.plutus_v3_cost_model.is_empty()).then(|| u5c::cardano::CostModel {
            values: genesis.conway.plutus_v3_cost_model.clone(),
        });

    let cost_models = u5c::cardano::CostModels {
        plutus_v1: plutus_v1.clone(),
        plutus_v2: plutus_v2.clone(),
        plutus_v3: plutus_v3.clone(),
    };

    let cost_model_map = u5c::cardano::CostModelMap {
        plutus_v1,
        plutus_v2,
        plutus_v3,
    };

    (
        Some(cost_models)
            .filter(|x| x.plutus_v1.is_some() || x.plutus_v2.is_some() || x.plutus_v3.is_some()),
        Some(cost_model_map)
            .filter(|x| x.plutus_v1.is_some() || x.plutus_v2.is_some() || x.plutus_v3.is_some()),
    )
}

fn map_genesis_protocol_params(genesis: &Genesis) -> u5c::cardano::PParams {
    let shelley = &genesis.shelley.protocol_params;
    let (cost_models, _) = map_cost_models(genesis);

    u5c::cardano::PParams {
        max_tx_size: shelley.max_tx_size.into(),
        min_fee_coefficient: u64_to_bigint(shelley.min_fee_a.into()),
        min_fee_constant: u64_to_bigint(shelley.min_fee_b.into()),
        max_block_body_size: shelley.max_block_body_size.into(),
        max_block_header_size: shelley.max_block_header_size.into(),
        stake_key_deposit: u64_to_bigint(shelley.key_deposit),
        pool_deposit: u64_to_bigint(shelley.pool_deposit),
        pool_retirement_epoch_bound: shelley.e_max,
        desired_number_of_pools: shelley.n_opt.into(),
        pool_influence: Some(rational_to_u5c(
            shelley.a0.numerator,
            shelley.a0.denominator,
        )),
        monetary_expansion: Some(rational_to_u5c(
            shelley.rho.numerator,
            shelley.rho.denominator,
        )),
        treasury_expansion: Some(rational_to_u5c(
            shelley.tau.numerator,
            shelley.tau.denominator,
        )),
        min_pool_cost: u64_to_bigint(shelley.min_pool_cost),
        protocol_version: Some(u5c::cardano::ProtocolVersion {
            major: shelley.protocol_version.major as u32,
            minor: shelley.protocol_version.minor as u32,
        }),
        max_value_size: genesis.alonzo.max_value_size.into(),
        collateral_percentage: genesis.alonzo.collateral_percentage.into(),
        max_collateral_inputs: genesis.alonzo.max_collateral_inputs.into(),
        cost_models,
        prices: Some(map_execution_prices(&genesis.alonzo.execution_prices)),
        max_execution_units_per_transaction: Some(map_execution_units(
            &genesis.alonzo.max_tx_ex_units,
        )),
        max_execution_units_per_block: Some(map_execution_units(
            &genesis.alonzo.max_block_ex_units,
        )),
        min_fee_script_ref_cost_per_byte: Some(rational_to_u5c(
            genesis.conway.min_fee_ref_script_cost_per_byte,
            1,
        )),
        pool_voting_thresholds: Some(u5c::cardano::VotingThresholds {
            thresholds: vec![
                float_to_u5c_rational(genesis.conway.pool_voting_thresholds.motion_no_confidence),
                float_to_u5c_rational(genesis.conway.pool_voting_thresholds.committee_normal),
                float_to_u5c_rational(
                    genesis
                        .conway
                        .pool_voting_thresholds
                        .committee_no_confidence,
                ),
                float_to_u5c_rational(genesis.conway.pool_voting_thresholds.hard_fork_initiation),
                float_to_u5c_rational(genesis.conway.pool_voting_thresholds.pp_security_group),
            ],
        }),
        drep_voting_thresholds: Some(u5c::cardano::VotingThresholds {
            thresholds: vec![
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.motion_no_confidence),
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.committee_normal),
                float_to_u5c_rational(
                    genesis
                        .conway
                        .d_rep_voting_thresholds
                        .committee_no_confidence,
                ),
                float_to_u5c_rational(
                    genesis
                        .conway
                        .d_rep_voting_thresholds
                        .update_to_constitution,
                ),
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.hard_fork_initiation),
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.pp_network_group),
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.pp_economic_group),
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.pp_technical_group),
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.pp_gov_group),
                float_to_u5c_rational(genesis.conway.d_rep_voting_thresholds.treasury_withdrawal),
            ],
        }),
        min_committee_size: genesis.conway.committee_min_size as u32,
        committee_term_limit: genesis.conway.committee_max_term_length.into(),
        governance_action_validity_period: genesis.conway.gov_action_lifetime.into(),
        governance_action_deposit: u64_to_bigint(genesis.conway.gov_action_deposit),
        drep_deposit: u64_to_bigint(genesis.conway.d_rep_deposit),
        drep_inactivity_period: genesis.conway.d_rep_activity.into(),
        ..Default::default()
    }
}

fn caip2_from_genesis(genesis: &Genesis) -> Result<String, Status> {
    match genesis.shelley.network_magic {
        Some(764824073) => Ok("cardano:mainnet".into()),
        Some(1) => Ok("cardano:preprod".into()),
        Some(2) => Ok("cardano:preview".into()),
        Some(x) => Ok(format!("cardano:{x}")),
        None => Err(Status::internal("missing Cardano network magic")),
    }
}

fn map_cardano_genesis(genesis: &Genesis) -> Result<u5c::cardano::Genesis, Status> {
    let (_, cost_model_map) = map_cost_models(genesis);
    let constitution_anchor_hash = hex::decode(&genesis.conway.constitution.anchor.data_hash)
        .map_err(|e| Status::internal(format!("invalid constitution anchor hash: {e}")))?;
    let constitution_hash = genesis
        .conway
        .constitution
        .script
        .as_deref()
        .map(hex::decode)
        .transpose()
        .map_err(|e| Status::internal(format!("invalid constitution script hash: {e}")))?
        .unwrap_or_default();

    Ok(u5c::cardano::Genesis {
        avvm_distr: genesis.byron.avvm_distr.clone(),
        block_version_data: Some(u5c::cardano::BlockVersionData {
            script_version: genesis.byron.block_version_data.script_version.into(),
            slot_duration: genesis.byron.block_version_data.slot_duration.to_string(),
            max_block_size: genesis.byron.block_version_data.max_block_size.to_string(),
            max_header_size: genesis.byron.block_version_data.max_header_size.to_string(),
            max_tx_size: genesis.byron.block_version_data.max_tx_size.to_string(),
            max_proposal_size: genesis
                .byron
                .block_version_data
                .max_proposal_size
                .to_string(),
            mpc_thd: genesis.byron.block_version_data.mpc_thd.to_string(),
            heavy_del_thd: genesis.byron.block_version_data.heavy_del_thd.to_string(),
            update_vote_thd: genesis.byron.block_version_data.update_vote_thd.to_string(),
            update_proposal_thd: genesis
                .byron
                .block_version_data
                .update_proposal_thd
                .to_string(),
            update_implicit: genesis.byron.block_version_data.update_implicit.to_string(),
            softfork_rule: Some(u5c::cardano::SoftforkRule {
                init_thd: genesis
                    .byron
                    .block_version_data
                    .softfork_rule
                    .init_thd
                    .to_string(),
                min_thd: genesis
                    .byron
                    .block_version_data
                    .softfork_rule
                    .min_thd
                    .to_string(),
                thd_decrement: genesis
                    .byron
                    .block_version_data
                    .softfork_rule
                    .thd_decrement
                    .to_string(),
            }),
            tx_fee_policy: Some(u5c::cardano::TxFeePolicy {
                multiplier: genesis
                    .byron
                    .block_version_data
                    .tx_fee_policy
                    .multiplier
                    .to_string(),
                summand: genesis
                    .byron
                    .block_version_data
                    .tx_fee_policy
                    .summand
                    .to_string(),
            }),
            unlock_stake_epoch: genesis
                .byron
                .block_version_data
                .unlock_stake_epoch
                .to_string(),
        }),
        fts_seed: genesis.byron.fts_seed.clone().unwrap_or_default(),
        protocol_consts: Some(u5c::cardano::ProtocolConsts {
            k: genesis.byron.protocol_consts.k as u32,
            protocol_magic: genesis.byron.protocol_consts.protocol_magic,
            vss_max_ttl: genesis
                .byron
                .protocol_consts
                .vss_max_ttl
                .unwrap_or_default(),
            vss_min_ttl: genesis
                .byron
                .protocol_consts
                .vss_min_ttl
                .unwrap_or_default(),
        }),
        start_time: genesis.byron.start_time,
        boot_stakeholders: genesis
            .byron
            .boot_stakeholders
            .iter()
            .map(|(k, v)| (k.clone(), (*v).into()))
            .collect(),
        heavy_delegation: genesis
            .byron
            .heavy_delegation
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    u5c::cardano::HeavyDelegation {
                        cert: v.cert.clone(),
                        delegate_pk: v.delegate_pk.clone(),
                        issuer_pk: v.issuer_pk.clone(),
                        omega: 0,
                    },
                )
            })
            .collect(),
        non_avvm_balances: genesis.byron.non_avvm_balances.clone(),
        vss_certs: genesis
            .byron
            .vss_certs
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    u5c::cardano::VssCert {
                        expiry_epoch: v.expiry_epoch,
                        signature: v.signature,
                        signing_key: v.signing_key,
                        vss_key: v.vss_key,
                    },
                )
            })
            .collect(),
        active_slots_coeff: genesis
            .shelley
            .active_slots_coeff
            .map(float_to_u5c_rational),
        epoch_length: genesis.shelley.epoch_length.unwrap_or_default(),
        gen_delegs: genesis
            .shelley
            .gen_delegs
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    u5c::cardano::GenDelegs {
                        delegate: v.delegate.unwrap_or_default(),
                        vrf: v.vrf.unwrap_or_default(),
                    },
                )
            })
            .collect(),
        initial_funds: genesis
            .shelley
            .initial_funds
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    u64_to_bigint(v).expect("u64 genesis funds must map to bigint"),
                )
            })
            .collect(),
        max_kes_evolutions: genesis.shelley.max_kes_evolutions.unwrap_or_default(),
        max_lovelace_supply: genesis.shelley.max_lovelace_supply.and_then(u64_to_bigint),
        network_id: genesis.shelley.network_id.clone().unwrap_or_default(),
        network_magic: genesis.shelley.network_magic.unwrap_or_default(),
        protocol_params: Some(map_genesis_protocol_params(genesis)),
        security_param: genesis.shelley.security_param.unwrap_or_default(),
        slot_length: genesis.shelley.slot_length.unwrap_or_default(),
        slots_per_kes_period: genesis.shelley.slots_per_kes_period.unwrap_or_default(),
        system_start: genesis.shelley.system_start.clone().unwrap_or_default(),
        update_quorum: genesis.shelley.update_quorum.unwrap_or_default(),
        lovelace_per_utxo_word: u64_to_bigint(genesis.alonzo.lovelace_per_utxo_word),
        execution_prices: Some(map_execution_prices(&genesis.alonzo.execution_prices)),
        max_tx_ex_units: Some(map_execution_units(&genesis.alonzo.max_tx_ex_units)),
        max_block_ex_units: Some(map_execution_units(&genesis.alonzo.max_block_ex_units)),
        max_value_size: genesis.alonzo.max_value_size,
        collateral_percentage: genesis.alonzo.collateral_percentage,
        max_collateral_inputs: genesis.alonzo.max_collateral_inputs,
        cost_models: cost_model_map,
        committee: Some(u5c::cardano::Committee {
            members: genesis.conway.committee.members.clone(),
            threshold: Some(rational_to_u5c(
                genesis.conway.committee.threshold.numerator,
                genesis.conway.committee.threshold.denominator,
            )),
        }),
        constitution: Some(u5c::cardano::Constitution {
            anchor: Some(u5c::cardano::Anchor {
                url: genesis.conway.constitution.anchor.url.clone(),
                content_hash: constitution_anchor_hash.into(),
            }),
            hash: constitution_hash.into(),
        }),
        committee_min_size: genesis.conway.committee_min_size,
        committee_max_term_length: genesis.conway.committee_max_term_length.into(),
        gov_action_lifetime: genesis.conway.gov_action_lifetime.into(),
        gov_action_deposit: u64_to_bigint(genesis.conway.gov_action_deposit),
        drep_deposit: u64_to_bigint(genesis.conway.d_rep_deposit),
        drep_activity: genesis.conway.d_rep_activity.into(),
        min_fee_ref_script_cost_per_byte: Some(rational_to_u5c(
            genesis.conway.min_fee_ref_script_cost_per_byte,
            1,
        )),
        drep_voting_thresholds: Some(u5c::cardano::DRepVotingThresholds {
            motion_no_confidence: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.motion_no_confidence,
            )),
            committee_normal: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.committee_normal,
            )),
            committee_no_confidence: Some(float_to_u5c_rational(
                genesis
                    .conway
                    .d_rep_voting_thresholds
                    .committee_no_confidence,
            )),
            update_to_constitution: Some(float_to_u5c_rational(
                genesis
                    .conway
                    .d_rep_voting_thresholds
                    .update_to_constitution,
            )),
            hard_fork_initiation: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.hard_fork_initiation,
            )),
            pp_network_group: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.pp_network_group,
            )),
            pp_economic_group: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.pp_economic_group,
            )),
            pp_technical_group: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.pp_technical_group,
            )),
            pp_gov_group: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.pp_gov_group,
            )),
            treasury_withdrawal: Some(float_to_u5c_rational(
                genesis.conway.d_rep_voting_thresholds.treasury_withdrawal,
            )),
        }),
        pool_voting_thresholds: Some(u5c::cardano::PoolVotingThresholds {
            motion_no_confidence: Some(float_to_u5c_rational(
                genesis.conway.pool_voting_thresholds.motion_no_confidence,
            )),
            committee_normal: Some(float_to_u5c_rational(
                genesis.conway.pool_voting_thresholds.committee_normal,
            )),
            committee_no_confidence: Some(float_to_u5c_rational(
                genesis
                    .conway
                    .pool_voting_thresholds
                    .committee_no_confidence,
            )),
            hard_fork_initiation: Some(float_to_u5c_rational(
                genesis.conway.pool_voting_thresholds.hard_fork_initiation,
            )),
            pp_security_group: Some(float_to_u5c_rational(
                genesis.conway.pool_voting_thresholds.pp_security_group,
            )),
        }),
    })
}

fn map_era_boundary(boundary: &dolos_cardano::EraBoundary) -> u5c::cardano::EraBoundary {
    u5c::cardano::EraBoundary {
        time: boundary.timestamp.saturating_mul(1000),
        slot: boundary.slot,
        epoch: boundary.epoch,
    }
}

fn protocol_to_era_name(protocol: u16) -> &'static str {
    match protocol {
        0..=1 => "byron",
        2 => "shelley",
        3 => "allegra",
        4 => "mary",
        5..=6 => "alonzo",
        7..=8 => "babbage",
        9..=10 => "conway",
        _ => "unknown",
    }
}

fn map_era_summary(
    era: &dolos_cardano::EraSummary,
    active_protocol: u16,
    active_params: &u5c::cardano::PParams,
) -> u5c::cardano::EraSummary {
    u5c::cardano::EraSummary {
        name: protocol_to_era_name(era.protocol).into(),
        start: Some(map_era_boundary(&era.start)),
        end: era.end.as_ref().map(map_era_boundary),
        protocol_params: (era.protocol == active_protocol).then(|| active_params.clone()),
    }
}

async fn into_u5c_utxo<S: Domain + LedgerContext>(
    txo: &TxoRef,
    body: &EraCbor,
    mapper: &interop::Mapper<S>,
    domain: &S,
) -> Result<u5c::query::AnyUtxoData, Box<dyn std::error::Error>> {
    use pallas::ledger::primitives::conway::DatumOption;

    let query = dolos_core::AsyncQueryFacade::new(domain.clone());

    let parsed_output = MultiEraOutput::try_from(body)?;
    let mut parsed = mapper.map_tx_output(&parsed_output, None);

    // If the output has a datum hash, try to fetch the datum value from storage
    if let Some(DatumOption::Hash(datum_hash)) = parsed_output.datum() {
        match query.get_datum(&datum_hash).await {
            Ok(Some(datum_bytes)) => {
                // Decode the datum and update the parsed output
                match pallas::codec::minicbor::decode::<
                    pallas::ledger::primitives::conway::PlutusData,
                >(&datum_bytes)
                {
                    Ok(plutus_data) => {
                        // Update the datum field with both hash and payload
                        parsed.datum = Some(u5c::cardano::Datum {
                            hash: datum_hash.to_vec().into(),
                            payload: Some(mapper.map_plutus_datum(&plutus_data)),
                            original_cbor: datum_bytes.into(),
                        });
                    }
                    Err(e) => {
                        warn!(
                            datum_hash = hex::encode(datum_hash),
                            error = %e,
                            "Failed to decode datum value from storage"
                        );
                    }
                }
            }
            Ok(None) => {
                warn!(
                    datum_hash = hex::encode(datum_hash),
                    txo_ref = format!("{}#{}", hex::encode(txo.0), txo.1),
                    "Datum value not found in storage for UTXO with datum hash"
                );
            }
            Err(e) => {
                warn!(
                    datum_hash = hex::encode(datum_hash),
                    error = %e,
                    "Error querying datum storage"
                );
            }
        }
    }

    Ok(u5c::query::AnyUtxoData {
        txo_ref: Some(u5c::query::TxoRef {
            hash: txo.0.to_vec().into(),
            index: txo.1,
        }),
        native_bytes: body.1.clone().into(),
        parsed_state: Some(u5c::query::any_utxo_data::ParsedState::Cardano(parsed)),
        block_ref: None,
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

        info!("received new grpc query - read_params");

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
            ledger_tip: Some(point_to_u5c(&self.domain, &tip)),
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

        info!("received new grpc query - read_data");

        todo!()
    }

    async fn read_utxos(
        &self,
        request: Request<u5c::query::ReadUtxosRequest>,
    ) -> Result<Response<u5c::query::ReadUtxosResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query - read_utxos");

        let keys: Vec<_> = message
            .keys
            .into_iter()
            .map(from_u5c_txoref)
            .try_collect()?;

        let utxos = StateStore::get_utxos(self.domain.state(), keys)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut items = Vec::new();
        for (k, v) in utxos.iter() {
            items.push(
                into_u5c_utxo(k, v, &self.mapper, &self.domain)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?,
            );
        }

        let cursor = self
            .domain
            .state()
            .read_cursor()
            .map_err(|e| Status::internal(e.to_string()))?
            .as_ref()
            .map(|p| point_to_u5c(&self.domain, p));

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

        info!("received new grpc query - search_utxos");

        let set = match message.predicate {
            Some(x) => match x.r#match {
                Some(x) => x.into_set(self.domain.indexes())?,
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

        let mut items = Vec::new();
        for (k, v) in utxos.iter() {
            items.push(
                into_u5c_utxo(k, v, &self.mapper, &self.domain)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?,
            );
        }

        let cursor = self
            .domain
            .state()
            .read_cursor()
            .map_err(|e| Status::internal(e.to_string()))?
            .as_ref()
            .map(|p| point_to_u5c(&self.domain, p));

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

        info!("received new grpc query - read_tx");

        let tx_hash = message.hash;

        let query = dolos_core::AsyncQueryFacade::new(self.domain.clone());
        let (block_bytes, tx_index) = query
            .block_by_tx_hash(tx_hash.to_vec())
            .await
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
            .map(|p| point_to_u5c(&self.domain, p));

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
        request: Request<u5c::query::ReadGenesisRequest>,
    ) -> Result<Response<u5c::query::ReadGenesisResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query - read_genesis");

        let genesis = self.domain.genesis();

        let mut response = u5c::query::ReadGenesisResponse {
            genesis: genesis.shelley_hash.to_vec().into(),
            caip2: caip2_from_genesis(&genesis)?,
            config: Some(u5c::query::read_genesis_response::Config::Cardano(
                map_cardano_genesis(&genesis)?,
            )),
        };

        if let Some(mask) = message.field_mask {
            response = apply_mask(response, mask.paths)
                .map_err(|e| Status::internal(format!("failed to apply field mask: {e}")))?;
        }

        Ok(Response::new(response))
    }

    async fn read_era_summary(
        &self,
        request: Request<u5c::query::ReadEraSummaryRequest>,
    ) -> Result<Response<u5c::query::ReadEraSummaryResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc query - read_era_summary");

        let chain_summary = dolos_cardano::load_era_summary::<D>(self.domain.state())
            .map_err(|e| Status::internal(format!("failed to load era summary: {e}")))?;

        let active_pparams = dolos_cardano::load_effective_pparams::<D>(self.domain.state())
            .map_err(|e| Status::internal(format!("failed to load current pparams: {e}")))?;
        let active_protocol = active_pparams.protocol_major_or_default();
        let active_params = self
            .mapper
            .map_pparams(dolos_cardano::utils::pparams_to_pallas(&active_pparams));

        let summaries = chain_summary
            .iter_all()
            .map(|era| map_era_summary(era, active_protocol, &active_params))
            .collect();

        let mut response = u5c::query::ReadEraSummaryResponse {
            summary: Some(u5c::query::read_era_summary_response::Summary::Cardano(
                u5c::cardano::EraSummaries { summaries },
            )),
        };

        if let Some(mask) = message.field_mask {
            response = apply_mask(response, mask.paths)
                .map_err(|e| Status::internal(format!("failed to apply field mask: {e}")))?;
        }

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dolos_testing::toy_domain::ToyDomain;
    use pallas::interop::utxorpc::spec::query::query_service_server::QueryService;

    use super::*;

    #[test]
    fn maps_known_networks_to_caip2() {
        let mainnet = dolos_cardano::include::mainnet::load();
        let preprod = dolos_cardano::include::preprod::load();
        let preview = dolos_cardano::include::preview::load();

        assert_eq!(caip2_from_genesis(&mainnet).unwrap(), "cardano:mainnet");
        assert_eq!(caip2_from_genesis(&preprod).unwrap(), "cardano:preprod");
        assert_eq!(caip2_from_genesis(&preview).unwrap(), "cardano:preview");
    }

    #[test]
    fn falls_back_to_network_magic_for_unknown_caip2() {
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.shelley.network_magic = Some(42);

        assert_eq!(caip2_from_genesis(&genesis).unwrap(), "cardano:42");
    }

    #[test]
    fn maps_representative_genesis_fields() {
        let genesis = dolos_cardano::include::preview::load();
        let mapped = map_cardano_genesis(&genesis).unwrap();
        let expected_anchor_hash =
            hex::decode(&genesis.conway.constitution.anchor.data_hash).unwrap();
        let expected_constitution_hash = hex::decode(
            genesis
                .conway
                .constitution
                .script
                .as_deref()
                .expect("preview genesis includes a constitution script"),
        )
        .unwrap();

        assert_eq!(mapped.network_magic, 2);
        assert_eq!(mapped.epoch_length, genesis.shelley.epoch_length.unwrap());
        assert_eq!(mapped.slot_length, genesis.shelley.slot_length.unwrap());
        assert_eq!(mapped.system_start, genesis.shelley.system_start.unwrap());
        assert!(mapped.protocol_params.is_some());
        assert!(mapped.execution_prices.is_some());
        assert!(mapped.cost_models.is_some());

        let constitution = mapped.constitution.expect("missing constitution");
        let anchor = constitution.anchor.expect("missing constitution anchor");

        assert_eq!(
            anchor.content_hash.as_ref(),
            expected_anchor_hash.as_slice()
        );
        assert_eq!(
            constitution.hash.as_ref(),
            expected_constitution_hash.as_slice()
        );
        assert_ne!(anchor.content_hash.as_ref(), constitution.hash.as_ref());
    }

    #[tokio::test]
    async fn read_genesis_applies_field_mask() {
        let domain = ToyDomain::new_with_genesis(
            Arc::new(dolos_cardano::include::preview::load()),
            None,
            None,
        );
        let service = QueryServiceImpl::new(domain);
        let mut request = u5c::query::ReadGenesisRequest {
            field_mask: Some(Default::default()),
        };
        request.field_mask.as_mut().unwrap().paths = vec!["caip2".into()];

        let response = QueryService::read_genesis(&service, Request::new(request))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.caip2, "cardano:preview");
        assert!(response.genesis.is_empty());
        assert!(response.config.is_none());
    }

    #[tokio::test]
    async fn read_genesis_returns_hash_and_config() {
        let genesis = Arc::new(dolos_cardano::include::preprod::load());
        let expected_hash = genesis.shelley_hash.to_vec();
        let domain = ToyDomain::new_with_genesis(genesis, None, None);
        let service = QueryServiceImpl::new(domain);

        let response = QueryService::read_genesis(
            &service,
            Request::new(u5c::query::ReadGenesisRequest { field_mask: None }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(response.genesis.as_ref(), expected_hash.as_slice());
        assert_eq!(response.caip2, "cardano:preprod");

        match response.config {
            Some(u5c::query::read_genesis_response::Config::Cardano(cardano)) => {
                assert_eq!(cardano.network_magic, 1);
                assert!(cardano.protocol_params.unwrap().max_value_size > 0);
            }
            _ => panic!("missing cardano genesis config"),
        }
    }

    #[test]
    fn maps_protocols_to_era_names() {
        assert_eq!(protocol_to_era_name(0), "byron");
        assert_eq!(protocol_to_era_name(2), "shelley");
        assert_eq!(protocol_to_era_name(3), "allegra");
        assert_eq!(protocol_to_era_name(4), "mary");
        assert_eq!(protocol_to_era_name(6), "alonzo");
        assert_eq!(protocol_to_era_name(8), "babbage");
        assert_eq!(protocol_to_era_name(10), "conway");
        assert_eq!(protocol_to_era_name(42), "unknown");
    }

    #[tokio::test]
    async fn read_era_summary_returns_active_era_params_only() {
        let domain = ToyDomain::new_with_genesis(
            Arc::new(dolos_cardano::include::preview::load()),
            None,
            None,
        );
        let service = QueryServiceImpl::new(domain);

        let response = QueryService::read_era_summary(
            &service,
            Request::new(u5c::query::ReadEraSummaryRequest { field_mask: None }),
        )
        .await
        .unwrap()
        .into_inner();

        let summaries = match response.summary {
            Some(u5c::query::read_era_summary_response::Summary::Cardano(cardano)) => {
                cardano.summaries
            }
            _ => panic!("missing cardano era summaries"),
        };

        assert!(!summaries.is_empty());

        let active_with_params = summaries
            .iter()
            .filter(|x| x.protocol_params.is_some())
            .count();
        assert_eq!(active_with_params, 1);

        let active = summaries
            .iter()
            .find(|x| x.protocol_params.is_some())
            .expect("expected active era with protocol params");

        assert_eq!(active.name, "alonzo");
        assert!(active.start.is_some());
    }

    #[tokio::test]
    async fn read_era_summary_returns_boundary_time_in_milliseconds() {
        let domain = ToyDomain::new_with_genesis(
            Arc::new(dolos_cardano::include::preview::load()),
            None,
            None,
        );
        let service = QueryServiceImpl::new(domain);

        let response = QueryService::read_era_summary(
            &service,
            Request::new(u5c::query::ReadEraSummaryRequest { field_mask: None }),
        )
        .await
        .unwrap()
        .into_inner();

        let summaries = match response.summary {
            Some(u5c::query::read_era_summary_response::Summary::Cardano(cardano)) => {
                cardano.summaries
            }
            _ => panic!("missing cardano era summaries"),
        };

        let first = summaries
            .first()
            .expect("expected at least one era summary");
        let start = first.start.as_ref().expect("expected era start");

        assert_eq!(start.slot, 0);
        assert_eq!(start.time % 1000, 0);
        assert!(start.time >= 1_666_656_000_000);
    }

    #[tokio::test]
    async fn read_era_summary_applies_field_mask() {
        let domain = ToyDomain::new_with_genesis(
            Arc::new(dolos_cardano::include::preview::load()),
            None,
            None,
        );
        let service = QueryServiceImpl::new(domain);
        let mut request = u5c::query::ReadEraSummaryRequest {
            field_mask: Some(Default::default()),
        };
        request.field_mask.as_mut().unwrap().paths = vec!["cardano.summaries".into()];

        let response = QueryService::read_era_summary(&service, Request::new(request))
            .await
            .unwrap()
            .into_inner();

        match response.summary {
            Some(u5c::query::read_era_summary_response::Summary::Cardano(cardano)) => {
                assert!(!cardano.summaries.is_empty());
            }
            _ => panic!("missing cardano era summaries"),
        }
    }
}
