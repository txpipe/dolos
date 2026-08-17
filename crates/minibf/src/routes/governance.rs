use std::collections::HashMap;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::proposal::{GovernanceType, Proposal};
use dolos_cardano::{
    model::{DRepState, ProposalAction, ProposalState},
    pallas_extras, ChainSummary, PParamsSet,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, Network, StakePayload},
        primitives::{
            conway::{GovAction, GovActionId, ProtocolParamUpdate},
            Epoch, RationalNumber, StakeCredential,
        },
        traverse::MultiEraTx,
    },
};
use serde_json::{json, Value};

use crate::{
    mapping::{bech32, stake_cred_to_address, IntoModel},
    Facade,
};

const GOV_ACTION_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("gov_action");

fn parse_drep_id(drep_id: &str) -> Result<(String, Vec<u8>, bool, bool), StatusCode> {
    match drep_id {
        "drep_always_abstain" => Ok((drep_id.to_string(), vec![0], false, true)),
        "drep_always_no_confidence" => Ok((drep_id.to_string(), vec![1], false, true)),
        drep_id => {
            let (hrp, payload) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

            match (hrp.as_str(), payload.len()) {
                ("drep", 29) => {
                    let header_byte = payload.first().ok_or(StatusCode::BAD_REQUEST)?;

                    // first 4 bits need to be equal to 0010
                    if header_byte & 0b11110000 != 0b00100000 {
                        return Err(StatusCode::BAD_REQUEST);
                    }

                    Ok((drep_id.to_string(), payload, false, false))
                }
                ("drep", 28) => Ok((
                    drep_id.to_string(),
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_vkh", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_script", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_SCRIPT_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                _ => Err(StatusCode::BAD_REQUEST),
            }
        }
    }
}

pub struct DrepModelBuilder<'a> {
    drep_id: String,
    drep_id_encoded: Vec<u8>,
    is_legacy: bool,
    state: Option<DRepState>,
    pparams: PParamsSet,
    chain: &'a ChainSummary,
    tip: BlockSlot,
}

impl<'a> DrepModelBuilder<'a> {
    fn is_special_case(&self) -> bool {
        ["drep_always_abstain", "drep_always_no_confidence"].contains(&self.drep_id.as_str())
    }

    fn first_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
            return None;
        }

        if self
            .state
            .as_ref()
            .map(|x| x.is_unregistered())
            .unwrap_or(true)
        {
            return None;
        }

        self.state
            .as_ref()?
            .registered_at
            .map(|x| self.chain.slot_epoch(x.0).0)
    }

    fn last_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
            return None;
        }

        self.state
            .as_ref()?
            .last_active_slot
            .map(|x| self.chain.slot_epoch(x).0)
    }

    fn is_drep_expired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }

        if self.is_drep_retired() {
            return false;
        }

        let last_active_epoch = self.last_active_epoch();

        let inactivity_period = self.pparams.drep_inactivity_period().unwrap_or_default();

        let expiring_epoch = last_active_epoch.map(|x| x + inactivity_period);

        let (current_epoch, _) = self.chain.slot_epoch(self.tip);

        expiring_epoch
            .map(|expiration| expiration <= current_epoch)
            .unwrap_or(false)
    }

    fn is_drep_retired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }

        let Some(state) = self.state.as_ref() else {
            return false;
        };

        match (state.registered_at, state.unregistered_at) {
            (Some(registered), Some(unregistered)) => unregistered > registered,
            (Some(_), None) => false,
            _ => false,
        }
    }

    fn is_drep_active(&self) -> bool {
        !self.is_drep_retired()
    }
}

impl<'a> IntoModel<blockfrost_openapi::models::drep::Drep> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<blockfrost_openapi::models::drep::Drep, StatusCode> {
        let expired = self.is_drep_expired();

        let out = blockfrost_openapi::models::drep::Drep {
            drep_id: self.drep_id.clone(),
            hex: if self.is_special_case() {
                "".to_string()
            } else if self.is_legacy {
                hex::encode(&self.drep_id_encoded[1..])
            } else {
                hex::encode(&self.drep_id_encoded)
            },
            amount: self
                .state
                .as_ref()
                .map(|x| x.voting_power.to_string())
                .unwrap_or_default(),
            active: self.is_drep_active(),
            active_epoch: self.first_active_epoch().map(|x| x as i32),
            has_script: pallas_extras::drep_id_is_script(&self.drep_id_encoded),
            retired: self.is_drep_retired(),
            expired,
            last_active_epoch: self.last_active_epoch().map(|x| x as i32),
        };

        Ok(out)
    }
}

pub async fn drep_by_id<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let (drep, drep_bytes, is_legacy, is_special_case) =
        parse_drep_id(&drep).map_err(|_| StatusCode::BAD_REQUEST)?;

    let drep_state = if is_special_case {
        None
    } else {
        Some(
            domain
                .read_cardano_entity::<DRepState>(drep_bytes.clone())?
                .ok_or(StatusCode::NOT_FOUND)?,
        )
    };

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let pparams = domain.get_current_effective_pparams()?;

    let model = DrepModelBuilder {
        drep_id: drep,
        drep_id_encoded: drep_bytes,
        is_legacy,
        state: drep_state,
        pparams,
        chain: &chain,
        tip,
    };

    model.into_response()
}

fn parse_tx_hash(tx_hash: &str) -> Result<Hash<32>, StatusCode> {
    let bytes = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| StatusCode::BAD_REQUEST)?;

    Ok(bytes.into())
}

/// Parse a CIP-0129 governance action id: bech32 payload with the 32-byte tx
/// hash followed by a 1-byte action index. The minimal encoding omits the
/// index byte when the index is 0.
fn parse_gov_action_id(id: &str) -> Result<(Hash<32>, u32), StatusCode> {
    let (hrp, payload) = bech32::decode(id).map_err(|_| StatusCode::BAD_REQUEST)?;

    if hrp.as_str() != "gov_action" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let idx = match payload.len() {
        32 => 0,
        33 => payload[32] as u32,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let tx: [u8; 32] = payload[..32]
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    Ok((tx.into(), idx))
}

fn gov_action_id_bech32(tx: Hash<32>, idx: u32) -> Result<String, StatusCode> {
    let idx: u8 = idx
        .try_into()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    bech32(GOV_ACTION_HRP, [tx.as_slice(), &[idx]].concat())
}

// The helpers below reconstruct the `governance_description` JSON that
// Blockfrost copies from db-sync. db-sync stores the cardano-ledger Aeson
// encoding of the submitted `GovAction`, so field names follow the ledger
// JSON instances.

fn rational_json(x: &RationalNumber) -> Value {
    json!({ "numerator": x.numerator, "denominator": x.denominator })
}

fn credential_json(cred: &StakeCredential) -> Value {
    match cred {
        StakeCredential::AddrKeyhash(x) => json!({ "keyHash": hex::encode(x) }),
        StakeCredential::ScriptHash(x) => json!({ "scriptHash": hex::encode(x) }),
    }
}

/// Credentials used as JSON map keys follow the ledger `ToJSONKey` text form.
fn credential_key(cred: &StakeCredential) -> String {
    match cred {
        StakeCredential::AddrKeyhash(x) => format!("keyHash-{}", hex::encode(x)),
        StakeCredential::ScriptHash(x) => format!("scriptHash-{}", hex::encode(x)),
    }
}

fn gov_action_id_json(id: &GovActionId) -> Value {
    json!({
        "txId": hex::encode(id.transaction_id),
        "govActionIx": id.action_index,
    })
}

/// Render a ratio like the ledger pparams JSON: a plain number when the
/// fraction has a terminating decimal form, else a numerator/denominator
/// object (cf. cardano-api `toRationalJSON`).
fn ledger_ratio_json(x: &RationalNumber) -> Value {
    fn gcd(a: u64, b: u64) -> u64 {
        if b == 0 {
            a
        } else {
            gcd(b, a % b)
        }
    }

    if x.denominator == 0 {
        return rational_json(x);
    }

    let mut d = x.denominator / gcd(x.numerator, x.denominator);

    while d.is_multiple_of(2) {
        d /= 2;
    }

    while d.is_multiple_of(5) {
        d /= 5;
    }

    if d != 1 {
        return rational_json(x);
    }

    serde_json::Number::from_f64(x.numerator as f64 / x.denominator as f64)
        .map(Value::Number)
        .unwrap_or_else(|| rational_json(x))
}

fn reward_account_json(account: &[u8]) -> Result<Value, StatusCode> {
    let address = Address::from_bytes(account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let Address::Stake(address) = address else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    let network = match address.network() {
        Network::Mainnet => "Mainnet",
        _ => "Testnet",
    };

    let credential = match address.payload() {
        StakePayload::Stake(x) => json!({ "keyHash": hex::encode(x) }),
        StakePayload::Script(x) => json!({ "scriptHash": hex::encode(x) }),
    };

    Ok(json!({ "network": network, "credential": credential }))
}

/// Render the changed params with the ledger conway `PParamsUpdate` JSON
/// names (cf. the `pparams-update.json` golden file in cardano-ledger).
fn pparams_update_json(update: &ProtocolParamUpdate) -> Value {
    let mut out = serde_json::Map::new();

    if let Some(x) = update.minfee_a {
        out.insert("txFeePerByte".into(), json!(x));
    }
    if let Some(x) = update.minfee_b {
        out.insert("txFeeFixed".into(), json!(x));
    }
    if let Some(x) = update.max_block_body_size {
        out.insert("maxBlockBodySize".into(), json!(x));
    }
    if let Some(x) = update.max_transaction_size {
        out.insert("maxTxSize".into(), json!(x));
    }
    if let Some(x) = update.max_block_header_size {
        out.insert("maxBlockHeaderSize".into(), json!(x));
    }
    if let Some(x) = update.key_deposit {
        out.insert("stakeAddressDeposit".into(), json!(x));
    }
    if let Some(x) = update.pool_deposit {
        out.insert("stakePoolDeposit".into(), json!(x));
    }
    if let Some(x) = update.maximum_epoch {
        out.insert("poolRetireMaxEpoch".into(), json!(x));
    }
    if let Some(x) = update.desired_number_of_stake_pools {
        out.insert("stakePoolTargetNum".into(), json!(x));
    }
    if let Some(x) = &update.pool_pledge_influence {
        out.insert("poolPledgeInfluence".into(), ledger_ratio_json(x));
    }
    if let Some(x) = &update.expansion_rate {
        out.insert("monetaryExpansion".into(), ledger_ratio_json(x));
    }
    if let Some(x) = &update.treasury_growth_rate {
        out.insert("treasuryCut".into(), ledger_ratio_json(x));
    }
    if let Some(x) = update.min_pool_cost {
        out.insert("minPoolCost".into(), json!(x));
    }
    if let Some(x) = update.ada_per_utxo_byte {
        out.insert("utxoCostPerByte".into(), json!(x));
    }
    if let Some(x) = &update.cost_models_for_script_languages {
        let mut cost_models = serde_json::Map::new();

        if let Some(v1) = &x.plutus_v1 {
            cost_models.insert("PlutusV1".into(), json!(v1));
        }
        if let Some(v2) = &x.plutus_v2 {
            cost_models.insert("PlutusV2".into(), json!(v2));
        }
        if let Some(v3) = &x.plutus_v3 {
            cost_models.insert("PlutusV3".into(), json!(v3));
        }

        out.insert("costModels".into(), Value::Object(cost_models));
    }
    if let Some(x) = &update.execution_costs {
        out.insert(
            "executionUnitPrices".into(),
            json!({
                "priceMemory": ledger_ratio_json(&x.mem_price),
                "priceSteps": ledger_ratio_json(&x.step_price),
            }),
        );
    }
    if let Some(x) = &update.max_tx_ex_units {
        out.insert(
            "maxTxExecutionUnits".into(),
            json!({ "memory": x.mem, "steps": x.steps }),
        );
    }
    if let Some(x) = &update.max_block_ex_units {
        out.insert(
            "maxBlockExecutionUnits".into(),
            json!({ "memory": x.mem, "steps": x.steps }),
        );
    }
    if let Some(x) = update.max_value_size {
        out.insert("maxValueSize".into(), json!(x));
    }
    if let Some(x) = update.collateral_percentage {
        out.insert("collateralPercentage".into(), json!(x));
    }
    if let Some(x) = update.max_collateral_inputs {
        out.insert("maxCollateralInputs".into(), json!(x));
    }
    if let Some(x) = &update.pool_voting_thresholds {
        out.insert(
            "poolVotingThresholds".into(),
            json!({
                "motionNoConfidence": ledger_ratio_json(&x.motion_no_confidence),
                "committeeNormal": ledger_ratio_json(&x.committee_normal),
                "committeeNoConfidence": ledger_ratio_json(&x.committee_no_confidence),
                "hardForkInitiation": ledger_ratio_json(&x.hard_fork_initiation),
                "ppSecurityGroup": ledger_ratio_json(&x.security_voting_threshold),
            }),
        );
    }
    if let Some(x) = &update.drep_voting_thresholds {
        out.insert(
            "dRepVotingThresholds".into(),
            json!({
                "motionNoConfidence": ledger_ratio_json(&x.motion_no_confidence),
                "committeeNormal": ledger_ratio_json(&x.committee_normal),
                "committeeNoConfidence": ledger_ratio_json(&x.committee_no_confidence),
                "updateToConstitution": ledger_ratio_json(&x.update_constitution),
                "hardForkInitiation": ledger_ratio_json(&x.hard_fork_initiation),
                "ppNetworkGroup": ledger_ratio_json(&x.pp_network_group),
                "ppEconomicGroup": ledger_ratio_json(&x.pp_economic_group),
                "ppTechnicalGroup": ledger_ratio_json(&x.pp_technical_group),
                "ppGovGroup": ledger_ratio_json(&x.pp_governance_group),
                "treasuryWithdrawal": ledger_ratio_json(&x.treasury_withdrawal),
            }),
        );
    }
    if let Some(x) = update.min_committee_size {
        out.insert("committeeMinSize".into(), json!(x));
    }
    if let Some(x) = update.committee_term_limit {
        out.insert("committeeMaxTermLength".into(), json!(x));
    }
    if let Some(x) = update.governance_action_validity_period {
        out.insert("govActionLifetime".into(), json!(x));
    }
    if let Some(x) = update.governance_action_deposit {
        out.insert("govActionDeposit".into(), json!(x));
    }
    if let Some(x) = update.drep_deposit {
        out.insert("dRepDeposit".into(), json!(x));
    }
    if let Some(x) = update.drep_inactivity_period {
        out.insert("dRepActivity".into(), json!(x));
    }
    if let Some(x) = &update.minfee_refscript_cost_per_byte {
        out.insert("minFeeRefScriptCostPerByte".into(), ledger_ratio_json(x));
    }

    Value::Object(out)
}

/// Build the `governance_description` object from the submitted action.
/// db-sync stores the cardano-ledger Aeson encoding of the `GovAction`, so
/// tags and field names follow the ledger JSON instances.
fn description_json(action: &GovAction) -> Result<HashMap<String, Value>, StatusCode> {
    fn parent_json(parent: &Option<GovActionId>) -> Value {
        parent
            .as_ref()
            .map(gov_action_id_json)
            .unwrap_or(Value::Null)
    }

    let (tag, contents) = match action {
        GovAction::ParameterChange(parent, update, policy) => (
            "ParameterChange",
            Some(json!([
                parent_json(parent),
                pparams_update_json(update),
                policy.as_ref().map(hex::encode),
            ])),
        ),
        GovAction::HardForkInitiation(parent, (major, minor)) => (
            "HardForkInitiation",
            Some(json!([parent_json(parent), { "major": major, "minor": minor }])),
        ),
        GovAction::TreasuryWithdrawals(withdrawals, policy) => {
            let withdrawals = withdrawals
                .iter()
                .map(|(account, coin)| Ok(json!([reward_account_json(account)?, coin])))
                .collect::<Result<Vec<_>, StatusCode>>()?;

            (
                "TreasuryWithdrawals",
                Some(json!([withdrawals, policy.as_ref().map(hex::encode)])),
            )
        }
        GovAction::NoConfidence(parent) => ("NoConfidence", Some(parent_json(parent))),
        GovAction::UpdateCommittee(parent, to_remove, to_add, threshold) => {
            let removed: Vec<Value> = to_remove.iter().map(credential_json).collect();

            let added: serde_json::Map<String, Value> = to_add
                .iter()
                .map(|(cred, epoch)| (credential_key(cred), json!(epoch)))
                .collect();

            (
                "UpdateCommittee",
                Some(json!([
                    parent_json(parent),
                    removed,
                    added,
                    rational_json(threshold),
                ])),
            )
        }
        GovAction::NewConstitution(parent, constitution) => (
            "NewConstitution",
            Some(json!([
                parent_json(parent),
                {
                    "anchor": {
                        "url": constitution.anchor.url,
                        "dataHash": hex::encode(constitution.anchor.content_hash),
                    },
                    "script": constitution.guardrail_script.as_ref().map(hex::encode),
                },
            ])),
        ),
        GovAction::Information => ("InfoAction", None),
    };

    let mut out = HashMap::from([("tag".to_string(), json!(tag))]);

    if let Some(contents) = contents {
        out.insert("contents".to_string(), contents);
    }

    Ok(out)
}

fn governance_type_from_action(action: &GovAction) -> GovernanceType {
    match action {
        GovAction::ParameterChange(..) => GovernanceType::ParameterChange,
        GovAction::HardForkInitiation(..) => GovernanceType::HardForkInitiation,
        GovAction::TreasuryWithdrawals(..) => GovernanceType::TreasuryWithdrawals,
        GovAction::NoConfidence(..) => GovernanceType::NoConfidence,
        GovAction::UpdateCommittee(..) => GovernanceType::NewCommittee,
        GovAction::NewConstitution(..) => GovernanceType::NewConstitution,
        GovAction::Information => GovernanceType::InfoAction,
    }
}

pub struct ProposalModelBuilder {
    state: ProposalState,
    gov_action: Option<GovAction>,
    network: Network,
    current_epoch: Epoch,
}

impl ProposalModelBuilder {
    fn governance_type(&self) -> GovernanceType {
        if let Some(action) = &self.gov_action {
            return governance_type_from_action(action);
        }

        match &self.state.action {
            ProposalAction::ParamChange(_) => GovernanceType::ParameterChange,
            ProposalAction::HardFork(_) => GovernanceType::HardForkInitiation,
            ProposalAction::TreasuryWithdrawal(_) => GovernanceType::TreasuryWithdrawals,
            ProposalAction::NoConfidence => GovernanceType::NoConfidence,
            ProposalAction::UpdateCommittee { .. } => GovernanceType::NewCommittee,
            ProposalAction::NewConstitution { .. } => GovernanceType::NewConstitution,
            ProposalAction::Info => GovernanceType::InfoAction,
            // Legacy rows do not keep the action detail. InfoAction is the
            // neutral fallback.
            ProposalAction::Other => GovernanceType::InfoAction,
        }
    }

    /// Dolos stores the epoch before the enactment boundary as
    /// `ratified_epoch`. db-sync stamps both `ratified_epoch` and
    /// `enacted_epoch` with the boundary epoch, so both fields map to the
    /// same value.
    fn enactment_epoch(&self) -> Option<Epoch> {
        let boundary = self.state.ratified_epoch? + 1;

        (self.current_epoch >= boundary).then_some(boundary)
    }

    fn expired_epoch(&self) -> Option<Epoch> {
        if self.state.ratified_epoch.is_some() || self.state.canceled_epoch.is_some() {
            return None;
        }

        let expires = self.state.expires_at()?;

        (self.current_epoch >= expires).then_some(expires)
    }

    /// db-sync marks a proposal as dropped when a competing action gets
    /// enacted (canceled in dolos terms) or one epoch after it marks the
    /// proposal as expired.
    fn dropped_epoch(&self) -> Option<Epoch> {
        if let Some(canceled) = self.state.canceled_epoch {
            return (self.current_epoch >= canceled).then_some(canceled);
        }

        let dropped = self.expired_epoch()? + 1;

        (self.current_epoch >= dropped).then_some(dropped)
    }
}

impl IntoModel<Proposal> for ProposalModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<Proposal, StatusCode> {
        let return_address = self
            .state
            .reward_account
            .as_ref()
            .map(|cred| stake_cred_to_address(cred, self.network).to_bech32())
            .transpose()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .unwrap_or_default();

        let enactment = self.enactment_epoch();

        let governance_description = self.gov_action.as_ref().map(description_json).transpose()?;

        let out = Proposal {
            id: gov_action_id_bech32(self.state.tx, self.state.idx)?,
            tx_hash: hex::encode(self.state.tx),
            cert_index: self.state.idx as i32,
            governance_type: self.governance_type(),
            governance_description,
            deposit: self.state.deposit.unwrap_or_default().to_string(),
            return_address,
            ratified_epoch: enactment.map(|x| x as i32),
            enacted_epoch: enactment.map(|x| x as i32),
            dropped_epoch: self.dropped_epoch().map(|x| x as i32),
            expired_epoch: self.expired_epoch().map(|x| x as i32),
            expiration: self.state.expires_at().unwrap_or_default() as i32,
        };

        Ok(out)
    }
}

/// Recover the submitted `GovAction` from the archived proposal tx. Returns
/// `None` when the tx is absent (pruned archive), pre-Conway, or carries no
/// procedure at the index.
async fn load_gov_action<D>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<Option<GovAction>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let Some(cbor) = domain.get_tx(tx).await? else {
        return Ok(None);
    };

    let Ok(era) = cbor.0.try_into() else {
        return Ok(None);
    };

    let Ok(decoded) = MultiEraTx::decode_for_era(era, &cbor.1) else {
        return Ok(None);
    };

    let MultiEraTx::Conway(conway_tx) = decoded else {
        return Ok(None);
    };

    let action = conway_tx
        .transaction_body
        .proposal_procedures
        .as_ref()
        .and_then(|procedures| procedures.get(idx as usize))
        .map(|procedure| procedure.gov_action.clone());

    Ok(action)
}

async fn read_proposal<D>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<Json<Proposal>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let key = ProposalState::build_entity_key(tx, idx);

    let state = domain
        .read_cardano_entity::<ProposalState>(key)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let gov_action = load_gov_action(domain, tx, idx).await?;

    let chain = domain.get_chain_summary()?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let (current_epoch, _) = chain.slot_epoch(tip);

    let network = domain.get_network_id()?;

    let model = ProposalModelBuilder {
        state,
        gov_action,
        network,
        current_epoch,
    };

    model.into_response()
}

pub async fn proposal_by_tx_index<D>(
    Path((tx_hash, cert_index)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Proposal>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let tx = parse_tx_hash(&tx_hash)?;
    let idx: u32 = cert_index.parse().map_err(|_| StatusCode::BAD_REQUEST)?;

    read_proposal(&domain, tx, idx).await
}

pub async fn proposal_by_gov_action_id<D>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Proposal>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let (tx, idx) = parse_gov_action_id(&gov_action_id)?;

    read_proposal(&domain, tx, idx).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use bech32::{Bech32, Hrp};
    use dolos_cardano::model::GovPurpose;
    use dolos_core::{StateStore as _, StateWriter as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};

    fn invalid_drep() -> &'static str {
        "not-a-drep"
    }

    fn missing_drep() -> String {
        let mut payload = Vec::with_capacity(29);
        payload.push(0b00100010);
        payload.extend_from_slice(&[8u8; 28]);
        let hrp = Hrp::parse_unchecked("drep");
        bech32::encode::<Bech32>(hrp, &payload).expect("failed to encode missing drep")
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _body) = app.get_bytes(path).await;
        assert_eq!(status, expected);
    }

    #[tokio::test]
    async fn governance_drep_happy_path() {
        let app = TestApp::new();
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let _model: blockfrost_openapi::models::drep::Drep =
            serde_json::from_slice(&body).expect("failed to parse drep model");
    }

    #[tokio::test]
    async fn governance_drep_bad_request() {
        let app = TestApp::new();
        let path = format!("/governance/dreps/{}", invalid_drep());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_not_found() {
        let app = TestApp::new();
        let missing = missing_drep();
        let path = format!("/governance/dreps/{missing}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    fn proposal_tx() -> Hash<32> {
        [7u8; 32].into()
    }

    fn seed_proposal(domain: &ToyDomain) {
        let state = ProposalState {
            slot: 1,
            tx: proposal_tx(),
            idx: 0,
            action: ProposalAction::HardFork((11, 0)),
            max_epoch: Some(1_000),
            ratified_epoch: None,
            canceled_epoch: None,
            deposit: Some(100_000_000),
            reward_account: Some(StakeCredential::AddrKeyhash([7u8; 28].into())),
            proposed_in: Some(2),
            parent: Some(GovActionId {
                transaction_id: [9u8; 32].into(),
                action_index: 0,
            }),
            purpose: Some(GovPurpose::HardFork),
            anchor: None,
            cc_votes: Default::default(),
            drep_votes: Default::default(),
            spo_votes: Default::default(),
        };

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&state.key(), &state)
            .expect("failed to write proposal");
        writer.commit().expect("failed to commit proposal");
    }

    fn proposal_app() -> TestApp {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain, _| seed_proposal(domain))
    }

    fn assert_proposal_body(body: &[u8]) {
        let model: Proposal = serde_json::from_slice(body).expect("failed to parse proposal");

        assert_eq!(model.tx_hash, hex::encode(proposal_tx()));
        assert_eq!(model.cert_index, 0);
        assert_eq!(
            model.id,
            gov_action_id_bech32(proposal_tx(), 0).expect("failed to encode gov action id")
        );
        assert_eq!(model.governance_type, GovernanceType::HardForkInitiation);
        // The seeded state has no archived tx, so no description derives.
        assert_eq!(model.governance_description, None);
        assert_eq!(model.deposit, "100000000");
        assert!(model.return_address.starts_with("stake_test"));
        assert_eq!(model.ratified_epoch, None);
        assert_eq!(model.enacted_epoch, None);
        assert_eq!(model.expiration, 1_001);
    }

    #[tokio::test]
    async fn governance_proposal_happy_path() {
        let app = proposal_app();
        let path = format!("/governance/proposals/{}/0", hex::encode(proposal_tx()));
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[tokio::test]
    async fn governance_proposal_bad_request() {
        let app = TestApp::new();
        let path = "/governance/proposals/not-a-tx-hash/0";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;

        let path = format!(
            "/governance/proposals/{}/not-a-number",
            hex::encode(proposal_tx())
        );
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_proposal_not_found() {
        let app = TestApp::new();
        let path = format!("/governance/proposals/{}/0", hex::encode([0xffu8; 32]));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = format!("/governance/proposals/{}/0", hex::encode(proposal_tx()));
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_happy_path() {
        let app = proposal_app();
        let id = gov_action_id_bech32(proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_minimal_encoding() {
        let app = proposal_app();
        // CIP-0129 minimal encoding: cert index 0 omits the suffix byte.
        let hrp = Hrp::parse_unchecked("gov_action");
        let id = bech32::encode::<Bech32>(hrp, proposal_tx().as_slice())
            .expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[test]
    fn ledger_ratio_renders_terminating_fractions_as_numbers() {
        let half = RationalNumber {
            numerator: 1,
            denominator: 2,
        };
        assert_eq!(ledger_ratio_json(&half), serde_json::json!(0.5));

        let repeating = RationalNumber {
            numerator: 7,
            denominator: 19,
        };
        assert_eq!(
            ledger_ratio_json(&repeating),
            serde_json::json!({ "numerator": 7, "denominator": 19 })
        );
    }

    #[test]
    fn description_uses_ledger_pparams_update_names() {
        let update = ProtocolParamUpdate {
            minfee_a: None,
            minfee_b: None,
            max_block_body_size: None,
            max_transaction_size: None,
            max_block_header_size: None,
            key_deposit: None,
            pool_deposit: None,
            maximum_epoch: None,
            desired_number_of_stake_pools: Some(600),
            pool_pledge_influence: None,
            expansion_rate: None,
            treasury_growth_rate: None,
            min_pool_cost: None,
            ada_per_utxo_byte: None,
            cost_models_for_script_languages: None,
            execution_costs: None,
            max_tx_ex_units: None,
            max_block_ex_units: None,
            max_value_size: None,
            collateral_percentage: None,
            max_collateral_inputs: None,
            pool_voting_thresholds: None,
            drep_voting_thresholds: None,
            min_committee_size: None,
            committee_term_limit: None,
            governance_action_validity_period: None,
            governance_action_deposit: None,
            drep_deposit: None,
            drep_inactivity_period: None,
            minfee_refscript_cost_per_byte: None,
        };

        let action = GovAction::ParameterChange(
            Some(GovActionId {
                transaction_id: [0x1fu8; 32].into(),
                action_index: 0,
            }),
            Box::new(update),
            Some([0xfau8; 28].into()),
        );

        let description = description_json(&action).expect("failed to build description");

        assert_eq!(description["tag"], serde_json::json!("ParameterChange"));
        assert_eq!(
            description["contents"],
            serde_json::json!([
                { "txId": hex::encode([0x1fu8; 32]), "govActionIx": 0 },
                { "stakePoolTargetNum": 600 },
                hex::encode([0xfau8; 28]),
            ])
        );
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_bad_request() {
        let app = TestApp::new();
        let path = "/governance/proposals/not-a-gov-action-id";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;

        // A valid bech32 string with the wrong prefix must fail too.
        let hrp = Hrp::parse_unchecked("drep");
        let payload = [8u8; 33];
        let wrong_hrp =
            bech32::encode::<Bech32>(hrp, &payload).expect("failed to encode bech32 id");
        let path = format!("/governance/proposals/{wrong_hrp}");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_not_found() {
        let app = TestApp::new();
        let hrp = Hrp::parse_unchecked("gov_action");
        let mut payload = [0xffu8; 33];
        payload[32] = 0;
        let id = bech32::encode::<Bech32>(hrp, &payload).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let id = gov_action_id_bech32(proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
