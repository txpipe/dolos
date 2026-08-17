use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    proposal::{self, Proposal},
    proposals_inner::{GovernanceType, ProposalsInner},
};
use dolos_cardano::{
    model::{DRepState, FixedNamespace as _, PParamValue, ProposalAction, ProposalState},
    pallas_extras, ChainSummary, PParamsSet,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, StateStore as _};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::Network,
        primitives::{conway::GovActionId, Epoch, RationalNumber, StakeCredential},
        traverse::MultiEraBlock,
    },
};
use serde_json::{json, Value};

use crate::{
    error::Error,
    mapping::{bech32, bech32_gov_action, stake_cred_to_address, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
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

struct ProposalRow {
    slot: BlockSlot,
    tx: Hash<32>,
    idx: u32,
    governance_type: GovernanceType,
}

fn governance_type(action: &ProposalAction) -> Option<GovernanceType> {
    match action {
        ProposalAction::ParamChange(_) => Some(GovernanceType::ParameterChange),
        ProposalAction::HardFork(_) => Some(GovernanceType::HardForkInitiation),
        ProposalAction::TreasuryWithdrawal(_) => Some(GovernanceType::TreasuryWithdrawals),
        ProposalAction::NoConfidence => Some(GovernanceType::NoConfidence),
        ProposalAction::UpdateCommittee { .. } => Some(GovernanceType::NewCommittee),
        ProposalAction::NewConstitution { .. } => Some(GovernanceType::NewConstitution),
        ProposalAction::Info => Some(GovernanceType::InfoAction),
        ProposalAction::Other => None,
    }
}

/// Whether the row is a Conway governance action rather than a pre-Conway
/// protocol update.
///
/// Dolos records both in the same namespace, but only the Conway ones are
/// governance actions: db-sync keeps the old update proposals in a table of
/// their own and Blockfrost never lists them. A proposal procedure always
/// carries a deposit and the reward account it goes back to, and an update
/// proposal, having no procedure behind it, carries neither.
fn is_gov_action(state: &ProposalState) -> bool {
    state.deposit.is_some() && state.reward_account.is_some()
}

/// Proposals of one block in the order the block itself puts them: by the
/// position of the proposing tx, then by the action index inside that tx.
///
/// The archive is the only place that order lives, since a state row knows the
/// slot its proposal landed on but not where in the block its tx sat. A block
/// the archive no longer holds leaves the group on its provisional tx hash
/// order, which is arbitrary but stable across requests.
fn order_within_block<D: Domain>(
    domain: &D,
    slot: BlockSlot,
    rows: &mut [ProposalRow],
) -> Result<(), Error> {
    if rows.len() < 2 {
        return Ok(());
    }

    let Some(body) = domain
        .archive()
        .get_block_by_slot(&slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(());
    };

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let positions: HashMap<Hash<32>, usize> = block
        .txs()
        .iter()
        .enumerate()
        .map(|(position, tx)| (tx.hash(), position))
        .collect();

    rows.sort_by_key(|row| {
        (
            positions.get(&row.tx).copied().unwrap_or(usize::MAX),
            row.idx,
        )
    });

    Ok(())
}

/// Order the listing the way Blockfrost does — by the order the chain saw the
/// proposals — and cut it down to the requested page.
///
/// Proposals of one block form a group whose place in the listing the slot
/// already fixes, so the block behind a group is only read once the page
/// reaches it: a page costs at most as many block reads as it has rows, and
/// only for blocks that proposed more than once.
fn select_proposals<D: Domain>(
    domain: &D,
    mut proposals: Vec<ProposalRow>,
    pagination: &Pagination,
) -> Result<Vec<ProposalRow>, Error> {
    proposals.sort_unstable_by_key(|row| (row.slot, row.tx, row.idx));

    let mut groups: Vec<Vec<ProposalRow>> = Vec::new();

    for row in proposals {
        match groups.last_mut() {
            Some(group) if group[0].slot == row.slot => group.push(row),
            _ => groups.push(vec![row]),
        }
    }

    let descending = matches!(pagination.order, Order::Desc);

    if descending {
        groups.reverse();
    }

    let from = pagination.from();
    let to = from + pagination.count;

    let mut out = Vec::new();
    let mut seen = 0;

    for mut group in groups {
        let end = seen + group.len();

        if end <= from {
            seen = end;
            continue;
        }

        if seen >= to {
            break;
        }

        order_within_block(domain, group[0].slot, &mut group)?;

        // desc is the whole asc listing read backwards, group order included
        if descending {
            group.reverse();
        }

        for (offset, row) in group.into_iter().enumerate() {
            if (from..to).contains(&(seen + offset)) {
                out.push(row);
            }
        }

        seen = end;
    }

    Ok(out)
}

/// The page of `GET /governance/proposals`, read off the state and ordered
/// against the archive.
///
/// Every proposal is walked to build one page. The namespace holds one row per
/// governance action ever submitted and each of those costs a deposit, so it
/// grows in the hundreds — 155 rows on mainnet and 1536 on preview, the
/// cheapest of the networks to propose on — against the million-row namespaces
/// the other listings here already walk. What the page actually pays for is
/// the archive, and that is bounded by the page size.
fn read_page<D: Domain>(domain: &D, pagination: &Pagination) -> Result<Vec<ProposalsInner>, Error> {
    let mut rows = Vec::new();

    let entities = domain
        .state()
        .iter_entities_typed::<ProposalState>(ProposalState::NS, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for entry in entities {
        let (_, state) = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if !is_gov_action(&state) {
            continue;
        }

        let Some(governance_type) = governance_type(&state.action) else {
            continue;
        };

        rows.push(ProposalRow {
            slot: state.slot,
            tx: state.tx,
            idx: state.idx,
            governance_type,
        });
    }

    select_proposals(domain, rows, pagination)?
        .into_iter()
        .map(|row| {
            Ok(ProposalsInner {
                id: bech32_gov_action(&row.tx, row.idx)?,
                tx_hash: hex::encode(row.tx),
                cert_index: row
                    .idx
                    .try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                governance_type: row.governance_type,
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()
        .map_err(Error::from)
}

/// `GET /governance/proposals`: every governance action, oldest first.
///
/// Blockfrost reads its listing straight off db-sync's proposal table with no
/// filter of its own, so the filtering here is only about telling governance
/// actions apart from the pre-Conway update proposals dolos keeps beside them.
pub async fn proposals<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ProposalsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let page = domain
        .query()
        .run_blocking(move |domain| Ok(read_page(&domain, &pagination)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(page))
}

fn parse_tx_hash(tx_hash: &str) -> Result<Hash<32>, StatusCode> {
    let bytes = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| StatusCode::BAD_REQUEST)?;

    Ok(bytes.into())
}

/// Parse a CIP-0129 governance action id: bech32 payload with the 32-byte tx
/// hash followed by a 1-byte action index.
fn parse_gov_action_id(id: &str) -> Result<(Hash<32>, u32), StatusCode> {
    let (hrp, payload) = bech32::decode(id).map_err(|_| StatusCode::BAD_REQUEST)?;

    if hrp.as_str() != "gov_action" || payload.len() != 33 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let tx: [u8; 32] = payload[..32]
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    Ok((tx.into(), payload[32] as u32))
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

fn account_address_json(cred: &StakeCredential, network: Network) -> Value {
    let network = match network {
        Network::Mainnet => "Mainnet",
        _ => "Testnet",
    };

    json!({ "network": network, "credential": credential_json(cred) })
}

/// Render the changed params with the ledger `PParamsUpdate` JSON names.
/// Params that a Conway action can not change are skipped.
fn pparams_update_json(update: &PParamsSet) -> Value {
    let mut out = serde_json::Map::new();
    let mut cost_models = serde_json::Map::new();

    for value in update.iter() {
        match value {
            PParamValue::MinFeeA(x) => {
                out.insert("minFeeA".into(), json!(x));
            }
            PParamValue::MinFeeB(x) => {
                out.insert("minFeeB".into(), json!(x));
            }
            PParamValue::MaxBlockBodySize(x) => {
                out.insert("maxBBSize".into(), json!(x));
            }
            PParamValue::MaxTransactionSize(x) => {
                out.insert("maxTxSize".into(), json!(x));
            }
            PParamValue::MaxBlockHeaderSize(x) => {
                out.insert("maxBHSize".into(), json!(x));
            }
            PParamValue::KeyDeposit(x) => {
                out.insert("keyDeposit".into(), json!(x));
            }
            PParamValue::PoolDeposit(x) => {
                out.insert("poolDeposit".into(), json!(x));
            }
            PParamValue::MaximumEpoch(x) => {
                out.insert("eMax".into(), json!(x));
            }
            PParamValue::DesiredNumberOfStakePools(x) => {
                out.insert("nOpt".into(), json!(x));
            }
            PParamValue::PoolPledgeInfluence(x) => {
                out.insert("a0".into(), rational_json(x));
            }
            PParamValue::ExpansionRate(x) => {
                out.insert("rho".into(), rational_json(x));
            }
            PParamValue::TreasuryGrowthRate(x) => {
                out.insert("tau".into(), rational_json(x));
            }
            PParamValue::ProtocolVersion((major, minor)) => {
                out.insert(
                    "protocolVersion".into(),
                    json!({ "major": major, "minor": minor }),
                );
            }
            PParamValue::MinPoolCost(x) => {
                out.insert("minPoolCost".into(), json!(x));
            }
            PParamValue::AdaPerUtxoByte(x) => {
                out.insert("coinsPerUTxOByte".into(), json!(x));
            }
            PParamValue::ExecutionCosts(x) => {
                out.insert(
                    "prices".into(),
                    json!({
                        "prMem": rational_json(&x.mem_price),
                        "prSteps": rational_json(&x.step_price),
                    }),
                );
            }
            PParamValue::MaxTxExUnits(x) => {
                out.insert(
                    "maxTxExUnits".into(),
                    json!({ "exUnitsMem": x.mem, "exUnitsSteps": x.steps }),
                );
            }
            PParamValue::MaxBlockExUnits(x) => {
                out.insert(
                    "maxBlockExUnits".into(),
                    json!({ "exUnitsMem": x.mem, "exUnitsSteps": x.steps }),
                );
            }
            PParamValue::MaxValueSize(x) => {
                out.insert("maxValSize".into(), json!(x));
            }
            PParamValue::CollateralPercentage(x) => {
                out.insert("collateralPercentage".into(), json!(x));
            }
            PParamValue::MaxCollateralInputs(x) => {
                out.insert("maxCollateralInputs".into(), json!(x));
            }
            PParamValue::PoolVotingThresholds(x) => {
                out.insert(
                    "poolVotingThresholds".into(),
                    json!({
                        "motionNoConfidence": rational_json(&x.motion_no_confidence),
                        "committeeNormal": rational_json(&x.committee_normal),
                        "committeeNoConfidence": rational_json(&x.committee_no_confidence),
                        "hardForkInitiation": rational_json(&x.hard_fork_initiation),
                        "ppSecurityGroup": rational_json(&x.security_voting_threshold),
                    }),
                );
            }
            PParamValue::DrepVotingThresholds(x) => {
                out.insert(
                    "dRepVotingThresholds".into(),
                    json!({
                        "motionNoConfidence": rational_json(&x.motion_no_confidence),
                        "committeeNormal": rational_json(&x.committee_normal),
                        "committeeNoConfidence": rational_json(&x.committee_no_confidence),
                        "updateToConstitution": rational_json(&x.update_constitution),
                        "hardForkInitiation": rational_json(&x.hard_fork_initiation),
                        "ppNetworkGroup": rational_json(&x.pp_network_group),
                        "ppEconomicGroup": rational_json(&x.pp_economic_group),
                        "ppTechnicalGroup": rational_json(&x.pp_technical_group),
                        "ppGovGroup": rational_json(&x.pp_governance_group),
                        "treasuryWithdrawal": rational_json(&x.treasury_withdrawal),
                    }),
                );
            }
            PParamValue::MinCommitteeSize(x) => {
                out.insert("committeeMinSize".into(), json!(x));
            }
            PParamValue::CommitteeTermLimit(x) => {
                out.insert("committeeMaxTermLength".into(), json!(x));
            }
            PParamValue::GovernanceActionValidityPeriod(x) => {
                out.insert("govActionLifetime".into(), json!(x));
            }
            PParamValue::GovernanceActionDeposit(x) => {
                out.insert("govActionDeposit".into(), json!(x));
            }
            PParamValue::DrepDeposit(x) => {
                out.insert("dRepDeposit".into(), json!(x));
            }
            PParamValue::DrepInactivityPeriod(x) => {
                out.insert("dRepActivity".into(), json!(x));
            }
            PParamValue::MinFeeRefScriptCostPerByte(x) => {
                out.insert("minFeeRefScriptCostPerByte".into(), rational_json(x));
            }
            PParamValue::CostModelsPlutusV1(x) => {
                cost_models.insert("PlutusV1".into(), json!(x));
            }
            PParamValue::CostModelsPlutusV2(x) => {
                cost_models.insert("PlutusV2".into(), json!(x));
            }
            PParamValue::CostModelsPlutusV3(x) => {
                cost_models.insert("PlutusV3".into(), json!(x));
            }
            _ => (),
        }
    }

    if !cost_models.is_empty() {
        out.insert("costModels".into(), Value::Object(cost_models));
    }

    Value::Object(out)
}

fn description_json(state: &ProposalState, network: Network) -> Option<HashMap<String, Value>> {
    let parent = state
        .parent
        .as_ref()
        .map(gov_action_id_json)
        .unwrap_or(Value::Null);

    let (tag, contents) = match &state.action {
        ProposalAction::ParamChange(update) => (
            // The guardrails script hash is not tracked, so the third
            // element is always null.
            "ParameterChange",
            Some(json!([parent, pparams_update_json(update), Value::Null])),
        ),
        ProposalAction::HardFork((major, minor)) => (
            "HardForkInitiation",
            Some(json!([parent, { "major": major, "minor": minor }])),
        ),
        ProposalAction::TreasuryWithdrawal(withdrawals) => {
            let withdrawals: Vec<Value> = withdrawals
                .iter()
                .map(|(cred, coin)| json!([account_address_json(cred, network), coin]))
                .collect();

            // The guardrails script hash is not tracked, so the second
            // element is always null.
            (
                "TreasuryWithdrawals",
                Some(json!([withdrawals, Value::Null])),
            )
        }
        ProposalAction::NoConfidence => ("NoConfidence", Some(parent)),
        ProposalAction::UpdateCommittee {
            to_remove,
            to_add,
            threshold,
        } => {
            let to_remove: Vec<Value> = to_remove.iter().map(credential_json).collect();

            let to_add: serde_json::Map<String, Value> = to_add
                .iter()
                .map(|(cred, epoch)| (credential_key(cred), json!(epoch)))
                .collect();

            (
                "UpdateCommittee",
                Some(json!([parent, to_remove, to_add, rational_json(threshold)])),
            )
        }
        ProposalAction::NewConstitution {
            anchor,
            guardrail_script,
        } => (
            "NewConstitution",
            Some(json!([
                parent,
                {
                    "anchor": {
                        "url": anchor.url,
                        "dataHash": hex::encode(anchor.content_hash),
                    },
                    "script": guardrail_script.as_ref().map(hex::encode),
                },
            ])),
        ),
        ProposalAction::Info => ("InfoAction", None),
        // Legacy rows do not keep the action detail.
        ProposalAction::Other => return None,
    };

    let mut out = HashMap::from([("tag".to_string(), json!(tag))]);

    if let Some(contents) = contents {
        out.insert("contents".to_string(), contents);
    }

    Some(out)
}

pub struct ProposalModelBuilder {
    state: ProposalState,
    network: Network,
    current_epoch: Epoch,
}

impl ProposalModelBuilder {
    fn governance_type(&self) -> proposal::GovernanceType {
        match &self.state.action {
            ProposalAction::ParamChange(_) => proposal::GovernanceType::ParameterChange,
            ProposalAction::HardFork(_) => proposal::GovernanceType::HardForkInitiation,
            ProposalAction::TreasuryWithdrawal(_) => proposal::GovernanceType::TreasuryWithdrawals,
            ProposalAction::NoConfidence => proposal::GovernanceType::NoConfidence,
            ProposalAction::UpdateCommittee { .. } => proposal::GovernanceType::NewCommittee,
            ProposalAction::NewConstitution { .. } => proposal::GovernanceType::NewConstitution,
            ProposalAction::Info => proposal::GovernanceType::InfoAction,
            // Legacy rows do not keep the action detail. InfoAction is the
            // neutral fallback.
            ProposalAction::Other => proposal::GovernanceType::InfoAction,
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
    /// enacted (canceled in dolos terms) or when the proposal expires.
    fn dropped_epoch(&self) -> Option<Epoch> {
        if let Some(canceled) = self.state.canceled_epoch {
            return (self.current_epoch >= canceled).then_some(canceled);
        }

        self.expired_epoch()
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

        let out = Proposal {
            id: gov_action_id_bech32(self.state.tx, self.state.idx)?,
            tx_hash: hex::encode(self.state.tx),
            cert_index: self.state.idx as i32,
            governance_type: self.governance_type(),
            governance_description: description_json(&self.state, self.network),
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

async fn read_proposal<D: Domain>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<Json<Proposal>, StatusCode>
where
    Option<ProposalState>: From<D::Entity>,
{
    let key = ProposalState::build_entity_key(tx, idx);

    let state = domain
        .read_cardano_entity::<ProposalState>(key)?
        .ok_or(StatusCode::NOT_FOUND)?;

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
        network,
        current_epoch,
    };

    model.into_response()
}

pub async fn proposal_by_tx_index<D: Domain>(
    Path((tx_hash, cert_index)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Proposal>, StatusCode>
where
    Option<ProposalState>: From<D::Entity>,
{
    let tx = parse_tx_hash(&tx_hash)?;
    let idx: u32 = cert_index.parse().map_err(|_| StatusCode::BAD_REQUEST)?;

    read_proposal(&domain, tx, idx).await
}

pub async fn proposal_by_gov_action_id<D: Domain>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Proposal>, StatusCode>
where
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
    use dolos_core::StateWriter as _;
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use itertools::Itertools;
    use pallas::ledger::primitives::conway::GovAction;

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

    /// Three blocks: the first tx of block 1 proposes two actions, block 2
    /// proposes none and block 3 proposes one. Enough to pin the listing
    /// order, the cert index within a tx and a few action types.
    fn proposal_app() -> TestApp {
        TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            gov_actions_by_block: vec![
                vec![vec![
                    GovAction::Information,
                    GovAction::HardForkInitiation(None, (10, 0)),
                ]],
                vec![],
                vec![vec![GovAction::NoConfidence(None)]],
            ],
            ..Default::default()
        })
    }

    async fn get_proposals(app: &TestApp, query: &str) -> Vec<ProposalsInner> {
        let path = format!("/governance/proposals{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse proposals")
    }

    fn tx_hash_of_block(app: &TestApp, block: usize) -> String {
        app.vectors().blocks[block].tx_hashes[0].clone()
    }

    fn as_rows(proposals: &[ProposalsInner]) -> Vec<(String, i32, GovernanceType)> {
        proposals
            .iter()
            .map(|x| (x.tx_hash.clone(), x.cert_index, x.governance_type))
            .collect_vec()
    }

    #[tokio::test]
    async fn governance_proposals_happy_path() {
        let app = proposal_app();
        let first_tx = tx_hash_of_block(&app, 0);
        let third_tx = tx_hash_of_block(&app, 2);

        let proposals = get_proposals(&app, "").await;

        assert_eq!(
            as_rows(&proposals),
            vec![
                (first_tx.clone(), 0, GovernanceType::InfoAction),
                (first_tx.clone(), 1, GovernanceType::HardForkInitiation),
                (third_tx, 0, GovernanceType::NoConfidence),
            ]
        );

        // the id names the same proposal as the tx hash and the cert index
        let tx: Hash<32> = first_tx.parse().expect("failed to parse tx hash");
        assert_eq!(proposals[1].id, bech32_gov_action(&tx, 1).unwrap());
    }

    #[tokio::test]
    async fn governance_proposals_orders_and_paginates() {
        let app = proposal_app();
        let first_tx = tx_hash_of_block(&app, 0);
        let third_tx = tx_hash_of_block(&app, 2);

        let desc = get_proposals(&app, "?order=desc").await;
        assert_eq!(
            as_rows(&desc),
            vec![
                (third_tx.clone(), 0, GovernanceType::NoConfidence),
                (first_tx.clone(), 1, GovernanceType::HardForkInitiation),
                (first_tx.clone(), 0, GovernanceType::InfoAction),
            ]
        );

        // asc: page 2 of size 1 is the second action of the first proposing tx
        let page = get_proposals(&app, "?page=2&count=1").await;
        assert_eq!(
            as_rows(&page),
            vec![(first_tx, 1, GovernanceType::HardForkInitiation)]
        );

        // desc: page 1 of size 1 is the newest proposal
        let page = get_proposals(&app, "?order=desc&page=1&count=1").await;
        assert_eq!(
            as_rows(&page),
            vec![(third_tx, 0, GovernanceType::NoConfidence)]
        );

        // a page past the end is empty, not an error
        let page = get_proposals(&app, "?page=4&count=1").await;
        assert!(page.is_empty());
    }

    /// Four txs of one block propose, so the whole listing sits in a single
    /// block and the order can only come from the position of each tx inside
    /// it — the hashes disagree, which is what Blockfrost's own ordering by
    /// db-sync row id exposes on preview.
    #[tokio::test]
    async fn governance_proposals_follow_tx_order_inside_a_block() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 4,
            gov_actions_by_block: vec![vec![
                vec![GovAction::Information],
                vec![GovAction::Information],
                vec![GovAction::Information],
                vec![GovAction::Information],
            ]],
            ..Default::default()
        });

        let txs = app.vectors().blocks[0].tx_hashes.clone();
        let by_hash = txs.iter().sorted().cloned().collect_vec();
        assert_ne!(
            txs, by_hash,
            "fixture stopped telling block order and hash order apart"
        );

        let proposals = get_proposals(&app, "").await;
        assert_eq!(
            proposals.iter().map(|x| x.tx_hash.clone()).collect_vec(),
            txs
        );

        // a page cutting the block's group in half keeps that order
        let page = get_proposals(&app, "?count=2&page=2").await;
        assert_eq!(
            page.iter().map(|x| x.tx_hash.clone()).collect_vec(),
            txs[2..].to_vec()
        );

        // desc is the same listing read backwards, inside the block too
        let desc = get_proposals(&app, "?order=desc").await;
        assert_eq!(
            desc.iter().map(|x| x.tx_hash.clone()).collect_vec(),
            txs.iter().rev().cloned().collect_vec()
        );
    }

    #[tokio::test]
    async fn governance_proposals_without_any_proposal() {
        // the default synthetic chain proposes nothing
        let app = TestApp::new();
        assert!(get_proposals(&app, "").await.is_empty());
    }

    #[tokio::test]
    async fn governance_proposals_bad_request() {
        let app = proposal_app();
        assert_status(
            &app,
            "/governance/proposals?count=0",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/proposals?page=x",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/proposals?order=sideways",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_proposals_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(
            &app,
            "/governance/proposals",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    #[test]
    fn governance_type_names_every_action() {
        let cases = [
            (
                ProposalAction::ParamChange(PParamsSet::default()),
                Some(GovernanceType::ParameterChange),
            ),
            (
                ProposalAction::HardFork((10, 0)),
                Some(GovernanceType::HardForkInitiation),
            ),
            (
                ProposalAction::TreasuryWithdrawal(vec![]),
                Some(GovernanceType::TreasuryWithdrawals),
            ),
            (
                ProposalAction::NoConfidence,
                Some(GovernanceType::NoConfidence),
            ),
            (
                ProposalAction::UpdateCommittee {
                    to_remove: vec![],
                    to_add: vec![],
                    threshold: pallas::ledger::primitives::RationalNumber {
                        numerator: 1,
                        denominator: 2,
                    },
                },
                Some(GovernanceType::NewCommittee),
            ),
            (
                ProposalAction::NewConstitution {
                    anchor: pallas::ledger::primitives::conway::Anchor {
                        url: "https://dolos.test".to_string(),
                        content_hash: Hash::from([1u8; 32]),
                    },
                    guardrail_script: None,
                },
                Some(GovernanceType::NewConstitution),
            ),
            (ProposalAction::Info, Some(GovernanceType::InfoAction)),
            // the legacy catch-all carries no action to name
            (ProposalAction::Other, None),
        ];

        for (action, expected) in cases {
            assert_eq!(governance_type(&action), expected, "{action:?}");
        }
    }

    /// CIP-129: the id is the proposing tx hash with the action index
    /// trailing it. The first two vectors come from the Blockfrost spec kept
    /// in `crates/minibf/openapi.yaml`; the last one pins the minimal
    /// big-endian rule Blockfrost encodes the index with.
    #[test]
    fn gov_action_id_follows_cip129() {
        let tx = Hash::<32>::from([0x11u8; 32]);
        assert_eq!(
            bech32_gov_action(&tx, 0).unwrap(),
            "gov_action1zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygsq6dmejn"
        );

        let tx: Hash<32> = "b2a591ac219ce6dcca5847e0248015209c7cb0436aa6bd6863d0c1f152a60bc5"
            .parse()
            .expect("failed to parse tx hash");
        assert_eq!(
            bech32_gov_action(&tx, 0).unwrap(),
            "gov_action1k2jertppnnndejjcglszfqq4yzw8evzrd2nt66rr6rqlz54xp0zsq05ecsn"
        );

        // one byte per index until it no longer fits, then two
        let payload = |idx| {
            let id = bech32_gov_action(&tx, idx).unwrap();
            bech32::decode(&id).unwrap().1[32..].to_vec()
        };
        assert_eq!(payload(0), vec![0x00]);
        assert_eq!(payload(1), vec![0x01]);
        assert_eq!(payload(255), vec![0xff]);
        assert_eq!(payload(256), vec![0x01, 0x00]);
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

    fn proposal_lookup_app() -> TestApp {
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
        assert_eq!(
            model.governance_type,
            proposal::GovernanceType::HardForkInitiation
        );
        assert_eq!(model.deposit, "100000000");
        assert!(model.return_address.starts_with("stake_test"));
        assert_eq!(model.ratified_epoch, None);
        assert_eq!(model.enacted_epoch, None);
        assert_eq!(model.expiration, 1_001);
    }

    #[tokio::test]
    async fn governance_proposal_happy_path() {
        let app = proposal_lookup_app();
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
        let app = proposal_lookup_app();
        let id = gov_action_id_bech32(proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
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
