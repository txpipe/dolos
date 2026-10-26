use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::proposal::Proposal;
use dolos_cardano::model::{ProposalAction, ProposalKey, ProposalState};
use dolos_core::{ArchiveStore as _, Domain};
use pallas::{
    crypto::hash::Hash,
    ledger::{primitives::conway::GovAction, traverse::MultiEraTx},
};

use crate::{
    error::Error,
    mapping::{governance::proposals::ProposalModelBuilder, parse_gov_action_id, IntoModel},
    Facade,
};

use super::{is_gov_action, read_proposal_row};

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
    let key = ProposalKey::new(tx, idx);

    let state = read_proposal_row(&domain.inner, &key)?
        .ok_or(StatusCode::NOT_FOUND)?
        .into_state();

    // Blockfrost never serves pre-Conway protocol updates here, and the
    // listing already hides them.
    if !is_gov_action(&state) {
        return Err(StatusCode::NOT_FOUND);
    }

    let gov_action = load_gov_action(domain, tx, idx).await?;

    // A legacy row keeps no action detail. When the archived tx cannot
    // resolve it either, any response would guess the type.
    if matches!(state.action, ProposalAction::Other) && gov_action.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

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
) -> Result<Json<Proposal>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let idx: u32 = cert_index.parse().map_err(|_| Error::InvalidCertIndex)?;

    // Blockfrost matches the hash as text against db-sync, so a malformed
    // one is a lookup that finds nothing rather than a bad request.
    let tx: Hash<32> = tx_hash.parse().map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(read_proposal(&domain, tx, idx).await?)
}

pub async fn proposal_by_gov_action_id<D>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Proposal>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<ProposalState>: From<D::Entity>,
{
    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    Ok(read_proposal(&domain, tx, idx).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::bech32_gov_action,
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use bech32::{Bech32, Hrp};
    use blockfrost_openapi::models::proposal;
    use dolos_cardano::model::GovPurpose;
    use dolos_core::{StateStore as _, StateWriter as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::ledger::primitives::{conway::GovActionId, StakeCredential};

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
            bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id")
        );
        assert_eq!(
            model.governance_type,
            proposal::GovernanceType::HardForkInitiation
        );
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
        let app = proposal_lookup_app();
        let path = format!("/governance/proposals/{}/0", hex::encode(proposal_tx()));
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[tokio::test]
    async fn governance_proposal_hides_legacy_rows() {
        // A pre-Conway protocol update row carries no deposit and no reward
        // account. Blockfrost never serves those from this endpoint.
        let seed_legacy = |domain: &ToyDomain| {
            let state = ProposalState {
                slot: 1,
                tx: proposal_tx(),
                idx: 0,
                action: ProposalAction::Other,
                max_epoch: None,
                ratified_epoch: None,
                canceled_epoch: None,
                deposit: None,
                reward_account: None,
                proposed_in: None,
                parent: None,
                purpose: None,
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
        };

        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, _| seed_legacy(domain));

        let path = format!("/governance/proposals/{}/0", hex::encode(proposal_tx()));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let id = bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_bad_request() {
        let app = TestApp::new();

        // Blockfrost treats a malformed hash as a miss, not a bad request
        let path = "/governance/proposals/not-a-tx-hash/0";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;

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
        let id = bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_proposal_body(&body);
    }

    #[tokio::test]
    async fn governance_proposal_by_gov_action_id_minimal_encoding() {
        let app = proposal_lookup_app();
        // CIP-0129 minimal encoding: cert index 0 omits the suffix byte.
        let hrp = Hrp::parse_unchecked("gov_action");
        let id = bech32::encode::<Bech32>(hrp, proposal_tx().as_slice())
            .expect("failed to encode gov action id");
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
        let id = bech32_gov_action(&proposal_tx(), 0).expect("failed to encode gov action id");
        let path = format!("/governance/proposals/{id}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
