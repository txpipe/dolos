use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::{committee::Committee, committee_quorum::CommitteeQuorum};
use dolos_cardano::model::gov::GovState;
use dolos_core::Domain;
use pallas::ledger::primitives::RationalNumber;

use crate::{
    mapping::{bech32_gov_action, governance::committee::committee_members, i32_or_500},
    Facade,
};

/// The `GET /governance/committee` endpoint returns the constitutional
/// committee in force. The response gives the members, the hot-key
/// authorization status of each member, the vote threshold, and the
/// `NewCommittee` action that seated the committee.
///
/// Dolos keeps three items in the governance singleton: the enacted
/// committee, the per-member authorization history, and the root of the
/// previous committee action. This is the same data that Blockfrost
/// reconstructs from the db-sync tables `committee`, `committee_member`, and
/// `committee_registration` or `_de_registration`. The Conway-genesis
/// committee has no seating action. Its `gov_action_id`, `proposal_tx_hash`,
/// and `proposal_index` are null.
///
/// A `NoConfidence` enactment dissolves the committee. Dolos sets the
/// committee value to null and keeps the last-seated committee as a snapshot,
/// with each member's authorization history as it stood and the action that
/// seated it. A dissolved committee has `is_dissolved` true and returns the
/// snapshot's members, quorum and seating action, as Blockfrost returns the
/// historical committee; the committee root then holds the `NoConfidence`,
/// which never seated anyone. A dissolved store without a snapshot returns an
/// empty member list and a zero quorum.
///
/// The `is_dissolved` flag depends on evidence of a committee lineage action,
/// not on governance activation. A previous committee action on record makes a
/// null committee a dissolution. A null committee with no such action is not a
/// dissolution.
///
/// A store migrated across the in-place upgrade gap has an unknown enact-state:
/// a null committee with no lineage root. The endpoint does not report this
/// state as a dissolution. A fresh sync recovers the true state. If governance
/// is not active (`active_since` unset), no committee exists. The endpoint then
/// returns 404 instead of an empty committee.
pub async fn committee<D>(State(domain): State<Facade<D>>) -> Result<Json<Committee>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<GovState>: From<D::Entity>,
{
    let gov = domain
        .read_cardano_entity::<GovState>(&())?
        .filter(|gov| gov.active_since.is_some())
        .ok_or(StatusCode::NOT_FOUND)?;

    let seating = match (gov.committee.as_ref(), gov.dissolved_committee.as_ref()) {
        (None, Some(dissolved)) => dissolved.seated_by.as_ref(),
        _ => gov.prev_gov_action_ids.committee.as_ref(),
    };

    let (gov_action_id, proposal_tx_hash, proposal_index) = match seating {
        Some(id) => (
            Some(bech32_gov_action(&id.transaction_id, id.action_index)?),
            Some(hex::encode(id.transaction_id)),
            Some(i32_or_500(id.action_index)?),
        ),
        None => (None, None, None),
    };

    let quorum = |threshold: &RationalNumber| -> Result<CommitteeQuorum, StatusCode> {
        Ok(CommitteeQuorum {
            numerator: i32_or_500(threshold.numerator)?,
            denominator: i32_or_500(threshold.denominator)?,
        })
    };

    let (quorum, members) = match (gov.committee.as_ref(), gov.dissolved_committee.as_ref()) {
        (Some(committee), _) => (
            quorum(&committee.threshold)?,
            committee_members(&committee.members, |cold| gov.committee_auth(cold))?,
        ),
        (None, Some(dissolved)) => (
            quorum(&dissolved.committee.threshold)?,
            committee_members(&dissolved.committee.members, |cold| {
                dissolved.committee_auth(cold)
            })?,
        ),
        (None, None) => (CommitteeQuorum::default(), Vec::new()),
    };

    Ok(Json(Committee {
        gov_action_id,
        proposal_tx_hash,
        proposal_index,
        is_dissolved: gov.committee.is_none() && gov.prev_gov_action_ids.committee.is_some(),
        quorum: Box::new(quorum),
        members,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::{bech32_committee_cold, bech32_committee_hot},
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::committee_members_inner::Status;
    use dolos_cardano::model::gov::CommitteeAuthorization;
    use dolos_core::BlockSlot;
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::{
        crypto::hash::Hash,
        ledger::primitives::{conway::GovActionId, RationalNumber, StakeCredential},
    };
    use std::collections::BTreeMap;

    fn cc_cold_script(byte: u8) -> StakeCredential {
        StakeCredential::ScriptHash(Hash::from([byte; 28]))
    }

    /// This function writes an active-governance committee, its per-member
    /// authorization history, and its action root to the governance singleton.
    /// The endpoint reads this data. The `seed_drep_anchor` helper writes
    /// equivalent data for a DRep registration.
    fn seed_committee(
        domain: &ToyDomain,
        committee: Option<dolos_cardano::model::gov::Committee>,
        auths: BTreeMap<StakeCredential, Vec<(BlockSlot, CommitteeAuthorization)>>,
        seating_action: Option<GovActionId>,
    ) {
        seed_gov(
            domain,
            GovState {
                committee,
                committee_auths: auths,
                prev_gov_action_ids: dolos_cardano::model::gov::GovRoots {
                    committee: seating_action,
                    ..Default::default()
                },
                active_since: Some(0),
                ..Default::default()
            },
        );
    }

    #[test]
    fn committee_credential_ids_follow_cip0129() {
        // For a cold key, the high nibble is 0x1 (cold), and the low nibble is 0x2
        // (key).
        let cold =
            bech32_committee_cold(&cc_cold_key(1)).expect("The encoder cannot encode the cold ID.");
        let (hrp, payload) = bech32::decode(&cold).expect("The decoder cannot decode the cold ID.");
        assert_eq!(hrp.as_str(), "cc_cold");
        assert_eq!(payload[0], 0x12);
        assert_eq!(&payload[1..], &[1u8; 28]);

        // For a cold script, the low nibble is 0x3 (script).
        let cold_script = bech32_committee_cold(&cc_cold_script(3))
            .expect("The encoder cannot encode the cold-script ID.");
        let (_, payload) =
            bech32::decode(&cold_script).expect("The decoder cannot decode the cold-script ID.");
        assert_eq!(payload[0], 0x13);

        // For a hot key, the high nibble is 0x0 (hot), and the low nibble is 0x2 (key).
        let hot =
            bech32_committee_hot(&cc_cold_key(11)).expect("The encoder cannot encode the hot ID.");
        let (hrp, payload) = bech32::decode(&hot).expect("The decoder cannot decode the hot ID.");
        assert_eq!(hrp.as_str(), "cc_hot");
        assert_eq!(payload[0], 0x02);

        // For a hot script, the low nibble is 0x3 (script).
        let hot_script = bech32_committee_hot(&cc_cold_script(9))
            .expect("The encoder cannot encode the hot-script ID.");
        let (_, payload) =
            bech32::decode(&hot_script).expect("The decoder cannot decode the hot-script ID.");
        assert_eq!(payload[0], 0x03);
    }

    #[tokio::test]
    async fn governance_committee_happy_path() {
        let seating = GovActionId {
            transaction_id: Hash::from([7u8; 32]),
            action_index: 3,
        };
        let seating_for_setup = seating.clone();

        let app = TestApp::new_with_cfg_and_setup(
            SyntheticBlockConfig::default(),
            move |domain, _vectors| {
                let committee = dolos_cardano::model::gov::Committee {
                    members: BTreeMap::from([
                        (cc_cold_key(1), 100),
                        (cc_cold_key(2), 200),
                        (cc_cold_script(3), 300),
                    ]),
                    threshold: RationalNumber {
                        numerator: 2,
                        denominator: 3,
                    },
                };

                let auths = BTreeMap::from([
                    (
                        cc_cold_key(1),
                        vec![(10, CommitteeAuthorization::HotCredential(cc_cold_key(11)))],
                    ),
                    (
                        cc_cold_script(3),
                        vec![(20, CommitteeAuthorization::Resigned(None))],
                    ),
                ]);

                seed_committee(domain, Some(committee), auths, Some(seating_for_setup));
            },
        );

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert_eq!(
            model.gov_action_id,
            Some(bech32_gov_action(&seating.transaction_id, seating.action_index).unwrap())
        );
        assert_eq!(model.proposal_tx_hash, Some(hex::encode([7u8; 32])));
        assert_eq!(model.proposal_index, Some(3));
        assert!(!model.is_dissolved);
        assert_eq!(model.quorum.numerator, 2);
        assert_eq!(model.quorum.denominator, 3);
        assert_eq!(model.members.len(), 3);

        // The endpoint orders the members by the raw cold-hash hex.
        let hexes: Vec<_> = model
            .members
            .iter()
            .map(|m| m.cc_cold_hex.clone())
            .collect();
        let mut sorted = hexes.clone();
        sorted.sort();
        assert_eq!(hexes, sorted);

        let authorized = &model.members[0];
        assert_eq!(authorized.cc_cold_hex, hex::encode([1u8; 28]));
        assert!(!authorized.cc_cold_has_script);
        assert!(authorized.cc_cold_id.starts_with("cc_cold1"));
        assert_eq!(authorized.status, Status::Authorized);
        assert_eq!(authorized.cc_hot_hex, Some(hex::encode([11u8; 28])));
        assert_eq!(authorized.cc_hot_has_script, Some(false));
        assert!(authorized
            .cc_hot_id
            .as_ref()
            .expect("The authorized member has no hot ID.")
            .starts_with("cc_hot1"));
        assert_eq!(authorized.expiration_epoch, 100);

        let unauthorized = &model.members[1];
        assert_eq!(unauthorized.status, Status::NotAuthorized);
        assert!(unauthorized.cc_hot_id.is_none());
        assert!(unauthorized.cc_hot_hex.is_none());
        assert!(unauthorized.cc_hot_has_script.is_none());
        assert_eq!(unauthorized.expiration_epoch, 200);

        let resigned = &model.members[2];
        assert_eq!(resigned.status, Status::Resigned);
        assert!(resigned.cc_cold_has_script);
        assert!(resigned.cc_hot_id.is_none());
        assert!(resigned.cc_hot_hex.is_none());
        assert!(resigned.cc_hot_has_script.is_none());
        assert_eq!(resigned.expiration_epoch, 300);
    }

    #[tokio::test]
    async fn governance_committee_genesis_has_no_seating_action() {
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                let committee = dolos_cardano::model::gov::Committee {
                    members: BTreeMap::from([(cc_cold_key(1), 100)]),
                    threshold: RationalNumber {
                        numerator: 1,
                        denominator: 2,
                    },
                };

                seed_committee(domain, Some(committee), BTreeMap::new(), None);
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        // The Conway-genesis committee has no seating action.
        assert!(model.gov_action_id.is_none());
        assert!(model.proposal_tx_hash.is_none());
        assert!(model.proposal_index.is_none());
        assert!(!model.is_dissolved);
        assert_eq!(model.members.len(), 1);
        assert_eq!(model.members[0].status, Status::NotAuthorized);
    }

    #[tokio::test]
    async fn governance_committee_migration_gap_is_not_dissolved() {
        // A store migrated across the in-place upgrade gap has an unknown
        // enact-state: a null committee with no lineage root. This state is not
        // a dissolution. The endpoint reports `is_dissolved` false and does not
        // read the null committee as no-confidence.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                seed_committee(domain, None, BTreeMap::new(), None);
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert!(!model.is_dissolved);
        assert!(model.members.is_empty());
        assert_eq!(model.quorum.numerator, 0);
        assert_eq!(model.quorum.denominator, 0);
    }

    #[tokio::test]
    async fn governance_committee_no_confidence_is_dissolved() {
        // A NoConfidence enactment clears the committee and writes the committee
        // lineage root. These two changes are the evidence of a dissolution.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                let action = GovActionId {
                    transaction_id: Hash::from([9u8; 32]),
                    action_index: 1,
                };
                seed_committee(domain, None, BTreeMap::new(), Some(action));
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert!(model.is_dissolved);
        assert!(model.members.is_empty());
    }

    #[tokio::test]
    async fn governance_committee_dissolved_serves_last_members() {
        // A NoConfidence enactment keeps the last-seated committee. While the
        // committee is dissolved, the endpoint serves the kept members, each
        // with the hot key it had, the kept quorum and the action that seated
        // them, not the NoConfidence the committee root now holds.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                let action = GovActionId {
                    transaction_id: Hash::from([9u8; 32]),
                    action_index: 1,
                };

                let seating = GovActionId {
                    transaction_id: Hash::from([8u8; 32]),
                    action_index: 0,
                };

                let committee = dolos_cardano::model::gov::Committee {
                    members: BTreeMap::from([(cc_cold_key(1), 100), (cc_cold_key(2), 200)]),
                    threshold: RationalNumber {
                        numerator: 2,
                        denominator: 3,
                    },
                };

                seed_gov(
                    domain,
                    GovState {
                        prev_gov_action_ids: dolos_cardano::model::gov::GovRoots {
                            committee: Some(action.clone()),
                            ..Default::default()
                        },
                        active_since: Some(0),
                        dissolved_committee: Some(dolos_cardano::model::gov::DissolvedCommittee {
                            committee,
                            auths: BTreeMap::from([(
                                cc_cold_key(1),
                                vec![(10, CommitteeAuthorization::HotCredential(cc_cold_key(11)))],
                            )]),
                            action,
                            epoch: 5,
                            seated_by: Some(seating),
                        }),
                        ..Default::default()
                    },
                );
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert!(model.is_dissolved);
        assert_eq!(
            model.gov_action_id,
            Some(bech32_gov_action(&Hash::from([8u8; 32]), 0).unwrap())
        );
        assert_eq!(model.proposal_tx_hash, Some(hex::encode([8u8; 32])));
        assert_eq!(model.proposal_index, Some(0));
        assert_eq!(model.quorum.numerator, 2);
        assert_eq!(model.quorum.denominator, 3);
        assert_eq!(model.members.len(), 2);

        let authorized = &model.members[0];
        assert_eq!(authorized.cc_cold_hex, hex::encode([1u8; 28]));
        assert_eq!(authorized.status, Status::Authorized);
        assert_eq!(authorized.cc_hot_hex, Some(hex::encode([11u8; 28])));
        assert_eq!(authorized.expiration_epoch, 100);

        let unauthorized = &model.members[1];
        assert_eq!(unauthorized.cc_cold_hex, hex::encode([2u8; 28]));
        assert_eq!(unauthorized.status, Status::NotAuthorized);
        assert!(unauthorized.cc_hot_hex.is_none());
        assert_eq!(unauthorized.expiration_epoch, 200);
    }

    #[tokio::test]
    async fn governance_committee_dissolved_genesis_has_no_seating_action() {
        // A NoConfidence that dissolves the Conway-genesis committee keeps no
        // seating action. The endpoint serves null action fields, not the
        // NoConfidence the committee root now holds.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                let action = GovActionId {
                    transaction_id: Hash::from([9u8; 32]),
                    action_index: 1,
                };

                let committee = dolos_cardano::model::gov::Committee {
                    members: BTreeMap::from([(cc_cold_key(1), 100)]),
                    threshold: RationalNumber {
                        numerator: 2,
                        denominator: 3,
                    },
                };

                seed_gov(
                    domain,
                    GovState {
                        prev_gov_action_ids: dolos_cardano::model::gov::GovRoots {
                            committee: Some(action.clone()),
                            ..Default::default()
                        },
                        active_since: Some(0),
                        dissolved_committee: Some(dolos_cardano::model::gov::DissolvedCommittee {
                            committee,
                            auths: BTreeMap::new(),
                            action,
                            epoch: 5,
                            seated_by: None,
                        }),
                        ..Default::default()
                    },
                );
            });

        let (status, body) = app.get_bytes("/governance/committee").await;
        assert_eq!(status, StatusCode::OK);

        let model: Committee = serde_json::from_slice(&body)
            .expect("The JSON parser cannot parse the committee response.");

        assert!(model.is_dissolved);
        assert_eq!(model.members.len(), 1);
        assert!(model.gov_action_id.is_none());
        assert!(model.proposal_tx_hash.is_none());
        assert!(model.proposal_index.is_none());
    }

    #[tokio::test]
    async fn governance_committee_pre_conway_returns_not_found() {
        // Before governance activates, the singleton exists but `active_since`
        // is unset. No committee exists, so the endpoint returns 404 instead of
        // an empty committee.
        let app =
            TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _vectors| {
                seed_gov(domain, GovState::default());
            });

        assert_status(&app, "/governance/committee", StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_committee_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(
            &app,
            "/governance/committee",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
