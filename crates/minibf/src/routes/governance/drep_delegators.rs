use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::drep_delegators_inner::DrepDelegatorsInner;
use dolos_cardano::model::{AccountState, DRepState};
use dolos_core::{BlockSlot, Domain, EntityKey, TxOrder};
use pallas::ledger::primitives::{conway::DRep, StakeCredential};

use crate::{
    error::Error,
    mapping::{
        governance::dreps::{drep_is_retired, parse_drep_id, DrepDelegatorModelBuilder},
        IntoModel,
    },
    pagination::{Order, Pagination, PaginationParameters},
    scan::{EntityScan, ENTITY_CHUNK},
    Facade,
};

use super::drep_key;

/// One account that delegates its vote to the requested DRep.
struct DrepDelegatorRow {
    delegated_at: Option<(BlockSlot, TxOrder)>,
    key: EntityKey,
    delegator: StakeCredential,
    live_stake: u64,
}

impl DrepDelegatorRow {
    fn new(key: EntityKey, account: AccountState) -> Self {
        Self {
            delegated_at: account.vote_delegated_at,
            key,
            live_stake: account.live_stake(),
            delegator: account.credential,
        }
    }

    /// Blockfrost orders delegators by their latest vote delegation.
    fn sort_key(&self) -> (Option<(BlockSlot, TxOrder)>, &EntityKey) {
        (self.delegated_at, &self.key)
    }
}

/// Whether Blockfrost counts `account` as a delegator of `drep`.
///
/// A delegation made before the DRep's latest registration does not count.
fn delegates_to(
    account: &AccountState,
    drep: &DRep,
    registered_at: Option<(BlockSlot, TxOrder)>,
) -> bool {
    if account.delegated_drep_live() != Some(drep) {
        return false;
    }

    match registered_at {
        Some(cutoff) => account.vote_delegated_at.is_some_and(|at| at >= cutoff),
        None => true,
    }
}

/// The accounts that Blockfrost counts as delegators of `drep`, in key order,
/// `chunk` accounts per blocking task.
async fn load_drep_delegator_rows<D: Domain>(
    domain: &Facade<D>,
    drep: DRep,
    registered_at: Option<(BlockSlot, TxOrder)>,
    chunk: usize,
) -> Result<Vec<DrepDelegatorRow>, StatusCode> {
    EntityScan::<AccountState>::all(chunk)
        .filter_map(domain, move |key, account| {
            delegates_to(&account, &drep, registered_at)
                .then(|| DrepDelegatorRow::new(EntityKey::of(&key), account))
        })
        .await
}

pub async fn drep_delegators<D>(
    Path(drep_id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepDelegatorsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
    Option<DRepState>: From<D::Entity>,
{
    let parsed = parse_drep_id(&drep_id)?;
    let pagination = Pagination::try_from(params)?;

    let drep_key = drep_key(&parsed.encoded)?;
    let drep = drep_key.0.clone();

    let registered_at = if parsed.is_special {
        None
    } else {
        match domain.read_cardano_entity::<DRepState>(&drep_key)? {
            Some(state) if !drep_is_retired(&state) => state.registered_at,
            // Blockfrost returns an empty list for an unknown or retired DRep.
            _ => return Ok(Json(vec![])),
        }
    };

    let network = domain.get_network_id()?;

    let mut rows = load_drep_delegator_rows(&domain, drep, registered_at, ENTITY_CHUNK).await?;

    rows.sort_unstable_by(|a, b| a.sort_key().cmp(&b.sort_key()));

    if matches!(pagination.order, Order::Desc) {
        rows.reverse();
    }

    let page = rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|row| {
            DrepDelegatorModelBuilder {
                delegator: row.delegator,
                live_stake: row.live_stake,
                network,
            }
            .into_model()
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::{bech32_drep, stake_cred_to_address},
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use dolos_cardano::model::{DRepDelegation, DRepKey, EpochValue, Stake};
    use dolos_core::{StateStore as _, StateWriter as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::ledger::{
        addresses::Network,
        primitives::{conway::DRep, Epoch},
    };

    fn drep_delegators_path(drep: &str) -> String {
        format!("/governance/dreps/{drep}/delegators")
    }

    async fn get_drep_delegators(app: &TestApp, path: &str) -> Vec<DrepDelegatorsInner> {
        let (status, body) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body).expect("failed to parse drep delegators")
    }

    /// Overwrites the synthetic DRep state with the given registration bounds.
    fn seed_drep(
        domain: &ToyDomain,
        registered_at: Option<(BlockSlot, TxOrder)>,
        unregistered_at: Option<(BlockSlot, TxOrder)>,
    ) {
        let drep = synthetic_drep();
        let mut state = DRepState::new(drep.clone());
        state.registered_at = registered_at;
        state.unregistered_at = unregistered_at;

        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&DRepKey::from(&drep), &state)
            .expect("failed to write drep");
        writer.commit().expect("failed to commit drep");
    }

    /// Slot and tx order of the synthetic account's latest vote delegation.
    /// Every synthetic tx carries the delegation, so it is the last tx of the
    /// last block.
    fn last_vote_delegation(
        vectors: &dolos_testing::synthetic::SyntheticVectors,
    ) -> (BlockSlot, TxOrder) {
        let last = vectors.blocks.last().expect("synthetic chain has blocks");
        (last.slot, last.tx_hashes.len() - 1)
    }

    fn tip_epoch(domain: &ToyDomain) -> Epoch {
        let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
            .expect("era summary");
        let tip = domain
            .state()
            .read_cursor()
            .expect("cursor read failed")
            .expect("missing tip")
            .slot();
        summary.slot_epoch(tip).0
    }

    fn seeded_credential(seed: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash([seed; 28].into())
    }

    fn seeded_stake_address(seed: u8) -> String {
        stake_cred_to_address(&seeded_credential(seed), Network::Testnet)
            .to_bech32()
            .expect("failed to encode stake address")
    }

    /// Writes an account that holds `utxo_sum` lovelace and delegates its
    /// vote to `drep` at `delegated_at`.
    fn seed_delegator(
        domain: &ToyDomain,
        seed: u8,
        drep: DRep,
        delegated_at: (BlockSlot, TxOrder),
        utxo_sum: u64,
    ) {
        let epoch = tip_epoch(domain);
        let credential = seeded_credential(seed);

        let mut account = AccountState::new(epoch, credential.clone());
        account.registered_at = Some(delegated_at.0);
        account.stake = EpochValue::with_live(
            epoch,
            Stake {
                utxo_sum,
                ..Default::default()
            },
        );
        account.drep = EpochValue::with_live(epoch, DRepDelegation::Delegated(drep));
        account.vote_delegated_at = Some(delegated_at);

        let key = dolos_cardano::AccountKey::from(&credential);
        let writer = domain
            .state()
            .start_writer()
            .expect("failed to start writer");
        writer
            .write_entity_typed(&key, &account)
            .expect("failed to write account");
        writer.commit().expect("failed to commit account");
    }

    fn delegator(seed: u8, amount: u64) -> DrepDelegatorsInner {
        DrepDelegatorsInner {
            address: seeded_stake_address(seed),
            amount: amount.to_string(),
        }
    }

    #[tokio::test]
    async fn governance_drep_delegators_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();

        let (status, body) = app.get_bytes(&format!("/accounts/{stake_address}")).await;
        assert_eq!(status, StatusCode::OK);
        let account: blockfrost_openapi::models::account_content::AccountContent =
            serde_json::from_slice(&body).expect("failed to parse account");

        let legacy = get_drep_delegators(&app, &drep_delegators_path(&app.vectors().drep_id)).await;
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].address, stake_address);
        assert_eq!(legacy[0].amount, account.controlled_amount);

        let cip129_id = bech32_drep(&synthetic_drep()).expect("failed to encode drep id");
        let cip129 = get_drep_delegators(&app, &drep_delegators_path(&cip129_id)).await;
        assert_eq!(cip129, legacy);
    }

    /// Three seeded delegators join the synthetic one, each at a later
    /// position. A fourth one delegated before the DRep registration and
    /// must not appear.
    #[tokio::test]
    async fn governance_drep_delegators_paginated() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (slot, _) = last_vote_delegation(vectors);
            let drep = synthetic_drep();
            seed_delegator(domain, 0x41, drep.clone(), (slot - 1, 0), 1_000);
            seed_delegator(domain, 0x42, drep.clone(), (slot + 1, 0), 2_000);
            seed_delegator(domain, 0x43, drep.clone(), (slot + 1, 1), 3_000);
            seed_delegator(domain, 0x44, drep, (slot + 2, 0), 4_000);
        });
        let base = drep_delegators_path(&app.vectors().drep_id);

        let asc = get_drep_delegators(&app, &base).await;
        assert_eq!(asc.len(), 4);
        assert_eq!(asc[0].address, app.vectors().stake_address);
        assert_eq!(
            asc[1..],
            [
                delegator(0x42, 2_000),
                delegator(0x43, 3_000),
                delegator(0x44, 4_000)
            ]
        );

        let page_1 = get_drep_delegators(&app, &format!("{base}?count=3&page=1")).await;
        let page_2 = get_drep_delegators(&app, &format!("{base}?count=3&page=2")).await;
        let page_3 = get_drep_delegators(&app, &format!("{base}?count=3&page=3")).await;
        assert_eq!(page_1, asc[..3]);
        assert_eq!(page_2, asc[3..]);
        assert!(page_3.is_empty());

        let desc = get_drep_delegators(&app, &format!("{base}?order=desc")).await;
        let reversed: Vec<_> = asc.iter().rev().cloned().collect();
        assert_eq!(desc, reversed);

        let desc_page_2 =
            get_drep_delegators(&app, &format!("{base}?order=desc&count=3&page=2")).await;
        assert_eq!(desc_page_2, reversed[3..]);
    }

    #[tokio::test]
    async fn governance_drep_delegators_bad_request() {
        let app = TestApp::new();
        let path = drep_delegators_path(invalid_drep());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        let path = format!("{}?count=0", drep_delegators_path(&app.vectors().drep_id));
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    /// Blockfrost answers `200 []` for a well-formed DRep id it has never seen.
    #[tokio::test]
    async fn governance_drep_delegators_unknown_drep_is_empty() {
        let app = TestApp::new();
        let rows = get_drep_delegators(&app, &drep_delegators_path(&missing_drep())).await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn governance_drep_delegators_special_dreps() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (slot, _) = last_vote_delegation(vectors);
            seed_delegator(domain, 0x51, DRep::Abstain, (slot, 0), 5_000);
            seed_delegator(domain, 0x52, DRep::NoConfidence, (slot, 1), 6_000);
        });

        let abstain = get_drep_delegators(&app, &drep_delegators_path("drep_always_abstain")).await;
        assert_eq!(abstain, [delegator(0x51, 5_000)]);

        let no_confidence =
            get_drep_delegators(&app, &drep_delegators_path("drep_always_no_confidence")).await;
        assert_eq!(no_confidence, [delegator(0x52, 6_000)]);

        // The seeded accounts do not leak into a regular DRep's list.
        let regular =
            get_drep_delegators(&app, &drep_delegators_path(&app.vectors().drep_id)).await;
        assert_eq!(regular.len(), 1);
        assert_eq!(regular[0].address, app.vectors().stake_address);
    }

    #[tokio::test]
    async fn governance_drep_delegators_retired_drep_is_empty() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, _| {
            seed_drep(domain, Some((1, 0)), Some((2, 0)))
        });

        let rows = get_drep_delegators(&app, &drep_delegators_path(&app.vectors().drep_id)).await;
        assert!(rows.is_empty());
    }

    /// A delegation in the same tx as the DRep registration counts. One made
    /// before the DRep's latest registration does not.
    #[tokio::test]
    async fn governance_drep_delegators_honor_registration_cutoff() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        let same_tx = TestApp::new_with_cfg_and_setup(cfg.clone(), |domain, vectors| {
            seed_drep(domain, Some(last_vote_delegation(vectors)), None)
        });
        let rows =
            get_drep_delegators(&same_tx, &drep_delegators_path(&same_tx.vectors().drep_id)).await;
        assert_eq!(rows.len(), 1);

        let reregistered = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (slot, order) = last_vote_delegation(vectors);
            seed_drep(domain, Some((slot, order + 1)), None)
        });
        let rows = get_drep_delegators(
            &reregistered,
            &drep_delegators_path(&reregistered.vectors().drep_id),
        )
        .await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn governance_drep_delegators_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = drep_delegators_path(&app.vectors().drep_id);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn chunked_drep_delegator_rows_match_unchunked() {
        let app = crate::test_support::replicated_app();
        let facade = app.facade();
        let drep = synthetic_drep();
        let registered_at = facade
            .read_cardano_entity::<DRepState>(&DRepKey::from(&drep))
            .expect("drep read")
            .expect("drep state")
            .registered_at;

        let rows = |registered_at, chunk| {
            let facade = facade.clone();
            let drep = drep.clone();
            async move {
                load_drep_delegator_rows(&facade, drep, registered_at, chunk)
                    .await
                    .expect("delegator rows")
                    .into_iter()
                    .map(|row| (row.key.to_string(), row.live_stake))
                    .collect::<Vec<_>>()
            }
        };

        assert!(
            rows(None, usize::MAX).await.len() > 2,
            "too few delegators to split into chunks"
        );

        for registered_at in [None, registered_at] {
            let whole = rows(registered_at, usize::MAX).await;

            for chunk in [1, 2] {
                assert_eq!(rows(registered_at, chunk).await, whole);
            }
        }
    }
}
