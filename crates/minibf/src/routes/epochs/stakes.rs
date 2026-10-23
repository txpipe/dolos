use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_stake_content_inner::EpochStakeContentInner;
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::bech32_pool,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{current_epoch, parse_epoch, stake_distribution_page};

pub async fn by_number_stakes<D: Domain>(
    Path(number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<EpochStakeContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;
    // Each page reads every row before it, so the page depth is the scan.
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;
    let epoch = parse_epoch(&number)?;

    let (chain, current) = current_epoch(&domain)?;

    // Blockfrost 404s future epochs. Every other epoch pages the distribution
    // logged for it, the current and previous ones from the rows ESTART
    // writes as it opens each. A page is empty before the first stake epoch,
    // and empty or partial for the two latest epochs while ESTART's rows are
    // missing: on a store upgraded mid-epoch, or after a rollback to the
    // epoch's start or a restart after a crash mid-ESTART (the node warns).
    // Those heal when EWRAP writes the full rows one boundary later.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let page = stake_distribution_page(&domain, &chain, epoch, None, &pagination).await?;

    let out = page
        .into_iter()
        .map(|(stake_address, log)| {
            let pool = log.pool_id.ok_or_else(|| {
                tracing::error!("account epoch log carries stake with no pool");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            Ok(EpochStakeContentInner {
                stake_address,
                pool_id: bech32_pool(pool)?,
                amount: log.active_stake.unwrap_or(0).to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};
    use dolos_testing::toy_domain::ToyDomain;

    #[tokio::test]
    async fn epochs_stakes_happy_path() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        let path = format!("/epochs/{epoch}/stakes");
        let stakes: Vec<EpochStakeContentInner> = get_ok(&app, &path).await;

        // The seeder writes the vectors' account plus one synthetic script
        // credential (both delegated to the vectors' pool) and one
        // zero-stake credential, which must be excluded for Blockfrost
        // parity.
        assert_eq!(stakes.len(), 2);
        assert!(stakes.iter().all(|x| x.amount != "0"));

        let seeded = stakes
            .iter()
            .find(|x| x.stake_address == app.vectors().stake_address)
            .expect("seeded stake address missing from distribution");

        assert_eq!(seeded.pool_id, app.vectors().pool_id);
        assert_eq!(seeded.amount, "7000000");
    }

    #[tokio::test]
    async fn epochs_stakes_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;

        let page_1: Vec<EpochStakeContentInner> =
            get_ok(&app, &format!("/epochs/{epoch}/stakes?count=1&page=1")).await;
        let page_2: Vec<EpochStakeContentInner> =
            get_ok(&app, &format!("/epochs/{epoch}/stakes?count=1&page=2")).await;

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].stake_address, page_2[0].stake_address);
    }

    #[tokio::test]
    async fn epochs_stakes_empty_epoch() {
        let app = TestApp::new();
        // Epoch 0 is in range but nothing is seeded there.
        let stakes: Vec<EpochStakeContentInner> = get_ok(&app, "/epochs/0/stakes").await;
        assert!(stakes.is_empty());
    }

    #[tokio::test]
    async fn epochs_stakes_within_scan_limit() {
        let app = TestApp::new_with_scan_limit(Default::default(), 2);
        let epoch = app.tip_epoch() - 1;
        let stakes: Vec<EpochStakeContentInner> =
            get_ok(&app, &format!("/epochs/{epoch}/stakes?count=1&page=2")).await;
        assert_eq!(stakes.len(), 1);
    }

    #[tokio::test]
    async fn epochs_stakes_rejects_deep_page() {
        let app = TestApp::new_with_scan_limit(Default::default(), 2);
        let epoch = app.tip_epoch() - 1;
        let (status, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes?count=1&page=3"))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body["message"],
            "pagination scan limit exceeded, reduce page number or count"
        );
    }

    /// A preview store with one genesis pool and one funded delegator: RUPD
    /// in every epoch and ESTART at every boundary up to `first_shelley + 3`,
    /// whose RUPD and closing EWRAP log the first stake epoch, the tip in it.
    /// The earlier boundaries skip EWRAP, which counts the genesis pool's
    /// deposit against pots that never funded it.
    fn store_past_the_first_stake_epoch(pool: [u8; 28], delegator: [u8; 28]) -> ToyDomain {
        use dolos_cardano::{
            estart::EstartWorkUnit, ewrap::EwrapWorkUnit, rupd::RupdWorkUnit, CardanoWorkUnit,
        };
        use dolos_core::{sync::execute_work_unit, StateStore as _, StateWriter as _};

        let mut genesis = dolos_cardano::include::preview::load();
        let (pool, delegator) = (hex::encode(pool), hex::encode(delegator));

        let staking = format!(
            r#"{{
                "pools": {{
                    "{pool}": {{
                        "cost": 0,
                        "margin": 0.0,
                        "pledge": 0,
                        "publicKey": "{pool}",
                        "relays": [],
                        "rewardAccount": {{
                            "credential": {{ "keyHash": "{delegator}" }},
                            "network": "Testnet"
                        }},
                        "vrf": "{}"
                    }}
                }},
                "stake": {{ "{delegator}": "{pool}" }}
            }}"#,
            hex::encode([0x77; 32])
        );
        genesis.shelley.staking = Some(serde_json::from_str(&staking).unwrap());

        // a testnet base address whose stake part is the delegator
        genesis
            .shelley
            .initial_funds
            .get_or_insert_with(Default::default)
            .insert(format!("00{}{delegator}", hex::encode([0x01; 28])), 2_000);

        let domain = ToyDomain::new_with_genesis(std::sync::Arc::new(genesis), None, None);
        let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
        let first_shelley = chain.first_shelley_epoch();
        let epoch = || {
            dolos_cardano::load_epoch::<ToyDomain>(domain.state())
                .unwrap()
                .number
        };

        loop {
            let start = chain.epoch_start(epoch());
            let next = chain.epoch_start(epoch() + 1);

            let mut rupd =
                CardanoWorkUnit::Rupd(Box::new(RupdWorkUnit::new(start + 1, domain.genesis())));
            execute_work_unit(&domain, &mut rupd).unwrap();

            let mut boundary = if epoch() == first_shelley + 3 {
                CardanoWorkUnit::Ewrap(Box::new(EwrapWorkUnit::new(next, domain.genesis())))
            } else {
                CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(next, domain.genesis())))
            };
            execute_work_unit(&domain, &mut boundary).unwrap();

            if matches!(boundary, CardanoWorkUnit::Ewrap(_)) {
                break;
            }
        }

        let writer = domain.state().start_writer().unwrap();
        let tip = chain.epoch_start(first_shelley + 3) + 1;
        writer
            .set_cursor(dolos_testing::slot_to_chainpoint(tip))
            .unwrap();
        writer.commit().unwrap();

        domain
    }

    /// The first stake epoch lists the distribution of the first Shelley
    /// epoch: the genesis pool's delegator, with its genesis funds.
    #[tokio::test]
    async fn epochs_stakes_first_stake_epoch_lists_the_genesis_pool() {
        let (pool, delegator) = ([0x71; 28], [0x72; 28]);

        let app = TestApp::new_with_cfg_and_domain(Default::default(), |_| {
            store_past_the_first_stake_epoch(pool, delegator)
        });

        let chain = app.facade().get_chain_summary().unwrap();
        let first_stake = chain.first_shelley_epoch() + 2;

        let stakes: Vec<EpochStakeContentInner> =
            get_ok(&app, &format!("/epochs/{first_stake}/stakes")).await;

        let address = pallas::ledger::addresses::StakeAddress::new(
            pallas::ledger::addresses::Network::Testnet,
            pallas::ledger::addresses::StakePayload::Stake(delegator.into()),
        );

        assert_eq!(stakes.len(), 1, "{stakes:?}");
        assert_eq!(stakes[0].stake_address, address.to_bech32().unwrap());
        assert_eq!(stakes[0].pool_id, bech32_pool(pool).unwrap());
        assert_eq!(stakes[0].amount, "2000");
    }

    /// The two latest epochs list their distributions before the EWRAP that
    /// writes their full rows has run, from the rows ESTART logged as it
    /// opened each. The next epoch stays a 404.
    #[tokio::test]
    async fn epochs_stakes_lists_the_two_latest_epochs() {
        use dolos_cardano::{estart::EstartWorkUnit, CardanoWorkUnit};
        use dolos_core::{sync::execute_work_unit, StateStore as _, StateWriter as _};

        let (pool, delegator) = ([0x73; 28], [0x74; 28]);

        let app = TestApp::new_with_cfg_and_domain(Default::default(), |_| {
            let domain = store_past_the_first_stake_epoch(pool, delegator);
            let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
            let epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state())
                .unwrap()
                .number;
            let next = chain.epoch_start(epoch + 1);

            let mut estart =
                CardanoWorkUnit::Estart(Box::new(EstartWorkUnit::new(next, domain.genesis())));
            execute_work_unit(&domain, &mut estart).unwrap();

            let writer = domain.state().start_writer().unwrap();
            writer
                .set_cursor(dolos_testing::slot_to_chainpoint(next + 1))
                .unwrap();
            writer.commit().unwrap();

            domain
        });

        let tip = app.tip_epoch();
        let first_stake = app
            .facade()
            .get_chain_summary()
            .unwrap()
            .first_shelley_epoch()
            + 2;
        // EWRAP last closed `first_stake + 1`, writing `first_stake`'s full
        // rows; the two latest epochs have only ESTART's.
        assert_eq!(tip, first_stake + 2);

        for epoch in [tip - 1, tip] {
            let stakes: Vec<EpochStakeContentInner> =
                get_ok(&app, &format!("/epochs/{epoch}/stakes")).await;

            assert_eq!(stakes.len(), 1, "epoch {epoch}: {stakes:?}");
            assert_eq!(stakes[0].pool_id, bech32_pool(pool).unwrap());
            assert_eq!(stakes[0].amount, "2000");
        }

        assert_status(
            &app,
            &format!("/epochs/{}/stakes", tip + 1),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_stakes_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/not-a-number/stakes", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_stakes_not_found() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/999999/stakes", StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_stakes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/epochs/0/stakes", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_stakes_archive_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/epochs/0/stakes", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_stakes_out_of_range() {
        // Past the `i32` range Blockfrost serves, the epoch is malformed, not
        // missing.
        let app = TestApp::new();
        assert_status(&app, "/epochs/2147483648/stakes", StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            "/epochs/18446744073709551615/stakes",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
}
