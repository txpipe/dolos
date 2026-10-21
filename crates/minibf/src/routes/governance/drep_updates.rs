use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::drep_updates_inner::{self, DrepUpdatesInner};
use dolos_cardano::{indexes::CardanoArchiveIndexExt as _, pallas_extras};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    mapping::{governance::dreps::parse_drep_id, i32_or_500},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::collect_block_rows;

/// This function returns the certificates of one DRep in one block, in
/// Blockfrost order: the position of the transaction first, then the position
/// of the certificate in the transaction.
///
/// The ledger does not apply the certificates of a phase-2-invalid
/// transaction, so the function skips those transactions.
fn drep_updates_in_block(
    block: &MultiEraBlock,
    drep: &[u8],
) -> Result<Vec<DrepUpdatesInner>, StatusCode> {
    use drep_updates_inner::Action;

    let mut out = Vec::new();

    for tx in block.txs() {
        if !tx.is_valid() {
            continue;
        }

        for (cert_index, cert) in tx.certs().iter().enumerate() {
            let update = if let Some(reg) = pallas_extras::cert_as_drep_registration(cert) {
                Some((reg.cred, Action::Registered, Some(reg.deposit.to_string())))
            } else if let Some(unreg) = pallas_extras::cert_as_drep_unregistration(cert) {
                Some((unreg.cred, Action::Deregistered, None))
            } else {
                pallas_extras::cert_as_drep_update(cert).map(|cred| (cred, Action::Updated, None))
            };

            let Some((cred, action, deposit)) = update else {
                continue;
            };

            // A block that is tagged for this DRep can also hold the
            // certificates of other DReps.
            if pallas_extras::drep_id_bytes(&cred) != drep {
                continue;
            }

            out.push(DrepUpdatesInner {
                tx_hash: tx.hash().to_string(),
                cert_index: i32_or_500(cert_index)?,
                action,
                deposit,
            });
        }
    }

    Ok(out)
}

/// This function reads the certificate blocks of one DRep from the archive, in
/// the requested order, and returns its first `needed` updates.
fn read_drep_updates<D: Domain>(
    domain: &D,
    drep: &[u8],
    tip: BlockSlot,
    descending: bool,
    needed: usize,
    mut budget: usize,
) -> Result<Vec<DrepUpdatesInner>, Error> {
    let archive = domain.archive();

    let slots = archive
        .slots_by_drep_certs(drep, 0, tip)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let slots: Box<dyn Iterator<Item = _>> = if descending {
        Box::new(slots.rev())
    } else {
        Box::new(slots)
    };

    collect_block_rows(
        slots,
        |slot| archive.get_block_by_slot(&slot),
        descending,
        needed,
        &mut budget,
        |block| drep_updates_in_block(block, drep),
    )
}

/// `GET /governance/dreps/{drep_id}/updates` lists the registration,
/// deregistration and update certificates of a DRep in chain order.
///
/// Blockfrost has no existence check on this endpoint. An unknown DRep, and
/// the two special DReps that have no certificates, return an empty list. They
/// do not return 404.
///
/// The `drep_certs` archive dimension gives the blocks that hold certificates
/// of the DRep. The endpoint reads blocks only until the requested page is
/// full. `max_scan_items` limits both the page depth and the number of blocks
/// that one request reads.
pub async fn drep_updates<D>(
    Path(drep_id): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepUpdatesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    // Drop `from`/`to` before validation: Blockfrost never reads them here,
    // so a malformed or reversed window is ignored rather than rejected.
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let parsed = parse_drep_id(&drep_id)?;

    if parsed.is_special {
        return Ok(Json(Vec::new()));
    }

    let drep_bytes = parsed.encoded;

    let tip = domain.get_tip_slot()?;
    let descending = matches!(pagination.order, Order::Desc);
    let needed = pagination.to();
    let budget = domain.config.max_scan_items() as usize;

    let rows = domain
        .query()
        .run_blocking(move |domain| {
            Ok(read_drep_updates(
                &domain,
                &drep_bytes,
                tip,
                descending,
                needed,
                budget,
            ))
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    let page = rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect();

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::bech32_drep,
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;
    use pallas::{
        crypto::hash::Hash,
        ledger::primitives::{conway::DRep, StakeCredential},
    };

    /// Three blocks with one tx each. Every synthetic tx registers the DRep in
    /// certificate 3. The tx of block 1 also updates the DRep, and the tx of
    /// block 2 also deregisters it, both in certificate 5.
    fn drep_updates_app() -> TestApp {
        use pallas::ledger::primitives::conway::Certificate;

        let cred = StakeCredential::AddrKeyhash(Hash::from([7u8; 28]));

        TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            extra_certs_by_block: vec![
                vec![],
                vec![vec![Certificate::UpdateDRepCert(cred.clone(), None)]],
                vec![vec![Certificate::UnRegDRepCert(cred, 1000)]],
            ],
            ..Default::default()
        })
    }

    fn expected_drep_updates(app: &TestApp) -> Vec<DrepUpdatesInner> {
        use drep_updates_inner::Action;

        let row = |block: usize, cert_index: i32, action: Action, deposit: Option<&str>| {
            DrepUpdatesInner {
                tx_hash: tx_hash_of_block(app, block),
                cert_index,
                action,
                deposit: deposit.map(str::to_string),
            }
        };

        vec![
            row(0, 3, Action::Registered, Some("1000")),
            row(1, 3, Action::Registered, Some("1000")),
            row(1, 5, Action::Updated, None),
            row(2, 3, Action::Registered, Some("1000")),
            row(2, 5, Action::Deregistered, None),
        ]
    }

    async fn get_drep_updates(app: &TestApp, drep: &str, query: &str) -> Vec<DrepUpdatesInner> {
        let path = format!("/governance/dreps/{drep}/updates{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "The request to {path} returned status {status}. The response body was {}.",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes)
            .expect("The DRep update response did not contain valid JSON.")
    }

    #[tokio::test]
    async fn governance_drep_updates_happy_path() {
        let app = drep_updates_app();
        let drep = app.vectors().drep_id.clone();

        assert_eq!(
            get_drep_updates(&app, &drep, "").await,
            expected_drep_updates(&app)
        );
    }

    #[tokio::test]
    async fn governance_drep_updates_orders_and_paginates() {
        let app = drep_updates_app();
        let drep = app.vectors().drep_id.clone();
        let expected = expected_drep_updates(&app);
        let reversed = expected.iter().rev().cloned().collect_vec();

        // desc is the exact reverse of asc, inside a tx too
        assert_eq!(get_drep_updates(&app, &drep, "?order=desc").await, reversed);

        assert_eq!(
            get_drep_updates(&app, &drep, "?count=2&page=2").await,
            expected[2..4].to_vec()
        );
        assert_eq!(
            get_drep_updates(&app, &drep, "?count=2&page=1&order=desc").await,
            reversed[..2].to_vec()
        );
        assert_eq!(
            get_drep_updates(&app, &drep, "?count=2&page=3").await,
            expected[4..].to_vec()
        );

        // a page past the end is empty, not an error
        assert!(get_drep_updates(&app, &drep, "?count=2&page=4")
            .await
            .is_empty());
    }

    /// Blockfrost has no existence check on this endpoint, so every
    /// well-formed id without certificates gets an empty list. That includes a
    /// script DRep with the same hash as the registered key DRep.
    #[tokio::test]
    async fn governance_drep_updates_without_rows() {
        let app = drep_updates_app();
        let script_drep = bech32_drep(&DRep::Script(Hash::from([7u8; 28]))).unwrap();

        for drep in [
            missing_drep(),
            script_drep,
            "drep_always_abstain".to_string(),
            "drep_always_no_confidence".to_string(),
        ] {
            assert!(
                get_drep_updates(&app, &drep, "").await.is_empty(),
                "{drep} must list no updates"
            );
        }
    }

    #[tokio::test]
    async fn governance_drep_updates_bad_request() {
        let app = drep_updates_app();
        let base = format!("/governance/dreps/{}/updates", app.vectors().drep_id);

        assert_status(&app, &format!("{base}?count=0"), StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            &format!("{base}?order=sideways"),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/governance/dreps/not-a-drep/updates",
            StatusCode::BAD_REQUEST,
        )
        .await;

        // Blockfrost decodes the id as bech32 only, so a hex id is a 400
        let hex_id = hex::encode([&[0x22u8][..], &[7u8; 28]].concat());
        let path = format!("/governance/dreps/{hex_id}/updates");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_updates_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/governance/dreps/{}/updates", app.vectors().drep_id);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    fn drep_updates_test_drep() -> Vec<u8> {
        pallas_extras::drep_id_bytes(&StakeCredential::AddrKeyhash(Hash::from([7u8; 28])))
    }

    /// The ledger does not apply the certificates of a phase-2-invalid tx,
    /// so its DRep certificates are not updates.
    #[test]
    fn drep_updates_skip_invalid_transactions() {
        let rows = |valid: bool| {
            let body = drep_registration_block(valid);
            let block = MultiEraBlock::decode(&body).unwrap();
            drep_updates_in_block(&block, &drep_updates_test_drep()).unwrap()
        };

        assert_eq!(rows(true).len(), 1);
        assert!(rows(false).is_empty());
    }

    /// A tagged block whose DRep certificate sits in a phase-2-invalid tx adds
    /// no row, but the scan still counts it. Enough of those blocks stop the
    /// request instead of letting it read without end.
    #[test]
    fn drep_updates_scan_stops_at_the_budget() {
        let drep = drep_updates_test_drep();
        let updates = |block: &MultiEraBlock| drep_updates_in_block(block, &drep);

        let slots = || (0..3).map(Ok);
        let fetch = |slot| Ok(Some(drep_registration_block(slot == 2)));

        // two empty blocks spend a budget of two before the row appears
        assert!(matches!(
            collect_block_rows(slots(), fetch, false, 1, &mut 2, updates),
            Err(Error::ScanBudgetExceeded)
        ));

        // a budget of three reaches the block that holds the row
        let rows = collect_block_rows(slots(), fetch, false, 1, &mut 3, updates).unwrap();
        assert_eq!(rows.len(), 1);

        // a full page stops the scan before the budget matters
        let fetch = |slot| Ok(Some(drep_registration_block(slot == 0)));
        let rows = collect_block_rows(slots(), fetch, false, 1, &mut 1, updates).unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// Blockfrost declares no `from`/`to` for this endpoint, so a malformed
    /// or reversed window is ignored, not rejected.
    #[tokio::test]
    async fn governance_drep_updates_ignores_from_and_to() {
        let app = drep_updates_app();
        let drep = app.vectors().drep_id.clone();
        let expected = expected_drep_updates(&app);

        for query in ["?from=bad", "?to=bad", "?from=200&to=100"] {
            assert_eq!(
                get_drep_updates(&app, &drep, query).await,
                expected,
                "{query} must be ignored"
            );
        }
    }
}
