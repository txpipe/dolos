use axum::http::StatusCode;
use blockfrost_openapi::models::asset_transactions_inner::AssetTransactionsInner;
use dolos_cardano::ChainSummary;
use dolos_core::{BlockBody, Domain};
use pallas::ledger::traverse::MultiEraTx;

use crate::{
    inputs::{match_block_txs, tx_touches_output, InputDeps, InputResolver},
    mapping::assets::output_has_subject,
    pagination::{Order, Pagination},
    Facade,
};

mod addresses;
mod all;
mod by_policy;
mod by_subject;
mod history;
mod transactions;
mod txs;
mod utxos;

pub use addresses::by_subject_addresses;
pub use all::all;
pub use by_policy::by_policy;
pub use by_subject::by_subject;
pub use history::by_subject_history;
pub use transactions::by_subject_transactions;
pub use txs::by_subject_txs;
pub use utxos::by_subject_utxos;

fn tx_has_subject(
    resolver: &mut InputResolver<'_>,
    subject: &[u8],
    tx: &MultiEraTx<'_>,
) -> Result<bool, StatusCode> {
    tx_touches_output(resolver, tx, |output| {
        Ok(output_has_subject(subject, output))
    })
}

async fn find_txs<D>(
    domain: &Facade<D>,
    deps: &mut InputDeps,
    subject: &[u8],
    chain: &ChainSummary,
    pagination: &Pagination,
    block: BlockBody,
) -> Result<Vec<AssetTransactionsInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let subject = subject.to_vec();
    let matched = match_block_txs(domain, deps, pagination, block, move |resolver, tx| {
        tx_has_subject(resolver, &subject, tx)
    })
    .await?;

    let mut matches: Vec<_> = matched
        .into_iter()
        .map(|tx| AssetTransactionsInner {
            tx_hash: hex::encode(tx.hash.as_slice()),
            tx_index: tx.index as i32,
            block_height: tx.block_number as i32,
            block_time: chain.slot_time(tx.block_slot) as i32,
        })
        .collect();

    if matches!(pagination.order, Order::Desc) {
        matches = matches.into_iter().rev().collect();
    }

    Ok(matches)
}

#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use crate::test_support::TestApp;

    pub fn invalid_assets() -> Vec<String> {
        vec![
            // non-hex characters
            "not-hex-asset".to_string(),
            // valid hex, shorter than a policy id
            "abcd".to_string(),
            // valid hex, longer than a policy id plus the longest asset name
            "f".repeat(122),
            // valid length, non-hex characters
            "z".repeat(56),
        ]
    }

    pub fn missing_asset() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    /// The hex of an id with each letter in uppercase, and with only its last
    /// letter in uppercase.
    pub fn uppercase_spellings(id: &str) -> [String; 2] {
        let last = id
            .rfind(|c: char| c.is_ascii_lowercase())
            .expect("the id has a hex letter");

        let mut mixed = id.to_string();
        mixed[last..=last].make_ascii_uppercase();

        [id.to_ascii_uppercase(), mixed]
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use crate::test_support::TestApp;
    use axum::http::StatusCode;

    /// The routes under `/assets/{asset}`, as the text after the asset.
    const SUBJECT_ROUTES: [&str; 6] = [
        "",
        "/history",
        "/txs",
        "/transactions",
        "/addresses",
        "/utxos",
    ];

    /// Blockfrost finds an asset only from its lowercase hex. An uppercase
    /// digit keeps the shape of the id valid, so each route gives 404, not
    /// 400.
    #[tokio::test]
    async fn assets_by_subject_routes_find_nothing_from_uppercase_hex() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        for route in SUBJECT_ROUTES {
            assert_status(&app, &format!("/assets/{asset}{route}"), StatusCode::OK).await;

            for spelling in uppercase_spellings(asset) {
                let path = format!("/assets/{spelling}{route}");
                assert_status(&app, &path, StatusCode::NOT_FOUND).await;
            }
        }
    }

    /// An uppercase digit does not hide an error that Blockfrost reports
    /// before it looks the asset up: an id of an invalid shape and a refused
    /// query parameter stay 400.
    #[tokio::test]
    async fn assets_by_subject_routes_refuse_a_malformed_uppercase_id() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.to_ascii_uppercase();

        let malformed = [
            // valid hex, shorter than a policy id
            "F".repeat(54),
            // valid hex, longer than a policy id plus the longest asset name
            "F".repeat(122),
            // valid length, non-hex characters
            "Z".repeat(56),
        ];

        for route in SUBJECT_ROUTES {
            for id in &malformed {
                let path = format!("/assets/{id}{route}");
                assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
            }

            // `/assets/{asset}` itself takes no query parameters
            if !route.is_empty() {
                let path = format!("/assets/{asset}{route}?count=0");
                assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
            }
        }
    }
}
