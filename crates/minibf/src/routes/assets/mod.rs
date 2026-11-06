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
