use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use dolos_cardano::{
    model::{AccountState, ProposalState},
    periods::stats,
};
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::{address_stats::AccountAddressesTotalBody, direct_json::DirectJson},
    Facade,
};

use super::{account_exists, parse_account_key_param};

/// `GET /accounts/{stake_address}/addresses/total`: lifetime sums and tx count
/// across every address of an account, from the Stats period logs through the
/// totals cache.
///
/// Pointer addresses are out of scope: an output's account is resolved
/// through `pallas_extras::address_account`, which has no answer for a
/// pointer, as for every other account endpoint.
pub async fn by_stake_addresses_total<D>(
    Path(stake_address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<DirectJson, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;

    if !account_exists(&domain, &account_key.account)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let chain = domain.get_chain_summary()?;
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);
    let epochs = chain.first_shelley_epoch()..=tip_epoch;
    let account = account_key.account.clone();

    let stake_address = account_key
        .address
        .to_bech32()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let body = async {
        let totals = stats::cached_account_totals(&domain.inner, &chain, epochs, &account).await?;

        domain
            .query()
            .run_blocking(move |_| {
                Ok(DirectJson::encode(&AccountAddressesTotalBody {
                    stake_address,
                    totals,
                }))
            })
            .await
    }
    .await
    .map_err(|err| {
        tracing::error!(?err);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{totals_bodies, TestApp, TestFault},
    };
    use axum::http::header;
    use blockfrost_openapi::models::{
        account_addresses_total::AccountAddressesTotal,
        account_addresses_total_received_sum_inner::AccountAddressesTotalReceivedSumInner,
    };
    use dolos_testing::{synthetic::SyntheticBlockConfig, MIN_UTXO_AMOUNT};

    /// The written body is byte for byte what the serde path encodes.
    fn assert_serde_bytes(item: &AccountAddressesTotal, bytes: &[u8]) {
        let expected = serde_json::to_vec(item).unwrap();
        assert_eq!(
            String::from_utf8_lossy(bytes),
            String::from_utf8_lossy(&expected)
        );
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_total_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses/total");
        let (status, headers, bytes) = app.get_with_headers(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AccountAddressesTotal =
            serde_json::from_slice(&bytes).expect("failed to parse account addresses total");

        assert_serde_bytes(&item, &bytes);

        assert_eq!(item.stake_address, stake_address);
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");

        // every synthetic tx produces a single output to one of the account
        // addresses, so the account totals cover all txs in all blocks
        let tx_count: usize = app.vectors().blocks.iter().map(|x| x.tx_hashes.len()).sum();
        assert_eq!(item.tx_count, tx_count as i32);

        assert_eq!(item.received_sum[0].unit, "lovelace");
        assert_eq!(
            item.received_sum[0].quantity,
            (tx_count as u64 * dolos_testing::MIN_UTXO_AMOUNT).to_string()
        );

        let asset = item
            .received_sum
            .iter()
            .find(|x| x.unit == app.vectors().asset_unit)
            .expect("expected synthetic asset in received_sum");
        assert_eq!(asset.quantity, tx_count.to_string());

        // the synthetic account addresses never spend, but lovelace must
        // still be present (and first) with a zero quantity
        assert_eq!(item.sent_sum.len(), 1);
        assert_eq!(item.sent_sum[0].unit, "lovelace");
        assert_eq!(item.sent_sum[0].quantity, "0");
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_total_counts_spends() {
        // the default seed address is payment-only and its UTxOs have no
        // producing tx in the archive, so the account never spends. Funding
        // each tx from the previous block's output instead gives every tx
        // after the first block a resolvable input the account owns.
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            spend_previous_outputs: true,
            ..Default::default()
        });

        let stake_address = app.vectors().stake_address.as_str();
        let asset_unit = app.vectors().asset_unit.clone();
        let path = format!("/accounts/{stake_address}/addresses/total");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AccountAddressesTotal =
            serde_json::from_slice(&bytes).expect("failed to parse account addresses total");

        assert_serde_bytes(&item, &bytes);

        let quantity = |sums: &[AccountAddressesTotalReceivedSumInner], unit: &str| {
            sums.iter()
                .find(|x| x.unit == unit)
                .unwrap_or_else(|| panic!("missing {unit} in sums"))
                .quantity
                .clone()
        };

        // a tx that both pays and spends an account address is still one tx
        assert_eq!(item.tx_count, 6);

        // every tx pays one account address
        assert_eq!(
            quantity(&item.received_sum, "lovelace"),
            (6 * MIN_UTXO_AMOUNT).to_string()
        );
        assert_eq!(quantity(&item.received_sum, &asset_unit), "6");

        // the two blocks that follow one spend it, two txs each
        assert_eq!(
            quantity(&item.sent_sum, "lovelace"),
            (4 * MIN_UTXO_AMOUNT).to_string()
        );
        assert_eq!(quantity(&item.sent_sum, &asset_unit), "4");
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_total_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses/total", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_total_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses/total", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    /// An account's body is the same bytes with the totals cache off, missed
    /// and hit.
    #[tokio::test]
    async fn accounts_by_stake_addresses_total_bodies_match_with_the_cache_off_missed_and_hit() {
        let [off, miss, hit] =
            totals_bodies(|vectors| format!("/accounts/{}/addresses/total", vectors.stake_address))
                .await;

        let item: AccountAddressesTotal = serde_json::from_slice(&off).unwrap();
        assert!(item.tx_count > 0);

        assert_eq!(
            String::from_utf8_lossy(&miss),
            String::from_utf8_lossy(&off)
        );
        assert_eq!(String::from_utf8_lossy(&hit), String::from_utf8_lossy(&off));
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_total_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses/total");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
