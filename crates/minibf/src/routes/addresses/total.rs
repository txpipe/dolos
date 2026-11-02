use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use dolos_cardano::{periods::stats, AddressKey, PaymentKey};
use dolos_core::Domain;
use itertools::Either;
use pallas::ledger::addresses::Address;

use crate::{
    error::Error,
    mapping::{address_stats::AddressTotalBody, direct_json::DirectJson},
    Facade,
};

use super::{parse_address, ParsedAddress};

/// `GET /addresses/{address}/total`: lifetime sums and tx count of an address
/// or of a payment credential's addresses, from the Stats period logs through
/// the totals cache.
pub async fn total<D>(
    Path(address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<DirectJson, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let parsed = parse_address(&address)?;

    let chain = domain.get_chain_summary()?;
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);
    let shelley = chain.first_shelley_epoch();

    let query = match parsed {
        ParsedAddress::Payment { key, .. } => match <[u8; 28]>::try_from(key.as_slice()) {
            Ok(hash) => Either::Left(PaymentKey(hash.into())),
            Err(_) => return Err(StatusCode::NOT_FOUND.into()),
        },
        ParsedAddress::Shelley { key, .. } | ParsedAddress::Byron { key } => {
            let address =
                Address::from_bytes(&key).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Either::Right(address)
        }
    };

    let body = async {
        let totals = match &query {
            Either::Left(payment) => {
                stats::cached_payment_totals(&domain.inner, &chain, shelley..=tip_epoch, payment)
                    .await?
            }
            Either::Right(parsed) => {
                let from = match parsed {
                    Address::Byron(_) => 0,
                    _ => shelley,
                };

                let key = AddressKey::of(parsed);
                stats::cached_address_totals(&domain.inner, &chain, from..=tip_epoch, &key).await?
            }
        };

        domain
            .query()
            .run_blocking(move |_| {
                Ok((totals.tx_count > 0)
                    .then(|| DirectJson::encode(&AddressTotalBody { address, totals })))
            })
            .await
    }
    .await
    .map_err(|err| {
        tracing::error!(?err);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    body.ok_or_else(|| StatusCode::NOT_FOUND.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::addresses::testing::*,
        test_support::{totals_bodies, TestApp, TestFault},
    };
    use axum::http::header;
    use blockfrost_openapi::models::{
        address_content_total::AddressContentTotal,
        tx_content_output_amount_inner::TxContentOutputAmountInner,
    };
    use dolos_testing::synthetic::SyntheticVectors;

    /// The written body is byte for byte what the serde path encodes.
    fn assert_serde_bytes(item: &AddressContentTotal, bytes: &[u8]) {
        let expected = serde_json::to_vec(item).unwrap();
        assert_eq!(
            String::from_utf8_lossy(bytes),
            String::from_utf8_lossy(&expected)
        );
    }

    fn assert_total_sums(app: &TestApp, item: &AddressContentTotal) {
        let block_count = app.vectors().blocks.len();

        assert_eq!(item.tx_count, block_count as i32);

        assert_eq!(item.received_sum[0].unit, "lovelace");
        assert_eq!(
            item.received_sum[0].quantity,
            (block_count as u64 * dolos_testing::MIN_UTXO_AMOUNT).to_string()
        );

        let asset = item
            .received_sum
            .iter()
            .find(|x| x.unit == app.vectors().asset_unit)
            .expect("expected synthetic asset in received_sum");
        assert_eq!(asset.quantity, block_count.to_string());

        // the synthetic address never spends, but lovelace must still be
        // present (and first) with a zero quantity
        assert_eq!(
            item.sent_sum,
            vec![TxContentOutputAmountInner {
                unit: "lovelace".to_string(),
                quantity: "0".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn addresses_total_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/total");
        let (status, headers, bytes) = app.get_with_headers(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContentTotal =
            serde_json::from_slice(&bytes).expect("failed to parse address total");

        assert_serde_bytes(&item, &bytes);

        assert_eq!(item.address, address);
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        assert_total_sums(&app, &item);
    }

    #[tokio::test]
    async fn addresses_total_payment_credential_happy_path() {
        let app = TestApp::new();
        let address = Address::from_bech32(app.vectors().address.as_str())
            .expect("invalid synthetic test address");

        let Address::Shelley(shelley) = address else {
            panic!("expected shelley test address")
        };

        let payment = shelley.payment().to_vec();

        let payment_cred = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("addr_vkh").expect("invalid hrp"),
            &payment,
        )
        .expect("failed to encode payment credential");

        let path = format!("/addresses/{payment_cred}/total");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContentTotal =
            serde_json::from_slice(&bytes).expect("failed to parse address total");

        assert_serde_bytes(&item, &bytes);

        assert_eq!(item.address, payment_cred);
        assert_total_sums(&app, &item);
    }

    #[tokio::test]
    async fn addresses_total_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/total", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_total_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/total", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_total_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/total");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    /// An address's body and a payment credential's are the same bytes with
    /// the totals cache off, missed and hit.
    #[tokio::test]
    async fn addresses_total_bodies_match_with_the_cache_off_missed_and_hit() {
        let address = |vectors: &SyntheticVectors| vectors.address.clone();

        let payment = |vectors: &SyntheticVectors| {
            let Ok(Address::Shelley(shelley)) = Address::from_bech32(&vectors.address) else {
                panic!("expected a shelley test address")
            };

            let hrp = bech32::Hrp::parse("addr_vkh").expect("invalid hrp");
            bech32::encode::<bech32::Bech32>(hrp, &shelley.payment().to_vec())
                .expect("failed to encode payment credential")
        };

        for credential in [&address as &dyn Fn(&SyntheticVectors) -> String, &payment] {
            let [off, miss, hit] =
                totals_bodies(|vectors| format!("/addresses/{}/total", credential(vectors))).await;

            let item: AddressContentTotal = serde_json::from_slice(&off).unwrap();
            assert!(item.tx_count > 0);

            assert_eq!(
                String::from_utf8_lossy(&miss),
                String::from_utf8_lossy(&off)
            );
            assert_eq!(String::from_utf8_lossy(&hit), String::from_utf8_lossy(&off));
        }
    }
}
