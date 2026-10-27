use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    address_content_extended::AddressContentExtended,
    address_content_extended_amount_inner::AddressContentExtendedAmountInner,
};
use dolos_core::Domain;
use futures_util::{StreamExt, TryStreamExt};

use crate::{
    error::Error,
    mapping::{assets::AssetAmountExtras, AddressExtendedModelBuilder, IntoModel},
    Facade,
};

use super::{amount_for_refs, is_address_in_chain, parse_address, refs_for_parsed_address};

/// This constant sets how many units of an address resolve their metadata at
/// the same time. Each unit needs one request to the off-chain registry. One
/// request at a time is slow for an address that holds many tokens.
const EXTENDED_ASSET_CONCURRENCY: usize = 16;

pub async fn extended<D>(
    Path(address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AddressContentExtended>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let parsed = parse_address(&address)?;
    let refs = refs_for_parsed_address(&domain, &parsed)?;

    if refs.is_empty() && !is_address_in_chain(&domain, &address).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let amount = futures_util::stream::iter(amount_for_refs(&domain, refs)?)
        .map(|entry| {
            let domain = &domain;

            async move {
                let extras = if entry.unit == "lovelace" {
                    AssetAmountExtras::lovelace()
                } else {
                    AssetAmountExtras::resolve(domain, &entry.unit).await?
                };

                Ok::<_, Error>(AddressContentExtendedAmountInner {
                    unit: entry.unit,
                    quantity: entry.quantity,
                    decimals: extras.decimals,
                    has_nft_onchain_metadata: extras.has_nft_onchain_metadata,
                })
            }
        })
        .buffered(EXTENDED_ASSET_CONCURRENCY)
        .try_collect()
        .await?;

    let model = AddressExtendedModelBuilder {
        address,
        amount,
        kind: parsed.into_model_kind(),
    }
    .into_model()
    .map_err(Error::Code)?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::addresses::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::{
        address_content::AddressContent, address_content_extended::Type as AddressExtendedType,
    };
    use pallas::ledger::addresses::Address;

    #[tokio::test]
    async fn addresses_extended_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/extended");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContentExtended =
            serde_json::from_slice(&bytes).expect("failed to parse extended address content");

        assert_eq!(item.address, address);
        assert_eq!(item.r#type, AddressExtendedType::Shelley);
        assert!(item.stake_address.is_some());
        assert!(!item.script);

        let lovelace = item.amount.first().expect("expected a lovelace entry");
        assert_eq!(lovelace.unit, "lovelace");
        assert_eq!(lovelace.decimals, Some(6));
        assert!(!lovelace.has_nft_onchain_metadata);

        // the test mints the synthetic asset with no metadata of any standard
        let asset = item
            .amount
            .iter()
            .find(|x| x.unit == app.vectors().asset_unit)
            .expect("expected the synthetic asset");
        assert_eq!(asset.decimals, None);
        assert!(!asset.has_nft_onchain_metadata);
    }

    #[tokio::test]
    async fn addresses_extended_matches_plain_amounts() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();

        let (_, plain) = app.get_bytes(&format!("/addresses/{address}")).await;
        let (_, extended) = app
            .get_bytes(&format!("/addresses/{address}/extended"))
            .await;

        let plain: AddressContent =
            serde_json::from_slice(&plain).expect("failed to parse address");
        let extended: AddressContentExtended =
            serde_json::from_slice(&extended).expect("failed to parse extended address");

        let plain: Vec<_> = plain
            .amount
            .iter()
            .map(|x| (&x.unit, &x.quantity))
            .collect();
        let extended: Vec<_> = extended
            .amount
            .iter()
            .map(|x| (&x.unit, &x.quantity))
            .collect();

        assert_eq!(plain, extended);
    }

    #[tokio::test]
    async fn addresses_extended_payment_credential_happy_path() {
        let app = TestApp::new();
        let address = Address::from_bech32(app.vectors().address.as_str())
            .expect("invalid synthetic test address");

        let Address::Shelley(shelley) = address else {
            panic!("expected shelley test address")
        };

        let payment_cred = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("addr_vkh").expect("invalid hrp"),
            &shelley.payment().to_vec(),
        )
        .expect("failed to encode payment credential");

        let path = format!("/addresses/{payment_cred}/extended");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContentExtended =
            serde_json::from_slice(&bytes).expect("failed to parse extended address content");

        assert_eq!(item.address, payment_cred);
        assert_eq!(item.r#type, AddressExtendedType::Shelley);
        assert_eq!(item.stake_address, None);
        assert!(!item.script);
        assert!(!item.amount.is_empty());
    }

    #[tokio::test]
    async fn addresses_extended_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/extended", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_extended_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/extended", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_extended_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/extended");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
