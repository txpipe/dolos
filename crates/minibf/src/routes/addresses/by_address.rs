use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::address_content::AddressContent;
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::{AddressModelBuilder, IntoModel},
    Facade,
};

use super::{amount_for_refs, is_address_in_chain, parse_address, refs_for_parsed_address};

pub async fn by_address<D>(
    Path(address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AddressContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let parsed = parse_address(&address)?;
    let refs = refs_for_parsed_address(&domain, &parsed)?;

    if refs.is_empty() && !is_address_in_chain(&domain, &address).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let amount = amount_for_refs(&domain, refs)?;

    let model = AddressModelBuilder {
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
    use blockfrost_openapi::models::address_content::{AddressContent, Type as AddressType};
    use pallas::ledger::addresses::Address;

    #[tokio::test]
    async fn addresses_by_address_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContent =
            serde_json::from_slice(&bytes).expect("failed to parse address content");

        assert_eq!(item.address, address);
        assert_eq!(item.r#type, AddressType::Shelley);
        assert!(item.stake_address.is_some());
        assert!(!item.amount.is_empty());
    }

    #[tokio::test]
    async fn addresses_by_address_payment_credential_happy_path() {
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

        let path = format!("/addresses/{payment_cred}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContent =
            serde_json::from_slice(&bytes).expect("failed to parse address content");

        assert_eq!(item.address, payment_cred);
        assert_eq!(item.r#type, AddressType::Shelley);
        assert_eq!(item.stake_address, None);
        assert!(!item.script);
        assert!(!item.amount.is_empty());
    }

    #[tokio::test]
    async fn addresses_by_address_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_by_address_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_by_address_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
