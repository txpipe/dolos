//! `/pools/{pool_id}/relays`: the relays of a pool's live registration.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content_pool_certs_inner_relays_inner::TxContentPoolCertsInnerRelaysInner;
use dolos_cardano::model::PoolState;
use dolos_core::Domain;

use crate::{error::Error, mapping::IntoModel, Facade};

use super::{parse_pool_id_bounded, read_pool_row};

pub async fn by_id_relays<D: Domain>(
    Path(id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolCertsInnerRelaysInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let Some(hash) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let pool = read_pool_row(&domain, hash)
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;

    let relays = pool
        .snapshot
        .live()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
        .params
        .relays
        .clone();

    let out = relays
        .into_iter()
        .map(|relay| relay.into_model())
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::{
            assert_error_message, assert_status, invalid_pool_id, missing_pool_id,
        },
        test_support::{TestApp, TestFault},
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::{codec::utils::Bytes, ledger::primitives::Relay};

    #[test]
    fn relay_ipv6_words_swapped_to_network_order() {
        // On-chain bytes of a real preview relay (pool1drrylt...): the ledger
        // serializes relay IPv6 as four little-endian 32-bit words, so the
        // mapped address must match what dbsync/Blockfrost display.
        let onchain: [u8; 16] = [
            0x61, 0x08, 0x01, 0x20, 0x80, 0x2b, 0x00, 0x40, 0xff, 0x3e, 0x16, 0x02, 0x09, 0x90,
            0x05, 0xfe,
        ];

        let relay = Relay::SingleHostAddr(Some(3001), None, Some(Bytes::from(onchain.to_vec())));
        let model = relay.into_model().expect("relay model");

        assert_eq!(
            model.ipv6.as_deref(),
            Some("2001:861:4000:2b80:216:3eff:fe05:9009")
        );
        assert_eq!(model.ipv4, None);
        assert_eq!(model.port, 3001);
    }

    #[tokio::test]
    async fn pools_relays_happy_path() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            pool_relays: vec![
                Relay::SingleHostAddr(Some(3001), Some(Bytes::from(vec![192, 168, 0, 1])), None),
                Relay::SingleHostName(Some(3002), "relay.example.com".to_string()),
                Relay::MultiHostName("_relays._tcp.example.com".to_string()),
            ],
            ..Default::default()
        });

        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/relays");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let relays: Vec<TxContentPoolCertsInnerRelaysInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool relays");

        assert_eq!(
            relays,
            vec![
                TxContentPoolCertsInnerRelaysInner {
                    ipv4: Some("192.168.0.1".to_string()),
                    ipv6: None,
                    dns: None,
                    dns_srv: None,
                    port: 3001,
                },
                TxContentPoolCertsInnerRelaysInner {
                    ipv4: None,
                    ipv6: None,
                    dns: Some("relay.example.com".to_string()),
                    dns_srv: None,
                    port: 3002,
                },
                TxContentPoolCertsInnerRelaysInner {
                    ipv4: None,
                    ipv6: None,
                    dns: None,
                    dns_srv: Some("_relays._tcp.example.com".to_string()),
                    port: 0,
                },
            ]
        );
    }

    #[tokio::test]
    async fn pools_relays_empty_without_registered_relays() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/relays");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let relays: Vec<TxContentPoolCertsInnerRelaysInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool relays");
        assert!(relays.is_empty());
    }

    #[tokio::test]
    async fn pools_relays_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/relays", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;
    }

    #[tokio::test]
    async fn pools_relays_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/relays", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pools_relays_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/relays");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
