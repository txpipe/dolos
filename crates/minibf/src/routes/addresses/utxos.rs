use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use dolos_core::Domain;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{address_tag, is_address_in_chain, parse_address};

pub async fn utxos<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let tag = address_tag(&parse_address(&address)?);

    let utxos = crate::routes::utxos::load_tag_page(&domain, &tag, &pagination).await?;

    // If the address holds no UTxO and is not seen on the chain, send 404.
    if utxos.is_empty() && crate::routes::utxos::tag_is_empty(&domain, &tag).await? {
        if is_address_in_chain(&domain, &address).await? {
            return Ok(Json(vec![]));
        }
        return Err(Error::Code(StatusCode::NOT_FOUND));
    }

    Ok(Json(utxos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::addresses::testing::*,
        test_support::{health_beside, TestApp, TestFault, SLOW_READ},
    };
    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::ledger::addresses::Address;

    #[tokio::test]
    async fn addresses_utxos_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address utxos");
    }

    #[tokio::test]
    async fn addresses_utxos_paginated() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path_page_1 = format!("/addresses/{address}/utxos?page=1&count=2");
        let path_page_2 = format!("/addresses/{address}/utxos?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse utxos page 1");
        let page_2: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse utxos page 2");

        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 2);

        let page_1_hashes: std::collections::HashSet<_> = page_1
            .into_iter()
            .map(|x| format!("{}#{}", x.tx_hash, x.output_index))
            .collect();
        let page_2_hashes: std::collections::HashSet<_> = page_2
            .into_iter()
            .map(|x| format!("{}#{}", x.tx_hash, x.output_index))
            .collect();
        assert!(page_1_hashes.is_disjoint(&page_2_hashes));
    }

    #[tokio::test]
    async fn addresses_utxos_order_asc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn addresses_utxos_order_desc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn addresses_utxos_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/utxos", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_utxos_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/utxos", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn addresses_utxos_lovelace_pages_are_full() {
        // the token outputs come first, so a filter that runs after the page
        // cut returns an empty first page
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 8,
            txs_per_block: 1,
            ada_only_by_block: vec![false, false, false, true, true, false, true, true],
            ..Default::default()
        });
        let address = app.vectors().address.clone();
        let path = format!("/addresses/{address}/utxos/lovelace");

        for order in ["asc", "desc"] {
            let (status, bytes) = app
                .get_bytes(&format!(
                    "/addresses/{address}/utxos?order={order}&count=100"
                ))
                .await;
            assert_eq!(status, StatusCode::OK);

            let all: Vec<AddressUtxoContentInner> =
                serde_json::from_slice(&bytes).expect("failed to parse address utxos");
            let ada_only: Vec<String> = all
                .into_iter()
                .filter(|x| x.amount.iter().all(|amount| amount.unit == "lovelace"))
                .map(|x| format!("{}#{}", x.tx_hash, x.output_index))
                .collect();

            let full = utxo_page(&app, &format!("{path}?order={order}&count=100")).await;
            assert_eq!(full.len(), 4);
            assert_eq!(full, ada_only, "order={order}");

            for count in [1, 2, 3] {
                assert_pages_join(&app, &path, order, count, &full).await;
            }
        }
    }

    /// A payment credential lists the UTxOs of every address with that
    /// payment part: here only the synthetic address has it.
    #[tokio::test]
    async fn addresses_utxos_payment_credential_lists_its_address() {
        let app = TestApp::new();
        let address = app.vectors().address.clone();

        let Address::Shelley(shelley) =
            Address::from_bech32(&address).expect("invalid synthetic test address")
        else {
            panic!("expected shelley test address")
        };

        let payment_cred = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("addr_vkh").expect("invalid hrp"),
            shelley.payment().to_vec().as_slice(),
        )
        .expect("failed to encode payment credential");

        for order in ["asc", "desc"] {
            let query = format!("utxos?order={order}&count=100");
            let by_address = utxo_page(&app, &format!("/addresses/{address}/{query}")).await;

            assert!(!by_address.is_empty());
            assert_eq!(
                utxo_page(&app, &format!("/addresses/{payment_cred}/{query}")).await,
                by_address,
                "order={order}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_utxo_listing_does_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowUtxoReads(SLOW_READ)));
        let path = format!("/addresses/{}/utxos", app.vectors().address);

        assert_eq!(health_beside(&app, &path).await, StatusCode::OK);
    }
}
