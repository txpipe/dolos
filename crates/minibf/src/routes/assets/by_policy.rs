use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset_policy_inner::AssetPolicyInner;
use dolos_cardano::{indexes::SlotOrder, model::AssetState, periods::first_mints};
use dolos_core::{Domain, DomainError};
use pallas::crypto::hash::Hash;

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// `GET /assets/policy/{policy}`: the assets a policy minted, ordered by
/// first mint, from the first-mint period logs.
pub async fn by_policy<D>(
    Path(policy_hex): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetPolicyInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let policy = hex::decode(&policy_hex).map_err(|_| Error::InvalidPolicy)?;
    let policy: Hash<28> = <[u8; 28]>::try_from(policy.as_slice())
        .map_err(|_| Error::InvalidPolicy)?
        .into();

    let chain = domain.get_chain_summary()?;
    let Some(mary_epoch) = chain.first_mary_epoch() else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let order = match pagination.order {
        Order::Asc => SlotOrder::Asc,
        Order::Desc => SlotOrder::Desc,
    };
    let (skip, count) = (pagination.skip(), pagination.count);

    let subjects = domain
        .query()
        .run_blocking(move |domain| {
            first_mints::policy_page(
                &domain,
                &chain,
                mary_epoch..=tip_epoch,
                &policy,
                order,
                skip,
                count,
            )
            .map_err(DomainError::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Blockfrost returns 404 when the policy has no mint operations.
    let subjects = subjects.ok_or(StatusCode::NOT_FOUND)?;

    let mut items = Vec::with_capacity(subjects.len());

    for subject in subjects {
        let asset_key = dolos_cardano::AssetKey::of(&subject);
        let asset_state = domain
            .read_cardano_entity::<AssetState>(&asset_key)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        items.push(AssetPolicyInner {
            asset: hex::encode(&subject),
            quantity: asset_state.quantity().to_string(),
        });
    }

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::assets::testing::*,
        test_support::{first_mint_chain, first_mint_policies, TestApp, TestFault},
    };
    use dolos_cardano::{indexes::AsyncCardanoQueryExt, model::PolicyMintChunk};
    use dolos_core::{ArchiveStore as _, AsyncQueryFacade, StateStore as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use futures_util::StreamExt;
    use pallas::ledger::traverse::MultiEraBlock;

    /// The scan the period logs replaced: the policy's tagged blocks from
    /// genesis, each name kept at its first appearance in a mint.
    async fn scan_policy(domain: &ToyDomain, policy: &[u8]) -> Vec<Vec<u8>> {
        let end_slot = domain.state().read_cursor().unwrap().unwrap().slot();
        let stream = AsyncQueryFacade::new(domain.clone()).blocks_by_policy_stream(
            policy,
            0,
            end_slot,
            SlotOrder::Asc,
        );
        let mut stream = Box::pin(stream);

        let mut seen = std::collections::HashSet::new();
        let mut subjects = Vec::new();

        while let Some(res) = stream.next().await {
            let Some(block) = res.unwrap().1 else {
                continue;
            };
            let block = MultiEraBlock::decode(&block).unwrap();

            for tx in block.txs() {
                for policy_assets in tx.mints() {
                    if policy_assets.policy().as_slice() != policy {
                        continue;
                    }

                    for asset in policy_assets.assets() {
                        let subject = [policy, asset.name()].concat();

                        if seen.insert(subject.clone()) {
                            subjects.push(subject);
                        }
                    }
                }
            }
        }

        subjects
    }

    fn unminted_policy() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    #[tokio::test]
    async fn assets_by_policy_happy_path() {
        let app = TestApp::new();
        let policy = app.vectors().policy_id.clone();
        let unit = app.vectors().asset_unit.clone();
        let path = format!("/assets/policy/{policy}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let assets: Vec<AssetPolicyInner> =
            serde_json::from_slice(&bytes).expect("failed to parse policy assets");
        assert!(assets.iter().any(|a| a.asset == unit));
    }

    #[tokio::test]
    async fn assets_by_policy_ascending_pagination_preserves_first_mint_order() {
        let asset_names = ["FIRST", "SECOND", "THIRD"];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: asset_names.len(),
            txs_per_block: 1,
            asset_names_by_block: asset_names.iter().map(|x| (*x).to_string()).collect(),
            ..Default::default()
        });
        let policy = app.vectors().policy_id.as_str();

        for (index, asset_name) in asset_names.into_iter().enumerate() {
            let page = index + 1;
            let path = format!("/assets/policy/{policy}?order=asc&page={page}&count=1");
            let (status, bytes) = app.get_bytes(&path).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "unexpected status {status} with body: {}",
                String::from_utf8_lossy(&bytes)
            );

            let assets: Vec<AssetPolicyInner> =
                serde_json::from_slice(&bytes).expect("failed to parse policy assets");
            let expected = format!("{policy}{}", hex::encode(asset_name));
            assert_eq!(assets.len(), 1);
            assert_eq!(assets[0].asset, expected);
            assert_eq!(assets[0].quantity, "1");
        }
    }

    /// Every page in both orders of every policy, as the scan the logs
    /// replaced answers it, over a chain whose first epochs are sealed and
    /// whose last are staged.
    #[tokio::test]
    async fn assets_by_policy_matches_the_policy_scan() {
        use first_mint_policies::*;

        let mut domain = None;
        let app = TestApp::new_with_cfg_and_setup(first_mint_chain(), |inner, _| {
            domain = Some(inner.clone())
        });
        let domain = domain.expect("setup ran");

        let sealed = domain
            .archive()
            .iter_logs_typed::<PolicyMintChunk>(None)
            .unwrap()
            .count();
        assert!(sealed > 4, "{sealed} sealed rows");

        for policy in [ALPHA, BETA, GAMMA, SIBLING] {
            let all = scan_policy(&domain, &policy).await;
            assert!(!all.is_empty());

            for (order, name) in [(Order::Asc, "asc"), (Order::Desc, "desc")] {
                let ordered: Vec<String> = match order {
                    Order::Asc => all.iter().map(hex::encode).collect(),
                    Order::Desc => all.iter().rev().map(hex::encode).collect(),
                };

                for count in [1, 3, 100] {
                    for page in 1..=all.len().div_ceil(count) + 1 {
                        let skip = (page - 1) * count;
                        let expected: Vec<String> =
                            ordered.iter().skip(skip).take(count).cloned().collect();

                        let path = format!(
                            "/assets/policy/{}?order={name}&count={count}&page={page}",
                            hex::encode(policy)
                        );
                        let (status, bytes) = app.get_bytes(&path).await;
                        assert_eq!(status, StatusCode::OK, "{path}");

                        let got: Vec<AssetPolicyInner> = serde_json::from_slice(&bytes).unwrap();
                        let got: Vec<String> = got.into_iter().map(|x| x.asset).collect();

                        assert_eq!(got, expected, "{path}");
                    }
                }
            }
        }

        let path = format!("/assets/policy/{}", unminted_policy());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_policy_bad_request() {
        let app = TestApp::new();
        // This policy contains non-hexadecimal characters.
        assert_status(&app, "/assets/policy/not-hex", StatusCode::BAD_REQUEST).await;
        // This policy contains fewer than 28 bytes.
        assert_status(&app, "/assets/policy/abcd", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn assets_by_policy_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/policy/{}", unminted_policy());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_policy_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let policy = app.vectors().policy_id.clone();
        let path = format!("/assets/policy/{policy}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
