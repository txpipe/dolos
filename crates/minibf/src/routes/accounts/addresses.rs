use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_addresses_content_inner::AccountAddressesContentInner;
use dolos_cardano::{
    indexes::SlotOrder,
    model::{AccountState, PoolState, ProposalState},
    periods::account_addresses,
};
use dolos_core::{Domain, DomainError};
use pallas::ledger::addresses::{Address, StakeAddress, StakePayload};

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{account_exists, parse_account_key_param};

/// Whether a pool registration the pool state still holds names the
/// credential, as reward account or owner.
///
/// db-sync registers a credential the moment a certificate names it, so
/// Blockfrost answers `200 []` for a credential seen only as a pool reward
/// account or owner. Dolos writes no `AccountState` for those, so the bare
/// existence guard would 404. This scan runs only on the would-be-404 path,
/// so the hot path pays nothing.
///
/// Known gap: `PoolState` keeps only the `next`, `live`, `mark`, `set` and
/// `go` parameter versions. A second registration in the same epoch
/// overwrites `next`, and each epoch transition rotates the oldest version
/// out of `go`. A credential named only by such a replaced registration is
/// gone from every version, so it still 404s. db-sync remembers it forever.
fn account_appears_in_pool_registrations<D>(
    domain: &Facade<D>,
    account: &StakeAddress,
) -> Result<bool, StatusCode>
where
    Option<PoolState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let reward_account = account.to_vec();

    // Pool owners are always key hashes, so a script account can only match
    // through the reward account.
    let owner_hash = match account.payload() {
        StakePayload::Stake(hash) => Some(*hash),
        StakePayload::Script(_) => None,
    };

    for item in domain.iter_cardano_entities::<PoolState>(None)? {
        let (_, pool) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Check every snapshot slot the entity still holds, so a credential a
        // recent re-registration replaced is still found: `live` for a new
        // pool, `next` for a mid-epoch re-registration, `mark`/`set`/`go` for
        // the older values.
        let snapshots = [
            pool.snapshot.live(),
            pool.snapshot.next(),
            pool.snapshot.mark(),
            pool.snapshot.set(),
            pool.snapshot.go(),
        ];

        for snapshot in snapshots.into_iter().flatten() {
            if snapshot.params.reward_account == reward_account {
                return Ok(true);
            }

            if owner_hash.is_some_and(|hash| snapshot.params.pool_owners.contains(&hash)) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// `GET /accounts/{stake_address}/addresses`: the addresses an account saw,
/// ordered by first appearance on chain, from the stake-address period logs.
/// `desc` is the reverse of that order.
///
/// Blockfrost declares only `count`, `page` and `order` for this endpoint.
/// The shared query struct also accepts `from` and `to`; they are ignored
/// here, as Blockfrost ignores them, so the list always covers the account's
/// whole history.
pub async fn by_stake_addresses<D>(
    Path(stake_address): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountAddressesContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    // Drop `from`/`to` before validation: Blockfrost never reads them here,
    // so a malformed or reversed window is ignored rather than rejected.
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;
    if !account_exists(&domain, &account_key.account)?
        && !account_appears_in_pool_registrations(&domain, &account_key.address)?
    {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let chain = domain.get_chain_summary()?;
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);
    let epochs = chain.first_shelley_epoch()..=tip_epoch;

    let order = SlotOrder::from(pagination.order);
    let (skip, count) = (pagination.skip(), pagination.count);
    let account = account_key.account.clone();

    let addresses = domain
        .query()
        .run_blocking(move |domain| {
            account_addresses::page(&domain, &chain, epochs, &account, order, skip, count)
                .map_err(DomainError::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items = addresses
        .into_iter()
        .map(|bytes| {
            Address::from_bytes(&bytes)
                .map(|address| AccountAddressesContentInner {
                    address: address.to_string(),
                })
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
        .collect::<Result<_, _>>()?;

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pagination::Order,
        routes::accounts::{address_belongs_to_account, testing::*},
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_addresses_content_inner::AccountAddressesContentInner;
    use dolos_cardano::{
        indexes::AsyncCardanoQueryExt,
        model::{EpochValue, PoolParams, PoolSnapshot},
        pallas_extras,
    };
    use dolos_core::{ArchiveStore as _, StateStore as _, StateWriter as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use futures_util::StreamExt;
    use pallas::{
        crypto::hash::Hash,
        ledger::{addresses::Network, traverse::MultiEraBlock},
    };

    #[tokio::test]
    async fn accounts_by_stake_addresses_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account addresses");
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_pool_only_account_returns_empty_list() {
        let app = TestApp::new();

        // The synthetic chain registers a pool owned by key hash [2u8; 28].
        // That credential never appears in an address or certificate, so no
        // account state exists for it. Blockfrost still answers with an empty
        // list because the credential is known through the pool.
        let owner = StakeAddress::new(Network::Testnet, StakePayload::Stake(Hash::from([2u8; 28])));
        let owner = owner.to_bech32().expect("failed to encode owner address");

        let path = format!("/accounts/{owner}/addresses");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let addresses: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account addresses");
        assert!(addresses.is_empty());
    }

    /// A pool state whose only link to an account is its reward account.
    fn reward_only_pool(operator: [u8; 28], reward_account: Vec<u8>) -> PoolState {
        let params = PoolParams {
            vrf_keyhash: Hash::from([0u8; 32]),
            pledge: 0,
            cost: 0,
            margin: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account,
            pool_owners: vec![],
            relays: vec![],
            pool_metadata: None,
        };
        let snapshot = PoolSnapshot {
            is_retired: false,
            blocks_minted: 0,
            params,
            is_new: false,
            stake: None,
            delegators: None,
            live_pledge: None,
        };
        PoolState {
            operator: Hash::from(operator),
            snapshot: EpochValue::with_live(3, snapshot),
            blocks_minted_total: 0,
            register_slot: 0,
            retiring_epoch: None,
            deposit: 0,
            calidus: None,
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_pool_reward_account_returns_empty_list() {
        // The synthetic pool pays rewards to the registered fixture account,
        // so the chain only exercises the owner path of the fallback. Two
        // extra pools cover the reward account path: one pays a key hash and
        // one pays a script hash. Neither credential has an account state.
        let key_hash_account =
            StakeAddress::new(Network::Testnet, StakePayload::Stake(Hash::from([7u8; 28])));
        let script_account = StakeAddress::new(
            Network::Testnet,
            StakePayload::Script(Hash::from([8u8; 28])),
        );
        let pools = [
            ([70u8; 28], key_hash_account.to_vec()),
            ([80u8; 28], script_account.to_vec()),
        ];

        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, _| {
            let writer = domain.state().start_writer().expect("state writer");
            for (operator, reward_account) in &pools {
                let pool = reward_only_pool(*operator, reward_account.clone());
                writer
                    .write_entity_typed(&dolos_cardano::PoolKey((*operator).into()), &pool)
                    .expect("failed to write pool state");
            }
            writer.commit().expect("failed to commit pool state");
        });

        for account in [key_hash_account, script_account] {
            let account = account.to_bech32().expect("failed to encode stake address");

            // No account state exists: the bare account endpoint has no
            // fallback, so it must still answer 404.
            assert_status(&app, &format!("/accounts/{account}"), StatusCode::NOT_FOUND).await;

            let (status, bytes) = app
                .get_bytes(&format!("/accounts/{account}/addresses"))
                .await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{account}: unexpected status {status} with body: {}",
                String::from_utf8_lossy(&bytes)
            );
            let addresses: Vec<AccountAddressesContentInner> =
                serde_json::from_slice(&bytes).expect("failed to parse account addresses");
            assert!(addresses.is_empty(), "{account}");
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_ignores_from_and_to() {
        // Blockfrost declares no `from`/`to` for this endpoint and its query
        // binds neither, so a windowed request answers the full list. The
        // window below starts at the newest first appearance: honoring it
        // would drop every older address.
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();

        let newest_first_appearance = app
            .vectors()
            .account_address_bounds
            .iter()
            .map(|(_, min, _)| *min)
            .max()
            .expect("vectors carry account addresses");
        let last_block = app
            .vectors()
            .account_address_bounds
            .iter()
            .map(|(_, _, max)| *max)
            .max()
            .expect("vectors carry account addresses");

        let (status, bytes) = app
            .get_bytes(&format!("/accounts/{stake_address}/addresses"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let full: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse addresses");
        assert!(full.len() > 1, "the window must have something to drop");

        let (status, bytes) = app
            .get_bytes(&format!(
                "/accounts/{stake_address}/addresses?from={newest_first_appearance}&to={last_block}"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let windowed: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse windowed addresses");

        assert_eq!(windowed, full);

        // Blockfrost answers 200 for a malformed or reversed window too,
        // because it never parses these parameters on this endpoint.
        for query in ["from=garbage", "from=100&to=50"] {
            let (status, bytes) = app
                .get_bytes(&format!("/accounts/{stake_address}/addresses?{query}"))
                .await;
            assert_eq!(status, StatusCode::OK, "{query}");
            let ignored: Vec<AccountAddressesContentInner> =
                serde_json::from_slice(&bytes).expect("failed to parse addresses");
            assert_eq!(ignored, full, "{query}");
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/addresses?page=1&count=1");
        let path_page_2 = format!("/accounts/{stake_address}/addresses?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse account addresses page 1");
        let page_2: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse account addresses page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].address, page_2[0].address);
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountAddressesContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse addresses asc");
        if asc.is_empty() {
            return;
        }
        let address_bounds = |addr: &str| {
            app.vectors()
                .account_address_bounds
                .iter()
                .find_map(|(known, min, max)| (known == addr).then_some((*min, *max)))
                .expect("missing address in vectors")
        };

        let asc_blocks: Vec<_> = asc.iter().map(|x| address_bounds(&x.address).0).collect();

        assert!(asc_blocks.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();

        let get = |query: &'static str| {
            let app = &app;
            async move {
                let (status, bytes) = app
                    .get_bytes(&format!("/accounts/{stake_address}/addresses?{query}"))
                    .await;
                assert_eq!(status, StatusCode::OK, "{query}");
                serde_json::from_slice::<Vec<AccountAddressesContentInner>>(&bytes)
                    .expect("failed to parse addresses")
                    .into_iter()
                    .map(|x| x.address)
                    .collect::<Vec<_>>()
            }
        };

        let asc = get("order=asc&count=100").await;
        let desc = get("order=desc&count=100").await;
        assert!(asc.len() >= 4, "the window below needs four addresses");

        // The synthetic chain reuses the primary address in every block and
        // pays some secondary addresses in two consecutive blocks. Placing
        // those at their last appearance would reorder `desc`; Blockfrost
        // defines `desc` as the reverse of `asc`.
        let mut reversed = asc.clone();
        reversed.reverse();
        assert_eq!(desc, reversed);

        // a `desc` page is a window into the reversed list
        let page = get("order=desc&count=2&page=2").await;
        assert_eq!(page, reversed[2..4].to_vec());
    }

    /// The addresses an account saw, ordered by first appearance, as a scan
    /// of every block tagged with the account answers it.
    async fn scan_stake_addresses(domain: &ToyDomain, account: &StakeAddress) -> Vec<String> {
        let end_slot = domain.state().read_cursor().unwrap().unwrap().slot();
        let stream = dolos_core::AsyncQueryFacade::new(domain.clone()).blocks_by_stake_stream(
            &account.to_vec(),
            0,
            end_slot,
            SlotOrder::Asc,
        );
        let mut stream = Box::pin(stream);

        let account = account.to_vec();
        let mut seen = std::collections::HashSet::new();
        let mut addresses = Vec::new();

        while let Some(res) = stream.next().await {
            let Some(block) = res.unwrap().1 else {
                continue;
            };
            let block = MultiEraBlock::decode(&block).unwrap();

            for (_, output) in block.txs().iter().flat_map(|tx| tx.produces()) {
                let address = output.address().unwrap();

                if address_belongs_to_account(&address, &account)
                    && seen.insert(address.to_string())
                {
                    addresses.push(address.to_string());
                }
            }
        }

        addresses
    }

    /// Every page in both orders of every account, as the scan the logs
    /// replaced answers it, over a chain whose first epochs are sealed and
    /// whose last are staged.
    #[tokio::test]
    async fn accounts_by_stake_addresses_matches_the_stake_scan() {
        use crate::test_support::{stake_address_accounts::*, stake_address_chain};

        let mut domain = None;
        let app = TestApp::new_with_cfg_and_setup(stake_address_chain(), |inner, _| {
            domain = Some(inner.clone())
        });
        let domain = domain.expect("setup ran");

        let sealed = domain
            .archive()
            .iter_logs_typed::<dolos_cardano::AccountAddressChunk>(None)
            .unwrap()
            .count();
        let staged = domain
            .state()
            .iter_entities_typed::<dolos_cardano::AccountAddressSet>(None)
            .unwrap()
            .count();
        assert!(
            sealed >= 4 && staged > 0,
            "{sealed} sealed rows, {staged} staged"
        );

        let network = Network::Testnet;

        for account in [primary(), other(), script()] {
            let stake = pallas_extras::stake_credential_to_address(network, &account);
            let all = scan_stake_addresses(&domain, &stake).await;
            assert!(!all.is_empty());

            if account == primary() {
                assert!(all.len() > 150, "{} addresses", all.len());
            }

            let stake = stake.to_bech32().unwrap();

            for (order, name) in [(Order::Asc, "asc"), (Order::Desc, "desc")] {
                let ordered: Vec<String> = match order {
                    Order::Asc => all.clone(),
                    Order::Desc => all.iter().rev().cloned().collect(),
                };

                for count in [1, 3, 100] {
                    for page in 1..=all.len().div_ceil(count) + 1 {
                        let skip = (page - 1) * count;
                        let expected: Vec<String> =
                            ordered.iter().skip(skip).take(count).cloned().collect();

                        let path = format!(
                            "/accounts/{stake}/addresses?order={name}&count={count}&page={page}"
                        );
                        let (status, bytes) = app.get_bytes(&path).await;
                        assert_eq!(status, StatusCode::OK, "{path}");

                        let got: Vec<AccountAddressesContentInner> =
                            serde_json::from_slice(&bytes).unwrap();
                        let got: Vec<String> = got.into_iter().map(|x| x.address).collect();

                        assert_eq!(got, expected, "{path}");
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
