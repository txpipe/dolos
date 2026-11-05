use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content_pool_certs_inner::TxContentPoolCertsInner;
use dolos_core::Domain;

use crate::{
    mapping::{txs::TxModelBuilder, IntoModel as _},
    Facade,
};

use super::parse_tx_hash;

pub async fn by_hash_pool_updates<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolCertsInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_tx_hash(&tx_hash)?;

    let network = domain.get_network_id()?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let chain = domain.get_chain_summary()?;

    let mut tx = TxModelBuilder::new(&raw, order)?
        .with_network(network)
        .with_chain(chain);
    tx.fetch_pool_metadata(domain.inner.cache()).await?;
    tx.set_pool_registrations(&domain)?;

    tx.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn txs_by_hash_pool_updates_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_updates");
        let _: Vec<TxContentPoolCertsInner> = get_ok(&app, &path).await;
    }

    #[tokio::test]
    async fn all_pool_metadata_routes_serve_a_cached_body_without_the_upstream() {
        use dolos_core::{CacheEntry, CacheStore as _};
        use dolos_testing::synthetic::SyntheticBlockConfig;
        use pallas::{
            crypto::hash::Hasher,
            ledger::primitives::{conway::Certificate, Bytes, PoolMetadata, RationalNumber, Set},
        };
        use std::convert::Infallible;

        let body = br#"{"name":"Cached pool","description":"A pool","ticker":"CACHE","homepage":"https://example.com"}"#;
        let metadata = PoolMetadata {
            url: "https://example.invalid/pool.json".to_string(),
            hash: Hasher::<256>::hash(body).to_vec().into(),
        };
        let operator = [0x79; 28];
        let registration = Certificate::PoolRegistration {
            operator: operator.into(),
            vrf_keyhash: [1u8; 32].into(),
            pledge: 0,
            cost: 0,
            margin: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account: Bytes::from([&[0xe0][..], &[0x21; 28][..]].concat()),
            pool_owners: Set::from(vec![[2u8; 28].into()]),
            relays: vec![],
            pool_metadata: Some(metadata.clone()),
        };
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            extra_certs_by_block: vec![vec![vec![registration]]],
            ..Default::default()
        });
        app.facade()
            .inner
            .cache()
            .get_or_fill_async(
                crate::offchain::POOL_OFFCHAIN.name,
                metadata.hash.as_slice(),
                async move { Ok::<_, Infallible>(CacheEntry::new(body.to_vec(), 0)) },
            )
            .await
            .unwrap();

        let pool_id = crate::mapping::bech32_pool(operator).unwrap();
        let tx_hash = &app.vectors().blocks[0].tx_hashes[0];
        let single: serde_json::Value = get_ok(&app, &format!("/pools/{pool_id}/metadata")).await;
        let extended: Vec<serde_json::Value> = get_ok(&app, "/pools/extended").await;
        let updates: Vec<serde_json::Value> =
            get_ok(&app, &format!("/txs/{tx_hash}/pool_updates")).await;
        let listed = extended
            .iter()
            .find(|row| row["pool_id"] == pool_id)
            .unwrap();
        let update = updates
            .iter()
            .find(|row| row["pool_id"] == pool_id)
            .unwrap();
        for metadata in [&single, &listed["metadata"], &update["metadata"]] {
            assert_eq!(metadata["ticker"], "CACHE");
            assert_eq!(metadata["name"], "Cached pool");
            assert_eq!(metadata["description"], "A pool");
            assert_eq!(metadata["homepage"], "https://example.com");
            assert_eq!(metadata["url"], "https://example.invalid/pool.json");
            assert_eq!(metadata["hash"], hex::encode(Hasher::<256>::hash(body)));
        }
        assert!(single["error"].is_null());
    }

    #[tokio::test]
    async fn the_pool_list_and_metadata_routes_share_a_cached_failure() {
        use dolos_testing::synthetic::SyntheticBlockConfig;
        use pallas::ledger::primitives::{
            conway::Certificate, Bytes, PoolMetadata, RationalNumber, Set,
        };

        let metadata = PoolMetadata {
            url: "https://example.invalid/failed.json".to_string(),
            hash: vec![7u8; 32].into(),
        };
        let operator = [0x7a; 28];
        let registration = Certificate::PoolRegistration {
            operator: operator.into(),
            vrf_keyhash: [1u8; 32].into(),
            pledge: 0,
            cost: 0,
            margin: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account: Bytes::from([&[0xe0][..], &[0x22; 28][..]].concat()),
            pool_owners: Set::from(vec![[3u8; 28].into()]),
            relays: vec![],
            pool_metadata: Some(metadata.clone()),
        };
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            extra_certs_by_block: vec![vec![vec![registration]]],
            ..Default::default()
        });
        let error = crate::mapping::pool_connection_error(&metadata.url);
        crate::offchain::cache_pool_failure(app.facade().inner.cache(), &metadata, error.clone())
            .await;

        let pool_id = crate::mapping::bech32_pool(operator).unwrap();
        let expected = serde_json::to_value(&error).unwrap();
        let single: serde_json::Value = get_ok(&app, &format!("/pools/{pool_id}/metadata")).await;
        let extended: Vec<serde_json::Value> = get_ok(&app, "/pools/extended").await;
        let listed = extended
            .iter()
            .find(|row| row["pool_id"] == pool_id)
            .unwrap();
        assert_eq!(single["error"], expected);
        assert_eq!(listed["metadata"]["error"], expected);
        assert!(listed["metadata"]["ticker"].is_null());
    }

    /// db-sync's rule: a certificate registers a pool that was not registered
    /// at the end of the previous block, because it never registered or its
    /// retirement took effect, and otherwise updates it, cancelling a pending
    /// retirement. A registration is active two epochs on and takes the
    /// deposit; an update three, and takes none.
    #[tokio::test]
    async fn txs_pool_updates_follow_the_registration_at_the_previous_block() {
        use blockfrost_openapi::models::tx_content::TxContent;
        use dolos_testing::synthetic::SyntheticBlockConfig;
        use pallas::ledger::primitives::{conway::Certificate, Bytes, RationalNumber, Set};

        use crate::mapping::bech32_pool;

        const PENDING: [u8; 28] = [0x77; 28];
        const REAPED: [u8; 28] = [0x78; 28];

        let register = |operator: [u8; 28]| Certificate::PoolRegistration {
            operator: operator.into(),
            vrf_keyhash: [1u8; 32].into(),
            pledge: 0,
            cost: 0,
            margin: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account: Bytes::from([&[0xe0][..], &[0x21; 28][..]].concat()),
            pool_owners: Set::from(vec![[2u8; 28].into()]),
            relays: vec![],
            pool_metadata: None,
        };

        // every block is past epoch 0 and short of epoch 1_000
        let retire =
            |operator: [u8; 28], epoch| Certificate::PoolRetirement(operator.into(), epoch);

        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 4,
            extra_certs_by_block: vec![
                vec![vec![register(PENDING), register(REAPED)]],
                vec![vec![retire(PENDING, 1_000), retire(REAPED, 0)]],
                vec![vec![register(PENDING), register(REAPED)]],
                vec![vec![]],
            ],
            ..Default::default()
        });

        let tx = |block: usize| app.vectors().blocks[block].tx_hashes[0].clone();
        let chain = app.facade().get_chain_summary().unwrap();

        let active_epochs = |block: usize| {
            let app = &app;
            let chain = chain.clone();

            async move {
                let content: TxContent = get_ok(app, &format!("/txs/{}", tx(block))).await;
                let (epoch, _) = chain.slot_epoch(content.slot as u64);

                let updates: Vec<TxContentPoolCertsInner> =
                    get_ok(app, &format!("/txs/{}/pool_updates", tx(block))).await;

                [PENDING, REAPED].map(|operator| {
                    let pool_id = bech32_pool(operator).unwrap();
                    let update = updates.iter().find(|x| x.pool_id == pool_id).unwrap();
                    update.active_epoch - epoch as i32
                })
            }
        };

        assert_eq!(active_epochs(0).await, [2, 2], "first registrations");
        assert_eq!(
            active_epochs(2).await,
            [3, 2],
            "the retirement cancelled, then reaped"
        );

        let deposit = |block: usize| {
            let app = &app;

            async move {
                let content: TxContent = get_ok(app, &format!("/txs/{}", tx(block))).await;
                content.deposit.parse::<i64>().unwrap()
            }
        };

        let pool_deposit = app
            .facade()
            .get_current_effective_pparams()
            .unwrap()
            .pool_deposit()
            .unwrap() as i64;

        // block 3 carries the synthetic certificates alone
        assert_eq!(
            deposit(2).await - deposit(3).await,
            pool_deposit,
            "the reaped pool's"
        );
    }

    /// What `/txs/{hash}` and `/txs/{hash}/pool_updates` make of the pool
    /// certificates in tx `tx` of `block`: `active_epoch` less the tx's epoch
    /// for each certificate of `pools`, in order, and the tx `deposit`.
    async fn pool_updates_of(
        app: &TestApp,
        block: usize,
        tx: usize,
        pools: &[[u8; 28]],
    ) -> (Vec<i32>, i64) {
        use blockfrost_openapi::models::tx_content::TxContent;

        use crate::mapping::bech32_pool;

        let tx = app.vectors().blocks[block].tx_hashes[tx].clone();
        let chain = app.facade().get_chain_summary().unwrap();

        let content: TxContent = get_ok(app, &format!("/txs/{tx}")).await;
        let (epoch, _) = chain.slot_epoch(content.slot as u64);

        let updates: Vec<TxContentPoolCertsInner> =
            get_ok(app, &format!("/txs/{tx}/pool_updates")).await;

        let pool_ids: Vec<String> = pools.iter().map(|x| bech32_pool(*x).unwrap()).collect();

        let active = updates
            .iter()
            .filter(|x| pool_ids.contains(&x.pool_id))
            .map(|x| x.active_epoch - epoch as i32)
            .collect();

        (active, content.deposit.parse().unwrap())
    }

    fn pool_deposit(app: &TestApp) -> i64 {
        app.facade()
            .get_current_effective_pparams()
            .unwrap()
            .pool_deposit()
            .unwrap() as i64
    }

    fn register(operator: [u8; 28]) -> pallas::ledger::primitives::conway::Certificate {
        use pallas::ledger::primitives::{conway::Certificate, Bytes, RationalNumber, Set};

        Certificate::PoolRegistration {
            operator: operator.into(),
            vrf_keyhash: [1u8; 32].into(),
            pledge: 0,
            cost: 0,
            margin: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account: Bytes::from([&[0xe0][..], &[0x21; 28][..]].concat()),
            pool_owners: Set::from(vec![[2u8; 28].into()]),
            relays: vec![],
            pool_metadata: None,
        }
    }

    /// A pool the Shelley genesis stakes is registered before any block, so
    /// its first certificate is an update: active three epochs on, with no
    /// deposit.
    #[tokio::test]
    async fn txs_pool_updates_treat_a_genesis_pool_as_registered() {
        use dolos_testing::synthetic::SyntheticBlockConfig;

        const GENESIS: [u8; 28] = [0x7c; 28];
        const FRESH: [u8; 28] = [0x7d; 28];

        // block 0 registers the synthetic chain's own pool, so the certificates
        // go in block 1, and block 2 carries that pool's update alone
        let app = TestApp::builder()
            .with_cfg(SyntheticBlockConfig {
                block_count: 3,
                extra_certs_by_block: vec![
                    vec![vec![]],
                    vec![vec![register(GENESIS), register(FRESH)]],
                    vec![vec![]],
                ],
                ..Default::default()
            })
            .with_genesis_pool(GENESIS)
            .build();

        let (active, deposit) = pool_updates_of(&app, 1, 0, &[GENESIS, FRESH]).await;
        let (_, baseline) = pool_updates_of(&app, 2, 0, &[]).await;

        assert_eq!(active, [3, 2]);
        assert_eq!(
            deposit - baseline,
            pool_deposit(&app),
            "the fresh pool's alone"
        );
    }

    /// db-sync reads the pools before the ledger ticks into a block's epoch.
    /// In the first block of the epoch a retirement takes effect in, the
    /// pool was still registered at the end of the previous block, so its
    /// certificate is an update, active three epochs on, while the ledger
    /// has reaped it and takes the deposit. In a later block of that epoch
    /// it is a registration.
    ///
    /// The rule reads the previous certificate only, so the pools retire
    /// without registering first: the synthetic txs fund no deposit, and one
    /// taken before the boundary would leave its pots unbalanced.
    #[tokio::test]
    async fn txs_pool_updates_in_the_first_block_of_the_retiring_epoch() {
        use dolos_testing::synthetic::SyntheticBlockConfig;
        use pallas::ledger::primitives::conway::Certificate;

        const FIRST: [u8; 28] = [0x7e; 28];
        const LATER: [u8; 28] = [0x7f; 28];

        // three blocks an epoch, from the start of epoch 2, so the deposits
        // of epoch 3 cross no boundary
        let third_of_epoch = 28_800;
        let retiring = 3;
        let retire = |operator: [u8; 28]| Certificate::PoolRetirement(operator.into(), retiring);

        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 6,
            slot_step: third_of_epoch,
            plain_txs: true,
            extra_certs_by_block: vec![
                vec![vec![]],
                vec![vec![retire(FIRST), retire(LATER)]],
                vec![vec![]],
                vec![vec![register(FIRST)]],
                vec![vec![register(LATER)]],
                vec![vec![]],
            ],
            // each tx pays a fee of 7 out of its seed, so the pots balance
            // across the boundary
            seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
            ..Default::default()
        });

        let chain = app.facade().get_chain_summary().unwrap();
        let block_epoch = |block: usize| chain.slot_epoch(app.vectors().blocks[block].slot).0;
        assert_eq!(
            [2, 3, 4, 5].map(block_epoch),
            [retiring - 1, retiring, retiring, retiring],
            "block 3 opens the retiring epoch"
        );

        let (_, baseline) = pool_updates_of(&app, 5, 0, &[]).await;

        let (active, deposit) = pool_updates_of(&app, 3, 0, &[FIRST]).await;
        assert_eq!(active, [3], "still registered at the previous block");
        assert_eq!(deposit - baseline, pool_deposit(&app), "reaped by the tick");

        let (active, deposit) = pool_updates_of(&app, 4, 0, &[LATER]).await;
        assert_eq!(active, [2]);
        assert_eq!(deposit - baseline, pool_deposit(&app));
    }

    /// The ledger applies a block's certificates one after another, and
    /// db-sync counts an update stored earlier in the block. A pool's second
    /// certificate in a block, in a later tx or the same one, is an update,
    /// active three epochs on, with no deposit.
    #[tokio::test]
    async fn txs_pool_updates_after_an_earlier_certificate_in_the_block() {
        use dolos_testing::synthetic::SyntheticBlockConfig;

        const ACROSS: [u8; 28] = [0x7a; 28];
        const WITHIN: [u8; 28] = [0x7b; 28];

        // block 2 carries the synthetic certificates alone, in both txs
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 3,
            extra_certs_by_block: vec![
                vec![vec![]],
                vec![
                    vec![register(ACROSS), register(WITHIN), register(WITHIN)],
                    vec![register(ACROSS)],
                ],
                vec![vec![]],
            ],
            ..Default::default()
        });

        let (_, baseline) = pool_updates_of(&app, 2, 0, &[]).await;

        let (active, deposit) = pool_updates_of(&app, 1, 0, &[ACROSS, WITHIN]).await;
        assert_eq!(active, [2, 2, 3]);
        assert_eq!(deposit - baseline, 2 * pool_deposit(&app), "once a pool");

        let (_, baseline) = pool_updates_of(&app, 2, 1, &[]).await;

        let (active, deposit) = pool_updates_of(&app, 1, 1, &[ACROSS]).await;
        assert_eq!(active, [3]);
        assert_eq!(deposit, baseline);
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{INVALID_HASH}/pool_updates");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{MISSING_HASH}/pool_updates");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_updates");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
