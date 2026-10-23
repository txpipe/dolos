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
    tx.fetch_pool_metadata().await?;
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
