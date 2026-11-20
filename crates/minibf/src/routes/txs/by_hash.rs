use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content::TxContent;
use dolos_cardano::{AccountState, DRepState, PoolState};
use dolos_core::Domain;
use pallas::ledger::traverse::Era;

use crate::{
    hacks::genesis_tx,
    inputs::InputDeps,
    mapping::{txs::TxModelBuilder, IntoModel as _},
    Facade,
};

use super::parse_tx_hash;

pub async fn by_hash<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
    Option<DRepState>: From<D::Entity>,
{
    let hash = parse_tx_hash(&tx_hash)?;

    let (raw, order) = match domain.get_block_by_tx_hash(&hash).await {
        Ok(block) => block,
        Err(StatusCode::NOT_FOUND) => {
            return Ok(Json(genesis_tx::content_for_hash(&domain, &hash)?));
        }
        Err(err) => return Err(err),
    };

    let chain = domain.get_chain_summary()?;

    let mut builder = TxModelBuilder::new(&raw, order)?
        .with_chain(chain)
        .with_historical_pparams::<D>(&domain)?;

    if builder.era() == Era::Byron {
        let mut deps = InputDeps::default();
        let mut resolver = {
            let tx = builder.tx()?;
            deps.prepare(&domain, [&tx])
                .await?
                .with_genesis(domain.genesis())
        };
        builder = builder.with_resolved_fee(&mut resolver)?;
    }

    builder.compute_deposit(&domain)?;

    builder.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn byron_txs_by_hash_reports_paid_fee_not_policy_minimum() {
        use crate::test_support::{byron_app, byron_block, byron_funding, byron_tx};
        use dolos_core::TxoRef;
        use pallas::ledger::traverse::MultiEraBlock;

        for paid in [200_000, 0] {
            let (funding, hash) = byron_funding(&[3_000_000, 1_700_000]);
            let raw = byron_block(
                11,
                vec![
                    byron_tx(&[TxoRef([0x77; 32].into(), 0)], &[1_000_000]),
                    byron_tx(
                        &[TxoRef(hash, 0), TxoRef(hash, 1)],
                        &[4_000_000, 700_000 - paid],
                    ),
                ],
            );
            let block = MultiEraBlock::decode(&raw).unwrap();
            let tx = block.txs()[1].clone();
            let tx_hash = tx.hash().to_string();
            assert_ne!(tx.fee_or_compute(), paid);
            let app = byron_app(vec![funding, raw.clone()]);
            let parsed: TxContent = get_ok(&app, &format!("/txs/{tx_hash}")).await;
            assert_eq!(parsed.hash, tx_hash);
            assert_eq!(parsed.index, 1);
            assert_eq!(parsed.fees, paid.to_string());
            assert_eq!(
                parsed.output_amount[0].quantity,
                (4_700_000 - paid).to_string()
            );
        }
    }

    #[tokio::test]
    async fn byron_tx_spending_a_genesis_output_reports_its_paid_fee() {
        use crate::test_support::{byron_app, byron_block, byron_tx};
        use dolos_core::TxoRef;
        use pallas::ledger::traverse::MultiEraBlock;

        use pallas::interop::hardano::configs::byron;

        let genesis = dolos_cardano::include::preview::load();
        let (source, _, amount) = byron::genesis_utxos(&genesis.byron)
            .into_iter()
            .max_by_key(|(_, _, amount)| *amount)
            .expect("the preview genesis has byron utxos");

        // a redemption pays no fee, which is below the policy minimum
        for paid in [170_000, 0] {
            let raw = byron_block(11, vec![byron_tx(&[TxoRef(source, 0)], &[amount - paid])]);
            let block = MultiEraBlock::decode(&raw).unwrap();
            let tx = &block.txs()[0];
            assert_ne!(tx.fee_or_compute(), paid);
            let tx_hash = tx.hash();

            let app = byron_app(vec![raw.clone()]);
            let parsed: TxContent = get_ok(&app, &format!("/txs/{tx_hash}")).await;
            assert_eq!(parsed.fees, paid.to_string());
        }
    }

    #[tokio::test]
    async fn byron_txs_by_hash_falls_back_if_any_source_output_is_missing() {
        use crate::test_support::{byron_app, byron_block, byron_funding, byron_tx};
        use dolos_core::TxoRef;
        use pallas::ledger::traverse::MultiEraBlock;

        let (funding, hash) = byron_funding(&[5_000_000]);
        let known = TxoRef(hash, 0);
        let missing = TxoRef([0x77; 32].into(), 0);
        for inputs in [
            vec![missing.clone()],
            vec![known.clone(), missing.clone()],
            vec![missing, known.clone()],
            vec![known, TxoRef(hash, 1)],
        ] {
            let raw = byron_block(11, vec![byron_tx(&inputs, &[1_000_000])]);
            let block = MultiEraBlock::decode(&raw).unwrap();
            let tx = block.txs()[0].clone();
            let tx_hash = tx.hash().to_string();
            let minimum = tx.fee_or_compute();
            assert_ne!(minimum, 4_000_000);
            let app = byron_app(vec![funding.clone(), raw.clone()]);
            let parsed: TxContent = get_ok(&app, &format!("/txs/{tx_hash}")).await;
            assert_eq!(parsed.fees, minimum.to_string());
            assert_eq!(parsed.utxo_count, (inputs.len() + 1) as i32);
        }
    }

    #[tokio::test]
    async fn byron_fees_fall_back_with_pruned_source_block() {
        use crate::test_support::{byron_app, byron_block, byron_funding, byron_tx};
        use blockfrost_openapi::models::block_content::BlockContent;
        use dolos_core::{ArchiveStore as _, ArchiveWriter as _, ChainPoint, TxoRef};
        use pallas::ledger::traverse::MultiEraBlock;

        let (funding, hash) = byron_funding(&[300_000]);
        let source = MultiEraBlock::decode(&funding).unwrap();
        let point = ChainPoint::Specific(source.slot(), source.hash());
        let raw = byron_block(11, vec![byron_tx(&[TxoRef(hash, 0)], &[129_193])]);
        let block = MultiEraBlock::decode(&raw).unwrap();
        let tx = block.txs()[0].clone();
        let minimum = tx.fee_or_compute().to_string();
        let app = byron_app(vec![funding, raw.clone()]);
        let facade = app.facade();
        let writer = facade.archive().start_writer().unwrap();
        writer.undo(&point).unwrap();
        for (slot, raw) in facade.archive().get_range(Some(12), None).unwrap() {
            let block = MultiEraBlock::decode(&raw).unwrap();
            writer
                .undo(&ChainPoint::Specific(slot, block.hash()))
                .unwrap();
        }
        writer.commit().unwrap();
        assert_eq!(
            facade.archive().slot_by_tx_hash(hash.as_ref()).unwrap(),
            Some(10)
        );

        let parsed: TxContent = get_ok(&app, &format!("/txs/{}", tx.hash())).await;
        assert_eq!(parsed.fees, minimum);
        let parsed: BlockContent = get_ok(&app, &format!("/blocks/{}", block.hash())).await;
        assert_eq!(parsed.fees, Some(minimum));
    }

    #[tokio::test]
    async fn txs_by_hash_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}");
        let parsed: TxContent = get_ok(&app, &path).await;
        assert_eq!(parsed.hash, tx_hash);
        assert!(!parsed.block.is_empty());
        assert!(parsed.block_height > 0);
    }

    #[tokio::test]
    async fn txs_by_hash_genesis_utxo() {
        let app = TestApp::new();
        let (tx_hash, amount) = genesis_utxo();
        let path = format!("/txs/{tx_hash}");
        let parsed: TxContent = get_ok(&app, &path).await;

        // served like Blockfrost does: a synthetic tx in the genesis block
        assert_eq!(parsed.hash, tx_hash);
        assert_eq!(parsed.block, crate::hacks::GENESIS_HASH_PREVIEW);
        assert_eq!(parsed.block_height, 0);
        assert_eq!(parsed.utxo_count, 1);
        assert_eq!(parsed.output_amount.len(), 1);
        assert_eq!(parsed.output_amount[0].quantity, amount.to_string());
    }

    #[tokio::test]
    async fn txs_by_hash_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{MISSING_HASH}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{INVALID_HASH}");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
