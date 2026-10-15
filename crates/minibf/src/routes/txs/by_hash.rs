use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content::TxContent;
use dolos_cardano::{AccountState, DRepState, PoolState};
use dolos_core::Domain;

use crate::{
    hacks::genesis_tx,
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

    builder.compute_deposit(&domain)?;

    builder.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::test_support::{TestApp, TestFault};

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
