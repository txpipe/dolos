use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content_utxo::TxContentUtxo;
use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::Domain;

use crate::{
    hacks::genesis_tx,
    log_and_500,
    mapping::{txs::TxModelBuilder, IntoModel as _},
    Facade,
};

use super::parse_tx_hash;

pub async fn by_hash_utxos<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContentUtxo>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_tx_hash(&tx_hash)?;

    let (raw, order) = match domain.get_block_by_tx_hash(&hash).await {
        Ok(block) => block,
        Err(StatusCode::NOT_FOUND) => {
            return Ok(Json(genesis_tx::utxos_for_hash(&domain, &hash).await?));
        }
        Err(err) => return Err(err),
    };

    let mut builder = TxModelBuilder::new(&raw, order)?;

    let mut consumed_deps = std::collections::HashMap::new();
    for x in builder.required_consumed_deps()? {
        let bytes: Vec<u8> = x.clone().into();
        let maybe = domain
            .query()
            .tx_by_spent_txo(&bytes)
            .await
            .map_err(log_and_500("failed to query tx by spent txo"))?;
        if let Some(tx) = maybe {
            consumed_deps.insert(x, tx);
        }
    }
    builder = builder.with_consumed_deps(consumed_deps);

    let deps = builder.required_deps()?;
    let deps = domain.get_tx_batch(deps).await?;

    for (key, cbor) in deps.iter() {
        if let Some(cbor) = cbor {
            builder.load_dep(*key, cbor)?;
        }
    }

    builder.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn txs_by_hash_utxos_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/utxos");
        let _: TxContentUtxo = get_ok(&app, &path).await;
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_genesis_utxo() {
        let app = TestApp::new();
        let (tx_hash, amount) = genesis_utxo();
        let path = format!("/txs/{tx_hash}/utxos");
        let parsed: TxContentUtxo = get_ok(&app, &path).await;

        // a genesis utxo has no inputs and one unspent output
        assert_eq!(parsed.hash, tx_hash);
        assert!(parsed.inputs.is_empty());
        assert_eq!(parsed.outputs.len(), 1);
        assert_eq!(parsed.outputs[0].output_index, 0);
        assert_eq!(parsed.outputs[0].amount[0].unit, "lovelace");
        assert_eq!(parsed.outputs[0].amount[0].quantity, amount.to_string());
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{INVALID_HASH}/utxos");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{MISSING_HASH}/utxos");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
