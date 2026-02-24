use jsonrpsee::types::Params;
use pallas::{codec::utils::NonEmptySet, ledger::primitives::conway::VKeyWitness};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use tx3_resolver::trp::{
    ChainPoint, CheckStatusResponse, DumpLogsResponse, InflightTx, PeekInflightResponse,
    PeekPendingResponse, PendingTx, ResolveParams, SubmitParams, SubmitResponse, TxEnvelope, TxLog,
    TxStatus, TxWitness,
};

use dolos_core::{Domain, MempoolAwareUtxoStore, MempoolStore as _, StateStore as _, SubmitExt};

use crate::{compiler::load_compiler, utxos::UtxoStoreAdapter};

use super::{Context, Error};

pub async fn trp_resolve<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<TxEnvelope, Error> {
    let params: ResolveParams = params.parse()?;

    let (tx, args) = tx3_resolver::trp::parse_resolve_request(params)?;

    let mut compiler = load_compiler::<D>(&context.domain, &context.config)?;

    let store = MempoolAwareUtxoStore::<D>::new(
        context.domain.state(),
        context.domain.indexes(),
        context.domain.mempool(),
    );

    let utxos = UtxoStoreAdapter::<D>::new(store);

    let resolved = tx3_resolver::resolve_tx(
        tx,
        &args,
        &mut compiler,
        &utxos,
        context.config.max_optimize_rounds.into(),
    )
    .await?;

    Ok(TxEnvelope {
        tx: hex::encode(resolved.payload),
        hash: hex::encode(resolved.hash),
    })
}

fn apply_witnesses(original: &[u8], witnesses: &[TxWitness]) -> Result<Vec<u8>, Error> {
    let tx = pallas::ledger::traverse::MultiEraTx::decode(original)?;

    let mut tx = tx.as_conway().ok_or(Error::UnsupportedTxEra)?.to_owned();

    let map_witness = |witness: &TxWitness| VKeyWitness {
        vkey: Vec::<u8>::from(witness.key.clone()).into(),
        signature: Vec::<u8>::from(witness.signature.clone()).into(),
    };

    let mut witness_set = tx.transaction_witness_set.unwrap();

    let old = witness_set
        .vkeywitness
        .iter()
        .flat_map(|x| x.iter())
        .cloned();

    let new = witnesses.iter().map(map_witness);

    let all: Vec<_> = old.chain(new).collect();

    witness_set.vkeywitness = NonEmptySet::from_vec(all);

    tx.transaction_witness_set = pallas::codec::utils::KeepRaw::from(witness_set);

    Ok(pallas::codec::minicbor::to_vec(&tx).unwrap())
}

pub async fn trp_submit<D: Domain + SubmitExt>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<SubmitResponse, Error> {
    let params: SubmitParams = params.parse()?;

    let mut bytes = Vec::<u8>::from(params.tx);

    if !params.witnesses.is_empty() {
        bytes = apply_witnesses(&bytes, &params.witnesses)?;
    }

    let chain = context.domain.read_chain();

    let hash = context.domain.receive_tx("trp", &chain, &bytes)?;

    Ok(SubmitResponse {
        hash: hash.to_string(),
    })
}

// ── trp.checkStatus ─────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CheckStatusParams {
    hashes: Vec<String>,
}

fn stage_to_string(stage: &dolos_core::MempoolTxStage) -> &'static str {
    match stage {
        dolos_core::MempoolTxStage::Pending => "pending",
        dolos_core::MempoolTxStage::Propagated => "propagated",
        dolos_core::MempoolTxStage::Acknowledged => "acknowledged",
        dolos_core::MempoolTxStage::Confirmed => "confirmed",
        dolos_core::MempoolTxStage::Finalized => "finalized",
        dolos_core::MempoolTxStage::Dropped => "dropped",
        dolos_core::MempoolTxStage::RolledBack => "rolled_back",
        dolos_core::MempoolTxStage::Unknown => "unknown",
    }
}

fn chain_point_to_spec(point: &dolos_core::ChainPoint) -> ChainPoint {
    ChainPoint {
        slot: point.slot(),
        block_hash: point
            .hash()
            .map(|h| hex::encode(h.as_ref()))
            .unwrap_or_default(),
    }
}

pub async fn trp_check_status<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<CheckStatusResponse, Error> {
    let params: CheckStatusParams = params.parse()?;

    let mempool = context.domain.mempool();
    let mut statuses = HashMap::new();

    for hash_hex in &params.hashes {
        let hash_bytes = hex::decode(hash_hex)
            .map_err(|e| Error::InvalidParams(format!("invalid hex hash: {e}")))?;

        if hash_bytes.len() != 32 {
            return Err(Error::InvalidParams(format!(
                "hash must be 32 bytes, got {}",
                hash_bytes.len()
            )));
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&hash_bytes);
        let tx_hash = dolos_core::TxHash::from(arr);

        let status = mempool.check_status(&tx_hash);

        statuses.insert(
            hash_hex.clone(),
            TxStatus {
                stage: stage_to_string(&status.stage).to_string(),
                confirmations: status.confirmations as u64,
                non_confirmations: status.non_confirmations as u64,
                confirmed_at: status.confirmed_at.as_ref().map(chain_point_to_spec),
            },
        );
    }

    Ok(CheckStatusResponse { statuses })
}

// ── trp.dumpLogs ────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DumpLogsParams {
    cursor: Option<u64>,
    limit: Option<usize>,
    include_payload: Option<bool>,
}

pub async fn trp_dump_logs<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<DumpLogsResponse, Error> {
    let params: DumpLogsParams = params.parse()?;

    let cursor = params.cursor.unwrap_or(0);
    let limit = params.limit.unwrap_or(50);
    let include_payload = params.include_payload.unwrap_or(false);

    let mempool = context.domain.mempool();
    let page = mempool.dump_finalized(cursor, limit);
    let entries = page.items;
    let next_cursor = page.next_cursor;

    let entries = entries
        .iter()
        .map(|e| TxLog {
            hash: hex::encode(e.hash.as_ref()),
            stage: stage_to_string(&e.stage).to_string(),
            payload: if include_payload {
                Some(hex::encode(&e.payload.1))
            } else {
                None
            },
            confirmations: e.confirmations as u64,
            non_confirmations: e.non_confirmations as u64,
            confirmed_at: e.confirmed_at.as_ref().map(chain_point_to_spec),
        })
        .collect();

    Ok(DumpLogsResponse {
        entries,
        next_cursor,
    })
}

// ── trp.peekPending ─────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PeekPendingParams {
    limit: Option<usize>,
    include_payload: Option<bool>,
}

pub async fn trp_peek_pending<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<PeekPendingResponse, Error> {
    let params: PeekPendingParams = params.parse()?;

    let limit = params.limit.unwrap_or(50);
    let include_payload = params.include_payload.unwrap_or(false);

    let mempool = context.domain.mempool();
    let peeked = mempool.peek_pending(limit + 1);

    let has_more = peeked.len() > limit;

    let entries = peeked
        .iter()
        .take(limit)
        .map(|tx| PendingTx {
            hash: hex::encode(tx.hash.as_ref()),
            payload: if include_payload {
                Some(hex::encode(&tx.payload.1))
            } else {
                None
            },
        })
        .collect();

    Ok(PeekPendingResponse { entries, has_more })
}

// ── trp.peekInflight ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PeekInflightParams {
    limit: Option<usize>,
    include_payload: Option<bool>,
}

pub async fn trp_peek_inflight<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<PeekInflightResponse, Error> {
    let params: PeekInflightParams = params.parse()?;

    let limit = params.limit.unwrap_or(50);
    let include_payload = params.include_payload.unwrap_or(false);

    let mempool = context.domain.mempool();
    let peeked = mempool.peek_inflight(limit + 1);

    let has_more = peeked.len() > limit;

    let entries = peeked
        .iter()
        .take(limit)
        .map(|tx| InflightTx {
            hash: hex::encode(tx.hash.as_ref()),
            stage: stage_to_string(&tx.stage).to_string(),
            confirmations: tx.confirmations as u64,
            non_confirmations: tx.non_confirmations as u64,
            confirmed_at: tx.confirmed_at.as_ref().map(chain_point_to_spec),
            payload: if include_payload {
                Some(hex::encode(&tx.payload.1))
            } else {
                None
            },
        })
        .collect();

    Ok(PeekInflightResponse { entries, has_more })
}

// ── health ──────────────────────────────────────────────────────────────

pub fn health<D: Domain>(context: &Context<D>) -> bool {
    context.domain.state().read_cursor().is_ok()
}

#[cfg(test)]
mod tests {
    use dolos_core::config::TrpConfig;
    use dolos_testing::toy_domain::ToyDomain;
    use dolos_testing::TestAddress::{Alice, Bob};
    use jsonrpsee::types::ErrorObjectOwned;
    use serde_json::json;

    use crate::metrics::Metrics;

    use super::*;

    async fn setup_test_context() -> Arc<Context<ToyDomain>> {
        let delta = dolos_testing::make_custom_utxo_delta(
            dolos_testing::TestAddress::everyone(),
            2..4,
            |x: &dolos_testing::TestAddress| {
                dolos_testing::utxo_with_random_amount(x, 4_000_000..5_000_000)
            },
        );

        let domain = ToyDomain::new(Some(delta), None);

        Arc::new(Context {
            domain,
            config: Arc::new(TrpConfig {
                max_optimize_rounds: 3,
                extra_fees: None,

                // next are dummy, not used
                listen_address: "[::]:1234".parse().unwrap(),
                permissive_cors: None,
            }),
            metrics: Metrics::default(),
        })
    }

    const SUBJECT_PROTOCOL: &str = r#"ab6466656573a1694576616c506172616d6a457870656374466565736a7265666572656e6365738066696e7075747381a3646e616d6566736f75726365657574786f73a1694576616c506172616da16b457870656374496e7075748266736f75726365a56761646472657373a1694576616c506172616da16b45787065637456616c7565826673656e64657267416464726573736a6d696e5f616d6f756e74a16b4576616c4275696c74496ea16341646482a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74a1694576616c506172616d6a4578706563744665657363726566644e6f6e65646d616e79f46a636f6c6c61746572616cf46872656465656d6572644e6f6e65676f75747075747382a46761646472657373a1694576616c506172616da16b45787065637456616c756582687265636569766572674164647265737365646174756d644e6f6e6566616d6f756e74a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74686f7074696f6e616cf4a46761646472657373a1694576616c506172616da16b45787065637456616c7565826673656e646572674164647265737365646174756d644e6f6e6566616d6f756e74a16b4576616c4275696c74496ea16353756282a16b4576616c4275696c74496ea16353756282a16a4576616c436f65726365a16a496e746f417373657473a1694576616c506172616da16b457870656374496e7075748266736f75726365a56761646472657373a1694576616c506172616da16b45787065637456616c7565826673656e64657267416464726573736a6d696e5f616d6f756e74a16b4576616c4275696c74496ea16341646482a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74a1694576616c506172616d6a4578706563744665657363726566644e6f6e65646d616e79f46a636f6c6c61746572616cf4a16641737365747381a366706f6c696379644e6f6e656a61737365745f6e616d65644e6f6e6566616d6f756e74a1694576616c506172616da16b45787065637456616c756582687175616e7469747963496e74a1694576616c506172616d6a45787065637446656573686f7074696f6e616cf46876616c6964697479f6656d696e747380656275726e7380656164686f63806a636f6c6c61746572616c80677369676e657273f6686d6574616461746180"#;

    async fn attempt_resolve(args: &serde_json::Value) -> Result<TxEnvelope, ErrorObjectOwned> {
        let req = json!({
            "tir": {
                "version": "v1beta0",
                "content": SUBJECT_PROTOCOL,
                "encoding": "hex"
            },
            "args": args,
            "env": {},
        })
        .to_string();

        let params = Params::new(Some(req.as_str()));

        let context = setup_test_context().await;

        let resolved = trp_resolve(params, context.clone()).await?;

        Ok(resolved)
    }

    #[tokio::test]
    async fn test_resolve_happy_path() {
        let args = json!({
            "quantity": 100,
            "sender": Alice.as_str(),
            "receiver": Bob.as_str(),
        });

        let resolved = attempt_resolve(&args).await.unwrap();

        let tx = hex::decode(resolved.tx).unwrap();

        let _ = pallas::ledger::traverse::MultiEraTx::decode(&tx).unwrap();
    }

    #[tokio::test]
    async fn test_resolve_missing_args() {
        let args = json!({});

        let resolved = attempt_resolve(&args).await;

        let err = resolved.unwrap_err();

        dbg!(&err);

        assert_eq!(err.code(), tx3_resolver::trp::errors::CODE_MISSING_TX_ARG);
    }

    #[tokio::test]
    async fn test_resolve_invalid_args() {
        let args = json!({
            "quantity": "abc",
            "sender": "Alice",
            "receiver": "Bob",
        });

        let resolved = attempt_resolve(&args).await;

        let err = resolved.unwrap_err();

        dbg!(&err);

        assert_eq!(err.code(), tx3_resolver::trp::errors::CODE_INTEROP_ERROR);
    }

    // ── trp.checkStatus tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_check_status_unknown() {
        let context = setup_test_context().await;
        let random_hash = hex::encode([0xABu8; 32]);
        let req = json!({ "hashes": [random_hash] }).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_check_status(params, context).await.unwrap();

        let status = response.statuses.get(&random_hash).unwrap();
        assert_eq!(status.stage, "unknown");
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());
    }

    #[tokio::test]
    async fn test_check_status_after_receive() {
        let context = setup_test_context().await;

        let hash = dolos_testing::tx_sequence_to_hash(42);
        let tx = dolos_testing::mempool::make_test_mempool_tx(hash);
        let hash_hex = hex::encode(hash.as_ref());

        dolos_core::MempoolStore::receive(context.domain.mempool(), tx).unwrap();

        let req = json!({ "hashes": [hash_hex] }).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_check_status(params, context).await.unwrap();

        let status = response.statuses.get(&hash_hex).unwrap();
        assert_eq!(status.stage, "pending");
        assert_eq!(status.confirmations, 0);
        assert!(status.confirmed_at.is_none());
    }

    // ── trp.peekPending tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_peek_pending_empty() {
        let context = setup_test_context().await;
        let req = json!({}).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_peek_pending(params, context).await.unwrap();

        assert!(response.entries.is_empty());
        assert!(!response.has_more);
    }

    #[tokio::test]
    async fn test_peek_pending_with_items() {
        let context = setup_test_context().await;

        for n in 0..3u64 {
            let hash = dolos_testing::tx_sequence_to_hash(n);
            let tx = dolos_testing::mempool::make_test_mempool_tx(hash);
            dolos_core::MempoolStore::receive(context.domain.mempool(), tx).unwrap();
        }

        // Peek with limit 2 — should get 2 items and has_more = true
        let req = json!({ "limit": 2 }).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_peek_pending(params, context.clone()).await.unwrap();

        assert_eq!(response.entries.len(), 2);
        assert!(response.has_more);
        assert!(response.entries.iter().all(|e| e.payload.is_none()));

        // Peek with default limit — should get all 3
        let req = json!({}).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_peek_pending(params, context.clone()).await.unwrap();

        assert_eq!(response.entries.len(), 3);
        assert!(!response.has_more);

        // Peek with include_payload — should include cbor
        let req = json!({ "include_payload": true }).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_peek_pending(params, context).await.unwrap();

        assert_eq!(response.entries.len(), 3);
        assert!(response.entries.iter().all(|e| e.payload.is_some()));
    }

    // ── trp.peekInflight tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_peek_inflight_empty() {
        let context = setup_test_context().await;
        let req = json!({}).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_peek_inflight(params, context).await.unwrap();

        assert!(response.entries.is_empty());
        assert!(!response.has_more);
    }

    // ── trp.dumpLogs tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_dump_logs_empty() {
        let context = setup_test_context().await;
        let req = json!({}).to_string();
        let params = Params::new(Some(req.as_str()));

        let response = trp_dump_logs(params, context).await.unwrap();

        assert!(response.entries.is_empty());
        assert!(response.next_cursor.is_none());
    }
}
