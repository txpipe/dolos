use std::collections::{hash_map::Entry, HashMap};

use axum::{
    extract::{Path, Query, State},
    Json,
};
use blockfrost_openapi::models::{
    script::{Script, Type as ScriptType},
    script_cbor::ScriptCbor,
    script_datum::ScriptDatum,
    script_datum_cbor::ScriptDatumCbor,
    script_json::ScriptJson,
    script_redeemers_inner::ScriptRedeemersInner,
    script_utxos_inner::ScriptUtxosInner,
};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, CardanoIndexExt, ScriptLanguage, SlotOrder};
use dolos_cardano::ChainSummary;
use dolos_core::Domain;
use futures_util::StreamExt;
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::alonzo::NativeScript;
use pallas::ledger::primitives::{conway::RedeemerTag, Epoch, ExUnitPrices};
use pallas::ledger::traverse::{ComputeHash, MultiEraBlock, MultiEraTx};
use pallas::{codec::minicbor, ledger::primitives::ToCanonicalJson};
use reqwest::StatusCode;

use crate::{
    error::Error,
    inputs::InputDeps,
    log_and_500,
    mapping::{
        redeemer_fee, redeemer_script_hash, script_redeemer_purpose, IntoModel, PlutusDataWrapper,
    },
    pagination::{Pagination, PaginationParameters},
    Facade,
};

fn parse_script_hash(script_hash: &str) -> Result<Hash<28>, StatusCode> {
    if script_hash.len() != 56 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Hash::<28>::from(
        hex::decode(script_hash)
            .map_err(|_| StatusCode::NOT_FOUND)?
            .as_slice(),
    ))
}

fn parse_datum_hash(datum_hash: &str) -> Result<Hash<32>, StatusCode> {
    if datum_hash.len() != 64 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Hash::<32>::from(
        hex::decode(datum_hash)
            .map_err(|_| StatusCode::NOT_FOUND)?
            .as_slice(),
    ))
}

pub async fn by_hash<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Script>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(Script {
        script_hash,
        r#type: match script.language {
            ScriptLanguage::Native => ScriptType::Timelock,
            ScriptLanguage::PlutusV1 => ScriptType::PlutusV1,
            ScriptLanguage::PlutusV2 => ScriptType::PlutusV2,
            ScriptLanguage::PlutusV3 => ScriptType::PlutusV3,
        },
        serialised_size: match script.language {
            ScriptLanguage::Native => None,
            _ => Some(script.script.len() as i32),
        },
    }))
}

pub async fn by_hash_json<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptJson>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let json = match script.language {
        ScriptLanguage::Native => {
            let native: NativeScript =
                minicbor::decode(&script.script).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            // Some(native_script_json(&native)?)
            Some(native.to_json())
        }
        _ => None,
    };

    Ok(Json(ScriptJson { json }))
}

pub async fn by_hash_cbor<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptCbor>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let cbor = match script.language {
        ScriptLanguage::Native => None,
        _ => Some(hex::encode(script.script)),
    };

    Ok(Json(ScriptCbor { cbor }))
}

pub async fn by_hash_utxos<D>(
    Path(script_hash): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ScriptUtxosInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_script_hash(&script_hash)?;
    let pagination = Pagination::try_from(params)?;

    let refs = domain
        .indexes()
        .utxos_by_script_ref(hash.as_slice())
        .map_err(log_and_500("failed to query script_ref index"))?;

    // an unknown script is a 404. a known script that no live UTxO holds as a
    // reference script is an empty page. the index cannot tell the two apart,
    // so the archive existence check runs only when the index returns nothing.
    if refs.is_empty() {
        domain
            .query()
            .script_by_hash(&hash)
            .await
            .map_err(log_and_500("failed to query script by hash"))?
            .ok_or(StatusCode::NOT_FOUND)?;

        return Ok(Json(vec![]));
    }

    let items = super::utxos::load_utxo_models(&domain, refs, pagination).await?;

    Ok(Json(items))
}

pub async fn by_hash_redeemers<D>(
    Path(script_hash): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ScriptRedeemersInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = parse_script_hash(&script_hash)?;

    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let scan =
        scan_script_redeemers(&domain, hash, &pagination, domain.config.max_scan_items()).await?;

    if scan.budget_exhausted {
        return Err(crate::pagination::PaginationError::ScanLimitExceeded.into());
    }

    // an unknown script is a 404. a known script with no redeemers is an
    // empty page. dimension activity alone proves the script is known, so
    // the archive existence lookup only runs when there is no activity —
    // on a pruned node the block that carried the script bytes can age
    // out while tagged executions stay inside the window.
    if !scan.saw_candidates {
        domain
            .query()
            .script_by_hash(&hash)
            .await
            .map_err(log_and_500("failed to query script by hash"))?
            .ok_or(StatusCode::NOT_FOUND)?;

        return Ok(Json(vec![]));
    }

    let items = scan
        .rows
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect();

    Ok(Json(items))
}

/// The outcome of a dimension scan: the rows found, whether any tagged
/// block was seen at all, and whether the scan gave up on its block budget.
struct RedeemerScan {
    rows: Vec<ScriptRedeemersInner>,
    saw_candidates: bool,
    budget_exhausted: bool,
}

/// Scan the chain for redeemers that point at `hash`, in the pagination's
/// order. The scan stops once it has enough matches to fill the requested
/// page, or once it has consumed `max_scan_items` tagged blocks — the
/// dimension also tags phase-2-failed executions, so a script whose
/// executions all filter out must not scan its history unbounded.
async fn scan_script_redeemers<D>(
    domain: &Facade<D>,
    hash: Hash<28>,
    pagination: &Pagination,
    max_scan_items: u64,
) -> Result<RedeemerScan, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let chain = domain.get_chain_summary()?;
    let end_slot = domain.get_tip_slot()?;
    let order = SlotOrder::from(pagination.order);

    let stream = domain
        .query()
        .blocks_by_script_redeemers_stream(&hash, 0, end_slot, order);

    let mut stream = Box::pin(stream);

    let mut deps = InputDeps::default();
    let mut prices_by_epoch: HashMap<Epoch, ExUnitPrices> = HashMap::new();
    let mut matches: Vec<ScriptRedeemersInner> = Vec::new();
    let target = pagination.from() + pagination.count;
    let mut scanned: u64 = 0;

    while let Some(next) = stream.next().await {
        if scanned >= max_scan_items {
            return Ok(RedeemerScan {
                rows: matches,
                saw_candidates: true,
                budget_exhausted: true,
            });
        }
        scanned += 1;

        let (slot, body) = next.map_err(log_and_500("failed to stream script activity"))?;

        let Some(body) = body else {
            continue;
        };

        let block = MultiEraBlock::decode(&body).map_err(log_and_500("failed to decode block"))?;

        let txs = block.txs();

        // db-sync stores no redeemers for phase-2-failed txs, so Blockfrost
        // lists none. skip them. their spend redeemers also point at regular
        // inputs, but a failed tx consumes only its collateral.
        let mut with_redeemers: Vec<&MultiEraTx> = txs
            .iter()
            .filter(|tx| tx.is_valid() && !tx.redeemers().is_empty())
            .collect();

        if with_redeemers.is_empty() {
            continue;
        }

        if matches!(order, SlotOrder::Desc) {
            with_redeemers.reverse();
        }

        // only txs with spend redeemers need input resolution
        let spending = with_redeemers.iter().copied().filter(|tx| {
            tx.redeemers()
                .iter()
                .any(|redeemer| matches!(redeemer.tag(), RedeemerTag::Spend))
        });

        let mut resolver = deps.prepare(domain, spending).await?;

        let (epoch, _) = chain.slot_epoch(slot);

        for tx in with_redeemers {
            let mut redeemers = tx.redeemers();

            if matches!(order, SlotOrder::Desc) {
                redeemers.reverse();
            }

            for redeemer in redeemers {
                // resolve() returns None when the producing tx is pruned. On
                // a history-pruned node, a spend row inside the window drops
                // when its consumed output predates the window — the same
                // limit every input-resolving endpoint has.
                let resolved = redeemer_script_hash(tx, &redeemer, &mut |input| {
                    let Some(output) = resolver.resolve(input)? else {
                        return Ok(None);
                    };

                    output
                        .address()
                        .map(Some)
                        .map_err(log_and_500("failed to decode input address"))
                })?;

                if resolved != Some(hash) {
                    continue;
                }

                let purpose = script_redeemer_purpose(redeemer.tag());

                let prices = prices_for_epoch(domain, &chain, epoch, &mut prices_by_epoch)?;
                let units = redeemer.ex_units();
                let fee = redeemer_fee(&units, prices)?;
                let data_hash = redeemer.data().compute_hash().to_string();

                matches.push(ScriptRedeemersInner {
                    tx_hash: tx.hash().to_string(),
                    tx_index: redeemer.index() as i32,
                    purpose,
                    redeemer_data_hash: data_hash.clone(),
                    // DEPRECATED in Blockfrost. same value as redeemer_data_hash.
                    datum_hash: data_hash,
                    unit_mem: units.mem.to_string(),
                    unit_steps: units.steps.to_string(),
                    fee: fee.to_string(),
                });

                if matches.len() >= target {
                    return Ok(RedeemerScan {
                        rows: matches,
                        saw_candidates: true,
                        budget_exhausted: false,
                    });
                }
            }
        }
    }

    Ok(RedeemerScan {
        saw_candidates: scanned > 0,
        rows: matches,
        budget_exhausted: false,
    })
}

/// The execution prices of an epoch, memoized: most scanned blocks
/// contribute no matches and must not pay a pparams lookup.
fn prices_for_epoch<'a, D>(
    domain: &Facade<D>,
    chain: &ChainSummary,
    epoch: Epoch,
    cache: &'a mut HashMap<Epoch, ExUnitPrices>,
) -> Result<&'a ExUnitPrices, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    match cache.entry(epoch) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            // the oldest retained epoch's own log is pruned when the history
            // cutoff falls inside the epoch. the next epoch's log still
            // carries that epoch's value in its mark (epoch - 1) slot.
            let pparams = match domain.get_effective_pparams_for_epoch(epoch, chain) {
                Ok(pparams) => pparams,
                Err(_) => domain
                    .get_epoch_log(epoch + 1, chain)?
                    .and_then(|log| log.pparams.mark().cloned())
                    .ok_or_else(|| {
                        tracing::error!(epoch, "no effective pparams for epoch");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?,
            };

            let prices = pparams.execution_costs().ok_or_else(|| {
                tracing::error!(epoch, "no execution prices in effective pparams");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            Ok(entry.insert(prices))
        }
    }
}

pub async fn by_datum_hash<D>(
    Path(datum_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptDatum>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let datum = domain
        .query()
        .plutus_data(&parse_datum_hash(&datum_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ScriptDatum {
        json_value: PlutusDataWrapper(datum).into_model()?,
    }))
}

pub async fn by_datum_hash_cbor<D>(
    Path(datum_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptDatumCbor>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let datum = domain
        .query()
        .plutus_data(&parse_datum_hash(&datum_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ScriptDatumCbor {
        cbor: hex::encode(minicbor::to_vec(&datum).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::script_redeemers_inner::Purpose;
    use dolos_testing::synthetic::{MintRedeemerConfig, SyntheticBlockConfig};

    fn fixture_app() -> TestApp {
        TestApp::new()
    }

    /// An app whose chain executes one mint script per block, plus one
    /// phase-2-invalid execution of the same script in the first block.
    fn redeemer_app() -> TestApp {
        let cfg = SyntheticBlockConfig {
            mint_redeemer: Some(MintRedeemerConfig {
                include_invalid_tx: true,
                ..Default::default()
            }),
            ..Default::default()
        };

        TestApp::new_with_cfg(cfg)
    }

    /// The exact rows the endpoint must return for the executed script,
    /// in ascending chain order.
    fn expected_redeemer_rows(app: &TestApp) -> Vec<ScriptRedeemersInner> {
        let vectors = app
            .vectors()
            .redeemers
            .as_ref()
            .expect("missing redeemer vectors");

        vectors
            .tx_hashes
            .iter()
            .map(|tx_hash| ScriptRedeemersInner {
                tx_hash: tx_hash.clone(),
                tx_index: vectors.redeemer_index as i32,
                purpose: Purpose::Mint,
                redeemer_data_hash: vectors.data_hash.clone(),
                datum_hash: vectors.data_hash.clone(),
                unit_mem: vectors.unit_mem.to_string(),
                unit_steps: vectors.unit_steps.to_string(),
                // preview prices: mem 577/10000, steps 721/10000000.
                // fee = ceil(1000000 * 577/10000 + 500000000 * 721/10000000)
                //     = 57700 + 36050 = 93750.
                fee: "93750".to_string(),
            })
            .collect()
    }

    async fn get_redeemer_rows(app: &TestApp, path: &str) -> Vec<ScriptRedeemersInner> {
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        serde_json::from_slice(&bytes).expect("failed to parse script redeemers")
    }

    fn invalid_script_hash() -> &'static str {
        "not-a-script-hash"
    }

    fn missing_script_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    fn invalid_datum_hash() -> &'static str {
        "not-a-datum-hash"
    }

    fn missing_datum_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[tokio::test]
    async fn scripts_by_hash_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: Script = serde_json::from_slice(&bytes).expect("failed to parse script");
        assert_eq!(item.script_hash, script_hash);
        assert_eq!(item.r#type, ScriptType::Timelock);
        assert_eq!(item.serialised_size, None);
    }

    #[tokio::test]
    async fn scripts_by_hash_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_json_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/json");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptJson = serde_json::from_slice(&bytes).expect("failed to parse script json");
        assert!(item.json.is_some());
    }

    #[tokio::test]
    async fn scripts_by_hash_json_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/json", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_json_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/json", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_json_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/json");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptCbor = serde_json::from_slice(&bytes).expect("failed to parse script cbor");
        assert_eq!(item.cbor, None);
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/cbor", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/cbor", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse script utxos");

        assert!(!items.is_empty());

        for item in items {
            assert_eq!(item.reference_script_hash, script_hash);
            assert_eq!(item.address, app.vectors().address);
            assert_eq!(
                item.data_hash.as_deref(),
                Some(app.vectors().datum_hash.as_str())
            );
            assert_eq!(item.inline_datum, None);
            assert!(!item.block.is_empty());
            assert!(item.amount.iter().any(|x| x.unit == "lovelace"));
        }
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_paginated() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();

        let path_page_1 = format!("/scripts/{script_hash}/utxos?page=1&count=1");
        let path_page_2 = format!("/scripts/{script_hash}/utxos?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse utxos page 1");
        let page_2: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse utxos page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);

        let key = |x: &ScriptUtxosInner| format!("{}#{}", x.tx_hash, x.output_index);
        assert_ne!(key(&page_1[0]), key(&page_2[0]));
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_order_asc() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?order=asc");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos asc");

        assert!(!asc.is_empty());

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
    async fn scripts_by_hash_utxos_order_desc() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?order=desc");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<ScriptUtxosInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos desc");

        assert!(!desc.is_empty());

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
    async fn scripts_by_hash_utxos_bad_request_for_invalid_pagination() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos?count=0");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_happy_path() {
        let app = redeemer_app();
        let script_hash = app
            .vectors()
            .redeemers
            .as_ref()
            .expect("missing redeemer vectors")
            .script_hash
            .clone();

        let path = format!("/scripts/{script_hash}/redeemers");
        let items = get_redeemer_rows(&app, &path).await;

        // exact rows in ascending chain order: one mint execution per block
        assert_eq!(items, expected_redeemer_rows(&app));
        assert!(!items.is_empty());
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_empty_for_script_without_executions() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/redeemers");
        let items = get_redeemer_rows(&app, &path).await;

        // the default synthetic chain executes no scripts. a known script
        // with no redeemers gives an empty page, not a 404.
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_order_desc_reverses_asc() {
        let app = redeemer_app();
        let script_hash = app
            .vectors()
            .redeemers
            .as_ref()
            .expect("missing redeemer vectors")
            .script_hash
            .clone();

        let asc =
            get_redeemer_rows(&app, &format!("/scripts/{script_hash}/redeemers?order=asc")).await;
        let desc = get_redeemer_rows(
            &app,
            &format!("/scripts/{script_hash}/redeemers?order=desc"),
        )
        .await;

        let expected = expected_redeemer_rows(&app);
        assert!(expected.len() >= 2, "ordering needs at least two rows");

        assert_eq!(asc, expected);

        let mut reversed = expected;
        reversed.reverse();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_paginated() {
        let app = redeemer_app();
        let script_hash = app
            .vectors()
            .redeemers
            .as_ref()
            .expect("missing redeemer vectors")
            .script_hash
            .clone();

        let page_1 = get_redeemer_rows(
            &app,
            &format!("/scripts/{script_hash}/redeemers?count=1&page=1"),
        )
        .await;
        let page_2 = get_redeemer_rows(
            &app,
            &format!("/scripts/{script_hash}/redeemers?count=1&page=2"),
        )
        .await;

        let expected = expected_redeemer_rows(&app);
        assert!(expected.len() >= 2, "pagination needs at least two rows");

        assert_eq!(page_1, expected[0..1]);
        assert_eq!(page_2, expected[1..2]);
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_skip_phase2_invalid_tx() {
        let app = redeemer_app();
        let vectors = app
            .vectors()
            .redeemers
            .as_ref()
            .expect("missing redeemer vectors");
        let script_hash = vectors.script_hash.clone();
        let invalid_tx_hash = vectors
            .invalid_tx_hash
            .clone()
            .expect("missing invalid tx hash");

        // the chain carries the failed execution
        assert!(app.vectors().blocks[0].tx_hashes.contains(&invalid_tx_hash));

        let path = format!("/scripts/{script_hash}/redeemers");
        let items = get_redeemer_rows(&app, &path).await;

        // db-sync stores no redeemers for phase-2-failed txs
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| item.tx_hash != invalid_tx_hash));
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_bad_request_for_invalid_pagination() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/redeemers?count=0");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/utxos", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/redeemers", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/utxos", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/redeemers", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_redeemers_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/redeemers");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_happy_path() {
        let app = fixture_app();
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptDatum =
            serde_json::from_slice(&bytes).expect("failed to parse script datum");
        assert_eq!(item.json_value.get("int"), Some(&serde_json::json!(42)));
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}", invalid_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}", missing_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_happy_path() {
        let app = fixture_app();
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptDatumCbor =
            serde_json::from_slice(&bytes).expect("failed to parse script datum cbor");
        assert_eq!(item.cbor, app.vectors().datum_cbor_hex);
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}/cbor", invalid_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}/cbor", missing_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
