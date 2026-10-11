//! `POST /utils/txs/evaluate` and `POST /utils/txs/evaluate/utxos`.
//!
//! Blockfrost passes these requests to Ogmios. Dolos evaluates the redeemers
//! itself and answers in the same Ogmios formats.

mod ogmios;
mod utxo_set;

use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
};

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    Json,
};
use base64::Engine as _;
use dolos_cardano::validate::{evaluate_redeemers, RedeemerReport};
use dolos_core::{
    Domain, EraCbor, MempoolStore as _, MempoolTxStage, StateStore as _, TxoRef, UtxoMap,
};
use pallas::{
    codec::minicbor,
    ledger::{
        primitives::{alonzo, conway},
        traverse::{Era, MultiEraOutput, MultiEraTx},
    },
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::sync::Semaphore;

use self::ogmios::Version;
use crate::{error::Error, log_and_500, Facade};

/// The first protocol version of the Alonzo era, which enables scripts.
const ALONZO_PROTOCOL: u16 = 5;

/// Limits the evaluations that run at the same time, across all requests. A
/// redeemer can run up to the transaction budget, so evaluations are CPU-bound.
static EVALUATIONS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    Arc::new(Semaphore::new(cores))
});

#[derive(Debug, Deserialize)]
pub struct EvaluateParams {
    version: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvaluateUtxosRequest {
    cbor: String,
    additional_utxo_set: Option<Vec<JsonValue>>,
}

/// What an evaluation request yields, before it gets an Ogmios format.
#[derive(Debug)]
enum Outcome {
    /// The payload is hexadecimal text that does not decode.
    InvalidPayload,
    /// No era decodes the transaction. The text is the error of the Conway
    /// decoder.
    Deserialisation(String),
    /// The transaction is from an era before Alonzo.
    IncompatibleEra(&'static str),
    /// The transaction is from an era after Mary but before Conway.
    UnsupportedEra(&'static str),
    /// The ledger is in an era before Alonzo.
    NodeTipTooOld(&'static str),
    /// These additional UTxO entries differ from the ledger entries with the
    /// same references.
    AdditionalUtxoOverlap(Vec<TxoRef>),
    /// The ledger evaluated each redeemer.
    Evaluated(Vec<RedeemerReport>),
}

enum Payload {
    Cbor(Vec<u8>),
    /// The text has only hexadecimal digits but does not decode, for example
    /// because its length is odd.
    InvalidBase16,
}

pub async fn txs_evaluate<D>(
    Query(params): Query<EvaluateParams>,
    State(domain): State<Facade<D>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<JsonValue>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    check_content_type(&headers, "application/cbor")?;
    let version = parse_version(params.version.as_deref())?;

    // Binary CBOR is never valid UTF-8, because a transaction starts with an
    // array header byte that cannot start a UTF-8 character.
    let payload = match std::str::from_utf8(&body) {
        Ok(text) => decode_payload(text)?,
        Err(_) => Payload::Cbor(body.to_vec()),
    };

    respond(domain, version, payload, vec![]).await
}

pub async fn txs_evaluate_utxos<D>(
    Query(params): Query<EvaluateParams>,
    State(domain): State<Facade<D>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<JsonValue>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    check_content_type(&headers, "application/json")?;
    let version = parse_version(params.version.as_deref())?;

    let request: EvaluateUtxosRequest = serde_json::from_slice(&body).map_err(|error| {
        Error::InvalidEvaluationRequest(format!("Invalid request body: {error}"))
    })?;

    let additional = utxo_set::parse(request.additional_utxo_set.as_deref().unwrap_or_default())
        .map_err(Error::InvalidEvaluationRequest)?;

    let payload = decode_payload(&request.cbor)?;

    respond(domain, version, payload, additional).await
}

async fn respond<D>(
    domain: Facade<D>,
    version: Version,
    payload: Payload,
    additional: Vec<(TxoRef, EraCbor)>,
) -> Result<Json<JsonValue>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let outcome = match payload {
        Payload::InvalidBase16 => Outcome::InvalidPayload,
        Payload::Cbor(cbor) => {
            let permit = EVALUATIONS
                .clone()
                .acquire_owned()
                .await
                .map_err(log_and_500("failed to wait for an evaluation slot"))?;

            // The task keeps the permit until it ends, also when the client
            // disconnects.
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                evaluate(&domain.inner, &cbor, additional)
            })
            .await
            .map_err(log_and_500("failed to join the evaluation task"))??
        }
    };

    // Blockfrost puts the id of its Ogmios request in the response.
    let id = uuid::Uuid::new_v4().to_string();

    Ok(Json(ogmios::render(&outcome, version, &id)))
}

/// Evaluates the redeemers of a transaction against the ledger UTxO set, the
/// outputs of the unconfirmed mempool transactions and the `additional`
/// entries. The UTxOs merge like in Ogmios v6.14, which Blockfrost runs.
fn evaluate<D: Domain>(
    domain: &D,
    cbor: &[u8],
    additional: Vec<(TxoRef, EraCbor)>,
) -> Result<Outcome, StatusCode> {
    let tx = match MultiEraTx::decode(cbor) {
        Ok(tx) => tx,
        // Ogmios names these after Mary, the most recent era whose decoder
        // accepts them.
        Err(_) if is_pre_alonzo_tx(cbor) => return Ok(Outcome::IncompatibleEra("mary")),
        Err(_) => {
            let error = minicbor::decode::<conway::Tx>(cbor)
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default();

            return Ok(Outcome::Deserialisation(error));
        }
    };

    if let Some(outcome) = era_outcome(&tx) {
        return Ok(outcome);
    }

    let pparams = dolos_cardano::load_effective_pparams::<D>(domain.state())
        .map_err(log_and_500("failed to load the protocol parameters"))?;

    let protocol = pparams.protocol_major_or_default();
    if protocol < ALONZO_PROTOCOL {
        return Ok(Outcome::NodeTipTooOld(protocol_era(protocol)));
    }

    let refs = tx
        .requires()
        .iter()
        .map(TxoRef::from)
        .collect::<HashSet<_>>();

    // A ledger UTxO stays available when a pending transaction spends it.
    let ledger = domain
        .state()
        .get_utxos(refs.iter().cloned().collect())
        .map_err(log_and_500("failed to read the transaction inputs"))?;

    // The mempool outputs replace request entries with the same reference.
    let mut provided = additional
        .into_iter()
        .map(|(txo_ref, utxo)| (txo_ref, Arc::new(utxo)))
        .collect::<UtxoMap>();
    provided.extend(mempool_utxos::<D>(domain.mempool()));

    // When one provided entry differs from the ledger, Ogmios reports every
    // provided reference that the ledger also holds.
    let mut overlap = provided
        .keys()
        .filter(|txo_ref| ledger.contains_key(*txo_ref))
        .cloned()
        .collect::<Vec<_>>();

    if overlap
        .iter()
        .any(|txo_ref| !same_output(&ledger[txo_ref], &provided[txo_ref]))
    {
        overlap.sort();
        return Ok(Outcome::AdditionalUtxoOverlap(overlap));
    }

    let mut utxos = ledger;
    utxos.extend(
        provided
            .into_iter()
            .filter(|(txo_ref, _)| refs.contains(txo_ref)),
    );

    let reports = evaluate_redeemers::<D>(&tx, &utxos, domain.state(), &domain.genesis())
        .map_err(log_and_500("failed to evaluate the transaction"))?;

    Ok(Outcome::Evaluated(reports))
}

/// Returns the outputs of the unconfirmed mempool transactions that no other
/// unconfirmed mempool transaction spends.
fn mempool_utxos<D: Domain>(mempool: &D::Mempool) -> UtxoMap {
    let txs = mempool
        .peek_pending()
        .into_iter()
        .chain(mempool.peek_inflight())
        .filter(|tx| {
            !matches!(
                tx.stage,
                MempoolTxStage::Confirmed | MempoolTxStage::Finalized | MempoolTxStage::Dropped
            )
        })
        .collect::<Vec<_>>();

    let mut produced = UtxoMap::new();
    let mut consumed = HashSet::new();

    for mempool_tx in &txs {
        let Ok(tx) = MultiEraTx::try_from(&mempool_tx.payload) else {
            continue;
        };

        for (index, output) in tx.produces() {
            let txo_ref = TxoRef(tx.hash(), index as u32);
            produced.insert(txo_ref, Arc::new(EraCbor::from(output)));
        }

        consumed.extend(tx.consumes().iter().map(TxoRef::from));
    }

    produced.retain(|txo_ref, _| !consumed.contains(txo_ref));
    produced
}

/// Finds why the ledger cannot evaluate a transaction of this era. Only Conway
/// transactions run.
fn era_outcome(tx: &MultiEraTx) -> Option<Outcome> {
    match tx.era() {
        Era::Byron => Some(Outcome::IncompatibleEra("byron")),
        Era::Shelley | Era::Allegra | Era::Mary => Some(Outcome::IncompatibleEra("mary")),
        Era::Alonzo => Some(Outcome::UnsupportedEra("alonzo")),
        Era::Babbage => Some(Outcome::UnsupportedEra("babbage")),
        // A later era than Conway reaches the evaluator, which fails each
        // redeemer.
        _ => None,
    }
}

/// Tells if the payload has the shape of a Shelley, Allegra or Mary
/// transaction: a body, a witness set and auxiliary data. Pallas decodes a
/// transaction only in the four-item shape of Alonzo and later.
fn is_pre_alonzo_tx(cbor: &[u8]) -> bool {
    let mut decoder = minicbor::Decoder::new(cbor);

    matches!(decoder.array(), Ok(Some(3)))
        && decoder.decode::<alonzo::TransactionBody>().is_ok()
        && decoder.decode::<alonzo::WitnessSet>().is_ok()
}

/// The era of a protocol version before Alonzo.
fn protocol_era(protocol: u16) -> &'static str {
    match protocol {
        0..=1 => "byron",
        2 => "shelley",
        3 => "allegra",
        _ => "mary",
    }
}

/// Tells if two outputs hold the same address, value, datum and script,
/// whatever their encoding.
fn same_output(left: &EraCbor, right: &EraCbor) -> bool {
    let parts = |utxo: &EraCbor| {
        MultiEraOutput::try_from(utxo).ok().map(|output| {
            let value = output.value();

            // A value can list an empty asset map. It equals the coin alone.
            let assets = value
                .assets()
                .iter()
                .flat_map(|policy| {
                    policy
                        .assets()
                        .iter()
                        .map(|asset| (*policy.policy(), asset.name().to_vec(), asset.any_coin()))
                        .collect::<Vec<_>>()
                })
                .filter(|(_, _, quantity)| *quantity != 0)
                .collect::<std::collections::BTreeSet<_>>();

            (
                output.address().ok().map(|address| address.to_vec()),
                value.coin(),
                assets,
                output
                    .datum()
                    .and_then(|datum| minicbor::to_vec(datum).ok()),
                output
                    .script_ref()
                    .and_then(|script| minicbor::to_vec(script).ok()),
            )
        })
    };

    match (parts(left), parts(right)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// Decodes a transaction that the request sends as base16 or base64 text.
/// Text with only hexadecimal digits is base16.
fn decode_payload(text: &str) -> Result<Payload, Error> {
    let text = text.trim();

    if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(hex::decode(text)
            .map(Payload::Cbor)
            .unwrap_or(Payload::InvalidBase16));
    }

    base64::engine::general_purpose::STANDARD
        .decode(text)
        .map(Payload::Cbor)
        .map_err(|_| Error::InvalidTxPayload)
}

/// Blockfrost answers in the Ogmios v6 format for `version=6` and in the v5
/// format for any other 32-bit integer.
fn parse_version(version: Option<&str>) -> Result<Version, Error> {
    match version.map(str::parse::<i32>) {
        None => Ok(Version::V5),
        Some(Ok(6)) => Ok(Version::V6),
        Some(Ok(_)) => Ok(Version::V5),
        Some(Err(_)) => Err(Error::InvalidOgmiosVersion),
    }
}

/// Rejects a request without a Content-Type header, or with one that names
/// another media type.
fn check_content_type(headers: &HeaderMap, expected: &'static str) -> Result<(), Error> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Err(Error::MissingContentType);
    };

    let essence = value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .map(str::trim);

    match essence {
        Some(essence) if essence.eq_ignore_ascii_case(expected) => Ok(()),
        _ => Err(Error::InvalidContentType(expected)),
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use dolos_cardano::model::{EpochState, PParamValue, SingletonEntity as _};
    use dolos_core::{MempoolStore as _, MempoolTx, StateStore as _, StateWriter as _};
    use dolos_testing::{
        synthetic::{SyntheticBlockConfig, SyntheticVectors},
        toy_domain::ToyDomain,
    };
    use pallas::{
        codec::utils::KeepRaw,
        ledger::{
            addresses::Address,
            primitives::conway::{PostAlonzoTransactionOutput, TransactionOutput},
        },
    };
    use serde_json::{json, Value};

    use super::*;
    use crate::test_support::{TestApp, TestFault};

    /// The PlutusV2 cost model of mainnet, preprod and preview at protocol
    /// version 11.
    const PLUTUS_V2_COST_MODEL: [i64; 332] = [
        100788, 420, 1, 1, 1000, 173, 0, 1, 1000, 59957, 4, 1, 11183, 32, 201305, 8356, 4, 16000,
        100, 16000, 100, 16000, 100, 16000, 100, 16000, 100, 16000, 100, 100, 100, 16000, 100,
        94375, 32, 132994, 32, 61462, 4, 72010, 178, 0, 1, 22151, 32, 91189, 769, 4, 2, 85848,
        228465, 122, 0, 1, 1, 1000, 42921, 4, 2, 30623, 28755, 75, 1, 898148, 27279, 1, 51775, 558,
        1, 39184, 1000, 60594, 1, 141895, 32, 83150, 32, 15299, 32, 76049, 1, 13169, 4, 22100, 10,
        28999, 74, 1, 28999, 74, 1, 43285, 552, 1, 44749, 541, 1, 33852, 32, 68246, 32, 72362, 32,
        7243, 32, 7391, 32, 11546, 32, 85848, 228465, 122, 0, 1, 1, 90434, 519, 0, 1, 74433, 32,
        85848, 228465, 122, 0, 1, 1, 85848, 228465, 122, 0, 1, 1, 955506, 213312, 0, 2, 270652,
        22588, 4, 1457325, 64566, 4, 20467, 1, 4, 0, 141992, 32, 100788, 420, 1, 1, 81663, 32,
        59498, 32, 20142, 32, 24588, 32, 20744, 32, 25933, 32, 24623, 32, 43053543, 10, 53384111,
        14333, 10, 43574283, 26308, 10, 1293828, 28716, 63, 0, 1, 1006041, 43623, 251, 0, 1, 16000,
        100, 16000, 100, 962335, 18, 2780678, 6, 442008, 1, 52538055, 3756, 18, 267929, 18,
        76433006, 8868, 18, 52948122, 18, 1995836, 36, 3227919, 12, 901022, 1, 166917843, 4307, 36,
        284546, 36, 158221314, 26549, 36, 74698472, 36, 333849714, 1, 254006273, 72, 2174038, 72,
        2261318, 64571, 4, 207616, 8310, 4, 100181, 726, 719, 0, 1, 100181, 726, 719, 0, 1, 100181,
        726, 719, 0, 1, 107878, 680, 0, 1, 95336, 1, 281145, 18848, 0, 1, 180194, 159, 1, 1,
        158519, 8942, 0, 1, 159378, 8813, 0, 1, 107490, 3298, 1, 106057, 655, 1, 1964219, 24520, 3,
        607153, 231697, 53144, 0, 1, 116711, 1957, 4, 231883, 10, 1000, 24838, 7, 1, 232010, 32,
        321837444, 25087669, 18, 617887431, 67302824, 36, 356924, 18413, 45, 21, 219951, 9444, 1,
        1000, 172116, 183150, 6, 24, 21, 213283, 618401, 1998, 28258, 1, 1000, 38159, 2, 22, 1000,
        95933, 1, 1, 11, 1000, 277577, 12, 21,
    ];

    /// An app whose ledger is in the Conway era, with the current PlutusV2
    /// cost model. The genesis of the synthetic chain is in the Alonzo era.
    fn conway_app() -> TestApp {
        conway_app_with(|_, _| {})
    }

    /// Like `conway_app`, and then runs `setup` on the domain.
    fn conway_app_with(setup: impl FnOnce(&ToyDomain, &SyntheticVectors)) -> TestApp {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain: &ToyDomain, vectors| {
            let mut epoch = dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap();

            let pparams = epoch.pparams.unwrap_live_mut();
            pparams.set(PParamValue::ProtocolVersion((11, 0)));
            pparams.set(PParamValue::CostModelsPlutusV2(
                PLUTUS_V2_COST_MODEL.to_vec(),
            ));

            let writer = domain.state().start_writer().unwrap();
            writer
                .write_entity_typed(&EpochState::singleton_key(), &epoch)
                .unwrap();
            writer.commit().unwrap();

            setup(domain, vectors);
        })
    }

    /// Adds a transaction in hexadecimal CBOR to the pending mempool queue.
    fn add_pending_tx(domain: &ToyDomain, tx: &str) {
        let cbor = hex::decode(tx).unwrap();
        let hash = MultiEraTx::decode(&cbor).unwrap().hash();
        let payload = EraCbor(Era::Conway.into(), cbor);

        domain
            .mempool()
            .receive(MempoolTx::new(hash, payload, vec![]))
            .unwrap();
    }

    /// A transaction without redeemers that spends `{input}#0` and pays to a
    /// key address.
    fn transfer_tx(input: &str) -> String {
        format!(
            "84a30081825820{input}00018182581d61{}1a001e8480021a000493e0a0f5f6",
            "22".repeat(28)
        )
    }

    /// `HELLO_TX` with `ttl` as the upper validity bound.
    fn hello_tx_with_ttl(ttl: u64) -> String {
        HELLO_TX.replacen("84A3", "84A4", 1).replacen(
            "0200A10581",
            &format!("0200031B{ttl:016X}A10581"),
            1,
        )
    }

    const SCRIPT_ADDRESS: &str = "addr_test1wrzqlyffcf5yq3htqge9h9k29zv6d7ny0rqam6d4c5eqdfgg0h7yw";

    /// The Plutus V2 script at `SCRIPT_ADDRESS`. It accepts the redeemer
    /// "Hello, World!".
    const SCRIPT: &str = "59010601000032323232323232323232322223253330083371e6eb8cc014c01c00520004890d48656c6c6f2c20576f726c642100149858cc020c94ccc020cdc3a400000226464a66601e60220042930a99806249334c6973742f5475706c652f436f6e73747220636f6e7461696e73206d6f7265206974656d73207468616e2065787065637465640016375c601e002600e0062a660149212b436f6e73747220696e64657820646964206e6f74206d6174636820616e7920747970652076617269616e740016300a37540040046600200290001111199980319b8700100300c233330050053370000890011807000801001118031baa0015734ae6d5ce2ab9d5573caae7d5d0aba201";

    /// Spends `ffff…ff#0` with the redeemer "Hello, World!". The transaction
    /// and the budget below come from the Blockfrost test fixtures.
    const HELLO_TX: &str = "84A30081825820FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00018182581D70C40F9129C2684046EB02325B96CA2899A6FA6478C1DDE9B5C53206A51A00D59F800200A10581840000D8799F4D48656C6C6F2C20576F726C6421FF820000F5F6";

    const HELLO_INPUT: &str =
        "0081825820FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00";

    /// A Mary-era transfer from the Blockfrost test fixtures.
    const MARY_TX: &str = "83a300818258200ac82ea5bc0967a17d4a60e2474b01df72440673429ff89b2802d3bd2a38ec3e01018282583900e2fbc47df26fcd065c074c451e792599ea8fc159f76163ca4c2b520b58adbef896164ee7456ccb4eaa965a87a602b0e3b2825d7b4ee789b01a000f4240825839003c77cd7f3c07b3b0ba72044848592d2e5687569ad25b93a926392f5e83892080b40900e146e1c68f12ef6811773bd8740196cd211f3211de1af9b0595d021a0002c5bda10081825820da818bbf3a082945884681d062147ca7dc3111d87fab415268749124a3ed1d31584059ca300a7d38abf454482a57281acdbbaab740b868978131f36117a224e6ba2be5248da0205296d7a8211506d6430a2873c201831e326e5db68ac9e1403e520ef6";

    fn script_utxo(tx_id: &str, index: u32) -> Value {
        json!([
            { "txId": tx_id, "index": index },
            {
                "address": SCRIPT_ADDRESS,
                "value": { "coins": 14_000_000 },
                "datum": "d87980",
                "script": { "plutus:v2": SCRIPT },
            }
        ])
    }

    /// `HELLO_TX` with other inputs. The redeemer points to the first input in
    /// ledger order.
    fn hello_tx_spending(inputs: &[(&str, u8)]) -> String {
        let mut encoded = format!("00{:02X}", 0x80 + inputs.len());
        for (tx_id, index) in inputs {
            encoded.push_str(&format!("825820{tx_id}{index:02X}"));
        }
        HELLO_TX.replacen(HELLO_INPUT, &encoded, 1)
    }

    async fn post(
        app: &TestApp,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> (StatusCode, Value) {
        let (status, bytes) = app.post_bytes(path, content_type, body).await;
        let body = serde_json::from_slice(&bytes).expect("JSON body");
        (status, body)
    }

    async fn evaluate_utxos(app: &TestApp, path: &str, request: Value) -> Value {
        let (status, body) = post(
            app,
            path,
            "application/json",
            request.to_string().into_bytes(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    async fn evaluate_cbor_at(app: &TestApp, path: &str, tx: &str) -> Value {
        let (status, body) = post(app, path, "application/cbor", tx.as_bytes().to_vec()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    async fn evaluate_cbor(app: &TestApp, body: impl Into<Vec<u8>>) -> Value {
        let (status, body) =
            post(app, "/utils/txs/evaluate", "application/cbor", body.into()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    fn assert_v5_response(body: &Value) -> &Value {
        assert_eq!(body["type"], "jsonwsp/response", "{body}");
        assert_eq!(body["version"], "1.0");
        assert_eq!(body["servicename"], "ogmios");
        assert_eq!(body["methodname"], "EvaluateTx");
        assert!(body["reflection"]["id"].is_string());
        &body["result"]
    }

    #[tokio::test]
    async fn txs_evaluate_utxos_happy_path() {
        let app = conway_app();

        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({ "cbor": HELLO_TX, "additionalUtxoSet": [script_utxo(&"FF".repeat(32), 0)] }),
        )
        .await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationResult": { "spend:0": { "memory": 15_694, "steps": 3_776_164 } } })
        );
    }

    #[tokio::test]
    async fn txs_evaluate_utxos_v6() {
        let app = conway_app();

        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos?version=6",
            json!({ "cbor": HELLO_TX, "additionalUtxoSet": [script_utxo(&"ff".repeat(32), 0)] }),
        )
        .await;

        assert_eq!(body["jsonrpc"], "2.0");
        assert_eq!(body["method"], "evaluateTransaction");
        assert!(body["id"].is_string());
        assert_eq!(
            body["result"],
            json!([{
                "validator": { "index": 0, "purpose": "spend" },
                "budget": { "memory": 15_694, "cpu": 3_776_164 },
            }])
        );
    }

    #[tokio::test]
    async fn txs_evaluate_missing_cost_model() {
        // The Alonzo genesis of the synthetic chain has no PlutusV2 cost model.
        let app = TestApp::new();

        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({ "cbor": HELLO_TX, "additionalUtxoSet": [script_utxo(&"ff".repeat(32), 0)] }),
        )
        .await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationFailure": { "ScriptFailures": {
                "spend:0": { "noCostModelForLanguage": "plutus:v2" }
            }}})
        );
    }

    #[tokio::test]
    async fn txs_evaluate_redeemer_on_unknown_input() {
        let app = TestApp::new();

        let body = evaluate_cbor(&app, HELLO_TX).await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationFailure": { "ScriptFailures": {
                "spend:0": { "extraRedeemers": ["spend:0"] }
            }}})
        );
    }

    #[tokio::test]
    async fn txs_evaluate_payload_encodings() {
        let app = TestApp::new();
        let bytes = hex::decode(HELLO_TX).unwrap();

        let base16 = evaluate_cbor(&app, HELLO_TX).await;
        let base16_lowercase = evaluate_cbor(&app, HELLO_TX.to_lowercase()).await;
        let base64 = evaluate_cbor(
            &app,
            base64::engine::general_purpose::STANDARD.encode(&bytes),
        )
        .await;
        let binary = evaluate_cbor(&app, bytes).await;

        for body in [&base16_lowercase, &base64, &binary] {
            assert_eq!(body["result"], base16["result"]);
        }
    }

    #[tokio::test]
    async fn txs_evaluate_missing_input_context() {
        let app = conway_app();
        let tx = hello_tx_spending(&[(&"00".repeat(32), 0), (&"FF".repeat(32), 0)]);

        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({ "cbor": tx, "additionalUtxoSet": [script_utxo(&"00".repeat(32), 0)] }),
        )
        .await;

        let reason = format!(
            "Unknown transaction input (missing from UTxO set): {}#0",
            "ff".repeat(32)
        );

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationFailure": { "ScriptFailures": {
                "spend:0": { "CannotCreateEvaluationContext": { "reason": reason } }
            }}})
        );
    }

    #[tokio::test]
    async fn txs_evaluate_validator_failure() {
        let app = conway_app();
        // "Hello, World?" instead of "Hello, World!".
        let tx = HELLO_TX.replacen("576F726C6421FF", "576F726C643FFF", 1);

        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({ "cbor": tx, "additionalUtxoSet": [script_utxo(&"ff".repeat(32), 0)] }),
        )
        .await;

        let failure = &assert_v5_response(&body)["EvaluationFailure"]["ScriptFailures"]["spend:0"]
            ["validatorFailed"];
        assert!(
            failure["error"]
                .as_str()
                .is_some_and(|error| !error.is_empty()),
            "{body}"
        );
        assert!(failure["traces"].is_array(), "{body}");
    }

    /// The PlutusV2 script that accepts any arguments, and its hash.
    const ALWAYS_SUCCEEDS: &str = "4d01000033222220051200120011";
    const ALWAYS_SUCCEEDS_HASH: &str = "793f8c8cffba081b2a56462fc219cc8fe652d6a338b62c7b134876e7";

    /// An unsigned Conway transaction that spends `ffff…ff#0`. It adds `body`
    /// to the body and has one redeemer with `tag` and index 0. With
    /// `with_script`, the witness set holds `ALWAYS_SUCCEEDS`.
    fn purpose_tx(body: &str, tag: u8, with_script: bool) -> String {
        script_tx(&"ff".repeat(32), &[body], tag, with_script)
    }

    /// Like `purpose_tx`, but spends `{input}#0` and adds each of `fields`.
    fn script_tx(input: &str, fields: &[&str], tag: u8, with_script: bool) -> String {
        let witnesses = if with_script {
            format!("a2058184{tag:02x}00d8798082000006814e{ALWAYS_SUCCEEDS}")
        } else {
            format!("a1058184{tag:02x}00d87980820000")
        };

        format!(
            "84{:02x}0081825820{input}00018182581d61{}1a001e8480021a000493e0{}{witnesses}f5f6",
            0xa3 + fields.len(),
            "22".repeat(28),
            fields.concat(),
        )
    }

    fn withdrawal(header: &str, hash: &str) -> String {
        format!("05a1581d{header}{hash}00")
    }

    fn deregistration(credential: &str, hash: &str) -> String {
        format!("04818201{credential}581c{hash}")
    }

    /// Blockfrost names a failed redeemer with its v6 purpose, except in the
    /// `missingRequiredScripts` list. Live Blockfrost mainnet gave these
    /// answers for the same transactions, with and without `version=5`.
    #[tokio::test]
    async fn txs_evaluate_failure_names_match_blockfrost() {
        let app = TestApp::new();
        let key = "11".repeat(28);

        let withdraw_key = purpose_tx(&withdrawal("e1", &key), 3, false);
        let withdraw_script = purpose_tx(&withdrawal("f1", ALWAYS_SUCCEEDS_HASH), 3, false);
        let publish_key = purpose_tx(&deregistration("8200", &key), 2, false);
        let publish_script = purpose_tx(&deregistration("8201", ALWAYS_SUCCEEDS_HASH), 2, false);

        // The transaction, its v5 failures, its v6 purpose and its v6 error.
        let cases = [
            (
                withdraw_key,
                json!({ "withdraw:0": { "extraRedeemers": ["withdraw:0"] } }),
                "withdraw",
                3110,
                "extraneousRedeemers",
            ),
            (
                withdraw_script,
                json!({ "withdraw:0": { "missingRequiredScripts": { "missing": ["withdrawal:0"] } } }),
                "withdraw",
                3011,
                "missingScripts",
            ),
            (
                publish_key,
                json!({ "publish:0": { "extraRedeemers": ["publish:0"] } }),
                "publish",
                3110,
                "extraneousRedeemers",
            ),
            (
                publish_script,
                json!({ "publish:0": { "missingRequiredScripts": { "missing": ["certificate:0"] } } }),
                "publish",
                3011,
                "missingScripts",
            ),
        ];

        for (tx, failures_v5, purpose, code, field) in cases {
            for path in ["/utils/txs/evaluate", "/utils/txs/evaluate?version=5"] {
                let body = evaluate_cbor_at(&app, path, &tx).await;
                assert_eq!(
                    assert_v5_response(&body),
                    &json!({ "EvaluationFailure": { "ScriptFailures": failures_v5 } }),
                    "{path}"
                );
            }

            let body = evaluate_cbor_at(&app, "/utils/txs/evaluate?version=6", &tx).await;
            let validator = json!({ "index": 0, "purpose": purpose });

            assert_eq!(body["error"]["code"], 3010);
            assert_eq!(body["error"]["data"][0]["validator"], validator);
            assert_eq!(body["error"]["data"][0]["error"]["code"], code);
            assert_eq!(
                body["error"]["data"][0]["error"]["data"],
                json!({ field: [validator] })
            );
        }
    }

    /// Live Blockfrost mainnet gave the same budgets for the same
    /// transactions.
    #[tokio::test]
    async fn txs_evaluate_success_names_match_blockfrost() {
        let app = conway_app();
        let input = json!([
            { "txId": "ff".repeat(32), "index": 0 },
            { "address": SCRIPT_ADDRESS, "value": { "coins": 5_000_000 } }
        ]);

        let cases = [
            (
                purpose_tx(&withdrawal("f1", ALWAYS_SUCCEEDS_HASH), 3, true),
                "withdrawal:0",
                "withdraw",
            ),
            (
                purpose_tx(&deregistration("8201", ALWAYS_SUCCEEDS_HASH), 2, true),
                "certificate:0",
                "publish",
            ),
        ];

        for (tx, key, purpose) in cases {
            let request = json!({ "cbor": tx, "additionalUtxoSet": [input.clone()] });

            let body = evaluate_utxos(&app, "/utils/txs/evaluate/utxos", request.clone()).await;
            assert_eq!(
                assert_v5_response(&body),
                &json!({ "EvaluationResult": { key: { "memory": 1_400, "steps": 208_100 } } })
            );

            let body = evaluate_utxos(&app, "/utils/txs/evaluate/utxos?version=6", request).await;
            assert_eq!(
                body["result"],
                json!([{
                    "validator": { "index": 0, "purpose": purpose },
                    "budget": { "memory": 1_400, "cpu": 208_100 },
                }])
            );
        }
    }

    #[tokio::test]
    async fn txs_evaluate_overlapping_utxo() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.clone();
        let tx = hello_tx_spending(&[(&tx_hash, 0)]);

        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({ "cbor": tx, "additionalUtxoSet": [script_utxo(&tx_hash, 0)] }),
        )
        .await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationFailure": { "AdditionalUtxoOverlap": [
                { "txId": tx_hash, "index": 0 }
            ]}})
        );
    }

    #[tokio::test]
    async fn txs_evaluate_incompatible_era() {
        let app = TestApp::new();

        let body = evaluate_cbor(&app, MARY_TX).await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationFailure": { "IncompatibleEra": "Mary" } })
        );
    }

    #[tokio::test]
    async fn txs_evaluate_client_faults() {
        let app = TestApp::new();

        let malformed = evaluate_cbor(&app, "80").await;
        assert_eq!(malformed["type"], "jsonwsp/fault");
        assert!(malformed["fault"]["string"].as_str().unwrap().starts_with(
            "Invalid request: Deserialisation failure while decoding serialised transaction. \
             CBOR failed with error:"
        ));

        let odd_length = evaluate_cbor(&app, "84a").await;
        assert_eq!(odd_length["type"], "jsonwsp/fault");
        assert_eq!(
            odd_length["fault"]["string"],
            "Invalid request: failed to decode payload from base64 or base16."
        );
    }

    #[tokio::test]
    async fn txs_evaluate_bad_request() {
        let app = TestApp::new();

        let cases = [
            (
                "/utils/txs/evaluate",
                "application/cbor",
                b"invalid CBOR".to_vec(),
            ),
            (
                "/utils/txs/evaluate",
                "application/json",
                HELLO_TX.as_bytes().to_vec(),
            ),
            (
                "/utils/txs/evaluate?version=six",
                "application/cbor",
                HELLO_TX.as_bytes().to_vec(),
            ),
            (
                "/utils/txs/evaluate?version=2147483648",
                "application/cbor",
                HELLO_TX.as_bytes().to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/cbor",
                b"{}".to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                b"{".to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                br#"{"cbor": 1}"#.to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                br#"{"cbor": "invalid CBOR"}"#.to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                json!({ "cbor": HELLO_TX, "additionalUtxoSet": [[]] })
                    .to_string()
                    .into_bytes(),
            ),
        ];

        for (path, content_type, body) in cases {
            let (status, error) = post(&app, path, content_type, body).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{path} {content_type} {error}"
            );
            assert_eq!(error["status_code"], 400);
            assert_eq!(error["error"], "Bad Request");
        }
    }

    #[tokio::test]
    async fn txs_evaluate_missing_content_type() {
        let app = TestApp::new();

        for path in ["/utils/txs/evaluate", "/utils/txs/evaluate/utxos"] {
            let (status, body) = post(&app, path, "", HELLO_TX.as_bytes().to_vec()).await;

            assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{path}");
            assert_eq!(
                body,
                json!({
                    "error": "Unsupported Media Type",
                    "message": "Unsupported Media Type: undefined",
                    "status_code": 415,
                })
            );
        }
    }

    #[tokio::test]
    async fn txs_evaluate_validity_bound_past_the_horizon() {
        let app = conway_app();
        let epoch = app.tip_epoch();

        // The preview safe zone is shorter than an epoch. So the horizon is
        // after the end of the tip epoch and at most at the start of the
        // epoch after the next one.
        let within = app.epoch_start(epoch + 1) - 1;
        let beyond = app.epoch_start(epoch + 2);

        for (ttl, passes) in [(within, true), (beyond, false)] {
            let request = json!({
                "cbor": hello_tx_with_ttl(ttl),
                "additionalUtxoSet": [script_utxo(&"ff".repeat(32), 0)],
            });

            let body = evaluate_utxos(&app, "/utils/txs/evaluate/utxos", request).await;
            let result = assert_v5_response(&body);

            if passes {
                assert!(result["EvaluationResult"]["spend:0"].is_object(), "{body}");
            } else {
                let reason = result["EvaluationFailure"]["ScriptFailures"]["spend:0"]
                    ["CannotCreateEvaluationContext"]["reason"]
                    .as_str()
                    .unwrap_or_default();

                assert!(
                    reason.starts_with(
                        "Uncomputable slot arithmetic; transaction's validity bounds go \
                         beyond the foreseeable end of the current era"
                    ),
                    "{body}"
                );
            }
        }
    }

    #[tokio::test]
    async fn txs_evaluate_ignores_collateral_scripts() {
        let app = conway_app();
        let holder = "cc".repeat(32);
        let withdrawal = withdrawal("f1", ALWAYS_SUCCEEDS_HASH);

        let utxos = json!([
            [
                { "txId": "ff".repeat(32), "index": 0 },
                { "address": SCRIPT_ADDRESS, "value": { "coins": 5_000_000 } }
            ],
            [
                { "txId": holder, "index": 0 },
                {
                    "address": SCRIPT_ADDRESS,
                    "value": { "coins": 5_000_000 },
                    "script": { "plutus:v2": ALWAYS_SUCCEEDS },
                }
            ],
        ]);

        let as_reference = format!("1281825820{holder}00");
        let tx = script_tx(&"ff".repeat(32), &[&withdrawal, &as_reference], 3, false);
        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({ "cbor": tx, "additionalUtxoSet": utxos }),
        )
        .await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationResult": { "withdrawal:0": { "memory": 1_400, "steps": 208_100 } } })
        );

        let as_collateral = format!("0d81825820{holder}00");
        let tx = script_tx(&"ff".repeat(32), &[&withdrawal, &as_collateral], 3, false);
        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({ "cbor": tx, "additionalUtxoSet": utxos }),
        )
        .await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationFailure": { "ScriptFailures": {
                "withdraw:0": { "missingRequiredScripts": { "missing": ["withdrawal:0"] } }
            }}})
        );
    }

    #[tokio::test]
    async fn txs_evaluate_keeps_ledger_inputs_of_pending_txs() {
        let app = conway_app_with(|domain, vectors| {
            add_pending_tx(domain, &transfer_tx(&vectors.tx_hash));
        });

        let input = app.vectors().tx_hash.clone();
        let tx = script_tx(
            &input,
            &[&deregistration("8201", ALWAYS_SUCCEEDS_HASH)],
            2,
            true,
        );

        let body = evaluate_cbor_at(&app, "/utils/txs/evaluate", &tx).await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationResult": { "certificate:0": { "memory": 1_400, "steps": 208_100 } } })
        );
    }

    #[tokio::test]
    async fn txs_evaluate_prefers_mempool_outputs_to_request_entries() {
        let pending = transfer_tx(&"dd".repeat(32));
        let pending_hash = MultiEraTx::decode(&hex::decode(&pending).unwrap())
            .unwrap()
            .hash()
            .to_string();

        let app = conway_app_with(|domain, _| add_pending_tx(domain, &pending));

        // The request locks `{pending}#0` with the script, but the mempool
        // output wins, and a key locks it.
        let body = evaluate_utxos(
            &app,
            "/utils/txs/evaluate/utxos",
            json!({
                "cbor": hello_tx_spending(&[(&pending_hash, 0)]),
                "additionalUtxoSet": [script_utxo(&pending_hash, 0)],
            }),
        )
        .await;

        assert_eq!(
            assert_v5_response(&body),
            &json!({ "EvaluationFailure": { "ScriptFailures": {
                "spend:0": { "extraRedeemers": ["spend:0"] }
            }}})
        );
    }

    #[tokio::test]
    async fn txs_evaluate_get_is_invalid_path() {
        let app = TestApp::new();

        for path in ["/utils/txs/evaluate", "/utils/txs/evaluate/utxos"] {
            let (status, bytes) = app.get_bytes(path).await;
            let error: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(error["message"], "Invalid path.", "{path}");
        }
    }

    #[tokio::test]
    async fn txs_evaluate_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));

        let (status, _) = app
            .post_bytes("/utils/txs/evaluate", "application/cbor", HELLO_TX.into())
            .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn same_output_ignores_the_encoding() {
        let address = Address::from_bech32(SCRIPT_ADDRESS).unwrap().to_vec();

        let legacy = |coin: u64| {
            let output = alonzo::TransactionOutput {
                address: address.clone().into(),
                amount: alonzo::Value::Coin(coin),
                datum_hash: None,
            };
            let cbor = minicbor::to_vec(TransactionOutput::Legacy(KeepRaw::from(output))).unwrap();
            EraCbor(Era::Alonzo.into(), cbor)
        };

        let current = |coin: u64| {
            let entry = json!([
                { "txId": "ff".repeat(32), "index": 0 },
                { "address": SCRIPT_ADDRESS, "value": { "coins": coin } }
            ]);
            utxo_set::parse(&[entry]).unwrap().remove(0).1
        };

        assert!(same_output(&legacy(5), &current(5)));
        assert!(!same_output(&legacy(5), &current(6)));

        // Mainnet holds outputs whose value is `[coin, {}]`.
        let empty_assets = |coin: u64| {
            let output = PostAlonzoTransactionOutput {
                address: address.clone().into(),
                value: conway::Value::Multiasset(coin, Default::default()),
                datum_option: None,
                script_ref: None,
            };
            let cbor =
                minicbor::to_vec(TransactionOutput::PostAlonzo(KeepRaw::from(output))).unwrap();
            EraCbor(Era::Conway.into(), cbor)
        };

        assert!(same_output(&empty_assets(5), &current(5)));
        assert!(!same_output(&empty_assets(5), &current(6)));
    }

    #[test]
    fn version_selects_the_format() {
        assert_eq!(parse_version(None).unwrap(), Version::V5);
        assert_eq!(parse_version(Some("5")).unwrap(), Version::V5);
        assert_eq!(parse_version(Some("7")).unwrap(), Version::V5);
        assert_eq!(parse_version(Some("6")).unwrap(), Version::V6);
        assert!(parse_version(Some("6.0")).is_err());
    }
}
