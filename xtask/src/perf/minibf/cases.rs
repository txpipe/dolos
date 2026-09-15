use axum::{
    body::{to_bytes, Body},
    http::Request,
    Router,
};
use dolos_testing::{performance::ApiFixture, toy_domain::ToyStores};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Case {
    pub name: String,
    pub path: String,
    pub fields: Vec<String>,
    pub expected: Value,
    pub array: bool,
    pub live: bool,
}

impl Case {
    pub fn verify(&self, value: &Value) -> anyhow::Result<()> {
        let project = |row: &Value| -> Value {
            if self.fields.is_empty() {
                row.clone()
            } else {
                Value::Array(
                    self.fields
                        .iter()
                        .map(|field| row.pointer(field).cloned().unwrap_or(Value::Null))
                        .collect(),
                )
            }
        };
        let actual = if self.array {
            let rows = value
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("{}: expected array", self.name))?;
            Value::Array(rows.iter().map(project).collect())
        } else {
            project(value)
        };
        anyhow::ensure!(
            actual == self.expected,
            "{}: response differs from fixture: expected {}, got {}",
            self.name,
            self.expected,
            actual
        );
        Ok(())
    }
}

pub fn cases<Stores: ToyStores>(fixture: &ApiFixture<Stores>) -> Vec<Case> {
    let shape = &fixture.shape;
    let vectors = &fixture.vectors;
    let epoch = fixture.epoch;
    let count = shape.page_size;
    let page = shape.page;
    let skip = (page - 1) * count;
    let issuer_hash = pallas::crypto::hash::Hasher::<224>::hash(&[0x10, 0x11]);
    let issuer_pool =
        bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("pool").unwrap(), issuer_hash.as_ref())
            .unwrap();
    let pool_expected: Vec<Value> = (0..shape.log_rows)
        .step_by(shape.pool_stride)
        .skip((page - 1) * count)
        .take(count)
        .map(|row| {
            json!([
                shape.stake_address(row),
                (1_000_000 + row as u64).to_string()
            ])
        })
        .collect();
    let tx_expected: Vec<Value> = vectors
        .blocks
        .iter()
        .skip(skip)
        .take(count)
        .map(|block| json!([block.tx_hashes[0]]))
        .collect();
    let utxo_expected: Vec<Value> = vectors
        .blocks
        .iter()
        .take(shape.blocks)
        .flat_map(|block| &block.tx_hashes)
        .take(4)
        .map(|hash| json!([hash, 0]))
        .collect();
    vec![
        Case {
            name: "epoch-latest".into(),
            path: "/epochs/latest".into(),
            fields: vec!["/epoch".into(), "/block_count".into()],
            expected: json!([epoch, shape.blocks]),
            array: false,
            live: false,
        },
        Case {
            name: "epoch-blocks-pool-page".into(),
            path: format!("/epochs/{epoch}/blocks/{issuer_pool}?count={count}&page={page}"),
            fields: vec![],
            expected: json!(vectors
                .blocks
                .iter()
                .skip(skip)
                .take(count)
                .map(|block| &block.block_hash)
                .collect::<Vec<_>>()),
            array: true,
            live: true,
        },
        Case {
            name: "epoch-stakes-sparse-pool".into(),
            path: format!(
                "/epochs/{epoch}/stakes/{}?count={count}&page={page}",
                vectors.pool_id
            ),
            fields: vec!["/stake_address".into(), "/amount".into()],
            expected: json!(pool_expected),
            array: true,
            live: true,
        },
        Case {
            name: "epoch-stakes-page".into(),
            path: format!("/epochs/{epoch}/stakes?count={count}&page={page}"),
            fields: vec!["/stake_address".into(), "/amount".into()],
            expected: json!((skip..skip + count)
                .map(|row| json!([
                    shape.stake_address(row),
                    (1_000_000 + row as u64).to_string()
                ]))
                .collect::<Vec<_>>()),
            array: true,
            live: true,
        },
        Case {
            name: "epoch-blocks-pool-no-match".into(),
            path: format!("/epochs/{epoch}/blocks/{}?count=5", vectors.pool_id),
            fields: vec![],
            expected: json!([]),
            array: true,
            live: true,
        },
        Case {
            name: "epoch-blocks-reverse".into(),
            path: format!("/epochs/{epoch}/blocks?count=2&order=desc"),
            fields: vec![],
            expected: json!(vectors
                .blocks
                .iter()
                .take(shape.blocks)
                .rev()
                .take(2)
                .map(|block| &block.block_hash)
                .collect::<Vec<_>>()),
            array: true,
            live: false,
        },
        Case {
            name: "address-transactions-page".into(),
            path: format!(
                "/addresses/{}/transactions?count={count}&page={page}&order=asc",
                vectors.address
            ),
            fields: vec!["/tx_hash".into()],
            expected: json!(tx_expected),
            array: true,
            live: true,
        },
        Case {
            name: "account-utxos-wide".into(),
            path: format!(
                "/accounts/{}/utxos?count=4&order=asc",
                vectors.stake_address
            ),
            fields: vec!["/tx_hash".into(), "/output_index".into()],
            expected: json!(utxo_expected),
            array: true,
            live: true,
        },
        Case {
            name: "transaction-utxos".into(),
            path: format!("/txs/{}/utxos", vectors.tx_hash),
            fields: vec![
                "/hash".into(),
                "/outputs/0/output_index".into(),
                "/outputs/0/address".into(),
            ],
            expected: json!([vectors.tx_hash, 0, vectors.address]),
            array: false,
            live: true,
        },
    ]
}

pub async fn request(router: Router, case: &Case) -> anyhow::Result<usize> {
    response(router, case).await.map(|(bytes, _)| bytes)
}

pub fn response_hash(value: &Value) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("JSON response"))
    )
}

pub async fn response(router: Router, case: &Case) -> anyhow::Result<(usize, String)> {
    let request = Request::builder().uri(&case.path).body(Body::empty())?;
    let response = router.oneshot(request).await?;
    let status = response.status();
    let body = to_bytes(response.into_body(), 16 * 1024 * 1024).await?;
    anyhow::ensure!(
        status == axum::http::StatusCode::OK,
        "{}: HTTP {status}: {}",
        case.name,
        String::from_utf8_lossy(&body)
    );
    let value: Value = serde_json::from_slice(&body)?;
    case.verify(&value)?;
    Ok((body.len(), response_hash(&value)))
}
