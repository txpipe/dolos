use std::time::Duration;

use axum::{routing::get, Json, Router};
use dolos_core::{config::MinibfConfig, Domain};
use dolos_testing::{
    performance::{ApiFixture, FixtureShape},
    toy_domain::{FjallStores, MemoryStores, ToyStores},
};
use serde_json::{json, Value};
use xtask::archive_bench::minibf::{
    cases, drive,
    report::{assess, Budgets},
};

async fn verify_fixture<Stores: ToyStores>(stores: Stores) {
    let fixture = ApiFixture::new(stores, FixtureShape::default()).unwrap();
    let router = dolos_minibf::build_router(
        MinibfConfig::new("127.0.0.1:0".parse().unwrap()),
        fixture.domain.clone(),
    );
    for case in cases::cases(&fixture) {
        fixture.domain.archive().counters.reset();
        cases::request(router.clone(), &case).await.unwrap();
        let work = fixture.domain.archive().counters.snapshot();
        match case.name.as_str() {
            "epoch-stakes-sparse-pool" => {
                assert!(work.log_rows > 5);
                assert_eq!(work.block_reads, 0);
            }
            "epoch-blocks-pool-no-match" => {
                assert_eq!(work.block_reads, fixture.shape.blocks as u64);
                assert!(work.decoded_bytes > 0);
            }
            "address-transactions-page" => assert!(work.tag_candidates > 0),
            "account-utxos-wide" => {
                assert_eq!(
                    work.utxo_refs,
                    (fixture.shape.blocks * fixture.shape.transactions_per_block) as u64
                );
                assert_eq!(work.exact_lookups, work.utxo_refs);
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn route_fixtures_and_work_guards_on_memory() {
    verify_fixture(MemoryStores::open()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn route_fixtures_and_work_guards_on_fjall() {
    verify_fixture(FjallStores::open()).await;
}

fn scalar_case() -> cases::Case {
    cases::Case {
        name: "fixture".into(),
        path: "/fixture".into(),
        fields: vec![],
        expected: json!([1]),
        array: true,
        live: true,
    }
}

#[tokio::test]
async fn overload_keeps_offered_requests_and_bounds_in_flight() {
    let router = Router::new().route(
        "/fixture",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Json(json!([1]))
        }),
    );
    let result = drive(router, scalar_case(), 20, 1, 10000, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(result["peak_in_flight"], 1);
    assert!(result["rejected"].as_u64().unwrap() > 0);
    assert_eq!(
        result["completed"].as_u64().unwrap() + result["rejected"].as_u64().unwrap(),
        20
    );
}

#[tokio::test]
async fn wrong_responses_and_deadlines_never_count_as_success() {
    let router = Router::new().route("/fixture", get(|| async { Json(json!([])) }));
    let result = drive(router, scalar_case(), 3, 1, 0, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(result["completed"], 0);
    assert_eq!(result["errors"], 3);
    let router = Router::new().route(
        "/fixture",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Json(json!([1]))
        }),
    );
    let result = drive(router, scalar_case(), 1, 1, 0, Duration::from_millis(1))
        .await
        .unwrap();
    assert_eq!(result["completed"], 0);
    assert_eq!(result["timeouts"], 1);
}

fn record(label: &str, repeat: u64, p95: f64) -> Value {
    json!({
        "schema": 1, "run": "test", "label": label, "repeat": repeat,
        "suite": ["test"],
        "binary_sha256": if label == "baseline" { "a".repeat(64) } else { "b".repeat(64) },
        "environment": {"os": "test", "revision": label},
        "fixture": {"schema": 1, "seed": 0, "response_sha256": "c".repeat(64)},
        "settings": {"requests": 1000, "rate": 0},
        "metrics": {
            "kind": "minibf", "workload": "test", "requests": 1000,
            "completed": 1000, "errors": 0, "timeouts": 0, "rejected": 0,
            "latency": {"count": 1000, "p95_us": p95, "p99_us": p95 * 2.0},
            "completed_per_second": 100.0,
        }
    })
}

fn paired() -> Vec<Value> {
    (0..3)
        .flat_map(|repeat| {
            [
                record("baseline", repeat, 100.0),
                record("candidate", repeat, 105.0),
            ]
        })
        .collect()
}

#[test]
fn pairing_refuses_missing_duplicate_incompatible_or_failed_evidence() {
    let records = paired();
    assert!(assess(&records, &Budgets::default()).1);
    let mut missing = records.clone();
    missing.pop();
    assert!(!assess(&missing, &Budgets::default()).1);
    let mut duplicate = records.clone();
    duplicate.push(records[0].clone());
    assert!(!assess(&duplicate, &Budgets::default()).1);
    for pointer in ["/settings/requests", "/fixture/seed", "/metrics/errors"] {
        let mut changed = records.clone();
        *changed[1].pointer_mut(pointer).unwrap() = json!(1);
        assert!(!assess(&changed, &Budgets::default()).1, "{pointer}");
    }
    let mut changed_binary = records.clone();
    changed_binary[3]["binary_sha256"] = json!("c".repeat(64));
    assert!(!assess(&changed_binary, &Budgets::default()).1);
    let mut absent_metric = records.clone();
    absent_metric[1]["metrics"]["latency"]["p95_us"] = Value::Null;
    assert!(!assess(&absent_metric, &Budgets::default()).1);
}

#[test]
fn gates_detect_regression_and_baseline_absolute_budget_failure() {
    let mut records = paired();
    for record in records
        .iter_mut()
        .filter(|record| record["label"] == "candidate")
    {
        record["metrics"]["latency"]["p95_us"] = json!(120);
    }
    assert!(assess(&records, &Budgets::default())
        .0
        .contains("FAIL: regression"));
    let budgets = Budgets {
        p95_budget_ms: Some(0.09),
        ..Default::default()
    };
    assert!(assess(&paired(), &budgets)
        .0
        .contains("FAIL: absolute budget"));
    let budgets = Budgets {
        min_samples: 2000,
        ..Default::default()
    };
    assert!(assess(&paired(), &budgets).0.contains("INSUFFICIENT"));
}

#[test]
fn live_replay_cli_records_writer_progress() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("live.jsonl");
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-xtask"))
        .args([
            "archive-bench",
            "minibf",
            "--run",
            "smoke",
            "--repeat",
            "1",
            "--requests",
            "3",
            "--rates",
            "20",
            "--timeout-ms",
            "1000",
            "--live",
            "--write-interval-ms",
            "500",
            "--cases",
            "epoch-blocks-pool-no-match",
        ])
        .arg("--work")
        .arg(temp.path())
        .arg("--out")
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let record: Value =
        serde_json::from_str(std::fs::read_to_string(output).unwrap().trim()).unwrap();
    assert_eq!(record["metrics"]["completed"], 3);
    assert!(record["metrics"]["writer"]["blocks"].as_u64().unwrap() > 0);
    assert_eq!(record["metrics"]["work_scope"], "api-and-writer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_calibration_checks_responses_and_refuses_tip_changes() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    let temp = tempfile::tempdir().unwrap();
    let changed = Arc::new(AtomicBool::new(false));
    let change_on_read = Arc::new(AtomicBool::new(false));
    let tip_changed = changed.clone();
    let trigger = change_on_read.clone();
    let read_changed = changed.clone();
    let router = Router::new()
        .route("/blocks/latest", get(move || {
            let changed = tip_changed.clone();
            async move { Json(json!({"hash": if changed.load(Ordering::Relaxed) { "b".repeat(64) } else { "a".repeat(64) }})) }
        }))
        .route("/fixture", get(move || {
            let trigger = trigger.clone();
            let changed = read_changed.clone();
            async move {
                if trigger.load(Ordering::Relaxed) { changed.store(true, Ordering::Relaxed); }
                Json(json!([1]))
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let manifest = temp.path().join("manifest.json");
    std::fs::write(&manifest, serde_json::to_vec(&json!({
        "schema": 1, "dataset": "test", "network": "test", "tip_hash": "a".repeat(64),
        "server_environment": {"host": "test"},
        "settings": {"storage_version": "v4", "dictionary": "test", "max_scan_items": 3000, "cache": "test", "durability": "test"},
        "cases": [scalar_case()],
    })).unwrap()).unwrap();
    for changes in [false, true] {
        change_on_read.store(changes, Ordering::Relaxed);
        let output = temp.path().join(format!("http-{changes}.jsonl"));
        let manifest = manifest.clone();
        let output_argument = output.clone();
        let result = tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_cargo-xtask"))
                .args([
                    "archive-bench",
                    "minibf-http",
                    "--run",
                    "smoke",
                    "--label",
                    "candidate",
                    "--server-revision",
                    "test",
                    "--requests",
                    "3",
                    "--repeat",
                    "1",
                ])
                .arg("--url")
                .arg(format!("http://{address}"))
                .arg("--server-binary")
                .arg(env!("CARGO_BIN_EXE_cargo-xtask"))
                .arg("--manifest")
                .arg(manifest)
                .arg("--out")
                .arg(output_argument)
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(
            result.status.success(),
            !changes,
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let record: Value =
            serde_json::from_str(std::fs::read_to_string(output).unwrap().trim()).unwrap();
        assert_eq!(record["metrics"]["tip_unchanged"], !changes);
        assert_eq!(record["metrics"]["resources_scope"], "client-only");
    }
    server.abort();
}

#[test]
fn incomplete_declared_suites_cannot_pass() {
    let mut records = paired();
    for record in &mut records {
        record["suite"] = json!(["test", "missing-workload"]);
    }
    assert!(!assess(&records, &Budgets::default()).1);
}

#[test]
fn p99_regressions_and_response_changes_cannot_pass() {
    let mut records = paired();
    for record in records
        .iter_mut()
        .filter(|record| record["label"] == "candidate")
    {
        record["metrics"]["latency"]["p99_us"] = json!(300);
    }
    assert!(assess(&records, &Budgets::default())
        .0
        .contains("FAIL: regression"));
    let mut records = paired();
    records[1]["fixture"]["response_sha256"] = json!("different-response");
    assert!(!assess(&records, &Budgets::default()).1);
}
