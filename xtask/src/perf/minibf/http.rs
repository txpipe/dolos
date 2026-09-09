use std::{fs::OpenOptions, io::Write, path::PathBuf, time::Duration};

use anyhow::Context;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::cases::Case;
use crate::perf::load::drive_requests;
use crate::perf::{command_line, measure, parse_list};

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    pub url: Url,
    #[arg(long)]
    pub manifest: PathBuf,
    #[arg(long)]
    pub server_binary: PathBuf,
    #[arg(long)]
    pub server_revision: String,
    #[arg(long)]
    pub out: PathBuf,
    #[arg(long)]
    pub run: String,
    #[arg(long)]
    pub label: String,
    #[arg(long, default_value_t = 3)]
    pub repeat: usize,
    #[arg(long, default_value_t = 0)]
    pub repeat_start: usize,
    #[arg(long, default_value_t = 1000)]
    pub requests: usize,
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    #[arg(long, default_value = "0")]
    pub rates: String,
    #[arg(long, default_value_t = 15_000)]
    pub timeout_ms: u64,
    #[arg(long)]
    pub project_id_env: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema: u32,
    pub dataset: String,
    pub network: String,
    pub tip_hash: String,
    pub server_environment: Value,
    pub settings: Value,
    pub cases: Vec<Case>,
}

impl Manifest {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.schema == 1, "unsupported manifest schema");
        anyhow::ensure!(
            !self.dataset.is_empty() && !self.network.is_empty(),
            "dataset and network are required"
        );
        anyhow::ensure!(
            self.tip_hash.len() == 64 && self.tip_hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "tip_hash must be a block hash"
        );
        anyhow::ensure!(
            self.server_environment.is_object() && self.settings.is_object(),
            "server_environment and settings must be objects"
        );
        for field in [
            "storage_version",
            "dictionary",
            "max_scan_items",
            "cache",
            "durability",
        ] {
            anyhow::ensure!(
                !self.settings[field].is_null(),
                "manifest settings missing {field}"
            );
        }
        anyhow::ensure!(
            !self.cases.is_empty(),
            "manifest must declare at least one case"
        );
        let mut names = std::collections::BTreeSet::new();
        for case in &self.cases {
            anyhow::ensure!(
                !case.name.is_empty() && names.insert(&case.name),
                "case names must be nonempty and unique"
            );
            anyhow::ensure!(
                case.path.starts_with('/')
                    && !case.path.starts_with("//")
                    && !case.path.contains('#'),
                "case path must be an absolute path on the same server"
            );
            anyhow::ensure!(
                !case.expected.is_null(),
                "each case needs an independent response oracle"
            );
        }
        Ok(())
    }
}

async fn json_response(client: &Client, url: Url) -> anyhow::Result<(Value, usize)> {
    let mut response = client.get(url).send().await?.error_for_status()?;
    anyhow::ensure!(
        response.status() == reqwest::StatusCode::OK,
        "expected HTTP 200, got {}",
        response.status()
    );
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            body.len() + chunk.len() <= 16 * 1024 * 1024,
            "response exceeds 16 MiB"
        );
        body.extend_from_slice(&chunk);
    }
    Ok((serde_json::from_slice(&body)?, body.len()))
}

async fn check_tip(client: &Client, base: &Url, manifest: &Manifest) -> anyhow::Result<()> {
    let (tip, _) = json_response(client, base.join("/blocks/latest")?).await?;
    anyhow::ensure!(
        tip["hash"] == manifest.tip_hash,
        "node tip differs from the manifest; use a fully populated stable-tip instance"
    );
    Ok(())
}

pub fn run(args: Args) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(args.url.scheme(), "http" | "https") && args.url.host_str().is_some(),
        "url must use HTTP(S)"
    );
    anyhow::ensure!(
        args.url.username().is_empty()
            && args.url.password().is_none()
            && args.url.query().is_none()
            && args.url.fragment().is_none(),
        "use an origin URL without credentials, query or fragment"
    );
    anyhow::ensure!(args.url.path() == "/", "url must be the server origin");
    anyhow::ensure!(
        args.requests > 0 && args.concurrency > 0 && args.repeat > 0 && args.timeout_ms > 0,
        "request counts, concurrency, repeats and timeout must be positive"
    );
    anyhow::ensure!(
        !args.run.is_empty() && !args.label.is_empty() && !args.server_revision.is_empty(),
        "run, label and server revision are required"
    );
    anyhow::ensure!(
        args.repeat_start.checked_add(args.repeat).is_some(),
        "repeat range overflow"
    );
    let rates: Vec<u64> = parse_list(&args.rates)?;
    let unique: std::collections::BTreeSet<_> = rates.iter().collect();
    anyhow::ensure!(
        !rates.is_empty() && rates.len() == unique.len(),
        "rates must be nonempty and unique"
    );
    let manifest_bytes = std::fs::read(&args.manifest)?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    manifest.validate()?;
    let timeout = Duration::from_millis(args.timeout_ms);
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(variable) = &args.project_id_env {
        let value =
            std::env::var(variable).context("project-id environment variable is unavailable")?;
        let mut value = reqwest::header::HeaderValue::from_str(&value)?;
        value.set_sensitive(true);
        headers.insert("project_id", value);
    }
    let client = Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .default_headers(headers)
        .build()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let environment = measure::environment(&[]);
    let binary_sha256 = format!("{:x}", Sha256::digest(std::fs::read(&args.server_binary)?));
    let fixture = json!({
        "manifest_sha256": format!("{:x}", Sha256::digest(serde_json::to_vec(&manifest)?)),
        "dataset": manifest.dataset, "network": manifest.network, "tip_hash": manifest.tip_hash,
        "server_environment": manifest.server_environment,
    });
    let suite: Vec<_> = manifest.cases.iter().map(|case| &case.name).collect();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.out)?;
    let mut failed = false;
    for repeat in args.repeat_start..args.repeat_start + args.repeat {
        for rate in &rates {
            for case in &manifest.cases {
                runtime.block_on(check_tip(&client, &args.url, &manifest))?;
                let url = args.url.join(&case.path)?;
                anyhow::ensure!(
                    url.origin() == args.url.origin(),
                    "case URL escaped the configured server origin"
                );
                let expected_response = runtime.block_on(async {
                    let (body, _) = json_response(&client, url.clone()).await?;
                    case.verify(&body)?;
                    Ok::<_, anyhow::Error>(super::cases::response_hash(&body))
                })?;
                let before = measure::counters();
                let mut metrics = runtime.block_on(drive_requests(
                    || {
                        let client = client.clone();
                        let url = url.clone();
                        let case = case.clone();
                        let expected = expected_response.clone();
                        async move {
                            let (body, bytes) = json_response(&client, url).await?;
                            case.verify(&body)?;
                            anyhow::ensure!(
                                super::cases::response_hash(&body) == expected,
                                "response changed after fixture validation"
                            );
                            Ok(bytes)
                        }
                    },
                    args.requests,
                    args.concurrency,
                    *rate,
                    timeout,
                ))?;
                metrics["resources"] = measure::counters().delta(&before).json();
                let tip_valid = runtime
                    .block_on(check_tip(&client, &args.url, &manifest))
                    .is_ok();
                metrics["tip_unchanged"] = json!(tip_valid);
                metrics["kind"] = json!("minibf");
                metrics["workload"] = json!(case.name);
                metrics["work_scope"] = json!("unavailable-http");
                metrics["resources_scope"] = json!("client-only");
                failed |= !tip_valid
                    || ["errors", "timeouts", "rejected"]
                        .iter()
                        .any(|field| metrics[*field].as_u64().unwrap_or(0) > 0);
                let mut case_fixture = fixture.clone();
                case_fixture["case"] = serde_json::to_value(case)?;
                case_fixture["response_sha256"] = json!(expected_response);
                let record = json!({
                    "schema": 1, "run": args.run, "label": args.label, "repeat": repeat,
                    "suite": suite, "binary_sha256": binary_sha256, "environment": environment,
                    "build": {
                        "revision": args.server_revision, "identity_source": "operator-supplied-server-binary",
                        "harness_revision": env!("VERGEN_GIT_SHA"), "harness_dirty": env!("VERGEN_GIT_DIRTY"),
                    },
                    "fixture": case_fixture, "metrics": metrics,
                    "settings": {
                        "transport": "http", "server": manifest.settings,
                        "requests": args.requests, "concurrency": args.concurrency, "rate": rate,
                        "timeout_ms": args.timeout_ms, "cache": "route-primed",
                        "mode": "read-only-stable-tip", "runtime_workers": 4,
                    },
                    "command": command_line(),
                });
                serde_json::to_writer(&mut file, &record)?;
                file.write_all(b"\n")?;
                file.flush()?;
            }
        }
    }
    anyhow::ensure!(
        !failed,
        "HTTP workloads failed, overloaded or changed tip; results retained"
    );
    Ok(())
}
