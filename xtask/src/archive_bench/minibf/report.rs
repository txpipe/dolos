use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use serde_json::Value;

#[derive(Clone, clap::Args)]
pub struct Budgets {
    #[arg(long, default_value_t = 1.10)]
    pub max_p95_ratio: f64,
    #[arg(long, default_value_t = 1.20)]
    pub max_p99_ratio: f64,
    #[arg(long, default_value_t = 0.90)]
    pub min_throughput_ratio: f64,
    #[arg(long, default_value_t = 3)]
    pub min_repeats: usize,
    #[arg(long, default_value_t = 1000)]
    pub min_samples: u64,
    #[arg(long)]
    pub p95_budget_ms: Option<f64>,
    #[arg(long)]
    pub p99_budget_ms: Option<f64>,
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            max_p95_ratio: 1.10,
            max_p99_ratio: 1.20,
            min_throughput_ratio: 0.90,
            min_repeats: 3,
            min_samples: 1000,
            p95_budget_ms: None,
            p99_budget_ms: None,
        }
    }
}

impl Budgets {
    pub fn validate(&self) -> anyhow::Result<()> {
        let positive = |number: f64| number.is_finite() && number > 0.0;
        anyhow::ensure!(
            positive(self.max_p95_ratio)
                && positive(self.max_p99_ratio)
                && positive(self.min_throughput_ratio),
            "ratios must be finite and positive"
        );
        anyhow::ensure!(
            self.min_repeats > 0 && self.min_samples > 0,
            "minimum repeats and samples must be positive"
        );
        anyhow::ensure!(
            self.p95_budget_ms
                .into_iter()
                .chain(self.p99_budget_ms)
                .all(positive),
            "latency budgets must be finite and positive"
        );
        Ok(())
    }
}

fn group_key(record: &Value) -> String {
    let mut environment = record["environment"].clone();
    if let Some(environment) = environment.as_object_mut() {
        environment.remove("revision");
        environment.remove("recorded_at");
    }
    serde_json::to_string(&serde_json::json!({
        "schema": record["schema"], "run": record["run"],
        "suite": record["suite"],
        "environment": environment, "fixture": record["fixture"], "settings": record["settings"],
        "build_profile": record["build"]["profile"],
    }))
    .unwrap()
}

fn metric(record: &Value, pointer: &str) -> Option<f64> {
    record
        .pointer(pointer)?
        .as_f64()
        .filter(|number| number.is_finite() && *number > 0.0)
}

fn valid(record: &Value) -> bool {
    let metrics = &record["metrics"];
    let count = metrics["completed"].as_u64();
    let hash = |value: &Value| {
        value.as_str().is_some_and(|value| {
            value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    };
    record["schema"] == 1
        && record["run"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
        && record["environment"].is_object()
        && record["fixture"].is_object()
        && record["settings"].is_object()
        && record["label"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
        && hash(&record["binary_sha256"])
        && hash(&record["fixture"]["response_sha256"])
        && count.is_some_and(|count| count > 0)
        && (record["settings"]["transport"] != "http" || metrics["tip_unchanged"] == true)
        && metrics["requests"].as_u64() == count
        && record["settings"]["requests"].as_u64() == count
        && metrics["latency"]["count"].as_u64() == count
        && ["errors", "timeouts", "rejected"]
            .iter()
            .all(|field| metrics[*field].as_u64() == Some(0))
        && metric(record, "/metrics/latency/p95_us").is_some()
        && metric(record, "/metrics/latency/p99_us").is_some()
        && metric(record, "/metrics/completed_per_second").is_some()
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn map_repeats<'a>(arm: &[&'a Value]) -> Option<BTreeMap<u64, &'a Value>> {
    let mut repeats = BTreeMap::new();
    let mut identities = BTreeSet::new();
    for record in arm {
        let repeat = record["repeat"].as_u64()?;
        if repeats.insert(repeat, *record).is_some() {
            return None;
        }
        identities.insert(record["binary_sha256"].as_str()?);
    }
    (identities.len() == 1).then_some(repeats)
}

pub fn assess(records: &[Value], budgets: &Budgets) -> (String, bool) {
    let mut groups: BTreeMap<String, BTreeMap<String, Vec<&Value>>> = BTreeMap::new();
    for record in records
        .iter()
        .filter(|record| record["metrics"]["kind"] == "minibf")
    {
        groups
            .entry(group_key(record))
            .or_default()
            .entry(record["label"].as_str().unwrap_or("<missing>").to_string())
            .or_default()
            .push(record);
    }
    if groups.is_empty() {
        return ("No minibf records.\n".into(), false);
    }
    let mut output = String::from("## Minibf paired gates\n\n");
    writeln!(output, "Budgets: p95 ratio ≤ {:.3}; p99 ratio ≤ {:.3}; completed throughput ratio ≥ {:.3}; at least {} paired repeats and {} successful requests per repeat.\n", budgets.max_p95_ratio, budgets.max_p99_ratio, budgets.min_throughput_ratio, budgets.min_repeats, budgets.min_samples).unwrap();
    output.push_str("| run / workload | candidate | pairs | p95/p99 ratios (p95 min–max) | throughput ratio | baseline p95/p99 ms | candidate p95/p99 ms | verdict |\n|---|---|---:|---:|---:|---:|---:|---|\n");
    let mut passed = true;
    let mut suites: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)> = BTreeMap::new();
    for record in records
        .iter()
        .filter(|record| record["metrics"]["kind"] == "minibf")
    {
        let mut scope = record.clone();
        if let Some(fixture) = scope["fixture"].as_object_mut() {
            fixture.remove("case");
            fixture.remove("response_sha256");
        }
        let scope = format!(
            "{}:{}:{}",
            group_key(&scope),
            record["label"],
            record["repeat"]
        );
        let (expected, actual) = suites.entry(scope).or_default();
        if let Some(names) = record["suite"].as_array() {
            expected.extend(names.iter().filter_map(Value::as_str).map(str::to_string));
        }
        if let Some(name) = record["metrics"]["workload"].as_str() {
            actual.insert(name.to_string());
        }
    }
    for (expected, actual) in suites.values() {
        if expected.is_empty() || expected != actual {
            output.push_str("| incomplete suite | — | 0 | — | — | — | — | INVALID: workload records missing or undeclared |\n");
            passed = false;
        }
    }
    for labels in groups.values() {
        let example = labels.values().next().unwrap()[0];
        let title = format!(
            "{} / {} / {} rps",
            example["run"].as_str().unwrap_or("?"),
            example["metrics"]["workload"].as_str().unwrap_or("?"),
            example["settings"]["rate"]
        );
        let Some(baseline) = labels.get("baseline") else {
            writeln!(
                output,
                "| {title} | — | 0 | — | — | — | — | UNPAIRED: no baseline |"
            )
            .unwrap();
            passed = false;
            continue;
        };
        if labels.len() == 1 {
            writeln!(
                output,
                "| {title} | — | 0 | — | — | — | — | UNPAIRED: no candidate |"
            )
            .unwrap();
            passed = false;
        }
        for (label, candidate) in labels
            .iter()
            .filter(|(label, _)| label.as_str() != "baseline")
        {
            let pairs = map_repeats(baseline)
                .zip(map_repeats(candidate))
                .filter(|(baseline, candidate)| baseline.keys().eq(candidate.keys()));
            let Some((baseline, candidate)) = pairs else {
                writeln!(output, "| {title} | {label} | 0 | — | — | — | — | UNPAIRED: duplicates, identities or repeats differ |").unwrap();
                passed = false;
                continue;
            };
            if !baseline
                .values()
                .chain(candidate.values())
                .all(|record| valid(record))
            {
                writeln!(output, "| {title} | {label} | {} | — | — | — | — | INVALID: failed requests or incomplete metrics |", baseline.len()).unwrap();
                passed = false;
                continue;
            }
            let mut latency = Vec::new();
            let mut tail_latency = Vec::new();
            let mut throughput = Vec::new();
            let mut baseline_p95 = Vec::new();
            let mut baseline_p99 = Vec::new();
            let mut candidate_p95 = Vec::new();
            let mut candidate_p99 = Vec::new();
            let mut absolute_pass = true;
            for (repeat, baseline_record) in &baseline {
                let candidate_record = candidate[repeat];
                let p95 = |record| metric(record, "/metrics/latency/p95_us").unwrap();
                let p99 = |record| metric(record, "/metrics/latency/p99_us").unwrap();
                latency.push(p95(candidate_record) / p95(baseline_record));
                tail_latency.push(p99(candidate_record) / p99(baseline_record));
                throughput.push(
                    metric(candidate_record, "/metrics/completed_per_second").unwrap()
                        / metric(baseline_record, "/metrics/completed_per_second").unwrap(),
                );
                baseline_p95.push(p95(baseline_record) / 1000.0);
                baseline_p99.push(p99(baseline_record) / 1000.0);
                candidate_p95.push(p95(candidate_record) / 1000.0);
                candidate_p99.push(p99(candidate_record) / 1000.0);
                for record in [*baseline_record, candidate_record] {
                    absolute_pass &= budgets
                        .p95_budget_ms
                        .is_none_or(|limit| p95(record) / 1000.0 <= limit);
                    absolute_pass &= budgets
                        .p99_budget_ms
                        .is_none_or(|limit| p99(record) / 1000.0 <= limit);
                }
            }
            let latency_ratio = median(&mut latency);
            let tail_ratio = median(&mut tail_latency);
            let throughput_ratio = median(&mut throughput);
            let enough = baseline.len() >= budgets.min_repeats
                && baseline.values().chain(candidate.values()).all(|record| {
                    record["metrics"]["completed"].as_u64().unwrap() >= budgets.min_samples
                });
            let verdict = if !enough {
                "INSUFFICIENT"
            } else if !absolute_pass {
                "FAIL: absolute budget"
            } else if latency_ratio > budgets.max_p95_ratio
                || tail_ratio > budgets.max_p99_ratio
                || throughput_ratio < budgets.min_throughput_ratio
            {
                "FAIL: regression"
            } else {
                "PASS"
            };
            passed &= verdict == "PASS";
            writeln!(output, "| {title} | {label} | {} | {:.3}/{:.3} ({:.3}–{:.3}) | {:.3} | {:.3}/{:.3} | {:.3}/{:.3} | {verdict} |",
                baseline.len(), latency_ratio, tail_ratio, latency[0], latency[latency.len() - 1], throughput_ratio,
                median(&mut baseline_p95), median(&mut baseline_p99), median(&mut candidate_p95), median(&mut candidate_p99)).unwrap();
        }
    }
    output.push_str("\nLatency includes scheduling delay; rejected arrivals and invalid/late responses prevent a pass. Small-fixture passes do not establish mainnet capacity.\n");
    output.push_str("\n## Minibf work and resources\n\n| label / repeat / workload | scope | log rows | tag candidates | block reads | CPU ms | peak RSS bytes | sync blocks |\n|---|---|---:|---:|---:|---:|---:|---:|\n");
    for record in records
        .iter()
        .filter(|record| record["metrics"]["kind"] == "minibf")
    {
        let metrics = &record["metrics"];
        writeln!(
            output,
            "| {} / {} / {} | {} | {} | {} | {} | {} | {} | {} |",
            record["label"].as_str().unwrap_or("?"),
            record["repeat"],
            metrics["workload"].as_str().unwrap_or("?"),
            metrics["work_scope"].as_str().unwrap_or("?"),
            metrics["work"]["log_rows"],
            metrics["work"]["tag_candidates"],
            metrics["work"]["block_reads"],
            metrics["resources"]["cpu_ms"],
            metrics["resources"]["max_rss_bytes"],
            metrics["writer"]["blocks"],
        )
        .unwrap();
    }
    (output, passed)
}
