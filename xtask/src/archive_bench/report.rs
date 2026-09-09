//! Markdown tables and gate verdicts over result records.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::path::Path;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn load(paths: &[impl AsRef<Path>]) -> io::Result<Vec<Value>> {
    let mut records = Vec::new();
    for path in paths {
        let text = std::fs::read_to_string(path)?;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            records.push(serde_json::from_str(line)?);
        }
    }
    Ok(records)
}

fn f(v: &Value, path: &[&str]) -> f64 {
    let mut cur = v;
    for p in path {
        cur = &cur[*p];
    }
    cur.as_f64().unwrap_or(0.0)
}

fn s<'a>(v: &'a Value, path: &[&str]) -> &'a str {
    let mut cur = v;
    for p in path {
        cur = &cur[*p];
    }
    cur.as_str().unwrap_or("-")
}

fn codec(r: &Value) -> &str {
    s(r, &["codec", "codec"])
}

fn workload(r: &Value) -> &str {
    s(r, &["metrics", "workload"])
}

fn kind(r: &Value) -> &str {
    s(r, &["metrics", "kind"])
}

/// `v` as text with object keys in one order, so equal values give equal
/// keys whatever order the record wrote them in.
fn canonical(v: &Value) -> String {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let fields: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonical(&map[k])
                    )
                })
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(canonical).collect();
            format!("[{}]", items.join(","))
        }
        other => other.to_string(),
    }
}

/// The run a record belongs to: the harness revision and dirty state, the
/// host and its counters. Records from different runs never share a row or
/// a median. `recorded_at` is left out, so a rerun at the same revision
/// lands in the same run and its repeats collide, which the pairing reports
/// instead of pooling.
fn run_key(r: &Value) -> String {
    let mut environment = r["environment"].clone();
    if let Some(e) = environment.as_object_mut() {
        e.remove("recorded_at");
    }
    canonical(&environment)
}

/// Eight hex digits naming a run key in the tables.
fn run_id(key: &str) -> String {
    let digest = Sha256::digest(key.as_bytes());
    digest[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// Everything about a measurement that must agree before two records are
/// compared, other than its codec and repeat. Missing fields stay null: an
/// older record that never wrote a setting is not assumed to match.
fn settings(r: &Value) -> Value {
    let m = &r["metrics"];
    match kind(r) {
        "write" => json!({
            "batch": m["batch"],
            "encode_threads": m["encode_threads"],
            "fsync": m["fsync"],
        }),
        "read" => json!({
            "cache": match r["cache"]["method"].as_str() {
                Some(method) => Value::String(method.into()),
                None if m["regime"] == "warm" => Value::String("primed".into()),
                None => Value::Null,
            },
            "mix": m["mix"],
            "ops": m["ops"],
            "scan": m["scan"],
            "seed": m["seed"],
        }),
        "concurrent" => json!({
            "batch": m["writer"]["batch"],
            "encode_threads": m["writer"]["encode_threads"],
            "fsync": m["writer"]["fsync"],
            "head_blocks": m["head_blocks"],
            "mix": m["mix"],
            "readers": m["readers"],
            "seed": m["seed"],
        }),
        "node-import" => json!({
            "chunk": m["chunk"],
            "blocks": m["blocks"],
            "commit_instrumented": m["instrumentation"]["commit_latency"].is_object(),
        }),
        "node-read" => json!({
            "cache": match r["cache"]["method"].as_str() {
                Some(method) => Value::String(method.into()),
                None if m["regime"] == "warm" => Value::String("primed".into()),
                None => Value::Null,
            },
            "mix": m["mix"],
            "ops": m["ops"],
            "scan": m["scan"],
            "seed": m["seed"],
        }),
        _ => Value::Null,
    }
}

/// `settings` on one line; a null is `?`.
fn compact(v: &Value) -> String {
    match v {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| match v {
                Value::Object(_) => format!("{k} ({})", compact(v)),
                _ => format!("{k} {}", compact(v)),
            })
            .collect::<Vec<_>>()
            .join(", "),
        Value::Null => "?".into(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn dictionary(r: &Value) -> Option<String> {
    r["codec"]["dictionary"]
        .as_str()
        .map(|d| d.chars().take(8).collect())
}

fn revision(r: &Value) -> String {
    let commit: String = s(r, &["environment", "revision", "commit"])
        .chars()
        .take(8)
        .collect();
    match r["environment"]["revision"]["dirty"].as_bool() {
        Some(true) => format!("{commit} (dirty)"),
        _ => commit,
    }
}

/// Each run and corpus the records cover, in first-seen order, with one
/// record to describe the pair.
fn runs(records: &[Value]) -> Vec<(String, &Value)> {
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut out = Vec::new();
    for r in records {
        let id = run_id(&run_key(r));
        let pair = (id.clone(), canonical(&r["corpus"]));
        if !seen.contains(&pair) {
            seen.push(pair);
            out.push((id, r));
        }
    }
    out
}

fn corpus_summary(r: &Value) -> String {
    let c = &r["corpus"];
    let source = match s(c, &["source", "kind"]) {
        "segments" => format!(
            "segments {} of {}",
            c["source"]["segments"]
                .as_array()
                .map(|a| a
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(","))
                .unwrap_or_default(),
            s(c, &["source", "dir"])
        ),
        "immutable" => format!("immutable {}", s(c, &["source", "dir"])),
        "synthetic" => format!("synthetic seed {}", c["source"]["seed"]),
        other => other.to_string(),
    };
    format!(
        "{source}: {} blocks, {:.1} MiB",
        c["blocks"],
        f(c, &["raw_bytes"]) / 1_048_576.0
    )
}

fn header(out: &mut String, cols: &[&str]) {
    let _ = writeln!(out, "| {} |", cols.join(" | "));
    let _ = writeln!(
        out,
        "|{}|",
        cols.iter().map(|_| "---").collect::<Vec<_>>().join("|")
    );
}

fn row(out: &mut String, cells: &[String]) {
    let _ = writeln!(out, "| {} |", cells.join(" | "));
}

fn ms(us: f64) -> String {
    format!("{:.2}", us / 1000.0)
}

/// Render every record kind present as a markdown section.
pub fn render(records: &[Value]) -> String {
    let mut out = String::new();
    let runs = runs(records);
    if !runs.is_empty() {
        out.push_str("## Runs\n\n");
        header(
            &mut out,
            &[
                "run",
                "revision",
                "harness",
                "host",
                "filesystem",
                "corpus",
                "first recorded",
            ],
        );
        for (id, r) in &runs {
            let e = &r["environment"];
            row(
                &mut out,
                &[
                    id.clone(),
                    revision(r),
                    s(e, &["harness"]).into(),
                    format!(
                        "{}, {} {}, {:.0} GiB, zstd {}",
                        s(e, &["cpu"]),
                        s(e, &["os"]),
                        s(e, &["os_release"]),
                        f(e, &["memory_bytes"]) / 1_073_741_824.0,
                        s(e, &["zstd"])
                    ),
                    s(&e["filesystems"][0], &["type"]).into(),
                    corpus_summary(r),
                    s(e, &["recorded_at"]).into(),
                ],
            );
        }
        out.push('\n');
    }
    let writes: Vec<&Value> = records.iter().filter(|r| kind(r) == "write").collect();
    if !writes.is_empty() {
        out.push_str("## Writes\n\n");
        header(
            &mut out,
            &[
                "run",
                "workload",
                "codec",
                "rep",
                "blocks/s",
                "raw MB/s",
                "ratio",
                "encode MB/s (cpu)",
                "batch p50 ms",
                "p95 ms",
                "p99 ms",
                "fsync p50 ms",
                "peak heap MiB",
                "cpu µs/block",
            ],
        );
        for r in writes {
            let m = &r["metrics"];
            row(
                &mut out,
                &[
                    run_id(&run_key(r)),
                    workload(r).into(),
                    codec(r).into(),
                    r["repeat"].to_string(),
                    format!("{:.0}", f(m, &["blocks_per_s"])),
                    format!("{:.1}", f(m, &["raw_mb_per_s"])),
                    format!("{:.3}", f(m, &["ratio"])),
                    format!("{:.0}", f(m, &["encode_mb_per_cpu_s"])),
                    ms(f(m, &["batch_latency", "p50_us"])),
                    ms(f(m, &["batch_latency", "p95_us"])),
                    ms(f(m, &["batch_latency", "p99_us"])),
                    ms(f(m, &["fsync_latency", "p50_us"])),
                    format!("{:.1}", f(m, &["heap_peak_bytes"]) / 1_048_576.0),
                    format!("{:.1}", f(m, &["cpu_us_per_block"])),
                ],
            );
        }
        out.push('\n');
    }

    let reads: Vec<&Value> = records.iter().filter(|r| kind(r) == "read").collect();
    if !reads.is_empty() {
        out.push_str("## Reads\n\n");
        header(
            &mut out,
            &[
                "run",
                "workload",
                "cache",
                "thr",
                "codec",
                "rep",
                "ops/s",
                "blocks/s",
                "point p50 µs",
                "p95 µs",
                "p99 µs",
                "page p50 ms",
                "p95 ms",
                "decode µs/blk",
                "cpu µs/op",
                "disk MiB",
                "amplif.",
            ],
        );
        for r in reads {
            let m = &r["metrics"];
            row(
                &mut out,
                &[
                    run_id(&run_key(r)),
                    workload(r).into(),
                    s(m, &["regime"]).into(),
                    m["threads"].to_string(),
                    codec(r).into(),
                    r["repeat"].to_string(),
                    format!("{:.0}", f(m, &["ops_per_s"])),
                    format!("{:.0}", f(m, &["blocks_per_s"])),
                    format!("{:.0}", f(m, &["point_latency", "p50_us"])),
                    format!("{:.0}", f(m, &["point_latency", "p95_us"])),
                    format!("{:.0}", f(m, &["point_latency", "p99_us"])),
                    ms(f(m, &["page_latency", "p50_us"])),
                    ms(f(m, &["page_latency", "p95_us"])),
                    format!("{:.1}", f(m, &["decode_us_per_block"])),
                    format!("{:.1}", f(m, &["cpu_us_per_op"])),
                    format!("{:.1}", f(m, &["process", "disk_read_bytes"]) / 1_048_576.0),
                    match m["disk_read_amplification"].as_f64() {
                        Some(a) => format!("{a:.2}"),
                        None => "n/a".into(),
                    },
                ],
            );
        }
        out.push('\n');
    }

    let conc: Vec<&Value> = records.iter().filter(|r| kind(r) == "concurrent").collect();
    if !conc.is_empty() {
        out.push_str("## Append under query load\n\n");
        header(
            &mut out,
            &[
                "run",
                "workload",
                "codec",
                "rep",
                "readers",
                "writer blocks/s",
                "batch p50 ms",
                "p95 ms",
                "p99 ms",
                "reader ops/s",
                "point p50 µs",
                "p95 µs",
                "page p95 ms",
                "wall s",
            ],
        );
        for r in conc {
            let m = &r["metrics"];
            row(
                &mut out,
                &[
                    run_id(&run_key(r)),
                    workload(r).into(),
                    codec(r).into(),
                    r["repeat"].to_string(),
                    m["readers"].to_string(),
                    format!("{:.0}", f(m, &["writer", "blocks_per_s"])),
                    ms(f(m, &["writer", "batch_latency", "p50_us"])),
                    ms(f(m, &["writer", "batch_latency", "p95_us"])),
                    ms(f(m, &["writer", "batch_latency", "p99_us"])),
                    format!("{:.0}", f(m, &["reader", "ops_per_s"])),
                    format!("{:.0}", f(m, &["reader", "point_latency", "p50_us"])),
                    format!("{:.0}", f(m, &["reader", "point_latency", "p95_us"])),
                    ms(f(m, &["reader", "page_latency", "p95_us"])),
                    format!("{:.1}", f(m, &["wall_s"])),
                ],
            );
        }
        out.push('\n');
    }

    let evals: Vec<&Value> = records.iter().filter(|r| kind(r) == "evaluate").collect();
    if !evals.is_empty() {
        out.push_str("## Dictionary evaluation\n\n");
        header(
            &mut out,
            &[
                "run",
                "fixture",
                "codec",
                "blocks",
                "raw MiB",
                "ratio",
                "encode MB/s (cpu)",
                "decode µs/blk",
                "per era",
            ],
        );
        for r in evals {
            let m = &r["metrics"];
            let eras = m["eras"]
                .as_object()
                .map(|e| {
                    e.iter()
                        .map(|(k, v)| format!("{k} {:.3}", f(v, &["ratio"])))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            row(
                &mut out,
                &[
                    run_id(&run_key(r)),
                    s(m, &["fixture"]).into(),
                    codec(r).into(),
                    m["blocks"].to_string(),
                    format!("{:.1}", f(m, &["raw_bytes"]) / 1_048_576.0),
                    format!("{:.3}", f(m, &["ratio"])),
                    format!("{:.0}", f(m, &["encode_mb_per_cpu_s"])),
                    format!("{:.1}", f(m, &["decode_us_per_block"])),
                    eras,
                ],
            );
        }
        out.push('\n');
    }

    let gates = gates(records);
    let writes: Vec<&Gate> = gates.iter().filter(|g| g.gated).collect();
    if !writes.is_empty() {
        out.push_str(
            "## Gates (writes: candidate against raw, medians over paired repeats within one run)\n\n",
        );
        header(
            &mut out,
            &[
                "run",
                "workload",
                "settings",
                "candidate",
                "dictionary",
                "throughput vs raw",
                "batch p95 vs raw",
                "verdict",
            ],
        );
        for g in writes {
            row(
                &mut out,
                &[
                    g.run.clone(),
                    g.workload.clone(),
                    g.settings.clone(),
                    g.candidate.clone(),
                    g.dictionary.clone().unwrap_or_default(),
                    format!("{:.3}", g.throughput),
                    format!("{:.3}", g.p95),
                    g.verdict(),
                ],
            );
        }
        out.push('\n');
    }
    let reads: Vec<&Gate> = gates.iter().filter(|g| !g.gated).collect();
    if !reads.is_empty() {
        out.push_str(
            "## Reads against raw (medians over paired repeats within one run; microbenchmark, not the API gate)\n\n",
        );
        header(
            &mut out,
            &[
                "run",
                "workload",
                "settings",
                "candidate",
                "dictionary",
                "ops/s vs raw",
                "point p95 vs raw",
                "raw p95 µs",
                "candidate p95 µs",
                "pairing",
            ],
        );
        for g in reads {
            row(
                &mut out,
                &[
                    g.run.clone(),
                    g.workload.clone(),
                    g.settings.clone(),
                    g.candidate.clone(),
                    g.dictionary.clone().unwrap_or_default(),
                    format!("{:.3}", g.throughput),
                    format!("{:.3}", g.p95),
                    format!("{:.0}", g.raw_p95_us),
                    format!("{:.0}", g.p95_us),
                    g.problem.clone().unwrap_or_else(|| "paired".into()),
                ],
            );
        }
        out.push('\n');
    }
    render_node(records, &mut out);
    out
}

/// One candidate codec against raw on one workload, within one run.
pub struct Gate {
    pub run: String,
    pub workload: String,
    /// The settings both sides were measured under, for the table.
    pub settings: String,
    pub candidate: String,
    /// The first eight hex digits of the candidate's dictionary id, if any.
    pub dictionary: Option<String>,
    /// Candidate throughput over raw throughput.
    pub throughput: f64,
    /// Candidate p95 over raw p95.
    pub p95: f64,
    pub raw_p95_us: f64,
    pub p95_us: f64,
    /// Whether the provisional gate applies: writes are gated, reads are
    /// reported.
    pub gated: bool,
    pub pass: bool,
    /// Why the samples could not be paired: no raw record in the run, or
    /// repeats that do not match one to one. Ratios are still shown; the
    /// verdict is withheld.
    pub problem: Option<String>,
}

impl Gate {
    pub fn verdict(&self) -> String {
        match (&self.problem, self.gated, self.pass) {
            (Some(problem), _, _) => format!("UNPAIRED: {problem}"),
            (None, false, _) => "-".into(),
            (None, true, true) => "PASS".into(),
            (None, true, false) => "FAIL".into(),
        }
    }
}

/// The samples of one codec on one workload of one run, by repeat.
#[derive(Default)]
struct Samples {
    label: String,
    dictionary: Option<String>,
    by_repeat: BTreeMap<u64, Vec<(f64, f64)>>,
}

impl Samples {
    fn throughput(&self) -> Vec<f64> {
        self.by_repeat.values().flatten().map(|s| s.0).collect()
    }

    fn p95(&self) -> Vec<f64> {
        self.by_repeat.values().flatten().map(|s| s.1).collect()
    }

    fn repeats(&self) -> String {
        self.by_repeat
            .keys()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn duplicates(&self) -> Vec<String> {
        self.by_repeat
            .iter()
            .filter(|(_, v)| v.len() > 1)
            .map(|(k, _)| k.to_string())
            .collect()
    }
}

/// Why `raw` and `candidate` are not one sample of each per repeat.
fn pairing_problem(raw: Option<&Samples>, candidate: &Samples) -> Option<String> {
    pairing_problem_against("raw", raw, candidate)
}

/// [`pairing_problem`] with the reference side named.
fn pairing_problem_against(
    base: &str,
    raw: Option<&Samples>,
    candidate: &Samples,
) -> Option<String> {
    let Some(raw) = raw else {
        return Some(format!("no {base} record in this run"));
    };
    for (side, samples) in [(base, raw), ("candidate", candidate)] {
        let dup = samples.duplicates();
        if !dup.is_empty() {
            return Some(format!(
                "{side} repeat {} recorded more than once",
                dup.join(",")
            ));
        }
    }
    if raw.by_repeat.keys().ne(candidate.by_repeat.keys()) {
        return Some(format!(
            "{base} repeats {} against candidate repeats {}",
            raw.repeats(),
            candidate.repeats()
        ));
    }
    None
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        0.0
    } else {
        v[v.len() / 2]
    }
}

/// The provisional gates: a compressed codec keeps at least 90% of raw
/// ingestion throughput and at most 10% more p95 commit latency on every
/// write workload, judged on the median over paired repeats. Only records
/// of one run, one corpus, one workload and one set of settings are
/// compared, the candidate keeps its dictionary identity, and a verdict
/// needs one raw and one candidate sample per repeat; anything else is
/// reported unpaired.
/// Read workloads get the same ratios without a verdict: the plan's read
/// gate is p95 API point latency, which the production path measures, and
/// against a raw sink a warm point read is a memcpy, so any decode at all is
/// a multiple of it.
pub fn gates(records: &[Value]) -> Vec<Gate> {
    type Group = BTreeMap<String, Samples>;
    let mut groups: BTreeMap<(String, String, String), Group> = BTreeMap::new();
    let mut gated: BTreeMap<String, bool> = BTreeMap::new();
    for r in records {
        let m = &r["metrics"];
        let (thr, p95, key, is_write) = match kind(r) {
            "write" => (
                f(m, &["blocks_per_s"]),
                f(m, &["batch_latency", "p95_us"]),
                workload(r).to_string(),
                true,
            ),
            "read" if m["point_ops"].as_u64().unwrap_or(0) > 0 => (
                f(m, &["ops_per_s"]),
                f(m, &["point_latency", "p95_us"]),
                format!("{} [{} t{}]", workload(r), s(m, &["regime"]), m["threads"]),
                false,
            ),
            "concurrent" => (
                f(m, &["writer", "blocks_per_s"]),
                f(m, &["writer", "batch_latency", "p95_us"]),
                format!("{} writer", workload(r)),
                true,
            ),
            _ => continue,
        };
        gated.insert(key.clone(), is_write);
        let scope = (
            run_key(r),
            key,
            canonical(&json!({ "corpus": r["corpus"], "settings": settings(r) })),
        );
        let entry = groups
            .entry(scope)
            .or_default()
            .entry(canonical(&r["codec"]))
            .or_default();
        entry.label = codec(r).to_string();
        entry.dictionary = dictionary(r);
        entry
            .by_repeat
            .entry(r["repeat"].as_u64().unwrap_or(0))
            .or_default()
            .push((thr, p95));
    }
    let mut out = Vec::new();
    for ((run, workload, settings), codecs) in &groups {
        let raw = codecs.values().find(|c| c.label == "raw");
        let raw_thr = raw.map(|r| median(r.throughput())).unwrap_or(0.0);
        let raw_p95 = raw.map(|r| median(r.p95())).unwrap_or(0.0);
        for candidate in codecs.values().filter(|c| c.label != "raw") {
            let throughput = if raw_thr > 0.0 {
                median(candidate.throughput()) / raw_thr
            } else {
                0.0
            };
            let p95_us = median(candidate.p95());
            let p95 = if raw_p95 > 0.0 { p95_us / raw_p95 } else { 0.0 };
            let problem = pairing_problem(raw, candidate);
            let is_write = gated.get(workload).copied().unwrap_or(false);
            out.push(Gate {
                run: run_id(run),
                workload: workload.clone(),
                settings: compact(
                    &serde_json::from_str::<Value>(settings)
                        .map(|v| v["settings"].clone())
                        .unwrap_or(Value::Null),
                ),
                candidate: candidate.label.clone(),
                dictionary: candidate.dictionary.clone(),
                throughput,
                p95,
                raw_p95_us: raw_p95,
                p95_us,
                gated: is_write,
                pass: is_write && problem.is_none() && throughput >= 0.9 && p95 <= 1.1,
                problem,
            });
        }
    }
    out
}

/// The label the node-level gates measure every other label against.
pub const BASELINE_LABEL: &str = "baseline";

fn node_label(r: &Value) -> &str {
    s(r, &["node", "label"])
}

fn node_run(r: &Value) -> &str {
    s(r, &["run"])
}

/// Node-level records: imports and reads per binary label, then the
/// `baseline` label against every other label.
fn render_node(records: &[Value], out: &mut String) {
    let imports: Vec<&Value> = records
        .iter()
        .filter(|r| kind(r) == "node-import")
        .collect();
    if !imports.is_empty() {
        out.push_str("## Node imports\n\n");
        header(
            out,
            &[
                "run",
                "workload",
                "label",
                "revision",
                "storage",
                "rep",
                "blocks",
                "blocks/s",
                "raw MB/s",
                "ms/batch",
                "cpu µs/block",
                "max RSS MiB",
                "segments MiB",
                "ratio",
                "index MiB",
                "commit p50 ms",
                "commit p95 ms",
                "commit p99 ms",
                "encoded buffers MiB",
            ],
        );
        for r in imports {
            let m = &r["metrics"];
            row(
                out,
                &[
                    node_run(r).into(),
                    workload(r).into(),
                    node_label(r).into(),
                    s(r, &["node", "version"]).into(),
                    s(r, &["node", "storage_version"]).into(),
                    r["repeat"].to_string(),
                    m["blocks"].to_string(),
                    format!("{:.0}", f(m, &["blocks_per_s"])),
                    format!("{:.1}", f(m, &["raw_mb_per_s"])),
                    format!("{:.2}", f(m, &["ms_per_batch"])),
                    format!("{:.1}", f(m, &["cpu_us_per_block"])),
                    format!("{:.0}", f(m, &["process", "max_rss_bytes"]) / 1_048_576.0),
                    format!("{:.1}", f(m, &["segment_bytes"]) / 1_048_576.0),
                    format!("{:.3}", f(m, &["ratio"])),
                    format!("{:.1}", f(m, &["index_bytes"]) / 1_048_576.0),
                    m["instrumentation"]["commit_latency"]["p50_us"]
                        .as_f64()
                        .map(ms)
                        .unwrap_or_else(|| "-".into()),
                    m["instrumentation"]["commit_latency"]["p95_us"]
                        .as_f64()
                        .map(ms)
                        .unwrap_or_else(|| "-".into()),
                    m["instrumentation"]["commit_latency"]["p99_us"]
                        .as_f64()
                        .map(ms)
                        .unwrap_or_else(|| "-".into()),
                    m["instrumentation"]["buffers"]["buffer_bytes_peak"]
                        .as_f64()
                        .map(|bytes| format!("{:.2}", bytes / 1_048_576.0))
                        .unwrap_or_else(|| "-".into()),
                ],
            );
        }
        out.push('\n');
    }

    let reads: Vec<&Value> = records.iter().filter(|r| kind(r) == "node-read").collect();
    if !reads.is_empty() {
        out.push_str("## Node reads (through minibf)\n\n");
        header(
            out,
            &[
                "run",
                "workload",
                "cache",
                "thr",
                "label",
                "rep",
                "ops/s",
                "blocks/s",
                "point p50 µs",
                "p95 µs",
                "p99 µs",
                "page p50 ms",
                "p95 ms",
                "server cpu ms",
                "server disk MiB",
            ],
        );
        for r in reads {
            let m = &r["metrics"];
            row(
                out,
                &[
                    node_run(r).into(),
                    workload(r).into(),
                    format!(
                        "{} {}",
                        s(m, &["regime"]),
                        r["cache"]["method"].as_str().unwrap_or("")
                    )
                    .trim()
                    .to_string(),
                    m["threads"].to_string(),
                    node_label(r).into(),
                    r["repeat"].to_string(),
                    format!("{:.0}", f(m, &["ops_per_s"])),
                    format!("{:.0}", f(m, &["blocks_per_s"])),
                    format!("{:.0}", f(m, &["point_latency", "p50_us"])),
                    format!("{:.0}", f(m, &["point_latency", "p95_us"])),
                    format!("{:.0}", f(m, &["point_latency", "p99_us"])),
                    ms(f(m, &["page_latency", "p50_us"])),
                    ms(f(m, &["page_latency", "p95_us"])),
                    format!("{:.0}", f(m, &["server", "cpu_ms"])),
                    format!("{:.1}", f(m, &["server", "disk_read_bytes"]) / 1_048_576.0),
                ],
            );
        }
        out.push('\n');
    }

    let gates = node_gates(records);
    if !gates.is_empty() {
        out.push_str(
            "## Node gates (each label against `baseline`; medians over paired repeats within one run name)\n\n",
        );
        header(
            out,
            &[
                "run",
                "workload",
                "settings",
                "label",
                "throughput vs baseline",
                "p95 vs baseline",
                "baseline p95 µs",
                "label p95 µs",
                "gate",
                "verdict",
            ],
        );
        for g in &gates {
            row(
                out,
                &[
                    g.run.clone(),
                    g.workload.clone(),
                    g.settings.clone(),
                    g.label.clone(),
                    format!("{:.3}", g.throughput),
                    g.p95
                        .map(|p| format!("{p:.3}"))
                        .unwrap_or_else(|| "-".into()),
                    g.baseline_p95_us
                        .map(|p| format!("{p:.0}"))
                        .unwrap_or_else(|| "-".into()),
                    g.p95_us
                        .map(|p| format!("{p:.0}"))
                        .unwrap_or_else(|| "-".into()),
                    g.gate.into(),
                    g.verdict(),
                ],
            );
        }
        out.push('\n');
    }
}

/// One label against `baseline` on one node-level workload of one run.
pub struct NodeGate {
    pub run: String,
    pub workload: String,
    pub settings: String,
    pub label: String,
    /// Label throughput over baseline throughput (blocks/s for imports and
    /// scans, ops/s for point and page workloads).
    pub throughput: f64,
    /// Label commit or point-read p95 over the matching baseline p95.
    pub p95: Option<f64>,
    pub baseline_p95_us: Option<f64>,
    pub p95_us: Option<f64>,
    /// Ingestion throughput and instrumented commit p95, API point p95,
    /// or no gate for pages and scans.
    pub gate: &'static str,
    pub pass: bool,
    pub problem: Option<String>,
}

impl NodeGate {
    pub fn verdict(&self) -> String {
        match (&self.problem, self.gate, self.pass) {
            (Some(problem), _, _) => format!("UNPAIRED: {problem}"),
            (None, "-", _) => "-".into(),
            (None, _, true) => "PASS".into(),
            (None, _, false) => "FAIL".into(),
        }
    }
}

/// The production gates: ingestion keeps at least 90% of the baseline
/// binary's throughput, and API point reads keep p95 latency within 10% of
/// it, judged on the median over paired repeats. Records pair only within
/// one run name, corpus, workload and set of settings, and need one sample
/// per label per repeat; anything else is reported unpaired.
pub fn node_gates(records: &[Value]) -> Vec<NodeGate> {
    type Group = BTreeMap<String, Samples>;
    let mut groups: BTreeMap<(String, String, String), Group> = BTreeMap::new();
    let mut gate_of: BTreeMap<(String, String, String), &'static str> = BTreeMap::new();
    for r in records {
        let m = &r["metrics"];
        let (thr, p95, key, gate) = match kind(r) {
            "node-import" => (
                f(m, &["blocks_per_s"]),
                m["instrumentation"]["commit_latency"]["p95_us"]
                    .as_f64()
                    .unwrap_or(f64::NAN),
                workload(r).to_string(),
                if m["instrumentation"]["commit_latency"].is_object() {
                    "throughput >= 0.9, commit p95 <= 1.1 (3 pairs)"
                } else {
                    "throughput >= 0.9"
                },
            ),
            "node-read" => {
                let key = format!("{} [{} t{}]", workload(r), s(m, &["regime"]), m["threads"]);
                if m["point_ops"].as_u64().unwrap_or(0) > 0 {
                    (
                        f(m, &["ops_per_s"]),
                        f(m, &["point_latency", "p95_us"]),
                        key,
                        "point p95 <= 1.1",
                    )
                } else {
                    let thr = if m["scan"].as_bool().unwrap_or(false) {
                        f(m, &["blocks_per_s"])
                    } else {
                        f(m, &["ops_per_s"])
                    };
                    (thr, f64::NAN, key, "-")
                }
            }
            _ => continue,
        };
        let scope = (
            node_run(r).to_string(),
            key,
            canonical(&json!({ "corpus": r["corpus"], "settings": settings(r) })),
        );
        gate_of.insert(scope.clone(), gate);
        let entry = groups
            .entry(scope)
            .or_default()
            .entry(node_label(r).to_string())
            .or_default();
        entry.label = node_label(r).to_string();
        entry
            .by_repeat
            .entry(r["repeat"].as_u64().unwrap_or(0))
            .or_default()
            .push((thr, p95));
    }
    let mut out = Vec::new();
    for ((run, workload, settings), labels) in &groups {
        let finite_median = |v: Vec<f64>| -> Option<f64> {
            let finite: Vec<f64> = v.into_iter().filter(|p| p.is_finite()).collect();
            (!finite.is_empty()).then(|| median(finite))
        };
        let baseline = labels.get(BASELINE_LABEL);
        let base_thr = baseline.map(|b| median(b.throughput())).unwrap_or(0.0);
        let base_p95 = baseline.and_then(|b| finite_median(b.p95()));
        let gate = gate_of
            .get(&(run.clone(), workload.clone(), settings.clone()))
            .copied()
            .unwrap_or("-");
        for candidate in labels.values().filter(|c| c.label != BASELINE_LABEL) {
            let throughput = if base_thr > 0.0 {
                median(candidate.throughput()) / base_thr
            } else {
                0.0
            };
            let p95_us = finite_median(candidate.p95());
            let p95 = match (base_p95, p95_us) {
                (Some(b), Some(c)) if b > 0.0 => Some(c / b),
                _ => None,
            };
            let problem = pairing_problem_against(BASELINE_LABEL, baseline, candidate);
            let pass = problem.is_none()
                && match gate {
                    "throughput >= 0.9" => throughput >= 0.9,
                    "throughput >= 0.9, commit p95 <= 1.1 (3 pairs)" => {
                        throughput >= 0.9
                            && p95.is_some_and(|ratio| ratio <= 1.1)
                            && candidate.by_repeat.len() >= 3
                    }
                    "point p95 <= 1.1" => p95.is_some_and(|p| p <= 1.1),
                    _ => false,
                };
            out.push(NodeGate {
                run: run.clone(),
                workload: workload.clone(),
                settings: compact(
                    &serde_json::from_str::<Value>(settings)
                        .map(|v| v["settings"].clone())
                        .unwrap_or(Value::Null),
                ),
                label: candidate.label.clone(),
                throughput,
                p95,
                baseline_p95_us: base_p95,
                p95_us,
                gate,
                pass,
                problem,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        kind: &str,
        codec: Value,
        repeat: u64,
        commit: &str,
        settings: Value,
        thr: f64,
        p95: f64,
    ) -> Value {
        let metrics = match kind {
            "write" => json!({
                "kind": "write",
                "workload": "write-100",
                "batch": 100,
                "encode_threads": settings["encode_threads"],
                "fsync": true,
                "blocks_per_s": thr,
                "batch_latency": { "p95_us": p95 },
            }),
            "concurrent" => json!({
                "kind": "concurrent",
                "workload": "append-query-100",
                "readers": 8,
                "mix": { "point_share": 0.9 },
                "seed": 0,
                "head_blocks": 200,
                "writer": {
                    "batch": 100,
                    "blocks_per_s": thr,
                    "batch_latency": { "p95_us": p95 },
                },
            }),
            other => panic!("no fixture for {other}"),
        };
        json!({
            "preset": "write",
            "environment": {
                "revision": { "commit": commit, "dirty": false },
                "recorded_at": format!("2026-09-08T00:00:0{repeat}Z"),
                "cpu": "test",
            },
            "corpus": { "source": { "kind": "synthetic", "seed": 1 }, "blocks": 400, "raw_bytes": 4096 },
            "codec": codec,
            "repeat": repeat,
            "cache": null,
            "metrics": metrics,
        })
    }

    fn raw() -> Value {
        json!({ "codec": "raw" })
    }

    #[test]
    fn instrumented_import_gate_requires_three_pairs_and_real_commit_p95() {
        let records = |repeats, p95| -> Vec<Value> {
            (0..repeats)
                .flat_map(|repeat| {
                    [("baseline", 100.0, 10.0), ("optimized", 95.0, p95)].map(
                        |(label, throughput, latency)| {
                            json!({
                                "run": "test", "repeat": repeat, "node": {"label": label},
                                "corpus": {"blocks": 1000},
                                "metrics": {
                                    "kind": "node-import", "workload": "import-500",
                                    "chunk": 500, "blocks": 1000, "blocks_per_s": throughput,
                                    "instrumentation": {"commit_latency": {"p95_us": latency}}
                                }
                            })
                        },
                    )
                })
                .collect()
        };
        assert!(!node_gates(&records(2, 10.0))[0].pass);
        assert!(node_gates(&records(3, 10.5))[0].pass);
        assert!(!node_gates(&records(3, 11.5))[0].pass);
        let mut mixed = records(3, 10.5);
        mixed[0]["metrics"]["instrumentation"] = Value::Null;
        assert!(!node_gates(&mixed)[0].pass);
    }

    fn dict(id: &str) -> Value {
        json!({ "codec": "zstd3-dict", "level": 3, "dictionary": id, "dictionary_bytes": 100 })
    }

    fn write(codec: Value, repeat: u64, commit: &str, threads: u64, thr: f64, p95: f64) -> Value {
        record(
            "write",
            codec,
            repeat,
            commit,
            json!({ "encode_threads": threads }),
            thr,
            p95,
        )
    }

    #[test]
    fn runs_at_different_revisions_are_never_pooled() {
        let mut records = Vec::new();
        for repeat in 0..3 {
            records.push(write(raw(), repeat, "aaaa1111", 1, 100.0, 10.0));
            records.push(write(dict("d1"), repeat, "aaaa1111", 1, 95.0, 10.5));
            records.push(write(raw(), repeat, "bbbb2222", 1, 100.0, 10.0));
            records.push(write(dict("d1"), repeat, "bbbb2222", 1, 80.0, 10.5));
        }
        let gates = gates(&records);
        assert_eq!(gates.len(), 2);
        assert_ne!(gates[0].run, gates[1].run);
        let verdicts: Vec<String> = gates.iter().map(Gate::verdict).collect();
        assert!(verdicts.contains(&"PASS".to_string()));
        assert!(verdicts.contains(&"FAIL".to_string()));
        let rendered = render(&records);
        assert!(rendered.contains("## Runs"));
        assert!(rendered.contains("aaaa1111"));
        assert!(rendered.contains("bbbb2222"));
    }

    #[test]
    fn different_settings_in_one_run_are_separate_groups() {
        let mut records = Vec::new();
        for repeat in 0..3 {
            records.push(write(raw(), repeat, "aaaa1111", 1, 100.0, 10.0));
            records.push(write(dict("d1"), repeat, "aaaa1111", 1, 95.0, 10.5));
            records.push(write(raw(), repeat, "aaaa1111", 4, 100.0, 10.0));
            records.push(write(dict("d1"), repeat, "aaaa1111", 4, 70.0, 10.5));
        }
        let gates = gates(&records);
        assert_eq!(gates.len(), 2);
        assert_eq!(gates[0].run, gates[1].run);
        let by_threads: BTreeMap<&str, bool> = gates
            .iter()
            .map(|g| (g.settings.as_str(), g.pass))
            .collect();
        assert_eq!(
            by_threads.get("batch 100, encode_threads 1, fsync true"),
            Some(&true)
        );
        assert_eq!(
            by_threads.get("batch 100, encode_threads 4, fsync true"),
            Some(&false)
        );
    }

    #[test]
    fn candidates_keep_their_dictionary_identity() {
        let mut records = Vec::new();
        for repeat in 0..3 {
            records.push(write(raw(), repeat, "aaaa1111", 1, 100.0, 10.0));
            records.push(write(dict("d1d1d1d1d1"), repeat, "aaaa1111", 1, 95.0, 10.5));
            records.push(write(dict("d2d2d2d2d2"), repeat, "aaaa1111", 1, 85.0, 10.5));
        }
        let gates = gates(&records);
        assert_eq!(gates.len(), 2);
        let ids: Vec<Option<String>> = gates.iter().map(|g| g.dictionary.clone()).collect();
        assert!(ids.contains(&Some("d1d1d1d1".into())));
        assert!(ids.contains(&Some("d2d2d2d2".into())));
        assert!(gates.iter().all(|g| g.candidate == "zstd3-dict"));
    }

    #[test]
    fn a_candidate_without_raw_gets_no_verdict() {
        let records: Vec<Value> = (0..3)
            .map(|repeat| write(dict("d1"), repeat, "aaaa1111", 1, 95.0, 10.5))
            .collect();
        let gates = gates(&records);
        assert_eq!(gates.len(), 1);
        assert!(!gates[0].pass);
        assert_eq!(gates[0].verdict(), "UNPAIRED: no raw record in this run");
        assert!(render(&records).contains("UNPAIRED"));
    }

    #[test]
    fn mismatched_repeats_get_no_verdict() {
        let mut records = Vec::new();
        for repeat in 0..3 {
            records.push(write(raw(), repeat, "aaaa1111", 1, 100.0, 10.0));
        }
        for repeat in 0..2 {
            records.push(write(dict("d1"), repeat, "aaaa1111", 1, 95.0, 10.5));
        }
        let gates = gates(&records);
        assert_eq!(gates.len(), 1);
        assert!(!gates[0].pass);
        assert_eq!(
            gates[0].verdict(),
            "UNPAIRED: raw repeats 0,1,2 against candidate repeats 0,1"
        );
    }

    #[test]
    fn a_repeat_recorded_twice_gets_no_verdict() {
        let mut records = Vec::new();
        for repeat in 0..3 {
            records.push(write(raw(), repeat, "aaaa1111", 1, 100.0, 10.0));
            records.push(write(dict("d1"), repeat, "aaaa1111", 1, 95.0, 10.5));
        }
        records.push(write(dict("d1"), 0, "aaaa1111", 1, 95.0, 10.5));
        let gates = gates(&records);
        assert_eq!(gates.len(), 1);
        assert!(!gates[0].pass);
        assert_eq!(
            gates[0].verdict(),
            "UNPAIRED: candidate repeat 0 recorded more than once"
        );
    }

    #[test]
    fn a_record_without_writer_settings_says_so() {
        let mut records = Vec::new();
        for repeat in 0..3 {
            records.push(record(
                "concurrent",
                raw(),
                repeat,
                "aaaa1111",
                json!({}),
                100.0,
                10.0,
            ));
            records.push(record(
                "concurrent",
                dict("d1"),
                repeat,
                "aaaa1111",
                json!({}),
                95.0,
                10.5,
            ));
        }
        let gates = gates(&records);
        assert_eq!(gates.len(), 1);
        assert!(gates[0].pass);
        assert!(gates[0].settings.contains("encode_threads ?"));
        assert!(gates[0].settings.contains("fsync ?"));
    }
}
