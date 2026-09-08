//! Markdown tables and gate verdicts over result records.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::path::Path;

use serde_json::Value;

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
    let writes: Vec<&Value> = records.iter().filter(|r| kind(r) == "write").collect();
    if !writes.is_empty() {
        out.push_str("## Writes\n\n");
        header(
            &mut out,
            &[
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
    if !gates.is_empty() {
        out.push_str("## Gates (candidate against raw, medians over repeats)\n\n");
        header(
            &mut out,
            &[
                "workload",
                "candidate",
                "throughput vs raw",
                "p95 vs raw",
                "verdict",
            ],
        );
        for g in gates {
            row(
                &mut out,
                &[
                    g.workload,
                    g.candidate,
                    format!("{:.3}", g.throughput),
                    format!("{:.3}", g.p95),
                    if g.pass { "PASS".into() } else { "FAIL".into() },
                ],
            );
        }
        out.push('\n');
    }
    out
}

pub struct Gate {
    pub workload: String,
    pub candidate: String,
    pub throughput: f64,
    pub p95: f64,
    pub pass: bool,
}

/// The paired samples of one workload and codec.
#[derive(Default)]
struct Samples {
    throughput: Vec<f64>,
    p95: Vec<f64>,
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
/// write workload, and at most 10% more p95 point latency on every read
/// workload, each judged on the median over paired repeats.
pub fn gates(records: &[Value]) -> Vec<Gate> {
    let mut out = Vec::new();
    let mut groups: BTreeMap<(String, String), Samples> = BTreeMap::new();
    for r in records {
        let m = &r["metrics"];
        let (thr, p95, key) = match kind(r) {
            "write" => (
                f(m, &["blocks_per_s"]),
                f(m, &["batch_latency", "p95_us"]),
                workload(r).to_string(),
            ),
            "read" if m["point_ops"].as_u64().unwrap_or(0) > 0 => (
                f(m, &["ops_per_s"]),
                f(m, &["point_latency", "p95_us"]),
                format!("{} [{} t{}]", workload(r), s(m, &["regime"]), m["threads"]),
            ),
            "concurrent" => (
                f(m, &["writer", "blocks_per_s"]),
                f(m, &["writer", "batch_latency", "p95_us"]),
                format!("{} writer", workload(r)),
            ),
            _ => continue,
        };
        let entry = groups.entry((key, codec(r).to_string())).or_default();
        entry.throughput.push(thr);
        entry.p95.push(p95);
    }
    let workloads: Vec<String> = {
        let mut w: Vec<String> = groups.keys().map(|k| k.0.clone()).collect();
        w.dedup();
        w
    };
    for w in workloads {
        let Some(raw) = groups.get(&(w.clone(), "raw".into())) else {
            continue;
        };
        let raw_thr = median(raw.throughput.clone());
        let raw_p95 = median(raw.p95.clone());
        for ((wl, c), samples) in &groups {
            if *wl != w || c == "raw" {
                continue;
            }
            let throughput = if raw_thr > 0.0 {
                median(samples.throughput.clone()) / raw_thr
            } else {
                0.0
            };
            let p95 = if raw_p95 > 0.0 {
                median(samples.p95.clone()) / raw_p95
            } else {
                0.0
            };
            out.push(Gate {
                workload: w.clone(),
                candidate: c.clone(),
                throughput,
                p95,
                pass: throughput >= 0.9 && p95 <= 1.1,
            });
        }
    }
    out
}
