//! The smoke preset over a synthetic corpus: every workload runs, every
//! body read back matches, and the records carry what a report needs.

use xtask::perf::dictionary::Dictionary;
use xtask::perf::report;
use xtask::perf::storage::codec::Codec;
use xtask::perf::storage::corpus::Corpus;
use xtask::perf::storage::presets::{run, Options, Preset};
use xtask::perf::storage::train::{evaluate, Fixture};
use xtask::perf::storage::workloads::{EvictOptions, Regime};

#[test]
fn automatic_store_selects_parallel_in_a_two_worker_process() {
    let work = tempfile::tempdir().unwrap();
    let output = work.path().join("results.jsonl");
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-xtask"))
        .env("RAYON_NUM_THREADS", "2")
        .args([
            "perf",
            "storage",
            "run",
            "--preset",
            "write",
            "--synthetic",
            "400",
            "--codecs",
            "store",
            "--write-batches",
            "400",
            "--repeat",
            "1",
            "--work",
        ])
        .arg(work.path().join("store"))
        .arg("--out")
        .arg(&output)
        .status()
        .unwrap();
    assert!(status.success());
    let contents = std::fs::read_to_string(output).unwrap();
    let record: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
    assert_eq!(record["metrics"]["import_buffers"]["encoders_peak"], 2);
}

#[test]
fn smoke_preset_runs_every_workload_and_verifies_bodies() {
    let work = tempfile::tempdir().unwrap();
    let corpus = Corpus::synthetic(1, 400, 2);
    assert!(
        corpus.segments().len() >= 2,
        "the corpus must cross a segment"
    );
    let opts = Options {
        work: work.path().join("work"),
        codecs: vec![
            ("raw".into(), Codec::Raw),
            ("zstd3".into(), Codec::zstd(3, None)),
            (
                "zstd3-dict".into(),
                Codec::zstd(3, Some(Dictionary::bundled())),
            ),
            ("store".into(), Codec::Store),
        ],
        repeat: 1,
        threads: vec![2],
        regimes: vec![Regime::Warm],
        evict: EvictOptions::default(),
        ops: 60,
        seed: 0,
        encode_threads: 1,
        fsync: true,
        keep: false,
        verify: true,
        write_batches: vec![1, 7, 400],
    };
    let mut out = Vec::new();
    run(Preset::Smoke, &corpus, &opts, &mut out).unwrap();

    let text = String::from_utf8(out).unwrap();
    let records: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let kinds: std::collections::BTreeSet<&str> = records
        .iter()
        .map(|r| r["metrics"]["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds.into_iter().collect::<Vec<_>>(),
        ["concurrent", "read", "write"]
    );

    let writes: Vec<_> = records
        .iter()
        .filter(|r| r["metrics"]["kind"] == "write")
        .collect();
    assert_eq!(writes.len(), 3 * 4);
    for w in &writes {
        assert_eq!(w["metrics"]["blocks"], 400);
        let crossings = w["metrics"]["segment_crossings"].as_u64().unwrap();
        assert_eq!(crossings >= 1, w["metrics"]["batch"] != 1);
        assert_eq!(w["metrics"]["segments"], 2);
    }
    let dict = writes
        .iter()
        .find(|w| w["codec"]["codec"] == "zstd3-dict" && w["metrics"]["batch"] == 7)
        .unwrap();
    assert!(dict["metrics"]["ratio"].as_f64().unwrap() < 0.8);
    assert_eq!(
        dict["metrics"]["encode_cpu_ms"].as_f64().unwrap() > 0.0,
        xtask::perf::measure::thread_cpu_available()
    );
    assert_eq!(dict["metrics"]["batch_latency"]["count"], 58);
    let store = writes
        .iter()
        .find(|w| w["codec"]["codec"] == "store" && w["metrics"]["batch"] == 7)
        .unwrap();
    assert!(store["metrics"]["ratio"].as_f64().unwrap() < 0.8);
    assert!(store["metrics"]["write_ms"].as_f64().unwrap() > 0.0);
    assert_eq!(store["codec"]["dictionary"], dict["codec"]["dictionary"]);
    assert_eq!(store["metrics"]["import_buffers"]["encoders_peak"], 0);
    let large = writes
        .iter()
        .find(|record| record["codec"]["codec"] == "store" && record["metrics"]["batch"] == 400)
        .unwrap();
    assert!(
        large["metrics"]["import_buffers"]["buffer_bytes_peak"]
            .as_u64()
            .unwrap()
            > 0
    );

    let reads: Vec<_> = records
        .iter()
        .filter(|r| r["metrics"]["kind"] == "read")
        .collect();
    let names: std::collections::BTreeSet<&str> = reads
        .iter()
        .map(|r| r["metrics"]["workload"].as_str().unwrap())
        .collect();
    assert_eq!(
        names.into_iter().collect::<Vec<_>>(),
        [
            "mix-10-90",
            "mix-50-50",
            "mix-90-10",
            "page-100",
            "point-local",
            "point-uniform",
            "scan"
        ]
    );
    let scan = reads
        .iter()
        .find(|r| r["metrics"]["workload"] == "scan" && r["codec"]["codec"] == "raw")
        .unwrap();
    assert_eq!(scan["metrics"]["blocks"], 400);
    assert_eq!(scan["metrics"]["body_bytes"], corpus.raw_bytes());

    let conc: Vec<_> = records
        .iter()
        .filter(|r| r["metrics"]["kind"] == "concurrent")
        .collect();
    assert_eq!(conc.len(), 4);
    assert_eq!(conc[0]["metrics"]["writer"]["blocks"], 200);
    assert!(conc[0]["metrics"]["reader"]["ops"].as_u64().unwrap() > 0);

    let rendered = report::render(&records);
    assert!(rendered.contains("## Writes"));
    assert!(rendered.contains("## Gates"));
    assert!(rendered.contains("## Reads against raw"));
    let gates = report::gates(&records);
    assert!(gates
        .iter()
        .any(|g| g.candidate == "zstd3-dict" && g.gated && g.workload == "write-7"));
    assert!(gates
        .iter()
        .any(|g| g.candidate == "zstd3-dict" && g.gated && g.workload == "append-query-7 writer"));
    assert!(gates
        .iter()
        .any(|g| g.candidate == "store" && g.gated && g.workload == "write-7"));
    assert!(gates
        .iter()
        .any(|g| g.candidate == "zstd3" && !g.gated && g.workload.starts_with("point-uniform")));
    assert!(gates.iter().all(|g| g.gated || !g.pass));
}

#[test]
fn evaluation_round_trips_and_ranks_the_dictionary() {
    let corpus = Corpus::synthetic(2, 300, 1);
    let fixtures = vec![Fixture {
        label: "synthetic".into(),
        corpus,
    }];
    let codecs = vec![
        ("zstd3".to_string(), Codec::zstd(3, None)),
        (
            "zstd3-dict".to_string(),
            Codec::zstd(3, Some(Dictionary::bundled())),
        ),
    ];
    let records = evaluate(&fixtures, &codecs).unwrap();
    assert_eq!(records.len(), 2);
    for r in &records {
        assert_eq!(r["metrics"]["blocks"], 300);
        assert!(r["metrics"]["ratio"].as_f64().unwrap() < 1.0);
    }
}

#[test]
fn write_outcome_carries_the_sink_segment_files() {
    use xtask::perf::storage::workloads::{write_corpus, WriteParams};

    let corpus = Corpus::synthetic(3, 400, 2);
    for codec in [Codec::Raw, Codec::Store] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("unrelated.txt"), "not a segment").unwrap();
        let outcome = write_corpus(
            &corpus,
            &codec,
            directory.path(),
            &WriteParams {
                name: "files".into(),
                batch: 100,
                encode_threads: 1,
                fsync: true,
            },
        )
        .unwrap();
        assert!(!outcome.files.is_empty());
        assert_eq!(outcome.metrics["segments"], outcome.files.len());
        assert!(outcome.files.iter().all(|file| file.is_file()
            && file
                .extension()
                .is_some_and(|extension| extension == "segment")));
    }
}
