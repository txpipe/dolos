//! The smoke preset over a synthetic corpus: every workload runs, every
//! body read back matches, and the records carry what a report needs.

use dolos_archive_bench::codec::Codec;
use dolos_archive_bench::corpus::Corpus;
use dolos_archive_bench::dictionary::Dictionary;
use dolos_archive_bench::presets::{run, Options, Preset};
use dolos_archive_bench::report;
use dolos_archive_bench::train::{evaluate, Fixture};
use dolos_archive_bench::workloads::{EvictOptions, Regime};

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
        write_batches: vec![1, 7],
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
    assert_eq!(writes.len(), 2 * 4);
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
    assert!(dict["metrics"]["encode_cpu_ms"].as_f64().unwrap() > 0.0);
    assert_eq!(dict["metrics"]["batch_latency"]["count"], 58);
    let store = writes
        .iter()
        .find(|w| w["codec"]["codec"] == "store" && w["metrics"]["batch"] == 7)
        .unwrap();
    assert!(store["metrics"]["ratio"].as_f64().unwrap() < 0.8);
    assert!(store["metrics"]["write_ms"].as_f64().unwrap() > 0.0);
    assert_eq!(store["codec"]["dictionary"], dict["codec"]["dictionary"]);

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
