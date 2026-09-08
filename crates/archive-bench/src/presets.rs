//! Fixed workload sets, so one command reruns the same measurements.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::codec::Codec;
use crate::corpus::Corpus;
use crate::workloads::{
    apply_regime, run_concurrent, run_reads, write_corpus, ConcurrentParams, EvictOptions, Mix,
    ReadParams, Regime, WriteParams,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Preset {
    /// Everything, at a size that finishes in seconds on a tiny corpus.
    Smoke,
    Write,
    Read,
    Mixed,
    Concurrent,
    All,
}

impl Preset {
    pub fn parse(s: &str) -> Result<Self, String> {
        Ok(match s {
            "smoke" => Preset::Smoke,
            "write" => Preset::Write,
            "read" => Preset::Read,
            "mixed" => Preset::Mixed,
            "concurrent" => Preset::Concurrent,
            "all" => Preset::All,
            other => return Err(format!("unknown preset {other:?}")),
        })
    }

    fn label(self) -> &'static str {
        match self {
            Preset::Smoke => "smoke",
            Preset::Write => "write",
            Preset::Read => "read",
            Preset::Mixed => "mixed",
            Preset::Concurrent => "concurrent",
            Preset::All => "all",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    pub work: PathBuf,
    pub codecs: Vec<(String, Codec)>,
    pub repeat: usize,
    pub threads: Vec<usize>,
    pub regimes: Vec<Regime>,
    pub evict: EvictOptions,
    /// Point operations per read workload; pages and mixes scale from it.
    pub ops: usize,
    pub seed: u64,
    pub encode_threads: usize,
    pub fsync: bool,
    pub keep: bool,
    pub verify: bool,
    pub write_batches: Vec<usize>,
}

struct Recorder<'a> {
    out: &'a mut dyn Write,
    environment: Value,
    corpus: Value,
    preset: &'static str,
}

impl Recorder<'_> {
    fn record(
        &mut self,
        codec: &Codec,
        repeat: usize,
        cache: Option<&Value>,
        metrics: Value,
    ) -> io::Result<()> {
        let record = json!({
            "preset": self.preset,
            "environment": self.environment,
            "corpus": self.corpus,
            "codec": codec.json(),
            "repeat": repeat,
            "cache": cache,
            "metrics": metrics,
        });
        serde_json::to_writer(&mut *self.out, &record)?;
        self.out.write_all(b"\n")?;
        self.out.flush()?;
        let name = metrics["workload"].as_str().unwrap_or("?");
        eprintln!(
            "  {} {:<24} {:<12} repeat {repeat}{}",
            self.preset,
            name,
            codec.label(),
            cache
                .and_then(|c| c["regime"].as_str())
                .map(|r| format!(" [{r}]"))
                .unwrap_or_default()
        );
        Ok(())
    }
}

fn clean(dir: &Path, keep: bool) -> io::Result<()> {
    if !keep && dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}

fn store_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "segment"))
        .collect();
    files.sort();
    Ok(files)
}

/// Run `preset` over `corpus`, appending one JSON record per measurement
/// to `out`.
pub fn run(preset: Preset, corpus: &Corpus, opts: &Options, out: &mut dyn Write) -> io::Result<()> {
    std::fs::create_dir_all(&opts.work)?;
    let mut rec = Recorder {
        out,
        environment: crate::measure::environment(&[&opts.work]),
        corpus: corpus.json(),
        preset: preset.label(),
    };
    eprintln!(
        "preset {} over {} blocks ({:.1} MiB) with codecs {:?}",
        preset.label(),
        corpus.blocks.len(),
        corpus.raw_bytes() as f64 / 1_048_576.0,
        opts.codecs
            .iter()
            .map(|(l, _)| l.as_str())
            .collect::<Vec<_>>()
    );

    let (writes, reads, mixed, concurrent) = match preset {
        Preset::Smoke | Preset::All => (true, true, true, true),
        Preset::Write => (true, false, false, false),
        Preset::Read => (false, true, false, false),
        Preset::Mixed => (false, false, true, false),
        Preset::Concurrent => (false, false, false, true),
    };

    if writes {
        for repeat in 0..opts.repeat.max(1) {
            for &batch in &opts.write_batches {
                for (label, codec) in &opts.codecs {
                    let dir = opts.work.join(label).join(format!("write-{batch}"));
                    clean(&dir, false)?;
                    let outcome = write_corpus(
                        corpus,
                        codec,
                        &dir,
                        &WriteParams {
                            name: format!("write-{batch}"),
                            batch,
                            encode_threads: opts.encode_threads,
                            fsync: opts.fsync,
                        },
                    )?;
                    rec.record(codec, repeat, None, outcome.metrics)?;
                    clean(&dir, opts.keep)?;
                }
            }
        }
    }

    if reads || mixed {
        let max_threads = opts.threads.iter().copied().max().unwrap_or(1);
        for repeat in 0..opts.repeat.max(1) {
            for (label, codec) in &opts.codecs {
                let dir = opts.work.join(label).join("store");
                clean(&dir, false)?;
                let outcome = write_corpus(
                    corpus,
                    codec,
                    &dir,
                    &WriteParams {
                        name: "store".into(),
                        batch: 100,
                        encode_threads: opts.encode_threads,
                        fsync: opts.fsync,
                    },
                )?;
                let locations = outcome.locations;
                let files = store_files(&dir)?;
                for &regime in &opts.regimes {
                    let Some(cache) = apply_regime(&files, regime, &opts.evict)? else {
                        eprintln!(
                            "  skipping regime {}: no eviction method on this host (pass --evict-from)",
                            regime.label()
                        );
                        continue;
                    };
                    let mut workloads: Vec<ReadParams> = Vec::new();
                    let base = |name: &str, mix: Mix, ops: usize, threads: usize| ReadParams {
                        name: name.to_string(),
                        mix,
                        ops,
                        threads,
                        seed: opts.seed,
                        regime,
                        scan: false,
                        verify: opts.verify,
                    };
                    if reads {
                        for &threads in &opts.threads {
                            workloads.push(base("point-uniform", Mix::points(), opts.ops, threads));
                            workloads.push(base(
                                "point-local",
                                Mix {
                                    locality: 0.8,
                                    ..Mix::points()
                                },
                                opts.ops,
                                threads,
                            ));
                            workloads.push(base(
                                "page-100",
                                Mix {
                                    point_share: 0.0,
                                    ..Mix::points()
                                },
                                (opts.ops / 20).max(1),
                                threads,
                            ));
                        }
                        let mut scan = base("scan", Mix::points(), 0, 1);
                        scan.scan = true;
                        workloads.push(scan);
                    }
                    if mixed {
                        for (name, share) in
                            [("mix-90-10", 0.9), ("mix-50-50", 0.5), ("mix-10-90", 0.1)]
                        {
                            workloads.push(base(
                                name,
                                Mix {
                                    point_share: share,
                                    locality: 0.5,
                                    ..Mix::points()
                                },
                                opts.ops,
                                max_threads,
                            ));
                        }
                    }
                    for params in &workloads {
                        // A cold regime is consumed by the first workload
                        // that touches the pages; restore it for each.
                        if regime != Regime::Warm && params.name != workloads[0].name {
                            apply_regime(&files, regime, &opts.evict)?;
                        }
                        let metrics = run_reads(&dir, codec, &locations, Some(corpus), params)?;
                        rec.record(codec, repeat, Some(&cache), metrics)?;
                    }
                }
                clean(&dir, opts.keep)?;
            }
        }
    }

    if concurrent {
        let readers = opts.threads.iter().copied().max().unwrap_or(1);
        let batches: Vec<usize> = if preset == Preset::Smoke {
            vec![7]
        } else {
            vec![1, 100]
        };
        for repeat in 0..opts.repeat.max(1) {
            for &batch in &batches {
                for (label, codec) in &opts.codecs {
                    let dir = opts.work.join(label).join(format!("concurrent-{batch}"));
                    clean(&dir, false)?;
                    let metrics = run_concurrent(
                        corpus,
                        codec,
                        &dir,
                        &ConcurrentParams {
                            name: format!("append-query-{batch}"),
                            split: 0.5,
                            batch,
                            encode_threads: opts.encode_threads,
                            fsync: opts.fsync,
                            readers,
                            mix: Mix {
                                point_share: 0.9,
                                locality: 0.5,
                                ..Mix::points()
                            },
                            seed: opts.seed,
                        },
                    )?;
                    rec.record(codec, repeat, None, metrics)?;
                    clean(&dir, opts.keep)?;
                }
            }
        }
    }
    Ok(())
}
