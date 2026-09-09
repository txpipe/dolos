use std::io::Write;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use dolos_archive_bench::codec::Codec;
use dolos_archive_bench::corpus::{parse_segments, Corpus};
use dolos_archive_bench::measure::PeakAlloc;
use dolos_archive_bench::presets::{self, Options, Preset};
use dolos_archive_bench::report;
use dolos_archive_bench::train::{self, Fixture, TrainSpec};
use dolos_archive_bench::workloads::{EvictOptions, Regime};

#[global_allocator]
static ALLOC: PeakAlloc = PeakAlloc;

#[derive(Parser)]
#[command(
    name = "dolos-archive-bench",
    about = "Benchmarks for per-block compressed archive segments"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Args)]
struct CorpusArgs {
    /// directory of raw `NNNNNN.segment` files
    #[arg(long, conflicts_with_all = ["immutable", "synthetic"])]
    corpus: Option<PathBuf>,

    /// segments to load from --corpus, as `446..449` or `5,30,100`
    #[arg(long, requires = "corpus")]
    segments: Option<String>,

    /// a Cardano node immutable directory instead of segments
    #[arg(long, conflicts_with = "synthetic")]
    immutable: Option<PathBuf>,

    /// blocks to skip at the start of --immutable
    #[arg(long, default_value_t = 0)]
    skip: usize,

    /// a seeded synthetic corpus of this many blocks
    #[arg(long)]
    synthetic: Option<usize>,

    /// most blocks per segment (or in total for --immutable)
    #[arg(long)]
    limit_blocks: Option<usize>,
}

impl CorpusArgs {
    fn load(&self, seed: u64) -> anyhow::Result<Corpus> {
        if let Some(count) = self.synthetic {
            return Ok(Corpus::synthetic(seed, count, 2));
        }
        if let Some(dir) = &self.immutable {
            return Ok(Corpus::from_immutable(
                dir,
                self.skip,
                self.limit_blocks.unwrap_or(20_000),
            )?);
        }
        let dir = self.corpus.as_ref().ok_or_else(|| {
            anyhow::anyhow!("one of --corpus, --immutable or --synthetic is required")
        })?;
        let segments = self
            .segments
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--segments is required with --corpus"))?;
        let segments = parse_segments(segments).map_err(anyhow::Error::msg)?;
        Ok(Corpus::from_segments(dir, &segments, self.limit_blocks)?)
    }
}

/// Options of the `bench` subcommand.
#[derive(clap::Args)]
struct BenchArgs {
    #[command(flatten)]
    corpus: CorpusArgs,

    /// smoke, write, read, mixed, concurrent or all
    #[arg(long, default_value = "all")]
    preset: String,

    /// scratch directory for the stores under test (on the target disk)
    #[arg(long)]
    work: PathBuf,

    /// results file, one JSON record per line
    #[arg(long)]
    out: PathBuf,

    /// codecs to pair: raw, zstd3, zstd3-dict, store
    #[arg(long, default_value = "raw,zstd3,zstd3-dict,store")]
    codecs: String,

    /// dictionary for the -dict codec: `bundled` or a file
    #[arg(long, default_value = "bundled")]
    dictionary: String,

    /// paired repeats of every measurement
    #[arg(long, default_value_t = 1)]
    repeat: usize,

    /// reader thread counts to measure
    #[arg(long, default_value = "1,8")]
    threads: String,

    /// cache regimes for read workloads: warm, evict, nocache
    #[arg(long, default_value = "warm")]
    cache: String,

    /// directory to stream through the page cache where eviction has
    /// no direct method (macOS)
    #[arg(long)]
    evict_from: Option<PathBuf>,

    /// bytes to stream for --evict-from, in GiB
    #[arg(long, default_value_t = 24)]
    evict_gib: u64,

    /// point operations per read workload
    #[arg(long, default_value_t = 20_000)]
    ops: usize,

    /// write batch sizes, in blocks
    #[arg(long, default_value = "1,100,500")]
    write_batches: String,

    /// encoder threads per batch (1 is the foreground design)
    #[arg(long, default_value_t = 1)]
    encode_threads: usize,

    /// skip the per-batch fdatasync (never for gate runs)
    #[arg(long)]
    no_fsync: bool,

    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// leave the written stores in --work
    #[arg(long)]
    keep: bool,

    /// compare every body read with the corpus
    #[arg(long)]
    verify: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Run a preset and append JSON records to --out
    Bench(Box<BenchArgs>),

    /// Train a dictionary on a seeded sample of segments
    Train {
        /// directory of raw `NNNNNN.segment` files
        #[arg(long)]
        corpus: PathBuf,

        /// segments to sample, as `446..449` or `5,30,100`
        #[arg(long, required_unless_present = "sample")]
        segments: Option<String>,

        /// blocks to sample from each of --segments
        #[arg(long, default_value_t = 1_500)]
        samples_per_segment: usize,

        /// a weighted group, `SEGMENTS=COUNT` (e.g. `440..447=1500`);
        /// repeatable, in addition to or instead of --segments
        #[arg(long = "sample")]
        sample: Vec<String>,

        #[arg(long, default_value_t = 0)]
        seed: u64,

        /// largest dictionary to produce, in bytes
        #[arg(long, default_value_t = 112_640)]
        max_size: usize,

        /// where to write the dictionary; provenance goes to `<out>.json`
        #[arg(long)]
        out: PathBuf,
    },

    /// Encode and decode fixtures with and without a dictionary
    Evaluate {
        /// `label=DIR:SEGMENTS` or `label=immutable:DIR[:SKIP]`, repeatable
        #[arg(long = "fixture", required = true)]
        fixtures: Vec<String>,

        /// dictionaries to evaluate: `bundled` or files, repeatable
        #[arg(long = "dictionary", default_value = "bundled")]
        dictionaries: Vec<String>,

        /// most blocks per segment or per immutable fixture
        #[arg(long, default_value_t = 20_000)]
        limit_blocks: usize,

        /// results file, one JSON record per line
        #[arg(long)]
        out: Option<PathBuf>,
    },

    /// Render markdown tables and gate verdicts from result files
    Report { files: Vec<PathBuf> },
}

fn parse_list<T: std::str::FromStr>(s: &str) -> anyhow::Result<Vec<T>>
where
    T::Err: std::fmt::Display,
{
    s.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|p| p.parse::<T>().map_err(|e| anyhow::anyhow!("{p}: {e}")))
        .collect()
}

fn codecs(spec: &str, dictionary: &str) -> anyhow::Result<Vec<(String, Codec)>> {
    let mut out = Vec::new();
    for name in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let codec = match name {
            "raw" => Codec::Raw,
            "zstd3" => Codec::zstd(3, None),
            "zstd3-dict" => Codec::zstd(3, Some(train::load_dictionary(dictionary)?)),
            "store" => Codec::Store,
            other => anyhow::bail!("unknown codec {other:?}; use raw, zstd3, zstd3-dict or store"),
        };
        out.push((name.to_string(), codec));
    }
    Ok(out)
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Bench(args) => {
            let BenchArgs {
                corpus,
                preset,
                work,
                out,
                codecs: codec_spec,
                dictionary,
                repeat,
                threads,
                cache,
                evict_from,
                evict_gib,
                ops,
                write_batches,
                encode_threads,
                no_fsync,
                seed,
                keep,
                verify,
            } = *args;
            let preset = Preset::parse(&preset).map_err(anyhow::Error::msg)?;
            let corpus = corpus.load(seed)?;
            let mut opts = Options {
                work,
                codecs: codecs(&codec_spec, &dictionary)?,
                repeat,
                threads: parse_list(&threads)?,
                regimes: cache
                    .split(',')
                    .map(|r| Regime::parse(r.trim()).map_err(anyhow::Error::msg))
                    .collect::<anyhow::Result<_>>()?,
                evict: EvictOptions {
                    from: evict_from,
                    bytes: evict_gib << 30,
                },
                ops,
                seed,
                encode_threads,
                fsync: !no_fsync,
                keep,
                verify,
                write_batches: parse_list(&write_batches)?,
            };
            if preset == Preset::Smoke {
                opts.ops = opts.ops.min(200);
                opts.write_batches = vec![1, 7];
            }
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&out)?;
            presets::run(preset, &corpus, &opts, &mut file)?;
            eprintln!("results appended to {}", out.display());
        }
        Command::Train {
            corpus,
            segments,
            samples_per_segment,
            sample,
            seed,
            max_size,
            out,
        } => {
            let mut groups: Vec<(u32, usize)> = Vec::new();
            if let Some(segments) = &segments {
                for s in parse_segments(segments).map_err(anyhow::Error::msg)? {
                    groups.push((s, samples_per_segment));
                }
            }
            for group in &sample {
                let (segments, count) = group
                    .split_once('=')
                    .ok_or_else(|| anyhow::anyhow!("--sample {group:?} is not SEGMENTS=COUNT"))?;
                let count: usize = count.trim().parse()?;
                for s in parse_segments(segments).map_err(anyhow::Error::msg)? {
                    groups.push((s, count));
                }
            }
            groups.sort_unstable();
            groups.dedup_by_key(|g| g.0);
            let spec = TrainSpec {
                corpus,
                segments: groups,
                seed,
                max_size,
            };
            let (dictionary, mut provenance) = train::train(&spec)?;
            provenance["command"] = serde_json::Value::String(
                std::env::args()
                    .map(|arg| shell_word(&arg))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            std::fs::write(&out, dictionary.bytes())?;
            let mut prov_path = out.clone().into_os_string();
            prov_path.push(".json");
            std::fs::write(&prov_path, serde_json::to_string_pretty(&provenance)?)?;
            println!(
                "dictionary {} ({} bytes) written to {}",
                dictionary.id(),
                dictionary.bytes().len(),
                out.display()
            );
        }
        Command::Evaluate {
            fixtures,
            dictionaries,
            limit_blocks,
            out,
        } => {
            let mut loaded = Vec::new();
            for spec in &fixtures {
                let (label, rest) = spec
                    .split_once('=')
                    .ok_or_else(|| anyhow::anyhow!("fixture {spec:?} is not label=DIR:SEGMENTS"))?;
                let corpus = if let Some(rest) = rest.strip_prefix("immutable:") {
                    let mut parts = rest.rsplitn(2, ':');
                    let (dir, skip) = match (parts.next(), parts.next()) {
                        (Some(skip), Some(dir)) if skip.parse::<usize>().is_ok() => {
                            (dir, skip.parse()?)
                        }
                        _ => (rest, 0),
                    };
                    Corpus::from_immutable(std::path::Path::new(dir), skip, limit_blocks)?
                } else {
                    let (dir, segments) = rest.rsplit_once(':').ok_or_else(|| {
                        anyhow::anyhow!("fixture {spec:?} is not label=DIR:SEGMENTS")
                    })?;
                    let segments = parse_segments(segments).map_err(anyhow::Error::msg)?;
                    Corpus::from_segments(std::path::Path::new(dir), &segments, Some(limit_blocks))?
                };
                eprintln!(
                    "fixture {label}: {} blocks, {:.1} MiB",
                    corpus.blocks.len(),
                    corpus.raw_bytes() as f64 / 1_048_576.0
                );
                loaded.push(Fixture {
                    label: label.to_string(),
                    corpus,
                });
            }
            let mut codec_list = vec![("zstd3".to_string(), Codec::zstd(3, None))];
            for d in &dictionaries {
                let dictionary = train::load_dictionary(d)?;
                let label = if d == "bundled" {
                    "zstd3-dict".to_string()
                } else {
                    format!(
                        "zstd3-dict:{}",
                        std::path::Path::new(d)
                            .file_stem()
                            .map(|s| s.to_string_lossy().into_owned())
                            .unwrap_or_default()
                    )
                };
                codec_list.push((label, Codec::zstd(3, Some(dictionary))));
            }
            let records = train::evaluate(&loaded, &codec_list)?;
            if let Some(out) = out {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&out)?;
                for r in &records {
                    serde_json::to_writer(&mut file, r)?;
                    file.write_all(b"\n")?;
                }
            }
            print!("{}", report::render(&records));
        }
        Command::Report { files } => {
            let records = report::load(&files)?;
            print!("{}", report::render(&records));
        }
    }
    Ok(())
}

/// `arg` as a shell would need it typed, so a recorded command replays.
fn shell_word(arg: &str) -> String {
    let plain = |c: char| c.is_ascii_alphanumeric() || "-_./=,:@+%".contains(c);
    if !arg.is_empty() && arg.chars().all(plain) {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}
