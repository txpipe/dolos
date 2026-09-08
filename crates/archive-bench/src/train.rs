//! Dictionary training from a seeded sample of real segments, and the
//! evaluation that decides whether a dictionary earns its place.

use std::io;
use std::path::Path;

use dolos_flatfiles::compressed::Dictionary;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::codec::Codec;
use crate::corpus::{select, walk_bytes, Corpus};
use crate::measure::thread_cpu_ns;

#[derive(Debug, Clone)]
pub struct TrainSpec {
    pub corpus: std::path::PathBuf,
    /// Segments to sample and how many blocks from each.
    pub segments: Vec<(u32, usize)>,
    pub seed: u64,
    pub max_size: usize,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Train one dictionary; the provenance names every input so the same
/// command over the same files yields the same bytes.
pub fn train(spec: &TrainSpec) -> io::Result<(Dictionary, Value)> {
    let mut samples: Vec<Vec<u8>> = Vec::new();
    let mut sources = Vec::new();
    for &(segment, count) in &spec.segments {
        let path = spec.corpus.join(format!("{segment:06}.segment"));
        let data = std::fs::read(&path)?;
        let digest = hex(&Sha256::digest(&data));
        let blocks = walk_bytes(&data, None, &path.display().to_string())?;
        let picked = select(blocks.len(), count, spec.seed ^ (segment as u64) << 20);
        let mut sample_bytes = 0u64;
        let mut eras = std::collections::BTreeMap::new();
        for &i in &picked {
            sample_bytes += blocks[i].body.len() as u64;
            *eras.entry(blocks[i].era).or_insert(0usize) += 1;
            samples.push(blocks[i].body.clone());
        }
        eprintln!(
            "  segment {segment:06}: {} of {} blocks, {} bytes",
            picked.len(),
            blocks.len(),
            sample_bytes
        );
        sources.push(json!({
            "segment": segment,
            "file": path.file_name().map(|f| f.to_string_lossy().into_owned()),
            "file_bytes": data.len(),
            "sha256": digest,
            "population": blocks.len(),
            "samples": picked.len(),
            "sample_bytes": sample_bytes,
            "eras": eras,
        }));
    }
    if samples.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no samples selected",
        ));
    }
    let total: u64 = samples.iter().map(|s| s.len() as u64).sum();
    let trained = zstd::dict::from_samples(&samples, spec.max_size)?;
    let dictionary = Dictionary::new(trained);
    let provenance = json!({
        "trained_at": chrono::Utc::now().to_rfc3339(),
        "corpus": spec.corpus.display().to_string(),
        "segments": sources,
        "samples": samples.len(),
        "sample_bytes": total,
        "seed": spec.seed,
        "max_size": spec.max_size,
        "zstd": zstd::zstd_safe::version_string(),
        "trainer": "ZDICT_trainFromBuffer (zstd::dict::from_samples)",
        "dictionary": {
            "sha256": dictionary.id().to_string(),
            "bytes": dictionary.bytes().len(),
            "zstd_id": dictionary.zstd_id(),
        },
    });
    Ok((dictionary, provenance))
}

pub struct Fixture {
    pub label: String,
    pub corpus: Corpus,
}

/// Encode and decode every block of every fixture with every codec, in
/// memory and single-threaded, checking the round trip.
pub fn evaluate(fixtures: &[Fixture], codecs: &[(String, Codec)]) -> io::Result<Vec<Value>> {
    let mut records = Vec::new();
    for fixture in fixtures {
        for (label, codec) in codecs {
            let mut encoder = codec.encoder()?;
            let mut decoder = codec.decoder()?;
            let mut frames: Vec<Vec<u8>> = Vec::with_capacity(fixture.corpus.blocks.len());
            let mut per_era: std::collections::BTreeMap<&str, (u64, u64)> = Default::default();
            let cpu = thread_cpu_ns();
            for block in &fixture.corpus.blocks {
                let frame = encoder.encode(&block.body)?;
                let e = per_era.entry(block.era).or_default();
                e.0 += block.body.len() as u64;
                e.1 += frame.len() as u64;
                frames.push(frame.to_vec());
            }
            let encode_ns = thread_cpu_ns().saturating_sub(cpu);
            let cpu = thread_cpu_ns();
            for (block, frame) in fixture.corpus.blocks.iter().zip(&frames) {
                let body = decoder.decode(frame)?;
                if body != block.body.as_slice() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{}: a block did not round-trip through {}",
                            fixture.label,
                            codec.label()
                        ),
                    ));
                }
            }
            let decode_ns = thread_cpu_ns().saturating_sub(cpu);
            let raw = fixture.corpus.raw_bytes();
            let encoded: u64 = frames.iter().map(|f| f.len() as u64).sum();
            let blocks = frames.len() as u64;
            let eras: serde_json::Map<String, Value> = per_era
                .iter()
                .map(|(era, (r, e))| {
                    (
                        era.to_string(),
                        json!({ "raw_bytes": r, "encoded_bytes": e, "ratio": *e as f64 / (*r).max(1) as f64 }),
                    )
                })
                .collect();
            let metrics = json!({
                "kind": "evaluate",
                "fixture": fixture.label,
                "blocks": blocks,
                "raw_bytes": raw,
                "encoded_bytes": encoded,
                "ratio": encoded as f64 / raw.max(1) as f64,
                "encode_cpu_s": encode_ns as f64 / 1e9,
                "encode_mb_per_cpu_s": if encode_ns > 0 { raw as f64 / 1_048_576.0 / (encode_ns as f64 / 1e9) } else { 0.0 },
                "decode_cpu_s": decode_ns as f64 / 1e9,
                "decode_us_per_block": if blocks > 0 { decode_ns as f64 / 1e3 / blocks as f64 } else { 0.0 },
                "eras": eras,
            });
            eprintln!(
                "  {:<28} {:<12} ratio {:.3}  encode {:>6.1} MB/s  decode {:>5.1} us/block",
                fixture.label,
                label,
                metrics["ratio"].as_f64().unwrap_or(0.0),
                metrics["encode_mb_per_cpu_s"].as_f64().unwrap_or(0.0),
                metrics["decode_us_per_block"].as_f64().unwrap_or(0.0)
            );
            records.push(json!({
                "preset": "evaluate",
                "environment": crate::measure::environment(&[]),
                "corpus": fixture.corpus.json(),
                "codec": codec.json_as(label),
                "repeat": 0,
                "metrics": metrics,
            }));
        }
    }
    Ok(records)
}

/// Load a dictionary by path, or the one bundled with the flatfiles crate.
pub fn load_dictionary(spec: &str) -> io::Result<Dictionary> {
    if spec == "bundled" {
        return Ok(dolos_flatfiles::compressed::bundled_dictionary());
    }
    Ok(Dictionary::new(std::fs::read(Path::new(spec))?))
}
