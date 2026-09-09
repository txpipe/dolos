//! Block corpora: real segments, node immutable files, or a seeded
//! synthetic stand-in for smoke tests.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use dolos_flatfiles::BlockLocation;
use pallas::codec::minicbor::Decoder;
use pallas::ledger::traverse::MultiEraBlock;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct Block {
    pub slot: u64,
    pub era: &'static str,
    pub body: Vec<u8>,
}

impl Block {
    pub fn segment(&self) -> u32 {
        BlockLocation::segment_for_slot(self.slot)
    }
}

#[derive(Debug, Clone)]
pub struct Corpus {
    pub source: Value,
    pub blocks: Vec<Block>,
}

/// SplitMix64, so a seed names a sample or a synthetic corpus.
#[derive(Clone)]
pub struct Rng(pub u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n.max(1) as u64) as usize
    }

    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn era_name(block: &MultiEraBlock) -> &'static str {
    use pallas::ledger::traverse::Era;
    match block.era() {
        Era::Byron => "byron",
        Era::Shelley => "shelley",
        Era::Allegra => "allegra",
        Era::Mary => "mary",
        Era::Alonzo => "alonzo",
        Era::Babbage => "babbage",
        Era::Conway => "conway",
        _ => "other",
    }
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Walk one raw segment file as the CBOR items it is, decoding each with
/// pallas for its slot and era. `limit` stops after that many blocks.
pub fn walk_segment(path: &Path, limit: Option<usize>) -> io::Result<Vec<Block>> {
    let data = std::fs::read(path)?;
    walk_bytes(&data, limit, &path.display().to_string())
}

/// [`walk_segment`] over bytes already in memory; `label` names them in
/// errors.
pub fn walk_bytes(data: &[u8], limit: Option<usize>, label: &str) -> io::Result<Vec<Block>> {
    let mut blocks = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        if limit.is_some_and(|l| blocks.len() >= l) {
            break;
        }
        let mut decoder = Decoder::new(&data[pos..]);
        decoder.skip().map_err(|e| {
            invalid(format!(
                "{label}: the bytes at offset {pos} are not a CBOR item: {e}"
            ))
        })?;
        let len = decoder.position();
        if len == 0 {
            return Err(invalid(format!(
                "{label}: zero-length item at offset {pos}"
            )));
        }
        let item = &data[pos..pos + len];
        let decoded = MultiEraBlock::decode(item).map_err(|e| {
            invalid(format!(
                "{label}: the item at offset {pos} is not a block: {e}"
            ))
        })?;
        blocks.push(Block {
            slot: decoded.slot(),
            era: era_name(&decoded),
            body: item.to_vec(),
        });
        pos += len;
    }
    Ok(blocks)
}

impl Corpus {
    pub fn new(source: Value, blocks: Vec<Block>) -> Self {
        Self { source, blocks }
    }

    /// The listed segments of a raw store directory, in order, each capped
    /// at `limit` blocks.
    pub fn from_segments(dir: &Path, segments: &[u32], limit: Option<usize>) -> io::Result<Self> {
        let mut blocks = Vec::new();
        for &segment in segments {
            let path = dir.join(format!("{segment:06}.segment"));
            blocks.extend(walk_segment(&path, limit)?);
        }
        Ok(Self::new(
            json!({
                "kind": "segments",
                "dir": dir.display().to_string(),
                "segments": segments,
                "limit_per_segment": limit,
            }),
            blocks,
        ))
    }

    /// Blocks out of a Cardano node immutable directory, skipping `skip`
    /// then taking `take`, through pallas's immutable reader.
    pub fn from_immutable(dir: &Path, skip: usize, take: usize) -> io::Result<Self> {
        let iter = pallas::interop::hardano::storage::immutable::read_blocks(dir)
            .map_err(|e| invalid(format!("{}: {e:?}", dir.display())))?;
        let mut blocks = Vec::new();
        for item in iter.skip(skip).take(take) {
            let body = item.map_err(|e| invalid(format!("{}: {e:?}", dir.display())))?;
            let decoded = MultiEraBlock::decode(&body)
                .map_err(|e| invalid(format!("{}: not a block: {e}", dir.display())))?;
            blocks.push(Block {
                slot: decoded.slot(),
                era: era_name(&decoded),
                body: body.clone(),
            });
        }
        Ok(Self::new(
            json!({
                "kind": "immutable",
                "dir": dir.display().to_string(),
                "skip": skip,
                "take": take,
            }),
            blocks,
        ))
    }

    /// A seeded stand-in: `count` blocks of 300–2,500 bytes made of a fixed
    /// vocabulary plus a random tail, spread evenly over `segments`
    /// segments so batches cross segment boundaries.
    pub fn synthetic(seed: u64, count: usize, segments: u32) -> Self {
        let mut rng = Rng::new(7);
        let words: Vec<Vec<u8>> = (0..64)
            .map(|_| {
                let len = 4 + rng.below(9);
                (0..len).map(|_| b'a' + rng.below(26) as u8).collect()
            })
            .collect();
        let mut rng = Rng::new(seed);
        let span = dolos_flatfiles::SLOTS_PER_SEGMENT * segments.max(1) as u64;
        let stride = (span / count.max(1) as u64).max(1);
        let blocks = (0..count)
            .map(|i| {
                let target = 300 + rng.below(2200);
                let mut body = Vec::with_capacity(target + 48);
                while body.len() < target {
                    body.extend_from_slice(&words[rng.below(64)]);
                    body.push(b' ');
                }
                for _ in 0..32 {
                    body.push(rng.next_u64() as u8);
                }
                Block {
                    slot: i as u64 * stride,
                    era: "synthetic",
                    body,
                }
            })
            .collect();
        Self::new(
            json!({ "kind": "synthetic", "seed": seed, "count": count, "segments": segments }),
            blocks,
        )
    }

    pub fn raw_bytes(&self) -> u64 {
        self.blocks.iter().map(|b| b.body.len() as u64).sum()
    }

    pub fn segments(&self) -> Vec<u32> {
        let mut s: Vec<u32> = self.blocks.iter().map(Block::segment).collect();
        s.dedup();
        s.sort_unstable();
        s.dedup();
        s
    }

    /// Blocks and bytes per era.
    pub fn eras(&self) -> BTreeMap<&'static str, (usize, u64)> {
        let mut eras = BTreeMap::new();
        for b in &self.blocks {
            let e = eras.entry(b.era).or_insert((0usize, 0u64));
            e.0 += 1;
            e.1 += b.body.len() as u64;
        }
        eras
    }

    pub fn json(&self) -> Value {
        let max = self.blocks.iter().map(|b| b.body.len()).max().unwrap_or(0);
        let eras: Value = self
            .eras()
            .into_iter()
            .map(|(era, (blocks, bytes))| {
                (era.to_string(), json!({ "blocks": blocks, "bytes": bytes }))
            })
            .collect::<serde_json::Map<_, _>>()
            .into();
        json!({
            "source": self.source,
            "blocks": self.blocks.len(),
            "raw_bytes": self.raw_bytes(),
            "mean_block_bytes": if self.blocks.is_empty() { 0 } else { self.raw_bytes() / self.blocks.len() as u64 },
            "max_block_bytes": max,
            "segments": self.segments(),
            "eras": eras,
        })
    }
}

/// `count` distinct positions out of `population`, by a seeded partial
/// shuffle: the same seed over the same population picks the same sample.
pub fn select(population: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut rng = Rng::new(seed);
    let mut positions: Vec<usize> = (0..population).collect();
    let count = count.min(population);
    for i in 0..count {
        let j = i + rng.below(population - i);
        positions.swap(i, j);
    }
    positions.truncate(count);
    positions.sort_unstable();
    positions
}

/// Parse `5,30..33,100` into a sorted list of segment ids.
pub fn parse_segments(spec: &str) -> Result<Vec<u32>, String> {
    let mut out = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some((a, b)) = part.split_once("..") {
            let a: u32 = a.trim().parse().map_err(|e| format!("{part}: {e}"))?;
            let b: u32 = b.trim().parse().map_err(|e| format!("{part}: {e}"))?;
            if b < a {
                return Err(format!("{part}: range end before start"));
            }
            out.extend(a..=b);
        } else {
            out.push(part.parse().map_err(|e| format!("{part}: {e}"))?);
        }
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        return Err("no segments given".into());
    }
    Ok(out)
}
