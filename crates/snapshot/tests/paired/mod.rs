//! A stele paired with a blocks repository, as decision 0080 lays the two
//! out, copied from an ordinary export.
//!
//! Export still lists each closed epoch's `blocks` layer in the stele's own
//! chain. [`pair`] copies such a chain twice: those layers' blocks into a
//! lineage of the `io.txpipe.cardano.blocks` profile, written with its plain
//! writer, and the stele's chain without them, each inscription naming the
//! blocks snapshot at its sequence in `position.blocks`. Every other layer is
//! copied record for record, so its `diffId` does not move, and the tip's
//! boundary block stays in the stele.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it.
#![allow(dead_code)]

use std::{collections::BTreeMap, fs::File, path::Path};

use dolos_snapshot::{restore::default_budget, DolosProfile, BLOCKS, COMPRESSION_LEVEL};
use serde_json::json;
use stelae::{
    dir::SnapshotDir,
    inscription::{Inscription, LayerDescriptor, Link},
    transport::BlobIndex,
    CanonicalCbor, Digest, LayerReader, LayerSpec, Profile, RecordSink, SnapshotReader,
    SnapshotWriter,
};
use stelae_blocks::{BlockRecord, BlocksProfile, EpochScope};

/// The inscription digests of a pair, by sequence.
#[derive(Debug, Clone)]
pub struct Paired {
    pub blocks: BTreeMap<u64, Digest>,
    pub dolos: BTreeMap<u64, Digest>,
}

impl Paired {
    /// The paired stele's head: its sequence and digest.
    pub fn head(&self) -> (u64, Digest) {
        let (sequence, digest) = self.dolos.last_key_value().unwrap();

        (*sequence, *digest)
    }
}

/// Pair the stele at `source`'s `latest`: its closed epochs' blocks into
/// `blocks` as a lineage from sequence 1, and its chain without them into
/// `dolos`.
pub fn pair<D: SnapshotWriter, B: SnapshotWriter>(source: &Path, dolos: &D, blocks: &B) -> Paired {
    let blocks = blocks_lineage(source, blocks, 1);
    let dolos = without_blocks(source, dolos, &blocks);

    Paired { blocks, dolos }
}

/// The closed epochs' blocks of the stele at `source`'s `latest`, sealed into
/// `writer` as a blocks lineage from sequence `first` up to the stele's own.
///
/// The snapshot at `first` lists every epoch before its cut and links to
/// nothing; each later one adds the epoch its cut closes.
pub fn blocks_lineage<W: SnapshotWriter>(
    source: &Path,
    writer: &W,
    first: u64,
) -> BTreeMap<u64, Digest> {
    let layout = SnapshotDir::open(source).unwrap();
    let stele = layout.resolve_latest(&DolosProfile).unwrap();
    let chain = stele.chain().unwrap();
    let index = stele.blob_index().unwrap();
    let sequence = chain.head().sequence;
    let magic = chain.head().position["network"]["magic"].as_u64().unwrap();

    let closed: BTreeMap<u64, &LayerDescriptor> = chain
        .descriptors()
        .filter(|layer| layer.kind == BLOCKS)
        .map(|layer| (layer.scope["epoch"].as_u64().unwrap(), layer))
        .filter(|(epoch, _)| *epoch < sequence)
        .collect();

    let mut heads = BTreeMap::new();
    let mut previous = None;

    for at in first..=sequence {
        let epochs = if at == first { 0..at } else { at - 1..at };

        let layers: Vec<LayerDescriptor> = epochs
            .map(|epoch| {
                let layer = closed[&epoch];
                let scope = EpochScope::read_descriptor(magic, &layer.scope).unwrap();
                let blocks = read_blocks(&layout, &index, layer);

                stelae_blocks::write_layer(writer, scope, stelae_blocks::COMPRESSION_LEVEL, blocks)
                    .unwrap()
                    .descriptor
            })
            .collect();

        let mut inscription = Inscription::new(
            &BlocksProfile,
            at,
            stelae_blocks::position(&stelae_blocks::Position {
                network_magic: magic,
                epoch: at,
            }),
            stelae_blocks::parameters(),
            stelae_blocks::compression(),
        );
        inscription.previous = previous;
        stelae_driver::place_layers(&BlocksProfile, &mut inscription, layers).unwrap();

        let digest = writer.seal(&BlocksProfile, &inscription).unwrap();

        previous = Some(Link {
            sequence: at,
            inscription_digest: digest,
        });
        heads.insert(at, digest);
    }

    heads
}

/// The stele chain at `source`'s `latest`, sealed into `writer` without its
/// closed epochs' `blocks` layers. Each inscription names `blocks` at its own
/// sequence in `position.blocks` and links to the copy of its predecessor.
pub fn without_blocks<W: SnapshotWriter>(
    source: &Path,
    writer: &W,
    blocks: &BTreeMap<u64, Digest>,
) -> BTreeMap<u64, Digest> {
    let layout = SnapshotDir::open(source).unwrap();
    let stele = layout.resolve_latest(&DolosProfile).unwrap();

    let mut heads = BTreeMap::new();
    let mut previous = None;

    for original in stele.chain().unwrap().inscriptions() {
        let own = layout
            .pull_sequence(&DolosProfile, original.sequence)
            .unwrap();
        let index = own.blob_index().unwrap();

        let mut inscription = original.clone();
        inscription.diff.retain(|layer| layer.kind != BLOCKS);
        inscription.position["blocks"] = json!(blocks[&inscription.sequence].to_string());
        inscription.previous = previous;

        for layer in inscription.diff.iter().chain(&inscription.tip) {
            copy_layer(&layout, &index, layer, writer);
        }

        let digest = writer.seal(&DolosProfile, &inscription).unwrap();

        previous = Some(Link {
            sequence: inscription.sequence,
            inscription_digest: digest,
        });
        heads.insert(inscription.sequence, digest);
    }

    heads
}

/// The blocks of one of the stele's own `blocks` layers. The two profiles'
/// records share one codec: slot, hash and body.
fn read_blocks(
    layout: &SnapshotDir,
    index: &BlobIndex,
    layer: &LayerDescriptor,
) -> Vec<BlockRecord> {
    let mut reader = open_layer(layout, index, &DolosProfile, layer);
    let mut blocks = Vec::new();

    while let Some(record) = reader.next_record() {
        blocks.push(stelae_blocks::record::decode(record.unwrap()).unwrap());
    }

    reader.finish().unwrap();

    blocks
}

/// Write `layer` into `writer` record for record, as `layout` holds it.
fn copy_layer<W: SnapshotWriter>(
    layout: &SnapshotDir,
    index: &BlobIndex,
    layer: &LayerDescriptor,
    writer: &W,
) {
    let mut reader = open_layer(layout, index, &DolosProfile, layer);

    let spec = LayerSpec::new(
        layer.kind.clone(),
        reader.header().scope.clone(),
        layer.scope.clone(),
    );
    let mut sink = writer
        .layer_sink(&DolosProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    while let Some(record) = reader.next_record() {
        let record = CanonicalCbor::new(record.unwrap().to_vec()).unwrap();
        sink.write_record(&record).unwrap();
    }

    reader.finish().unwrap();

    let copied = sink.finish().unwrap().descriptor;
    assert_eq!(&copied, layer, "a copied layer changed identity");
}

fn open_layer(
    layout: &SnapshotDir,
    index: &BlobIndex,
    profile: &dyn Profile,
    layer: &LayerDescriptor,
) -> LayerReader<File> {
    let blob = index.blob_for(&layer.diff_id).unwrap();
    let file = File::open(layout.blob_path(&blob)).unwrap();

    LayerReader::new(file, profile, layer, default_budget().limits).unwrap()
}
