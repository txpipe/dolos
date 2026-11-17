//! A snapshot paired with a blocks repository, as decision 0080 lays the two
//! out, copied from an export into a network root.
//!
//! An export writes the closed epochs' blocks into the root's `blocks`
//! lineage and the snapshot, naming it, into its `dolos` one. [`pair`] copies
//! the two into other writers, a registry above all: the blocks into a lineage
//! of the `io.txpipe.cardano.blocks` profile, written with its plain writer
//! from any sequence, and the snapshot's chain re-pointed at it, each
//! inscription naming the blocks snapshot at its sequence in `position.blocks`.
//! Every snapshot layer is copied record for record, so its `diffId` does not
//! move, and the tip's boundary block stays in the snapshot.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it.
#![allow(dead_code)]

use std::{collections::BTreeMap, fs::File, path::Path};

use dolos_snapshot::{
    export::{BLOCKS_REPOSITORY, DOLOS_REPOSITORY},
    restore::default_budget,
    DolosProfile, EpochScope as DolosScope, Scope as _, BLOCKS, COMPRESSION_LEVEL,
};
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
    /// The paired snapshot's head: its sequence and digest.
    pub fn head(&self) -> (u64, Digest) {
        let (sequence, digest) = self.dolos.last_key_value().unwrap();

        (*sequence, *digest)
    }
}

/// The digests of the pair an export wrote into the network root at
/// `source`, by sequence.
pub fn digests(source: &Path) -> Paired {
    let heads = |repository: &str, profile: &dyn Profile| -> BTreeMap<u64, Digest> {
        let layout = SnapshotDir::open(source.join(repository)).unwrap();
        let latest = layout.resolve_latest(profile).unwrap();

        latest
            .chain()
            .unwrap()
            .inscriptions()
            .iter()
            .map(|inscription| (inscription.sequence, inscription.digest().unwrap()))
            .collect()
    };

    Paired {
        blocks: heads(BLOCKS_REPOSITORY, &BlocksProfile),
        dolos: heads(DOLOS_REPOSITORY, &DolosProfile),
    }
}

/// Pair the network root at `source`: the closed epochs' blocks of its
/// `latest` into `blocks` as a lineage from sequence 1, and its snapshot chain,
/// naming them, into `dolos`.
pub fn pair<D: SnapshotWriter, B: SnapshotWriter>(source: &Path, dolos: &D, blocks: &B) -> Paired {
    pair_tampered(source, dolos, blocks, None)
}

/// [`pair`], with the blocks layer of the epoch `tampered` names written past
/// the plain writer's checks ([`Tamper`]).
pub fn pair_tampered<D: SnapshotWriter, B: SnapshotWriter>(
    source: &Path,
    dolos: &D,
    blocks: &B,
    tampered: Option<(u64, Tamper)>,
) -> Paired {
    let blocks = lineage(source, blocks, 1, tampered);
    let dolos = without_blocks(source, dolos, &blocks);

    Paired { blocks, dolos }
}

/// A blocks layer that breaks the profile's reader rules, as a careless
/// publisher could write it by skipping `BlocksWriter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tamper {
    /// The header's end slot is one past the descriptor's.
    HeaderScope,
    /// The header names the next epoch, over the descriptor's slots.
    HeaderEpoch,
    /// The last block's slot is one past the epoch's end.
    OutsideEpoch,
    /// The first block's slot is the one before the epoch's start.
    BeforeEpoch,
    /// The blocks run in descending slot.
    Backwards,
}

/// The closed epochs' blocks of the network root at `source`'s `latest`,
/// sealed into `writer` as a blocks lineage from sequence `first` up to the
/// root's own.
///
/// The snapshot at `first` lists every epoch before its cut and links to
/// nothing; each later one adds the epoch its cut closes.
pub fn blocks_lineage<W: SnapshotWriter>(
    source: &Path,
    writer: &W,
    first: u64,
) -> BTreeMap<u64, Digest> {
    lineage(source, writer, first, None)
}

fn lineage<W: SnapshotWriter>(
    source: &Path,
    writer: &W,
    first: u64,
    tampered: Option<(u64, Tamper)>,
) -> BTreeMap<u64, Digest> {
    let layout = SnapshotDir::open(source.join(BLOCKS_REPOSITORY)).unwrap();
    let snapshot = layout.resolve_latest(&BlocksProfile).unwrap();
    let chain = snapshot.chain().unwrap();
    let index = snapshot.blob_index().unwrap();
    let sequence = chain.head().sequence;
    let magic = stelae_blocks::read_position(&chain.head().position)
        .unwrap()
        .network_magic;

    let closed: BTreeMap<u64, &LayerDescriptor> = chain
        .descriptors()
        .map(|layer| (layer.scope["epoch"].as_u64().unwrap(), layer))
        .collect();
    assert_eq!(closed.len() as u64, sequence, "every epoch before the cut");

    let mut heads = BTreeMap::new();
    let mut previous = None;

    for at in first..=sequence {
        let epochs = if at == first { 0..at } else { at - 1..at };

        let layers: Vec<LayerDescriptor> = epochs
            .map(|epoch| {
                let layer = closed[&epoch];
                let scope = EpochScope::read_descriptor(magic, &layer.scope).unwrap();
                let blocks = read_blocks(&layout, &index, layer);

                match tampered {
                    Some((at, tamper)) if at == epoch => {
                        write_tampered(writer, scope, blocks, tamper)
                    }
                    _ => {
                        stelae_blocks::write_layer(
                            writer,
                            scope,
                            stelae_blocks::COMPRESSION_LEVEL,
                            blocks,
                        )
                        .unwrap()
                        .descriptor
                    }
                }
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

/// The snapshot chain of the network root at `source`'s `latest`, sealed into
/// `writer`. Each inscription names `blocks` at its own sequence in
/// `position.blocks` and links to the copy of its predecessor.
pub fn without_blocks<W: SnapshotWriter>(
    source: &Path,
    writer: &W,
    blocks: &BTreeMap<u64, Digest>,
) -> BTreeMap<u64, Digest> {
    relinked(source, writer, blocks, None)
}

/// [`without_blocks`], with the inscription at `epoch + 1` also listing a
/// `blocks` layer of this profile for the epoch it closes, as the snapshot did
/// before decision 0080.
pub fn listing_closed_blocks<W: SnapshotWriter>(
    source: &Path,
    writer: &W,
    epoch: u64,
) -> BTreeMap<u64, Digest> {
    relinked(source, writer, &digests(source).blocks, Some(epoch))
}

fn relinked<W: SnapshotWriter>(
    source: &Path,
    writer: &W,
    blocks: &BTreeMap<u64, Digest>,
    closed: Option<u64>,
) -> BTreeMap<u64, Digest> {
    let layout = SnapshotDir::open(source.join(DOLOS_REPOSITORY)).unwrap();
    let snapshot = layout.resolve_latest(&DolosProfile).unwrap();

    let mut heads = BTreeMap::new();
    let mut previous = None;

    for original in snapshot.chain().unwrap().inscriptions() {
        let own = layout
            .pull_sequence(&DolosProfile, original.sequence)
            .unwrap();
        let index = own.blob_index().unwrap();

        let mut inscription = original.clone();
        inscription.position["blocks"] = json!(blocks[&inscription.sequence].to_string());
        inscription.previous = previous;

        if let Some(epoch) = closed.filter(|epoch| epoch + 1 == inscription.sequence) {
            inscription
                .diff
                .insert(0, closed_blocks(source, writer, epoch));
        }

        for layer in inscription.diff.iter().chain(&inscription.tip) {
            if layer.kind != BLOCKS || layer.scope["epoch"] == inscription.sequence {
                copy_layer(&layout, &index, layer, writer);
            }
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

/// Epoch `epoch`'s blocks from the root at `source`'s blocks lineage, written
/// into `writer` as a `blocks` layer of this profile.
fn closed_blocks<W: SnapshotWriter>(source: &Path, writer: &W, epoch: u64) -> LayerDescriptor {
    let layout = SnapshotDir::open(source.join(BLOCKS_REPOSITORY)).unwrap();
    let snapshot = layout.resolve_latest(&BlocksProfile).unwrap();
    let index = snapshot.blob_index().unwrap();
    let chain = snapshot.chain().unwrap();
    let magic = stelae_blocks::read_position(&chain.head().position)
        .unwrap()
        .network_magic;

    let layer = chain
        .descriptors()
        .find(|layer| layer.scope["epoch"] == epoch)
        .unwrap();
    let scope = EpochScope::read_descriptor(magic, &layer.scope).unwrap();
    let records: Vec<CanonicalCbor> = read_blocks(&layout, &index, layer)
        .iter()
        .map(|block| stelae_blocks::record::encode(block).unwrap())
        .collect();

    let scope = DolosScope {
        network_magic: magic,
        epoch,
        start_slot: scope.start_slot,
        end_slot: scope.end_slot,
    };

    writer
        .write_layer(
            &DolosProfile,
            &scope.layer_spec(BLOCKS).unwrap(),
            COMPRESSION_LEVEL,
            &records,
        )
        .unwrap()
        .descriptor
}

/// `blocks` as `scope`'s layer, broken as `tamper` says, through a raw sink
/// that checks nothing a blocks reader does.
fn write_tampered<W: SnapshotWriter>(
    writer: &W,
    scope: EpochScope,
    mut blocks: Vec<BlockRecord>,
    tamper: Tamper,
) -> LayerDescriptor {
    let mut header = scope;

    assert!(
        blocks.first().unwrap().slot < blocks.last().unwrap().slot,
        "epoch {} needs blocks at two slots to be tampered with",
        scope.epoch
    );

    match tamper {
        Tamper::HeaderScope => header.end_slot += 1,
        Tamper::HeaderEpoch => header.epoch += 1,
        Tamper::OutsideEpoch => blocks.last_mut().unwrap().slot = scope.end_slot + 1,
        Tamper::BeforeEpoch => blocks.first_mut().unwrap().slot = scope.start_slot - 1,
        Tamper::Backwards => blocks.reverse(),
    }

    let spec = LayerSpec::new(
        stelae_blocks::BLOCKS,
        header.header().unwrap(),
        scope.descriptor(),
    );
    let mut sink = writer
        .layer_sink(&BlocksProfile, &spec, stelae_blocks::COMPRESSION_LEVEL)
        .unwrap();

    for block in &blocks {
        sink.write_record(&stelae_blocks::record::encode(block).unwrap())
            .unwrap();
    }

    sink.finish().unwrap().descriptor
}

/// The blocks of one layer of a blocks lineage.
fn read_blocks(
    layout: &SnapshotDir,
    index: &BlobIndex,
    layer: &LayerDescriptor,
) -> Vec<BlockRecord> {
    let mut reader = open_layer(layout, index, &BlocksProfile, layer);
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
