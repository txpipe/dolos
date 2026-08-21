//! One small, hand-pinned Dolos stele, shared by the roundtrip and golden
//! tests.
//!
//! Every value here is a literal. Nothing is drawn from `dolos-testing`'s
//! seeder, and nothing is derived from a store: a golden digest whose input can
//! move when an unrelated fixture is edited is not a golden, and the drift it
//! was there to catch would be re-pinned as a matter of routine.
//!
//! The records are synthetic — synthetic block bodies, synthetic hashes,
//! synthetic key hashes. That is deliberate and sufficient: this crate's job is
//! the *shape* of a record, and no part of it interprets a block, a hash or a
//! stored key. What the fixture does have to be is *complete*, because the
//! golden digests are what freeze the vocabulary: every archive dimension,
//! every exact-record kind, every state namespace and every log *kind* appears
//! at least once. Namespaces are frozen through their kinds now, for the state
//! layers as for the logs — neither record carries one any more.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use dolos_cardano::indexes::archive_dimensions;
use dolos_core::{
    key_hash, BlockHash, EntityKey, EraCbor, ExactKind, ExactRecord, IndexRecord, LogKey,
    Namespace, TagRecord, TxoRef, VERBATIM_KEY_DIMENSION,
};
use dolos_snapshot::{
    layers::{
        blocks::BlockRecord,
        digests::ImmutableDigests,
        logs::{LogRecord, LOG_KEY_LEN},
        state::{self, StateRecord, ENTITY_KEY_LEN},
    },
    DigestsScope, DolosProfile, EpochScope, Error, Network, Scope, StateScope, BLOCKS, DIGESTS,
    INDEXES, LOG_KINDS, LOG_NAMESPACES, NAMESPACES, STATE_KINDS, UTXOS,
};
use stelae::{
    dir::{BlobIndex, SteleDir, WrittenLayer},
    frame::{CanonicalCbor, LayerHeader, Limits},
    inscription::LayerDescriptor,
    Digest, Profile, SteleReader, SteleWriter,
};

pub const NETWORK_MAGIC: u64 = dolos_snapshot::MAINNET_MAGIC;

/// The stele's sequence, which for this profile is the epoch.
pub const EPOCH: u64 = 7;
pub const START_SLOT: u64 = 100;
pub const END_SLOT: u64 = 101;
pub const LAST_IMMUTABLE: u64 = 3;

/// The boundary block this stele stands at.
pub const POINT_HASH: [u8; 32] = [0x0b; 32];

/// A transaction-metadata label, carried verbatim rather than hashed.
pub const METADATA_LABEL: u64 = 674;

/// The shards the fixture populates for a sixteen-way state kind. Sixteen
/// exist; a fixture that wrote all of them would say nothing more than two do
/// about the split. A single-blob kind has only shard 0, so it takes the first
/// of these and stops — see [`state_layers`].
pub const SHARDS: [u8; 2] = [0, 1];

pub fn network() -> Network {
    Network::for_magic(NETWORK_MAGIC)
}

pub fn epoch_scope() -> EpochScope {
    EpochScope {
        network_magic: NETWORK_MAGIC,
        epoch: EPOCH,
        start_slot: START_SLOT,
        end_slot: END_SLOT,
    }
}

pub fn state_scope(shard: u8) -> StateScope {
    StateScope {
        network_magic: NETWORK_MAGIC,
        epoch: EPOCH,
        shard,
    }
}

pub fn digests_scope() -> DigestsScope {
    DigestsScope {
        network_magic: NETWORK_MAGIC,
        epoch: EPOCH,
        last_immutable: LAST_IMMUTABLE,
    }
}

/// Three blocks, two of which share a slot — the Byron end-of-epoch boundary
/// case, whose order no comparator recovers.
pub fn blocks() -> Vec<BlockRecord> {
    vec![
        BlockRecord::new(
            START_SLOT,
            BlockHash::new([0x41; 32]),
            vec![0x82, 0x00, 0x00],
        ),
        BlockRecord::new(
            START_SLOT,
            BlockHash::new([0x42; 32]),
            vec![0x82, 0x00, 0x01],
        ),
        BlockRecord::new(END_SLOT, BlockHash::new([0x43; 32]), vec![0x82, 0x01, 0x00]),
    ]
}

/// One tag record per archive dimension, then one exact record per kind.
///
/// The tag run is sorted by dimension name, which is not the order the registry
/// declares them in — the layer's order is the contract, the registry's is
/// documentation.
pub fn indexes() -> Vec<IndexRecord> {
    let mut dimensions = archive_dimensions::ALL;
    dimensions.sort_unstable();

    let mut records: Vec<IndexRecord> = dimensions
        .into_iter()
        .enumerate()
        .map(|(i, dimension)| {
            let stored = if dimension == VERBATIM_KEY_DIMENSION {
                // The exception, and the only place the fixture derives a stored
                // key rather than pinning one: the point of this record is that
                // the label survives untouched.
                key_hash(dimension, &METADATA_LABEL.to_be_bytes()).unwrap()
            } else {
                [0x10 + i as u8; 8]
            };

            TagRecord::new(dimension, stored, START_SLOT).into()
        })
        .collect();

    records.extend([
        IndexRecord::Exact(
            ExactRecord::new(ExactKind::BlockHash, &[0x21; 32], START_SLOT).unwrap(),
        ),
        IndexRecord::Exact(
            ExactRecord::new(ExactKind::BlockNumber, &4242u64.to_be_bytes(), END_SLOT).unwrap(),
        ),
        IndexRecord::Exact(ExactRecord::new(ExactKind::TxHash, &[0x23; 32], END_SLOT).unwrap()),
    ]);

    records
}

/// One log for `ns`, which is one layer's worth.
///
/// The namespace is no longer in the record — it is the layer's kind — so what
/// distinguishes one log layer from another here is the record's key and value,
/// and both are derived from the namespace's position in [`LOG_NAMESPACES`].
/// The six kind strings are frozen by the layer headers instead, which is where
/// they now live on the wire.
pub fn logs(ns: Namespace) -> Vec<LogRecord> {
    let i = LOG_NAMESPACES
        .iter()
        .position(|known| *known == ns)
        .expect("a log namespace") as u8;

    vec![LogRecord::new(log_key(START_SLOT, 0x30 + i), vec![0x81, i])]
}

/// Every state layer the fixture writes: `(kind, namespace, shard)`, in
/// inscription order.
///
/// One layer per namespace now, rather than sixteen layers carrying all
/// seventeen namespaces between them, so a kind's shards are the shards its
/// spec'd count allows: both of [`SHARDS`] for the four sixteen-way kinds, and
/// shard 0 alone for the thirteen single blobs. Twenty-one layers, and every
/// namespace among them — which is what
/// `the_golden_state_layers_cover_every_namespace` holds this to.
pub fn state_layers() -> Vec<(&'static str, Namespace, u8)> {
    STATE_KINDS
        .into_iter()
        .flat_map(|(kind, ns, shards)| {
            SHARDS
                .into_iter()
                .filter(move |shard| *shard < shards)
                .map(move |shard| (kind, ns, shard))
        })
        .collect()
}

/// One record for `ns`, which is one layer's worth.
///
/// The namespace is no longer in the record — it is the layer's kind — so what
/// distinguishes one state layer's content from another's is the record's key
/// and value, both derived from the namespace's position in [`NAMESPACES`].
/// The seventeen namespace strings are frozen by the kinds in the layer
/// headers instead, which is where they now live on the wire.
///
/// The key's first byte carries the shard in its high nibble, so the record
/// lands in the layer that claims it under [`state_layers`] — the routing rule
/// [`dolos_snapshot::shard_of`] applies, spelled out here because the fixture
/// writes layers directly rather than through the export's router.
pub fn state(ns: Namespace, shard: u8) -> Vec<StateRecord> {
    let i = NAMESPACES
        .iter()
        .position(|known| *known == ns)
        .expect("a state namespace") as u8;

    let byte = (shard << 4) | (i & 0x0f);

    vec![if ns == UTXOS {
        state::utxo(
            &TxoRef([byte; ENTITY_KEY_LEN].into(), 3),
            &EraCbor(6, vec![0xa0, byte]),
        )
        .unwrap()
    } else {
        state::entity(&EntityKey::from(&[byte; ENTITY_KEY_LEN]), &vec![byte, 0xff])
    }]
}

pub fn digests() -> Vec<ImmutableDigests> {
    (0..2u64)
        .map(|n| ImmutableDigests {
            immutable_number: n,
            chunk: Digest::from_bytes([0x50 | n as u8; 32]),
            primary: Digest::from_bytes([0x60 | n as u8; 32]),
            secondary: Digest::from_bytes([0x70 | n as u8; 32]),
        })
        .collect()
}

pub fn log_key(slot: u64, entity: u8) -> LogKey {
    let mut raw = [0u8; LOG_KEY_LEN];
    raw[..8].copy_from_slice(&slot.to_be_bytes());
    raw[8..].fill(entity);

    LogKey::from(raw.as_slice())
}

pub fn encode_all<T>(
    records: &[T],
    encode: impl Fn(&T) -> Result<CanonicalCbor, Error>,
) -> Vec<CanonicalCbor> {
    records.iter().map(|r| encode(r).unwrap()).collect()
}

/// The uncompressed byte string of a layer: its header record, then its
/// content.
///
/// This is exactly what a `diffId` covers, so a golden computed from it is the
/// same number `SteleDir::write_layer` puts in a descriptor —
/// `descriptors_match_the_per_kind_goldens` holds the two against each other.
pub fn sequence(kind: &str, scope: &dyn Scope, records: &[CanonicalCbor]) -> Vec<u8> {
    let header = LayerHeader::new(DolosProfile.name(), kind, scope.header().unwrap())
        .encode()
        .unwrap();

    let mut bytes = header.into_bytes();

    for record in records {
        bytes.extend_from_slice(record.as_bytes());
    }

    bytes
}

pub fn write_layer(
    stele: &SteleDir,
    kind: &str,
    scope: &dyn Scope,
    records: &[CanonicalCbor],
) -> WrittenLayer {
    stele
        .write_layer(
            &DolosProfile,
            &scope.layer_spec(kind).unwrap(),
            dolos_snapshot::COMPRESSION_LEVEL,
            records,
        )
        .unwrap()
}

/// The fixture's kinds, encoded, in inscription order.
///
/// Every log kind and every state kind appears, which is what makes the goldens
/// freeze all twenty-three namespace-bearing kind strings: the namespace lives
/// in the layer header now, and nowhere else on the wire. A sixteen-way state
/// kind appears twice — one layer per populated shard — which is the normal
/// case for this profile rather than an edge one.
pub fn all_layers() -> Vec<(&'static str, Box<dyn Scope>, Vec<CanonicalCbor>)> {
    use dolos_snapshot::layers::{
        blocks, digests as digests_layer, indexes as indexes_layer, logs,
    };

    let mut layers: Vec<(&'static str, Box<dyn Scope>, Vec<CanonicalCbor>)> = vec![
        (
            BLOCKS,
            Box::new(epoch_scope()),
            encode_all(&self::blocks(), blocks::encode),
        ),
        (
            INDEXES,
            Box::new(epoch_scope()),
            encode_all(&self::indexes(), indexes_layer::encode),
        ),
    ];

    for (kind, ns) in LOG_KINDS {
        layers.push((
            kind,
            Box::new(epoch_scope()),
            encode_all(&self::logs(ns), logs::encode),
        ));
    }

    for (kind, ns, shard) in state_layers() {
        layers.push((
            kind,
            Box::new(state_scope(shard)),
            encode_all(&state(ns, shard), |record| state::encode(ns, record)),
        ));
    }

    layers.push((
        DIGESTS,
        Box::new(digests_scope()),
        encode_all(&self::digests(), digests_layer::encode),
    ));

    layers
}

/// Read a layer both ways and insist the two paths agree.
///
/// The same discipline `crates/stelae/tests/toy_profile.rs` applies to the toy
/// profile: a layer that reads back through one path and not the other is a
/// determinism bug in the format, and this profile is the first real one to put
/// that to the test.
pub fn read_both_ways(
    stele: &SteleDir,
    index: &BlobIndex,
    descriptor: &LayerDescriptor,
) -> Vec<Vec<u8>> {
    let buffered = stele.read_layer(index, &DolosProfile, descriptor).unwrap();

    // A window far below one record, so the refill path is exercised rather
    // than swallowing the layer in a single read.
    let limits = Limits {
        window: 8,
        ..Limits::default()
    };

    let mut reader = stele
        .stream_layer(index, &DolosProfile, descriptor, limits)
        .unwrap();

    let mut streamed = Vec::new();
    while let Some(record) = reader.next_record() {
        streamed.push(record.unwrap().to_vec());
    }

    assert_eq!(&reader.finish().unwrap(), buffered.digests(), "digests");

    let held: Vec<Vec<u8>> = buffered.records().map(|r| r.unwrap().to_vec()).collect();
    assert_eq!(held, streamed, "records, layer {:?}", descriptor.kind);

    streamed
}
