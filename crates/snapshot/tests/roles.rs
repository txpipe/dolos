//! The diff-chain rule against the chained publish.
//!
//! Over a ledger that writes each kind when the log anchors say it does, each
//! chained publish lists exactly its `diff` and its `tip` under
//! [`dolos_snapshot::roles`], and the snapshot its chain resolves to is,
//! layer for layer, the one a lineage started at the same boundary lists —
//! which is the whole snapshot a publish listed before the diff chain.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use dolos_cardano::{
    eras::ChainSummary,
    indexes::archive_dimensions,
    model::{
        AccountAddressEpochs, AccountEpochLog, AccountState, AccountStats, AddressStats,
        AssetMintChunk, DRepEpochLog, DRepState, EpochState, EraState, FirstMintChunk,
        FirstScriptChunk, FixedNamespace as _, PaymentAddressEpochs, PolicyFirstMintChunk,
        PoolEpochLog, PoolState, ProposalState,
    },
    periods::newly_sealable,
    EraBoundary, EraKey,
};
use dolos_core::{
    builtin::{MemoryArchiveStore, MemoryStateStore},
    ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _, ChainPoint, EntityKey, Namespace,
    StateStore as _, StateWriter as _, Tag,
};
use dolos_snapshot::{
    export,
    restore::{self, Restoring, Summary, Target},
    roles::{Anchor, Role, Rule, ANCHORS},
    DolosProfile, ENTITY_KINDS, MAINNET_MAGIC,
};
use dolos_testing::blocks::make_conway_block_with_prev;
use stelae::{
    dir::SnapshotDir,
    inscription::{Inscription, LayerDescriptor},
    progress::Observer,
    SnapshotReader as _,
};

/// Epochs 0 to 3 are 40 slots long and every later one 100, so the seal
/// trails by three boundaries, then two, and boundary 5 seals two epochs.
const SHORT: u64 = 40;
const LONG: u64 = 100;
const LONG_FROM: u64 = 4;
const MUTABLE_SLOTS: u64 = 50;

/// Ten publishes, the first a lineage start.
const LAST: u64 = 10;

/// The logs the seal at a boundary writes for each epoch it seals.
const PERIOD: [Namespace; 8] = [
    AccountAddressEpochs::NS,
    AccountStats::NS,
    AddressStats::NS,
    AssetMintChunk::NS,
    FirstMintChunk::NS,
    FirstScriptChunk::NS,
    PaymentAddressEpochs::NS,
    PolicyFirstMintChunk::NS,
];

/// A layer by kind and scope, the identity a publish inherits by.
type Key = (String, String);

fn key(kind: &str, scope: &serde_json::Value) -> Key {
    (kind.to_owned(), scope.to_string())
}

fn eras() -> [(u16, EraState); 2] {
    let boundary = |epoch, slot| EraBoundary {
        epoch,
        slot,
        timestamp: slot,
    };
    let split = LONG_FROM * SHORT;

    [
        (
            5,
            EraState {
                start: boundary(0, 0),
                end: Some(boundary(LONG_FROM, split)),
                epoch_length: SHORT,
                slot_length: 1,
                protocol: 5,
            },
        ),
        (
            6,
            EraState {
                start: boundary(LONG_FROM, split),
                end: None,
                epoch_length: LONG,
                slot_length: 1,
                protocol: 6,
            },
        ),
    ]
}

/// Stores written the way the ledger writes them: each boundary closes,
/// settles and seals, then commits its epoch's first block; the rest of the
/// epoch writes the reward update and one more block.
struct Ledger {
    summary: ChainSummary,
    archive: MemoryArchiveStore,
    state: MemoryStateStore,
    tip: Option<ChainPoint>,
    number: u64,
    epoch: u64,
}

impl Ledger {
    fn new() -> Self {
        let mut summary = ChainSummary::default();

        for (protocol, era) in eras() {
            summary.append_era(protocol, era);
        }

        let mut ledger = Self {
            summary,
            archive: MemoryArchiveStore::new(dolos_cardano::model::build_log_schema()),
            state: MemoryStateStore::new(),
            tip: None,
            number: 0,
            epoch: 0,
        };

        ledger.open(0);

        ledger
    }

    fn start(&self, epoch: u64) -> u64 {
        self.summary.epoch_start(epoch)
    }

    fn advance_to(&mut self, epoch: u64) {
        while self.epoch < epoch {
            self.finish();
            self.epoch += 1;
            self.open(self.epoch);
        }
    }

    fn log(&self, writer: &impl dolos_core::ArchiveWriter, ns: Namespace, epoch: u64, value: u8) {
        for byte in [1, 2] {
            writer
                .write_log(
                    ns,
                    &common::log_key(ns, self.start(epoch), byte),
                    &vec![0x82, value, epoch as u8],
                )
                .unwrap();
        }
    }

    /// The boundary into `epoch` and the epoch's first block.
    fn open(&mut self, epoch: u64) {
        let writer = self.archive.start_writer().unwrap();

        if let Some(closed) = epoch.checked_sub(1) {
            // EWRAP closing the epoch before
            self.log(&writer, EpochState::NS, closed, 10);
            self.log(&writer, DRepEpochLog::NS, closed, 10);

            if let Some(settled) = closed.checked_sub(1) {
                self.log(&writer, AccountEpochLog::NS, settled, 22);
                self.log(&writer, PoolEpochLog::NS, settled, 13);
            }

            // ESTART moving the closed epoch's final rows
            self.log(&writer, PoolState::NS, closed, 14);
            self.log(&writer, DRepState::NS, closed, 14);

            if closed % 2 == 1 {
                self.log(&writer, ProposalState::NS, closed, 14);
            }
        }

        // ESTART's stake leg for the epoch it opens
        self.log(&writer, AccountEpochLog::NS, epoch, 21);

        for sealed in newly_sealable(&self.summary, MUTABLE_SLOTS, epoch) {
            for ns in PERIOD {
                self.log(&writer, ns, sealed, 11);
            }

            if sealed.is_multiple_of(2) {
                for (_, kind) in ENTITY_KINDS {
                    let id = vec![0x30 + sealed as u8; kind.key_len()];

                    writer
                        .write_entity(kind, &id, self.start(sealed) + 5)
                        .unwrap();
                }
            }
        }

        writer.commit().unwrap();

        self.block(self.start(epoch) + 3);
        self.write_state(1);
    }

    /// The rest of the open epoch: the reward update and one more block.
    fn finish(&mut self) {
        if let Some(rewarded) = self.epoch.checked_sub(1) {
            let writer = self.archive.start_writer().unwrap();
            self.log(&writer, PoolEpochLog::NS, rewarded, 12);
            writer.commit().unwrap();
        }

        self.block(self.start(self.epoch) + 20);
        self.write_state(2);
    }

    fn block(&mut self, slot: u64) {
        let previous = self.tip.as_ref().and_then(ChainPoint::hash);
        let (point, body) = make_conway_block_with_prev(slot, previous, self.number);

        let delta = ArchiveIndexDelta {
            slot,
            block_hash: point.hash().unwrap().to_vec(),
            block_number: Some(self.number),
            tx_hashes: vec![[[0xee; 24].as_slice(), &slot.to_be_bytes()].concat()],
            tags: vec![Tag::new(
                archive_dimensions::ALL[0],
                [[0x01; 20].as_slice(), &slot.to_be_bytes()].concat(),
            )],
        };

        let writer = self.archive.start_writer().unwrap();
        writer.apply(&point, &body).unwrap();
        writer.apply_index(std::slice::from_ref(&delta)).unwrap();
        writer.commit().unwrap();

        self.tip = Some(point);
        self.number += 1;
    }

    /// State rows that change every time the ledger moves.
    fn write_state(&self, marker: u8) {
        let writer = self.state.start_writer().unwrap();

        for (protocol, era) in eras() {
            writer
                .write_entity_typed::<EraState>(&EraKey::from(protocol), &era)
                .unwrap();
        }

        for ns in [PoolState::NS, AccountState::NS] {
            writer
                .write_entity(
                    ns,
                    &EntityKey::from_raw(&common::state_key(ns, 0x40)),
                    &vec![0x82, marker, self.epoch as u8],
                )
                .unwrap();
        }

        writer.set_cursor(self.tip.clone().unwrap()).unwrap();
        writer.commit().unwrap();
    }
}

/// Whether [`Ledger`] writes any record of `kind` for window `epoch`: proposals
/// close in odd epochs, and entities are first seen in even ones.
fn writes(kind: &str, epoch: u64) -> bool {
    match kind {
        "log-proposals" => epoch % 2 == 1,
        kind if kind.starts_with("entity-") => epoch.is_multiple_of(2),
        _ => true,
    }
}

fn by_key<'a>(
    layers: impl IntoIterator<Item = &'a LayerDescriptor>,
) -> BTreeMap<Key, LayerDescriptor> {
    layers
        .into_iter()
        .map(|layer| (key(&layer.kind, &layer.scope), layer.clone()))
        .collect()
}

/// Restore the latest stele of the network root at `root` into empty stores.
fn restored(root: &std::path::Path) -> Summary {
    let storage = tempfile::tempdir().unwrap();
    let archive = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
    let state = MemoryStateStore::new();

    let node = Restoring {
        network_magic: MAINNET_MAGIC,
        max_history: None,
        storage_path: storage.path(),
        resume: false,
        skip_space_check: true,
        journals: 0,
        read_ahead: restore::DEFAULT_READ_AHEAD,
        source: None,
    };

    let (_, _, summary) = restore::restore_dir(
        root.join(export::DOLOS_REPOSITORY),
        Some(&root.join(export::BLOCKS_REPOSITORY)),
        node,
        Target::new(&archive, &state),
        &Observer::silent(),
    )
    .unwrap();

    summary
}

/// What a restore wrote, without the layer counts that tell the two apart.
fn records(summary: &Summary) -> [u64; 6] {
    [
        summary.blocks,
        summary.logs,
        summary.index_records,
        summary.entity_index,
        summary.entities,
        summary.utxos,
    ]
}

/// `PROFILE.md`'s "Log anchors" table names every log kind once, with the
/// first and the final stele of [`ANCHORS`].
#[test]
fn the_profile_states_the_log_anchors_of_the_code() {
    let profile = include_str!("../PROFILE.md");
    let rows: Vec<&str> = profile
        .lines()
        .skip_while(|line| !line.starts_with("Log anchors."))
        .skip_while(|line| !line.starts_with('|'))
        .take_while(|line| line.starts_with('|'))
        .skip(2)
        .collect();

    let offset = |offset: u64| match offset {
        0 => "`e`".to_owned(),
        offset => format!("`e + {offset}`"),
    };

    let mut stated = BTreeMap::new();

    for row in rows {
        let cells: Vec<&str> = row.split('|').map(str::trim).collect();
        let [_, logs, _, first, last, _] = cells[..] else {
            panic!("a log anchors row has four cells: {row}");
        };

        // the names are the backticked spans of the first cell
        for name in logs.split('`').skip(1).step_by(2) {
            let previous = stated.insert(format!("log-{name}"), (first, last));
            assert!(previous.is_none(), "{name} is stated twice");
        }
    }

    let anchored: BTreeMap<_, _> = ANCHORS
        .iter()
        .filter(|(kind, _)| kind.starts_with("log-"))
        .map(|(kind, anchor)| (kind.to_string(), *anchor))
        .collect();

    assert_eq!(
        stated.keys().collect::<Vec<_>>(),
        anchored.keys().collect::<Vec<_>>()
    );

    for (kind, anchor) in anchored {
        let (first, last) = stated[&kind];

        match anchor {
            Anchor::After { first: at, settled } => {
                assert_eq!((first, last), (&*offset(at), &*offset(settled)), "{kind}");
            }
            Anchor::Seal => {
                assert!(first.starts_with("`b`"), "{kind}: {first}");
                assert_eq!(last, "`b`", "{kind}");
            }
            Anchor::Cut => panic!("{kind}: a log kind is anchored to the cut"),
        }
    }
}

/// Each publish into one layout chains onto the one before; at every
/// boundary the chain resolves to the snapshot a lineage started there lists,
/// and at the sixth both restore the same records.
#[test]
fn a_chained_publish_lists_its_diff_and_its_tip_and_resolves_to_the_whole() {
    let mut ledger = Ledger::new();
    let summary = ledger.summary.clone();
    let rule = Rule::new(&summary, MUTABLE_SLOTS);
    let root = tempfile::tempdir().unwrap();
    let chain = root.path().join("chain");

    let mut previous: Option<Inscription> = None;
    let mut diffs: BTreeMap<Key, LayerDescriptor> = BTreeMap::new();
    let mut sealed: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();

    for sequence in 1..=LAST {
        ledger.advance_to(sequence);

        let plan = export::plan(&ledger.state, MAINNET_MAGIC, MUTABLE_SLOTS).unwrap();
        assert_eq!(plan.sequence, sequence);

        let inscription = export::publish(
            &chain,
            &plan,
            &ledger.archive,
            &ledger.state,
            &Observer::silent(),
        )
        .unwrap();

        assert_eq!(
            inscription.previous.map(|link| link.sequence),
            previous.as_ref().map(|previous| previous.sequence),
            "the publish at {sequence} links to the one before"
        );

        let mut diff = Vec::new();
        let mut tip = Vec::new();

        for layer in inscription.layers() {
            let window = layer.scope.get("epoch").and_then(serde_json::Value::as_u64);
            let role = rule.role(&layer.kind, window, sequence).unwrap();
            let first = rule.first_listed(&layer.kind, window).unwrap();

            match role {
                Some(Role::Tip) => tip.push(layer.clone()),
                // a lineage starts with everything final up to its cut
                Some(Role::Diff) if previous.is_none() || first == Some(sequence) => {
                    diff.push(layer.clone())
                }
                Some(Role::Diff) => panic!(
                    "{} {} is final before {sequence} and listed again there",
                    layer.kind, layer.scope
                ),
                None => panic!(
                    "{} {} is in the snapshot at {sequence}, where the rule holds none",
                    layer.kind, layer.scope
                ),
            }

            if layer.kind == "log-first-script-chunks" && first == Some(sequence) {
                sealed.entry(sequence).or_default().extend(window);
            }
        }

        assert_eq!(inscription.diff, diff, "the diff at {sequence}");
        assert_eq!(inscription.tip, tip, "the tip at {sequence}");

        // and the snapshot holds a layer wherever the rule does and this
        // ledger wrote a record
        diffs.extend(by_key(&diff));

        let mut whole = diffs.clone();
        whole.extend(by_key(&tip));

        let held: BTreeSet<(&str, u64)> = ANCHORS
            .iter()
            .flat_map(|(kind, _)| (0..=sequence).map(move |epoch| (*kind, epoch)))
            .filter(|(kind, epoch)| {
                rule.role(kind, Some(*epoch), sequence).unwrap().is_some() && writes(kind, *epoch)
            })
            .collect();
        let present: BTreeSet<(&str, u64)> = whole
            .values()
            .filter_map(|layer| {
                let window = layer.scope.get("epoch")?.as_u64()?;
                Some((layer.kind.as_str(), window))
            })
            .collect();

        assert_eq!(
            present, held,
            "the windows the snapshot at {sequence} holds"
        );

        // The chain resolves to every diff up to it and its tip, and that is
        // what a lineage started at this boundary lists, digest for digest.
        let resolved = SnapshotDir::open(chain.join(export::DOLOS_REPOSITORY))
            .unwrap()
            .resolve_sequence(&DolosProfile, sequence)
            .unwrap();
        let layer_set = by_key(resolved.chain().unwrap().descriptors());

        assert_eq!(layer_set, whole, "the chain at {sequence}");
        assert_eq!(resolved.read_inscription().unwrap(), inscription);

        let alone = root.path().join(format!("alone-{sequence}"));
        let started = export::publish(
            &alone,
            &plan,
            &ledger.archive,
            &ledger.state,
            &Observer::silent(),
        )
        .unwrap();

        assert!(started.previous.is_none());
        assert_eq!(
            by_key(started.layers()),
            layer_set,
            "a lineage started at {sequence}"
        );

        if sequence == 6 {
            let chained = restored(&chain);
            let single = restored(&alone);

            assert_eq!(records(&chained), records(&single));
            assert_eq!(chained.layers_fetched, single.layers_fetched);
            // every kind but the UTxOs, which this ledger never writes
            assert!(records(&chained)[..5].iter().all(|count| *count > 0));
            // two blocks in each closed epoch, and the boundary's first
            assert_eq!(chained.blocks, 2 * sequence + 1);
        }

        previous = Some(inscription);
    }

    // The chain crossed seals of one and of two epochs, and boundaries that
    // sealed none.
    let expected: BTreeMap<u64, BTreeSet<u64>> = (1..=LAST)
        .map(|boundary| (boundary, rule.sealed_at(boundary)))
        .filter(|(_, epochs)| !epochs.is_empty())
        .map(|(boundary, epochs)| (boundary, epochs.into_iter().collect()))
        .collect();

    assert_eq!(sealed, expected);
    assert_eq!(rule.sealed_at(5), [2, 3]);
    assert!(rule.sealed_at(2).is_empty());
}
