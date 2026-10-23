//! First-seen scripts.
//!
//! Records where each script was first seen (the `scripts` witness, see
//! `model/scripts.rs`) and stages it in the `first-scripts` row of that
//! block's epoch. A visitor cannot read the store and deltas only meet their
//! entity once the whole batch is crawled, so which of the batch's scripts the
//! witness already holds is read once per batch, ahead of the crawl, and kept
//! current by hand from block to block.

use std::collections::{BTreeSet, HashSet};

use dolos_core::{ChainError, Genesis, StateStore};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{ComputeHash, MultiEraBlock, MultiEraOutput, MultiEraTx, OriginalHash},
    },
};

use super::{BlockVisitor, WorkDeltas};
use crate::{
    model::FirstScriptSet, pallas_extras, PParamsSet, ScriptFirstSeen, ScriptKey, ScriptState,
    StageFirstScript,
};

fn reference_script(output: &MultiEraOutput) -> Option<Hash<28>> {
    output
        .script_ref()
        .map(|script_ref| pallas_extras::script_ref_hash(&script_ref))
}

/// Hashes of the scripts `tx` carries, in the order cardano-db-sync first
/// sees them (`Tx.hs`, db-sync 13.7.2.1):
///
/// 1. the outputs' reference scripts, in output order;
/// 2. the collateral return's reference script, valid tx or not;
/// 3. the witness scripts, native and Plutus merged, by hash;
/// 4. the auxiliary-data scripts: native, then Plutus V1, each in list order.
///
/// A tx that failed phase-2 validation contributes its collateral return
/// alone. pallas drops the auxiliary Plutus V2 and V3 scripts, so those are
/// missing here.
pub fn tx_script_hashes(tx: &MultiEraTx<'_>) -> Vec<Hash<28>> {
    let valid = tx.is_valid();

    let mut hashes: Vec<Hash<28>> = Vec::new();

    if valid {
        hashes.extend(tx.outputs().iter().filter_map(reference_script));
    }

    hashes.extend(tx.collateral_return().as_ref().and_then(reference_script));

    if !valid {
        return hashes;
    }

    let mut witnesses = BTreeSet::new();
    witnesses.extend(tx.native_scripts().iter().map(|x| x.original_hash()));
    witnesses.extend(tx.plutus_v1_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v2_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v3_scripts().iter().map(|x| x.compute_hash()));

    hashes.extend(witnesses);

    hashes.extend(
        pallas_extras::aux_native_scripts(tx)
            .iter()
            .map(|x| pallas_extras::native_script_hash(x)),
    );
    hashes.extend(tx.aux_plutus_v1_scripts().iter().map(|x| x.compute_hash()));

    hashes
}

/// The scripts `block` carries, each with where in the block it is seen, in
/// first-seen order. A script carried twice is listed at each sighting.
pub fn block_script_sightings(block: &MultiEraBlock<'_>) -> Vec<(Hash<28>, FirstScriptSet)> {
    sightings(block.slot(), &block.txs())
}

fn sightings(slot: u64, txs: &[MultiEraTx<'_>]) -> Vec<(Hash<28>, FirstScriptSet)> {
    txs.iter()
        .enumerate()
        .flat_map(|(tx_index, tx)| {
            tx_script_hashes(tx)
                .into_iter()
                .enumerate()
                .map(move |(order, hash)| {
                    let first = FirstScriptSet {
                        slot,
                        tx_index: tx_index as u32,
                        order: order as u32,
                    };

                    (hash, first)
                })
        })
        .collect()
}

/// What the roll knows about the witness while it crawls a batch.
#[derive(Debug, Default)]
pub struct ScriptRegistryContext {
    /// The scripts the batch carries that the witness holds already, plus the
    /// ones the batch has seen so far.
    known: HashSet<Hash<28>>,
}

impl ScriptRegistryContext {
    /// Read the witness for a batch whose blocks carry `carried`.
    pub fn load<S: StateStore>(
        state: &S,
        carried: &[Vec<(Hash<28>, FirstScriptSet)>],
    ) -> Result<Self, ChainError> {
        let scripts: Vec<Hash<28>> = carried
            .iter()
            .flatten()
            .map(|(hash, _)| *hash)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if scripts.is_empty() {
            return Ok(Self::default());
        }

        let keys: Vec<ScriptKey> = scripts.iter().map(|x| ScriptKey(*x)).collect();

        let found = state.read_entities_typed::<ScriptState>(&keys.iter().collect::<Vec<_>>())?;

        let known = scripts
            .into_iter()
            .zip(found)
            .filter_map(|(script, row)| row.map(|_| script))
            .collect();

        Ok(Self { known })
    }
}

/// Emits the deltas of the scripts a block is the first to carry.
#[derive(Default)]
pub struct ScriptRegistryVisitor {
    context: ScriptRegistryContext,
    carried: Vec<(Hash<28>, FirstScriptSet)>,
}

impl ScriptRegistryVisitor {
    pub fn new(context: ScriptRegistryContext, carried: Vec<(Hash<28>, FirstScriptSet)>) -> Self {
        Self { context, carried }
    }

    /// The context after this block, for the next one of the batch.
    pub fn take_context(&mut self) -> ScriptRegistryContext {
        std::mem::take(&mut self.context)
    }

    /// Record and stage the carried scripts the witness does not hold.
    fn register(&mut self, deltas: &mut WorkDeltas, epoch: Epoch) {
        for (script, first) in std::mem::take(&mut self.carried) {
            if !self.context.known.insert(script) {
                continue;
            }

            deltas.add_for_entity(ScriptFirstSeen::new(script, first.into()));
            deltas.add_for_entity(StageFirstScript::new(epoch, script, first));
        }
    }
}

impl BlockVisitor for ScriptRegistryVisitor {
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        epoch: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.register(deltas, epoch);

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use dolos_core::{builtin::MemoryStateStore, Entity as _, EntityDelta as _, StateWriter as _};

    use crate::CardanoEntity;
    use pallas::{
        codec::{
            minicbor,
            utils::{Bytes, CborWrap, KeepRaw, Nullable},
        },
        ledger::primitives::{
            alonzo::{AuxiliaryData, NativeScript},
            conway::{
                PostAlonzoTransactionOutput, ScriptRef, TransactionBody, TransactionInput,
                TransactionOutput, Tx, Value, WitnessSet,
            },
            NonEmptySet, PlutusScript, Set,
        },
    };

    use super::*;

    /// The scripts of one Conway tx, by where it carries them.
    #[derive(Default)]
    pub(crate) struct ScriptedTx {
        pub success: bool,
        /// One output per script, carrying it as its reference script.
        pub outputs: Vec<NativeScript>,
        pub collateral_return: Option<NativeScript>,
        pub native: Vec<NativeScript>,
        /// In wire order.
        pub plutus: Vec<PlutusScript<2>>,
        /// The auxiliary data as written.
        pub aux: Option<Vec<u8>>,
    }

    fn output(script: Option<&NativeScript>) -> TransactionOutput<'static> {
        TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
            address: Bytes::from(vec![0x60; 29]),
            value: Value::Coin(1_500_000),
            datum_option: None,
            script_ref: script.map(|x| CborWrap(ScriptRef::NativeScript(KeepRaw::from(x.clone())))),
        }))
    }

    impl ScriptedTx {
        pub fn cbor(&self) -> Vec<u8> {
            let input = TransactionInput {
                transaction_id: [1u8; 32].into(),
                index: 0,
            };

            let body = TransactionBody {
                inputs: Set::from(vec![input.clone()]),
                outputs: self.outputs.iter().map(|x| output(Some(x))).collect(),
                fee: 200_000,
                ttl: None,
                certificates: None,
                withdrawals: None,
                auxiliary_data_hash: None,
                validity_interval_start: None,
                mint: None,
                script_data_hash: None,
                collateral: Some(NonEmptySet::try_from(vec![input]).expect("non-empty")),
                required_signers: None,
                network_id: None,
                collateral_return: self.collateral_return.as_ref().map(|x| output(Some(x))),
                total_collateral: Some(500_000),
                reference_inputs: None,
                voting_procedures: None,
                proposal_procedures: None,
                treasury_value: None,
                donation: None,
            };

            let body = minicbor::to_vec(&body).expect("failed to encode tx body");
            let body = minicbor::decode::<KeepRaw<'_, TransactionBody<'_>>>(&body)
                .expect("failed to decode tx body")
                .to_owned();

            let native = self
                .native
                .iter()
                .map(|x| KeepRaw::from(x.clone()))
                .collect::<Vec<_>>();

            let witness_set = WitnessSet {
                vkeywitness: None,
                native_script: NonEmptySet::try_from(native).ok(),
                bootstrap_witness: None,
                plutus_v1_script: None,
                plutus_data: None,
                redeemer: None,
                plutus_v2_script: NonEmptySet::try_from(self.plutus.clone()).ok(),
                plutus_v3_script: None,
            };

            let auxiliary_data = match &self.aux {
                Some(raw) => Nullable::Some(
                    minicbor::decode::<KeepRaw<'_, AuxiliaryData>>(raw)
                        .expect("failed to decode auxiliary data"),
                ),
                None => Nullable::Null,
            };

            let tx = Tx {
                transaction_body: body,
                transaction_witness_set: KeepRaw::from(witness_set),
                success: self.success,
                auxiliary_data,
            };

            minicbor::to_vec(tx).expect("failed to encode tx")
        }
    }

    /// Alonzo auxiliary data holding `scripts`, each as written.
    pub(crate) fn aux_with_native(scripts: &[&[u8]]) -> Vec<u8> {
        // #6.259({1: [scripts]})
        let mut raw = vec![0xd9, 0x01, 0x03, 0xa1, 0x01, 0x80 + scripts.len() as u8];

        for script in scripts {
            raw.extend_from_slice(script);
        }

        raw
    }

    fn plutus(byte: u8) -> PlutusScript<2> {
        PlutusScript::<2>(Bytes::from(vec![0x46, 0x01, 0x00, 0x00, 0x22, 0x26, byte]))
    }

    /// A Conway tx with a native reference script on its collateral return, a
    /// plutus script among its witnesses and a native script in its auxiliary
    /// data. Returns the three hashes in that order.
    pub(crate) fn tx_cbor(success: bool) -> (Vec<u8>, Hash<28>, Hash<28>, Hash<28>) {
        let native = NativeScript::InvalidHereafter(7);
        let plutus = plutus(0x01);
        let auxiliary = NativeScript::InvalidBefore(9);

        let tx = ScriptedTx {
            success,
            collateral_return: Some(native.clone()),
            plutus: vec![plutus.clone()],
            aux: Some(aux_with_native(&[&minicbor::to_vec(&auxiliary).unwrap()])),
            ..Default::default()
        };

        (
            tx.cbor(),
            native.compute_hash(),
            plutus.compute_hash(),
            auxiliary.compute_hash(),
        )
    }

    fn hashes(tx: &ScriptedTx) -> Vec<Hash<28>> {
        let cbor = tx.cbor();
        let tx = MultiEraTx::decode(&cbor).expect("failed to decode tx");
        tx_script_hashes(&tx)
    }

    /// Every place a tx carries scripts, in db-sync's order: the outputs'
    /// reference scripts, the collateral return's, the witnesses by hash
    /// whatever their wire order, then the auxiliary data's.
    pub(crate) fn every_place() -> (ScriptedTx, Vec<Hash<28>>) {
        let outputs = vec![
            NativeScript::InvalidBefore(1),
            NativeScript::InvalidBefore(2),
        ];
        let collateral_return = NativeScript::InvalidBefore(3);
        let native = NativeScript::InvalidBefore(4);

        // the wire order is not the hash order
        let mut plutus: Vec<_> = (0..3).map(plutus).collect();
        plutus.sort_by_key(|x| std::cmp::Reverse(x.compute_hash()));

        // 9 written in two bytes where one would do: re-encoding drops one
        let auxiliary: &[u8] = &[0x82, 0x04, 0x18, 0x09];

        let mut witnesses: Vec<Hash<28>> = plutus.iter().map(|x| x.compute_hash()).collect();
        witnesses.push(native.compute_hash());
        witnesses.sort();

        let expected = outputs
            .iter()
            .chain([&collateral_return])
            .map(|x| x.compute_hash())
            .chain(witnesses)
            .chain([pallas_extras::native_script_hash(auxiliary)])
            .collect();

        let tx = ScriptedTx {
            success: true,
            outputs,
            collateral_return: Some(collateral_return),
            native: vec![native],
            plutus,
            aux: Some(aux_with_native(&[auxiliary])),
        };

        (tx, expected)
    }

    #[test]
    fn a_valid_tx_lists_its_scripts_in_db_sync_order() {
        let (tx, expected) = every_place();

        assert_ne!(
            tx.plutus
                .iter()
                .map(|x| x.compute_hash())
                .collect::<Vec<_>>(),
            {
                let mut sorted: Vec<_> = tx.plutus.iter().map(|x| x.compute_hash()).collect();
                sorted.sort();
                sorted
            },
            "the witnesses are already in hash order"
        );

        assert_eq!(hashes(&tx), expected);
    }

    /// The auxiliary native script hashes over its bytes as written, which a
    /// re-encoding would not reproduce.
    #[test]
    fn an_auxiliary_native_script_hashes_as_written() {
        let written: &[u8] = &[0x82, 0x04, 0x18, 0x09];
        let decoded: NativeScript = minicbor::decode(written).unwrap();
        assert_ne!(
            decoded.compute_hash(),
            pallas_extras::native_script_hash(written)
        );

        let tx = ScriptedTx {
            success: true,
            aux: Some(aux_with_native(&[written])),
            ..Default::default()
        };

        assert_eq!(
            hashes(&tx),
            vec![pallas_extras::native_script_hash(written)]
        );

        // and in the Allegra form: [metadata, [scripts]]
        let mut allegra = vec![0x82, 0xa0, 0x81];
        allegra.extend_from_slice(written);

        let tx = ScriptedTx {
            success: true,
            aux: Some(allegra),
            ..Default::default()
        };

        assert_eq!(
            hashes(&tx),
            vec![pallas_extras::native_script_hash(written)]
        );
    }

    /// A tx that failed phase-2 validation contributes the reference script
    /// of its collateral return and nothing else.
    #[test]
    fn an_invalid_tx_lists_its_collateral_return_alone() {
        let (mut tx, expected) = every_place();
        tx.success = false;

        assert_eq!(hashes(&tx), vec![expected[2]]);

        // with no reference script there, nothing at all
        tx.collateral_return = None;
        assert!(hashes(&tx).is_empty());
    }

    #[test]
    fn a_valid_tx_lists_its_collateral_return() {
        let (cbor, collateral_return, plutus, auxiliary) = tx_cbor(true);
        let tx = MultiEraTx::decode(&cbor).unwrap();

        assert_eq!(
            tx_script_hashes(&tx),
            vec![collateral_return, plutus, auxiliary]
        );
    }

    fn script(byte: u8) -> Hash<28> {
        Hash::new([byte; 28])
    }

    fn at(slot: u64, tx_index: u32, order: u32) -> FirstScriptSet {
        FirstScriptSet {
            slot,
            tx_index,
            order,
        }
    }

    /// Apply a block's deltas, the way the roll does after the crawl, and
    /// return them for undoing.
    fn commit(state: &MemoryStateStore, deltas: WorkDeltas) -> WorkDeltas {
        let writer = state.start_writer().unwrap();
        let mut applied = WorkDeltas::default();

        for (ns_key, deltas) in deltas.entities {
            let mut entity = read(state, &ns_key);

            for mut delta in deltas {
                delta.apply(&mut entity);
                applied.add_for_entity(delta);
            }

            write(&writer, &ns_key, entity);
        }

        writer.commit().unwrap();

        applied
    }

    fn undo(state: &MemoryStateStore, deltas: WorkDeltas) {
        let writer = state.start_writer().unwrap();

        for (ns_key, deltas) in deltas.entities {
            let mut entity = read(state, &ns_key);

            for delta in deltas.iter().rev() {
                delta.undo(&mut entity);
            }

            write(&writer, &ns_key, entity);
        }

        writer.commit().unwrap();
    }

    fn read(state: &MemoryStateStore, ns_key: &dolos_core::NsKey) -> Option<CardanoEntity> {
        state
            .read_entities(ns_key.ns(), &[ns_key.key()])
            .unwrap()
            .remove(0)
            .map(|x| CardanoEntity::decode_entity(ns_key.ns(), &x).unwrap())
    }

    fn write(
        writer: &<MemoryStateStore as StateStore>::Writer,
        ns_key: &dolos_core::NsKey,
        entity: Option<CardanoEntity>,
    ) {
        match entity {
            Some(entity) => {
                let (ns, value) = CardanoEntity::encode_entity(&entity);
                writer.write_entity(ns, ns_key.key(), &value).unwrap();
            }
            None => writer.delete_entity(ns_key.ns(), ns_key.key()).unwrap(),
        }
    }

    fn staged(state: &MemoryStateStore) -> Vec<(Epoch, Hash<28>, FirstScriptSet)> {
        state
            .iter_entities_typed::<FirstScriptSet>(None)
            .unwrap()
            .map(|row| {
                let (key, row) = row.unwrap();
                (key.epoch, key.member.0, row)
            })
            .collect()
    }

    fn witnessed(state: &MemoryStateStore) -> Vec<(Hash<28>, ScriptState)> {
        state
            .iter_entities_typed::<ScriptState>(None)
            .unwrap()
            .map(|row| {
                let (key, row) = row.unwrap();
                (key.0, row)
            })
            .collect()
    }

    /// Run `blocks` through one batch's visitors, each block `(epoch,
    /// sightings)`, and commit them.
    fn roll(state: &MemoryStateStore, blocks: Vec<(Epoch, Vec<(Hash<28>, FirstScriptSet)>)>) {
        let carried: Vec<_> = blocks.iter().map(|(_, x)| x.clone()).collect();
        let mut context = ScriptRegistryContext::load(state, &carried).unwrap();

        for (epoch, carried) in blocks {
            let mut visitor = ScriptRegistryVisitor::new(std::mem::take(&mut context), carried);
            let mut deltas = WorkDeltas::default();

            visitor.register(&mut deltas, epoch);

            context = visitor.take_context();
            commit(state, deltas);
        }
    }

    #[test]
    fn a_batch_records_each_script_once_where_first_seen() {
        let state = MemoryStateStore::new();

        // three blocks of one batch: the second repeats a script of the first,
        // and nothing is committed in between
        roll(
            &state,
            vec![
                (
                    4,
                    vec![(script(1), at(10, 0, 0)), (script(2), at(10, 0, 1))],
                ),
                (
                    4,
                    vec![(script(2), at(11, 0, 0)), (script(3), at(11, 1, 0))],
                ),
                (4, vec![]),
            ],
        );

        // the next batch knows what is recorded
        let carried = vec![vec![(script(3), at(20, 0, 0)), (script(4), at(20, 0, 1))]];
        let context = ScriptRegistryContext::load(&state, &carried).unwrap();
        assert_eq!(context.known, HashSet::from([script(3)]));

        roll(
            &state,
            vec![(
                5,
                vec![(script(3), at(20, 0, 0)), (script(4), at(20, 0, 1))],
            )],
        );

        assert_eq!(
            witnessed(&state),
            vec![
                (script(1), ScriptState { slot: 10 }),
                (script(2), ScriptState { slot: 10 }),
                (script(3), ScriptState { slot: 11 }),
                (script(4), ScriptState { slot: 20 }),
            ]
        );

        assert_eq!(
            staged(&state),
            vec![
                (4, script(1), at(10, 0, 0)),
                (4, script(2), at(10, 0, 1)),
                (4, script(3), at(11, 1, 0)),
                (5, script(4), at(20, 0, 1)),
            ]
        );
    }

    /// Several new scripts in one tx, witnessed in an order other than their
    /// hashes', are recorded at their db-sync positions.
    #[test]
    fn a_tx_records_its_new_scripts_at_their_positions() {
        let (tx, expected) = every_place();
        let cbor = tx.cbor();
        let tx = MultiEraTx::decode(&cbor).unwrap();

        let state = MemoryStateStore::new();
        roll(&state, vec![(3, sightings(77, std::slice::from_ref(&tx)))]);

        let mut listed: Vec<_> = staged(&state)
            .into_iter()
            .map(|(_, hash, row)| (row, hash))
            .collect();
        listed.sort();

        assert_eq!(
            listed.into_iter().map(|(_, hash)| hash).collect::<Vec<_>>(),
            expected
        );
    }

    /// A script first seen among an invalid tx's witnesses is not recorded
    /// there, and is where a later valid tx carries it.
    #[test]
    fn an_invalid_tx_witness_is_listed_where_a_valid_tx_carries_it() {
        let witness = plutus(0x07);

        let invalid = ScriptedTx {
            success: false,
            plutus: vec![witness.clone()],
            ..Default::default()
        }
        .cbor();
        let valid = ScriptedTx {
            success: true,
            plutus: vec![witness.clone()],
            ..Default::default()
        }
        .cbor();

        let txs = [
            MultiEraTx::decode(&invalid).unwrap(),
            MultiEraTx::decode(&valid).unwrap(),
        ];

        let state = MemoryStateStore::new();
        roll(&state, vec![(3, sightings(50, &txs))]);

        assert_eq!(
            witnessed(&state),
            vec![(witness.compute_hash(), ScriptState { slot: 50 })]
        );
        assert_eq!(
            staged(&state),
            vec![(3, witness.compute_hash(), at(50, 1, 0))]
        );
    }

    /// Undoing a block's deltas leaves no witness or set row of it, and the
    /// rows of the block before it stay.
    #[test]
    fn undoing_a_block_removes_its_rows() {
        let state = MemoryStateStore::new();

        roll(&state, vec![(4, vec![(script(1), at(10, 0, 0))])]);

        let before = (witnessed(&state), staged(&state));

        let carried = vec![vec![(script(1), at(11, 0, 0)), (script(2), at(11, 0, 1))]];
        let context = ScriptRegistryContext::load(&state, &carried).unwrap();
        let mut visitor = ScriptRegistryVisitor::new(context, carried[0].clone());
        let mut deltas = WorkDeltas::default();
        visitor.register(&mut deltas, 4);
        let applied = commit(&state, deltas);

        assert_eq!(witnessed(&state).len(), 2);

        undo(&state, applied);

        assert_eq!((witnessed(&state), staged(&state)), before);
    }
}
