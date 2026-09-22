//! Script registry maintenance.
//!
//! Registers every script the first time a block carries it and hands it the
//! next sequence number (see `model/scripts.rs`). A visitor cannot read the
//! store and deltas only meet their entity once the whole batch is crawled,
//! so what the registry already holds is read once per batch, ahead of the
//! crawl, and kept current by hand from block to block.

use std::collections::{BTreeSet, HashSet};

use dolos_core::{ChainError, Genesis, StateStore};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{ComputeHash, MultiEraBlock, MultiEraTx, OriginalHash},
    },
};

use super::{BlockVisitor, WorkDeltas};
use crate::{
    pallas_extras, FixedNamespace as _, PParamsSet, ScriptFirstSeen, ScriptSeqAssigned,
    ScriptSeqState, ScriptState,
};

/// Hashes of the scripts `tx` carries, in the order cardano-db-sync numbers
/// them.
///
/// db-sync writes its `script` rows per tx: the reference script of each
/// output first, then the witness scripts out of a map keyed by script hash,
/// so sorted by hash, then the scripts of the auxiliary data as written. A tx
/// that failed phase-2 validation stops at its outputs (the collateral
/// return): db-sync reads neither its witnesses nor its auxiliary data.
pub fn tx_script_hashes(tx: &MultiEraTx<'_>) -> Vec<Hash<28>> {
    let mut hashes: Vec<Hash<28>> = tx
        .produces()
        .iter()
        .filter_map(|(_, output)| output.script_ref())
        .map(|script_ref| pallas_extras::script_ref_hash(&script_ref))
        .collect();

    if !tx.is_valid() {
        return hashes;
    }

    let mut witnesses = BTreeSet::new();
    witnesses.extend(tx.native_scripts().iter().map(|x| x.original_hash()));
    witnesses.extend(tx.plutus_v1_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v2_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v3_scripts().iter().map(|x| x.compute_hash()));

    hashes.extend(witnesses);

    hashes.extend(tx.aux_native_scripts().iter().map(|x| x.compute_hash()));
    hashes.extend(tx.aux_plutus_v1_scripts().iter().map(|x| x.compute_hash()));

    hashes
}

/// Hashes of the scripts `block` carries, in listing order and once each.
pub fn block_script_hashes(block: &MultiEraBlock<'_>) -> Vec<Hash<28>> {
    let mut seen = HashSet::new();

    block
        .txs()
        .iter()
        .flat_map(tx_script_hashes)
        .filter(|hash| seen.insert(*hash))
        .collect()
}

/// What the roll knows about the registry while it crawls a batch.
#[derive(Debug, Default)]
pub struct ScriptRegistryContext {
    /// The registered ones among the scripts the batch carries, plus the ones
    /// it has registered so far.
    known: HashSet<Hash<28>>,

    /// The next free sequence number.
    next_seq: u64,
}

impl ScriptRegistryContext {
    /// Read the registry for a batch whose blocks carry `carried`.
    pub fn load<S: StateStore>(state: &S, carried: &[Vec<Hash<28>>]) -> Result<Self, ChainError> {
        let scripts: Vec<Hash<28>> = carried
            .iter()
            .flatten()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        // nothing to register, nothing to number
        if scripts.is_empty() {
            return Ok(Self::default());
        }

        let keys: Vec<_> = scripts.iter().map(|x| x.as_slice().into()).collect();

        let found = state.read_entities(ScriptState::NS, &keys.iter().collect::<Vec<_>>())?;

        let known = scripts
            .into_iter()
            .zip(found)
            .filter_map(|(script, row)| row.map(|_| script))
            .collect();

        Ok(Self {
            known,
            next_seq: ScriptSeqState::count(state)?,
        })
    }
}

/// Emits the registry deltas of a block out of the scripts it carries.
#[derive(Default)]
pub struct ScriptRegistryVisitor {
    context: ScriptRegistryContext,
    carried: Vec<Hash<28>>,
}

impl ScriptRegistryVisitor {
    pub fn new(context: ScriptRegistryContext, carried: Vec<Hash<28>>) -> Self {
        Self { context, carried }
    }

    /// The context after this block, for the next one of the batch.
    pub fn take_context(&mut self) -> ScriptRegistryContext {
        std::mem::take(&mut self.context)
    }

    /// Register the carried scripts the registry does not know, in order.
    fn register(&mut self, deltas: &mut WorkDeltas, slot: u64) {
        for script in std::mem::take(&mut self.carried) {
            if !self.context.known.insert(script) {
                continue;
            }

            let seq = self.context.next_seq;
            self.context.next_seq += 1;

            deltas.add_for_entity(ScriptFirstSeen::new(script, seq, slot));
            deltas.add_for_entity(ScriptSeqAssigned::new(seq, script));
        }
    }
}

impl BlockVisitor for ScriptRegistryVisitor {
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        _: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.register(deltas, block.slot());

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use dolos_core::{builtin::MemoryStateStore, EntityDelta as _, NsKey, StateWriter as _};
    use pallas::{
        codec::{
            minicbor,
            utils::{Bytes, CborWrap, KeepRaw, Nullable},
        },
        ledger::primitives::{
            alonzo::{AuxiliaryData, NativeScript, PostAlonzoAuxiliaryData},
            conway::{
                PostAlonzoTransactionOutput, ScriptRef, TransactionBody, TransactionInput,
                TransactionOutput, Tx, Value, WitnessSet,
            },
            NonEmptySet, PlutusScript, Set,
        },
    };

    use super::*;

    /// A Conway tx with a native reference script on its collateral return, a
    /// plutus script among its witnesses and a native script in its auxiliary
    /// data. Returns the three hashes in that order.
    pub(crate) fn tx_cbor(success: bool) -> (Vec<u8>, Hash<28>, Hash<28>, Hash<28>) {
        let native = NativeScript::InvalidHereafter(7);
        let native_hash = native.compute_hash();

        let plutus = PlutusScript::<2>(Bytes::from(vec![0x46, 0x01, 0x00, 0x00, 0x22, 0x26, 0x01]));
        let plutus_hash = plutus.compute_hash();

        let auxiliary = NativeScript::InvalidBefore(9);
        let auxiliary_hash = auxiliary.compute_hash();

        let collateral_return = PostAlonzoTransactionOutput {
            address: Bytes::from(vec![0x60; 29]),
            value: Value::Coin(1_500_000),
            datum_option: None,
            script_ref: Some(CborWrap(ScriptRef::NativeScript(KeepRaw::from(native)))),
        };

        let input = TransactionInput {
            transaction_id: [1u8; 32].into(),
            index: 0,
        };

        let body = TransactionBody {
            inputs: Set::from(vec![input.clone()]),
            outputs: vec![],
            fee: 200_000,
            ttl: None,
            certificates: None,
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: Some(NonEmptySet::try_from(vec![input]).expect("non-empty collateral")),
            required_signers: None,
            network_id: None,
            collateral_return: Some(TransactionOutput::PostAlonzo(KeepRaw::from(
                collateral_return,
            ))),
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

        let witness_set = WitnessSet {
            vkeywitness: None,
            native_script: None,
            bootstrap_witness: None,
            plutus_v1_script: None,
            plutus_data: None,
            redeemer: None,
            plutus_v2_script: Some(
                NonEmptySet::try_from(vec![plutus]).expect("non-empty plutus script set"),
            ),
            plutus_v3_script: None,
        };

        let tx = Tx {
            transaction_body: body,
            transaction_witness_set: KeepRaw::from(witness_set),
            success,
            auxiliary_data: Nullable::Some(KeepRaw::from(AuxiliaryData::PostAlonzo(
                PostAlonzoAuxiliaryData {
                    metadata: None,
                    native_scripts: Some(vec![auxiliary]),
                    plutus_scripts: None,
                },
            ))),
        };

        let cbor = minicbor::to_vec(tx).expect("failed to encode tx");

        (cbor, native_hash, plutus_hash, auxiliary_hash)
    }

    #[test]
    fn invalid_tx_lists_its_collateral_return_only() {
        // db-sync reads the collateral return of a tx that failed phase-2
        // validation and nothing else
        let (cbor, native, _, _) = tx_cbor(false);
        let tx = MultiEraTx::decode(&cbor).expect("failed to decode tx");
        assert!(!tx.is_valid());
        assert_eq!(tx_script_hashes(&tx), vec![native]);

        // the same tx, valid: no collateral return is produced, and the
        // witnesses count, then the auxiliary data
        let (cbor, _, plutus, auxiliary) = tx_cbor(true);
        let tx = MultiEraTx::decode(&cbor).expect("failed to decode tx");
        assert!(tx.is_valid());
        assert_eq!(tx_script_hashes(&tx), vec![plutus, auxiliary]);
    }

    fn script(byte: u8) -> Hash<28> {
        Hash::new([byte; 28])
    }

    /// Apply a block's registry deltas, the way the roll does after the crawl.
    fn commit(state: &MemoryStateStore, deltas: WorkDeltas) {
        let writer = state.start_writer().unwrap();

        for (NsKey(ns, key), deltas) in deltas.entities {
            let mut entity = None;

            for mut delta in deltas {
                delta.apply(&mut entity);
            }

            writer.save_entity_typed(ns, &key, entity.as_ref()).unwrap();
        }

        writer.commit().unwrap();
    }

    fn listing(state: &MemoryStateStore) -> Vec<Hash<28>> {
        state
            .iter_entities_typed::<ScriptSeqState>(ScriptSeqState::NS, None)
            .unwrap()
            .map(|row| row.unwrap().1.script_hash)
            .collect()
    }

    #[test]
    fn a_batch_registers_each_script_once_and_in_order() {
        let state = MemoryStateStore::new();

        // three blocks of one batch: the second repeats a script of the first,
        // and nothing is committed in between
        let carried = vec![
            vec![script(1), script(2)],
            vec![script(2), script(3)],
            vec![],
        ];

        let mut context = ScriptRegistryContext::load(&state, &carried).unwrap();
        let mut batch = Vec::new();

        for (slot, carried) in carried.into_iter().enumerate() {
            let mut visitor = ScriptRegistryVisitor::new(std::mem::take(&mut context), carried);
            let mut deltas = WorkDeltas::default();

            visitor.register(&mut deltas, slot as u64);

            context = visitor.take_context();
            batch.push(deltas);
        }

        assert_eq!(batch[0].entities.len(), 4);
        assert_eq!(batch[1].entities.len(), 2);
        assert!(batch[2].entities.is_empty());

        for deltas in batch {
            commit(&state, deltas);
        }

        assert_eq!(listing(&state), vec![script(1), script(2), script(3)]);

        // the next batch picks the numbering up and knows what is registered
        let carried = vec![vec![script(3), script(4), script(1)]];
        let context = ScriptRegistryContext::load(&state, &carried).unwrap();
        assert_eq!(context.next_seq, 3);
        assert_eq!(context.known, HashSet::from([script(1), script(3)]));

        let mut visitor = ScriptRegistryVisitor::new(context, carried[0].clone());
        let mut deltas = WorkDeltas::default();
        visitor.register(&mut deltas, 10);
        commit(&state, deltas);

        assert_eq!(
            listing(&state),
            vec![script(1), script(2), script(3), script(4)]
        );

        let row = state
            .read_entity_typed::<ScriptState>(ScriptState::NS, &script(4).as_slice().into())
            .unwrap();
        assert_eq!(
            row,
            Some(ScriptState {
                seq: 3,
                first_slot: 10
            })
        );
    }
}
