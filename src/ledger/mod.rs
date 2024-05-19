use itertools::Itertools;
use pallas::ledger::traverse::{Era, MultiEraBlock, MultiEraTx};
use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraOutput};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

pub mod pparams;
pub mod store;
//pub mod validate;

pub type TxHash = Hash<32>;
pub type TxoIdx = u32;
pub type BlockSlot = u64;
pub type BlockHash = Hash<32>;
pub type TxOrder = usize;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct EraCbor(pub Era, pub Vec<u8>);

impl<'a> From<MultiEraOutput<'a>> for EraCbor {
    fn from(value: MultiEraOutput) -> Self {
        EraCbor(value.era(), value.encode())
    }
}

impl<'a> TryFrom<&'a EraCbor> for MultiEraOutput<'a> {
    type Error = pallas::codec::minicbor::decode::Error;

    fn try_from(value: &'a EraCbor) -> Result<Self, Self::Error> {
        MultiEraOutput::decode(value.0, &value.1)
    }
}

pub type UtxoBody<'a> = MultiEraOutput<'a>;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct TxoRef(pub TxHash, pub TxoIdx);

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ChainPoint(pub BlockSlot, pub BlockHash);

#[derive(Debug)]
pub struct PParamsBody(pub Era, pub Vec<u8>);

#[derive(Debug, Error)]
pub enum BrokenInvariant {
    #[error("missing utxo {0:?}")]
    MissingUtxo(TxoRef),
}

/// A persistent store for ledger state
pub trait LedgerStore {
    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<HashMap<TxoRef, EraCbor>, redb::Error>;
}

/// A slice of the ledger relevant for a specific task
///
/// A ledger slice represents a partial view of the ledger which is optimized
/// for a particular task, such tx validation. In essence, it is a subset of all
/// the UTxO which are being consumed or referenced by a block or tx.
#[derive(Clone)]
pub struct LedgerSlice {
    pub resolved_inputs: HashMap<TxoRef, EraCbor>,
}

pub fn load_slice_for_block<S>(
    block: &MultiEraBlock,
    store: &S,
    unapplied_deltas: &[LedgerDelta],
) -> Result<LedgerSlice, redb::Error>
where
    S: LedgerStore,
{
    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    // TODO: turn this into "referenced utxos" intead of just consumed.
    let consumed: HashSet<_> = txs
        .values()
        .flat_map(MultiEraTx::consumes)
        .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
        .collect();

    let consumed_same_block: HashMap<_, _> = txs
        .iter()
        .flat_map(|(tx_hash, tx)| {
            tx.produces()
                .into_iter()
                .map(|(idx, utxo)| (TxoRef(*tx_hash, idx as u32), utxo.into()))
        })
        .filter(|(x, _)| consumed.contains(x))
        .collect();

    let consumed_unapplied_deltas: HashMap<_, _> = unapplied_deltas
        .iter()
        .flat_map(|d| d.produced_utxo.iter().chain(d.recovered_stxi.iter()))
        .filter(|(x, _)| consumed.contains(x))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let to_fetch = consumed
        .into_iter()
        .filter(|x| !consumed_same_block.contains_key(x))
        .filter(|x| !consumed_unapplied_deltas.contains_key(x))
        .collect_vec();

    let mut resolved_inputs = store.get_utxos(to_fetch)?;
    resolved_inputs.extend(consumed_same_block);
    resolved_inputs.extend(consumed_unapplied_deltas);

    // TODO: include reference scripts and collateral

    Ok(LedgerSlice { resolved_inputs })
}

#[derive(Default, Debug)]
pub struct LedgerDelta {
    pub new_position: Option<ChainPoint>,
    pub undone_position: Option<ChainPoint>,
    pub produced_utxo: HashMap<TxoRef, EraCbor>,
    pub consumed_utxo: HashMap<TxoRef, EraCbor>,
    pub recovered_stxi: HashMap<TxoRef, EraCbor>,
    pub undone_utxo: HashMap<TxoRef, EraCbor>,
    pub new_pparams: Vec<PParamsBody>,
}

/// Computes the ledger delta of applying a particular block.
///
/// The output represent a self-contained description of the changes that need
/// to occur at the data layer to advance the ledger to the new position (new
/// slot).
///
/// The function is pure (stateless and without side-effects) with the goal of
/// allowing the logic to execute as an idem-potent, atomic operation, allowing
/// higher-layers to retry the logic if required.
///
/// This method assumes that the block has already been validated, it will
/// return an error if any of the assumed invariant have been broken in the
/// process of computing the delta, but it own't provide a comprehensive
/// validation of the ledger rules.
pub fn compute_delta(
    block: &MultiEraBlock,
    mut context: LedgerSlice,
) -> Result<LedgerDelta, BrokenInvariant> {
    let mut delta = LedgerDelta {
        new_position: Some(ChainPoint(block.slot(), block.hash())),
        ..Default::default()
    };

    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    for (tx_hash, tx) in txs.iter() {
        for (idx, produced) in tx.produces() {
            let uxto_ref = TxoRef(*tx_hash, idx as u32);

            delta.produced_utxo.insert(uxto_ref, produced.into());
        }

        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);

            let stxi_body = context
                .resolved_inputs
                .remove(&stxi_ref)
                .ok_or_else(|| BrokenInvariant::MissingUtxo(stxi_ref.clone()))?;

            delta.consumed_utxo.insert(stxi_ref, stxi_body);
        }

        if let Some(update) = tx.update() {
            delta
                .new_pparams
                .push(PParamsBody(tx.era(), update.encode()));
        }
    }

    // check block-level updates (because of f#!@#@ byron)
    if let Some(update) = block.update() {
        delta
            .new_pparams
            .push(PParamsBody(block.era(), update.encode()));
    }

    Ok(delta)
}

pub fn compute_undo_delta(
    block: &MultiEraBlock,
    mut context: LedgerSlice,
) -> Result<LedgerDelta, BrokenInvariant> {
    let mut delta = LedgerDelta {
        undone_position: Some(ChainPoint(block.slot(), block.hash())),
        ..Default::default()
    };

    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    for (tx_hash, tx) in txs.iter() {
        for (idx, body) in tx.produces() {
            let utxo_ref = TxoRef(*tx_hash, idx as u32);
            delta.undone_utxo.insert(utxo_ref, body.into());
        }
    }

    for (_, tx) in txs.iter() {
        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);

            let stxi_body = context
                .resolved_inputs
                .remove(&stxi_ref)
                .ok_or_else(|| BrokenInvariant::MissingUtxo(stxi_ref.clone()))?;

            delta.recovered_stxi.insert(stxi_ref, stxi_body);
        }
    }

    Ok(delta)
}

pub fn compute_origin_delta(byron: &pallas::ledger::configs::byron::GenesisFile) -> LedgerDelta {
    let mut delta = LedgerDelta::default();

    let utxos = pallas::ledger::configs::byron::genesis_utxos(byron);

    for (tx, addr, amount) in utxos {
        let utxo_ref = TxoRef(tx, 0);
        let utxo_body = pallas::ledger::primitives::byron::TxOut {
            address: pallas::ledger::primitives::byron::Address {
                payload: addr.payload,
                crc: addr.crc,
            },
            amount,
        };

        let utxo_body = MultiEraOutput::from_byron(&utxo_body).to_owned();
        delta.produced_utxo.insert(utxo_ref, utxo_body.into());
    }

    delta
}

#[cfg(test)]
mod tests {
    use pallas::{crypto::hash::Hash, ledger::addresses::Address};
    use std::str::FromStr;

    use super::*;

    struct MockStore;

    impl LedgerStore for MockStore {
        fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<HashMap<TxoRef, EraCbor>, redb::Error> {
            let mut out = HashMap::new();

            for i in refs {
                out.insert(i, EraCbor(Era::Alonzo, vec![]));
            }

            Ok(out)
        }
    }

    fn assert_genesis_utxo_exists(db: &LedgerDelta, tx_hex: &str, addr_base58: &str, amount: u64) {
        let tx = Hash::<32>::from_str(tx_hex).unwrap();

        let utxo_body = db.produced_utxo.get(&TxoRef(tx, 0));

        assert!(utxo_body.is_some(), "utxo not found");
        let utxo_body = MultiEraOutput::try_from(utxo_body.unwrap()).unwrap();

        assert_eq!(utxo_body.era(), Era::Byron);

        assert_eq!(
            utxo_body.lovelace_amount(),
            amount,
            "utxo amount doesn't match"
        );

        let addr = match utxo_body.address() {
            Ok(Address::Byron(x)) => x.to_base58(),
            _ => panic!(),
        };

        assert_eq!(addr, addr_base58);
    }

    #[test]
    fn test_mainnet_genesis_utxos() {
        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("examples")
            .join("sync-mainnet")
            .join("byron.json");

        let byron = pallas::ledger::configs::byron::from_file(&path).unwrap();
        let delta = compute_origin_delta(&byron);

        assert_genesis_utxo_exists(
            &delta,
            "0ae3da29711600e94a33fb7441d2e76876a9a1e98b5ebdefbf2e3bc535617616",
            "Ae2tdPwUPEZKQuZh2UndEoTKEakMYHGNjJVYmNZgJk2qqgHouxDsA5oT83n",
            2_463_071_701_000_000,
        )
    }

    #[test]
    fn test_preview_genesis_utxos() {
        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("examples")
            .join("sync-preview")
            .join("byron.json");

        let byron = pallas::ledger::configs::byron::from_file(&path).unwrap();
        let delta = compute_origin_delta(&byron);

        assert_genesis_utxo_exists(
            &delta,
            "4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd",
            "FHnt4NL7yPXvDWHa8bVs73UEUdJd64VxWXSFNqetECtYfTd9TtJguJ14Lu3feth",
            30_000_000_000_000_000,
        );
    }

    fn load_test_block(name: &str) -> Vec<u8> {
        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("test_data")
            .join(name);

        let content = std::fs::read_to_string(path).unwrap();
        hex::decode(content).unwrap()
    }

    #[test]
    fn test_apply_delta() {
        // nice block with several txs, it includes chaining edge case
        let cbor = load_test_block("alonzo27.block");

        let block = MultiEraBlock::decode(&cbor).unwrap();

        let store = MockStore;
        let context = super::load_slice_for_block(&block, &store, &[]).unwrap();
        let delta = super::compute_delta(&block, context).unwrap();

        for tx in block.txs() {
            for input in tx.consumes() {
                let consumed = delta
                    .consumed_utxo
                    .contains_key(&TxoRef(*input.hash(), input.index() as u32));

                assert!(consumed);
            }

            for (idx, expected) in tx.produces() {
                let utxo = delta.produced_utxo.get(&TxoRef(tx.hash(), idx as u32));
                let utxo = MultiEraOutput::try_from(utxo.unwrap()).unwrap();
                assert_eq!(utxo, expected);
            }
        }
    }

    #[test]
    fn test_undo_block() {
        // nice block with several txs, it includes chaining edge case
        let cbor = load_test_block("alonzo27.block");

        let block = MultiEraBlock::decode(&cbor).unwrap();

        let store = MockStore;
        let context = super::load_slice_for_block(&block, &store, &[]).unwrap();

        let apply = super::compute_delta(&block, context.clone()).unwrap();
        let undo = super::compute_undo_delta(&block, context).unwrap();

        for (produced, _) in apply.produced_utxo.iter() {
            assert!(undo.undone_utxo.contains_key(produced));
        }

        for (consumed, _) in apply.consumed_utxo.iter() {
            assert!(undo.recovered_stxi.contains_key(consumed));
        }

        assert_eq!(apply.new_position, undo.undone_position);
    }
}
