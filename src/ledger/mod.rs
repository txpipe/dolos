use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraBlock;
use std::collections::{HashMap, HashSet};

pub mod pparams;
pub mod store;
//pub mod validate;

pub type Era = u16;
pub type TxHash = Hash<32>;
pub type TxoIdx = u32;
pub type BlockSlot = u64;
pub type BlockHash = Hash<32>;
pub type TxOrder = usize;

#[derive(Debug)]
pub struct UtxoBody(Era, Vec<u8>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct TxoRef(TxHash, TxoIdx);

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ChainPoint(pub BlockSlot, pub BlockHash);

#[derive(Debug)]
pub struct PParamsBody(pub Era, pub Vec<u8>);

pub enum BrokenInvariant {
    MissingUtxo(TxoRef),
}

pub trait LedgerSlice<'a> {
    fn get_tip(&'a self) -> ChainPoint;
    fn get_utxo(&'a self, txo_ref: &TxoRef) -> Option<&'a UtxoBody>;
    fn pparams(&'a self, until: BlockSlot) -> Vec<PParamsBody>;
}

#[derive(Default, Debug)]
pub struct LedgerDelta {
    pub new_position: Option<ChainPoint>,
    pub undone_position: Option<ChainPoint>,
    pub produced_utxo: HashMap<TxoRef, UtxoBody>,
    pub consumed_utxo: HashSet<TxoRef>,
    pub recovered_stxi: HashSet<TxoRef>,
    pub undone_utxo: HashSet<TxoRef>,
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
pub fn compute_delta(block: &MultiEraBlock) -> LedgerDelta {
    let mut delta = LedgerDelta::default();

    delta.new_position = Some(ChainPoint(block.slot(), block.hash()));

    let txs = block.txs();

    for tx in txs.iter() {
        for (idx, produced) in tx.produces() {
            let uxto_ref = TxoRef(tx.hash(), idx as u32);
            let utxo_body = UtxoBody(tx.era().into(), produced.encode());

            delta.produced_utxo.insert(uxto_ref, utxo_body);
        }

        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);
            delta.consumed_utxo.insert(stxi_ref);
        }

        if let Some(update) = tx.update() {
            delta
                .new_pparams
                .push(PParamsBody(tx.era().into(), update.encode()));
        }
    }

    // check block-level updates (because of f#!@#@ byron)
    if let Some(update) = block.update() {
        delta
            .new_pparams
            .push(PParamsBody(block.era().into(), update.encode()));
    }

    delta
}

pub fn compute_undo_delta(block: &MultiEraBlock) -> LedgerDelta {
    let mut delta = LedgerDelta::default();

    delta.undone_position = Some(ChainPoint(block.slot(), block.hash()));

    for tx in block.txs() {
        for (idx, _) in tx.produces() {
            let utxo_ref = TxoRef(tx.hash(), idx as u32);
            delta.undone_utxo.insert(utxo_ref);
        }
    }

    for tx in block.txs() {
        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);
            delta.recovered_stxi.insert(stxi_ref);
        }
    }

    delta
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

        let utxo_body = pallas::codec::minicbor::to_vec(utxo_body).unwrap();
        let utxo_body = UtxoBody(0, utxo_body);

        delta.produced_utxo.insert(utxo_ref, utxo_body);
    }

    delta
}

#[cfg(test)]
mod tests {
    use pallas::crypto::hash::Hash;
    use std::str::FromStr;

    use super::*;

    fn assert_genesis_utxo_exists(db: &LedgerDelta, tx_hex: &str, addr_base58: &str, amount: u64) {
        let tx = Hash::<32>::from_str(tx_hex).unwrap();

        let utxo_body = db.produced_utxo.get(&TxoRef(tx, 0));

        assert!(utxo_body.is_some(), "utxo not found");
        let UtxoBody(era, cbor) = utxo_body.unwrap();

        assert_eq!(*era, 0);

        let txout: Result<pallas::ledger::primitives::byron::TxOut, _> =
            pallas::codec::minicbor::decode(&cbor);

        assert!(txout.is_ok(), "can't parse utxo cbor");
        let txout = txout.unwrap();

        assert_eq!(txout.amount, amount, "utxo amount doesn't match");

        let addr = pallas::ledger::addresses::ByronAddress::new(
            txout.address.payload.as_ref(),
            txout.address.crc,
        );

        assert_eq!(addr.to_base58(), addr_base58);
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

        let delta = super::compute_delta(&block);

        for tx in block.txs() {
            for input in tx.consumes() {
                // assert that consumed utxos are no longer in the unspent set
                let consumed = delta
                    .consumed_utxo
                    .contains(&TxoRef(*input.hash(), input.index() as u32));

                assert!(consumed);
            }

            for (idx, expected) in tx.produces() {
                let utxo = delta.produced_utxo.get(&TxoRef(tx.hash(), idx as u32));

                match utxo {
                    Some(UtxoBody(era, cbor)) => {
                        assert_eq!(
                            tx.era() as u16,
                            *era,
                            "expected produced utxo era doesn't match"
                        );

                        let expected_cbor = expected.encode();

                        assert_eq!(
                            &expected_cbor, cbor,
                            "expected produced utxo cbor doesn't match"
                        );
                    }
                    None => panic!("expected produced utxo is not in not in delta"),
                }
            }
        }
    }

    #[test]
    fn test_undo_block() {
        // nice block with several txs, it includes chaining edge case
        let cbor = load_test_block("alonzo27.block");

        let block = MultiEraBlock::decode(&cbor).unwrap();

        let apply = super::compute_delta(&block);
        let undo = super::compute_undo_delta(&block);

        for (produced, _) in apply.produced_utxo.iter() {
            assert!(undo.undone_utxo.contains(produced));
        }

        for consumed in apply.consumed_utxo.iter() {
            assert!(undo.recovered_stxi.contains(consumed));
        }

        assert_eq!(apply.new_position, undo.undone_position);
    }
}
