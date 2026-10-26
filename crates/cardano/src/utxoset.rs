use dolos_core::*;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::owned::OwnedMultiEraOutput;

/// The value a block's output is stored with: the output, and the position of
/// the transaction at `tx_index` in the block.
fn produced_value(block: &MultiEraBlock, tx_index: usize, output: MultiEraOutput) -> UtxoValue {
    let position = UtxoPosition::of_tx(block.slot(), tx_index);
    UtxoValue::new(output.into(), Some(position))
}

pub fn compute_block_dependencies(block: &MultiEraBlock, loaded: &mut RawUtxoMap) -> Vec<TxoRef> {
    let txs = block.txs();

    // TODO: turn this into "referenced utxos" instead of just consumed.

    // add all produced utxos to the loaded map
    for (tx_index, tx) in txs.iter().enumerate() {
        let tx_hash = tx.hash();

        for (idx, utxo) in tx.produces() {
            let utxo_ref = TxoRef(tx_hash, idx as u32);
            let value = produced_value(block, tx_index, utxo);
            loaded.insert(utxo_ref, Arc::new(value));
        }
    }

    // find all consumed utxos in the block
    let consumed: HashSet<_> = txs
        .iter()
        .flat_map(MultiEraTx::consumes)
        .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
        .collect();

    // find all missing utxos that are not already in the loaded map

    consumed
        .into_iter()
        .filter(|x| !loaded.contains_key(x))
        .collect::<Vec<_>>()
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
/// return an error if any of the assumed invariants have been broken in the
/// process of computing the delta, but it doesn't provide a comprehensive
/// validation of the ledger rules.
///
/// Each produced UTxO carries its creation position: the block's slot and its
/// transaction's index in the block. A consumed UTxO keeps the value it was
/// loaded with, position included.
pub fn compute_apply_delta(
    block: &MultiEraBlock,
    loaded: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<UtxoSetDelta, BrokenInvariant> {
    let mut delta = UtxoSetDelta::default();

    for (tx_index, tx) in block.txs().iter().enumerate() {
        let tx_hash = tx.hash();

        for (idx, produced) in tx.produces() {
            let uxto_ref = TxoRef(tx_hash, idx as u32);
            let value = produced_value(block, tx_index, produced);
            delta.produced_utxo.insert(uxto_ref, Arc::new(value));
        }

        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);

            let stxi_body = loaded
                .get(&stxi_ref)
                .ok_or_else(|| BrokenInvariant::MissingUtxo(stxi_ref.clone()))?;

            let stxi_body_arc = stxi_body.borrow_owner().clone();

            delta.consumed_utxo.insert(stxi_ref, stxi_body_arc);
        }
    }

    Ok(delta)
}

/// The reverse of [`compute_apply_delta`]: the block's outputs are undone at
/// the position they were produced at, and the UTxOs it consumed are recovered
/// with the value the WAL kept for them, position included.
pub fn compute_undo_delta(
    block: &MultiEraBlock,
    context: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<UtxoSetDelta, BrokenInvariant> {
    let mut delta = UtxoSetDelta::default();

    let txs = block.txs();

    for (tx_index, tx) in txs.iter().enumerate() {
        let tx_hash = tx.hash();

        for (idx, body) in tx.produces() {
            let utxo_ref = TxoRef(tx_hash, idx as u32);
            let value = produced_value(block, tx_index, body);
            delta.undone_utxo.insert(utxo_ref, Arc::new(value));
        }
    }

    for tx in txs.iter() {
        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);

            let stxi_body = context
                .get(&stxi_ref)
                .ok_or_else(|| BrokenInvariant::MissingUtxo(stxi_ref.clone()))?;

            let stxi_body_arc = stxi_body.borrow_owner().clone();

            delta.recovered_stxi.insert(stxi_ref, stxi_body_arc);
        }
    }

    Ok(delta)
}

/// The genesis UTxOs. No block produced them, so they carry no position.
pub fn compute_origin_delta(genesis: &Genesis) -> UtxoSetDelta {
    let mut delta = UtxoSetDelta::default();

    // byron
    {
        let utxos = pallas::interop::hardano::configs::byron::genesis_utxos(&genesis.byron);

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
            let value = UtxoValue::new(utxo_body.into(), None);
            delta.produced_utxo.insert(utxo_ref, Arc::new(value));
        }
    }
    // shelley
    {
        let utxos = pallas::interop::hardano::configs::shelley::shelley_utxos(&genesis.shelley);

        for (tx, addr, amount) in utxos {
            let utxo_ref = TxoRef(tx, 0);
            let utxo_body = pallas::ledger::primitives::alonzo::TransactionOutput {
                address: addr.to_vec().into(),
                amount: pallas::ledger::primitives::alonzo::Value::Coin(amount),
                datum_hash: None,
            };
            let utxo_body =
                pallas::ledger::primitives::conway::TransactionOutput::Legacy(utxo_body.into());

            let utxo_body = MultiEraOutput::from_conway(&utxo_body).to_owned();
            let value = UtxoValue::new(utxo_body.into(), None);

            delta.produced_utxo.insert(utxo_ref, Arc::new(value));
        }
    }

    delta
}

/// The configured UTxOs. No block produced them, so they carry no position.
pub fn build_custom_utxos_delta(
    config: &crate::CardanoLogicConfig,
) -> Result<UtxoSetDelta, ChainError> {
    let mut delta = UtxoSetDelta::default();

    for utxo in config.custom_utxos.iter() {
        let era = utxo
            .era
            .unwrap_or(pallas::ledger::traverse::Era::Conway.into());

        let value = UtxoValue::new(EraCbor(era, utxo.cbor.clone()), None);

        delta
            .produced_utxo
            .insert(utxo.ref_.clone(), Arc::new(value));
    }

    Ok(delta)
}

#[cfg(test)]
mod tests {
    use pallas::{
        crypto::hash::Hash,
        ledger::{addresses::Address, traverse::MultiEraTx},
    };
    use std::str::FromStr;

    use super::*;

    /// The block's inputs, each stored at a position of its own: slot 1, tx
    /// index the input's rank among the block's inputs.
    fn fake_slice_for_block(block: &MultiEraBlock) -> HashMap<TxoRef, OwnedMultiEraOutput> {
        let valid_utxo = block
            .txs()
            .first()
            .unwrap()
            .produces()
            .first()
            .unwrap()
            .1
            .encode();
        let consumed: HashMap<_, _> = block
            .txs()
            .iter()
            .flat_map(MultiEraTx::consumes)
            .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
            .enumerate()
            .map(|(rank, key)| {
                (
                    key,
                    OwnedMultiEraOutput::decode(Arc::new(UtxoValue::new(
                        EraCbor(block.era().into(), valid_utxo.clone()),
                        Some(UtxoPosition::of_tx(1, rank)),
                    )))
                    .unwrap(),
                )
            })
            .collect();

        consumed
    }

    fn assert_genesis_utxo_exists(db: &UtxoSetDelta, tx_hex: &str, addr_base58: &str, amount: u64) {
        let tx = Hash::<32>::from_str(tx_hex).unwrap();

        let utxo_body = db.produced_utxo.get(&TxoRef(tx, 0));

        assert!(utxo_body.is_some(), "utxo not found");
        let utxo_body = utxo_body.unwrap();
        let utxo_body = MultiEraOutput::try_from(utxo_body.as_ref()).unwrap();

        assert_eq!(utxo_body.era(), pallas::ledger::traverse::Era::Byron);

        assert_eq!(
            utxo_body.value().coin(),
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
            .join("test_data")
            .join("mainnet")
            .join("genesis");

        let genesis = crate::utils::load_genesis(&path);

        let delta = compute_origin_delta(&genesis);

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
            .join("test_data")
            .join("preview")
            .join("genesis");

        let genesis = crate::utils::load_genesis(&path);

        let delta = compute_origin_delta(&genesis);

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
        let context = fake_slice_for_block(&block);

        let delta = super::compute_apply_delta(&block, &context).unwrap();

        for tx in block.txs() {
            for input in tx.consumes() {
                let consumed = delta
                    .consumed_utxo
                    .contains_key(&TxoRef(*input.hash(), input.index() as u32));

                assert!(consumed);
            }

            for (idx, expected) in tx.produces() {
                let utxo = delta.produced_utxo.get(&TxoRef(tx.hash(), idx as u32));
                let utxo = utxo.unwrap();
                let utxo = MultiEraOutput::try_from(utxo.as_ref()).unwrap();
                assert_eq!(utxo, expected);
            }
        }
    }

    /// Every produced UTxO is stored at its block's slot and its tx's index in
    /// the block; every consumed one keeps the value, position included, it
    /// was loaded with.
    #[test]
    fn apply_gives_each_produced_utxo_its_tx_position() {
        let cbor = load_test_block("alonzo27.block");
        let block = MultiEraBlock::decode(&cbor).unwrap();
        let context = fake_slice_for_block(&block);

        assert!(block.txs().len() > 1, "the block needs several txs");

        let delta = super::compute_apply_delta(&block, &context).unwrap();

        for (tx_index, tx) in block.txs().iter().enumerate() {
            let expected = UtxoPosition::of_tx(block.slot(), tx_index);

            for (idx, _) in tx.produces() {
                let value = &delta.produced_utxo[&TxoRef(tx.hash(), idx as u32)];
                assert_eq!(value.position(), Some(expected));
            }

            for input in tx.consumes() {
                let txo_ref = TxoRef(*input.hash(), input.index() as u32);
                let loaded = context[&txo_ref].borrow_owner();
                assert_eq!(delta.consumed_utxo[&txo_ref], *loaded);
            }
        }
    }

    /// The dependencies a block resolves itself — its own outputs, which a
    /// later tx in the batch may spend — carry the same position the apply
    /// path stores them at.
    #[test]
    fn block_dependencies_carry_the_produced_positions() {
        let cbor = load_test_block("alonzo27.block");
        let block = MultiEraBlock::decode(&cbor).unwrap();
        let context = fake_slice_for_block(&block);

        let mut loaded = RawUtxoMap::new();
        compute_block_dependencies(&block, &mut loaded);

        let delta = super::compute_apply_delta(&block, &context).unwrap();

        for (txo_ref, value) in &delta.produced_utxo {
            assert_eq!(&loaded[txo_ref], value);
        }
    }

    /// Undo takes the block's outputs back out at the position apply stored
    /// them at, and restores each consumed UTxO at its original position.
    #[test]
    fn undo_restores_consumed_utxos_at_their_original_position() {
        let cbor = load_test_block("alonzo27.block");
        let block = MultiEraBlock::decode(&cbor).unwrap();
        let context = fake_slice_for_block(&block);

        let apply = super::compute_apply_delta(&block, &context).unwrap();
        let undo = super::compute_undo_delta(&block, &context).unwrap();

        assert_eq!(undo.undone_utxo, apply.produced_utxo);
        assert_eq!(undo.recovered_stxi, apply.consumed_utxo);

        let positions: HashSet<_> = undo
            .recovered_stxi
            .values()
            .map(|value| value.position())
            .collect();
        assert!(
            positions.len() > 1,
            "the inputs need distinct positions to tell them apart"
        );
    }

    /// No block produced the genesis UTxOs (AVVM ones included) or the
    /// configured ones, so none has a position.
    #[test]
    fn origin_and_configured_utxos_carry_no_position() {
        for network in ["mainnet", "preview"] {
            let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
                .join("test_data")
                .join(network)
                .join("genesis");

            let genesis = crate::utils::load_genesis(&path);
            let delta = compute_origin_delta(&genesis);

            assert!(!delta.produced_utxo.is_empty());
            assert!(delta
                .produced_utxo
                .values()
                .all(|value| value.position().is_none()));
        }

        let config = crate::CardanoLogicConfig {
            custom_utxos: vec![dolos_core::config::CustomUtxo {
                ref_: TxoRef(Hash::from([7; 32]), 1),
                era: None,
                cbor: vec![0x82, 0x00, 0x00],
            }],
            ..Default::default()
        };

        let delta = build_custom_utxos_delta(&config).unwrap();
        assert_eq!(delta.produced_utxo.len(), 1);
        assert!(delta
            .produced_utxo
            .values()
            .all(|value| value.position().is_none()));
    }

    #[test]
    fn test_undo_block() {
        // nice block with several txs, it includes chaining edge case
        let cbor = load_test_block("alonzo27.block");
        let block = MultiEraBlock::decode(&cbor).unwrap();
        let context = fake_slice_for_block(&block);

        let apply = super::compute_apply_delta(&block, &context).unwrap();
        let undo = super::compute_undo_delta(&block, &context).unwrap();

        for (produced, _) in apply.produced_utxo.iter() {
            assert!(undo.undone_utxo.contains_key(produced));
        }

        for (consumed, _) in apply.consumed_utxo.iter() {
            assert!(undo.recovered_stxi.contains_key(consumed));
        }
    }
}
