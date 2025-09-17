use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dolos_core::*;

use crate::owned::OwnedMultiEraOutput;

pub fn compute_block_dependencies(block: &MultiEraBlock, loaded: &mut RawUtxoMap) -> Vec<TxoRef> {
    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    // TODO: turn this into "referenced utxos" intead of just consumed.

    // add all produced utxos to the loaded map
    for (tx_hash, tx) in txs.iter() {
        for (idx, utxo) in tx.produces() {
            let utxo_ref = TxoRef(*tx_hash, idx as u32);
            loaded.insert(utxo_ref, Arc::new(utxo.into()));
        }
    }

    // find all consumed utxos in the block
    let consumed: HashSet<_> = txs
        .values()
        .flat_map(MultiEraTx::consumes)
        .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
        .collect();

    // find all missing utxos that are not already in the loaded map
    let missing = consumed
        .into_iter()
        .filter(|x| !loaded.contains_key(x))
        .collect::<Vec<_>>();

    missing
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
pub fn compute_apply_delta(
    block: &MultiEraBlock,
    loaded: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<UtxoSetDelta, BrokenInvariant> {
    let mut delta = UtxoSetDelta::default();

    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    for (tx_hash, tx) in txs.iter() {
        for (idx, produced) in tx.produces() {
            let uxto_ref = TxoRef(*tx_hash, idx as u32);

            delta
                .produced_utxo
                .insert(uxto_ref, Arc::new(produced.into()));
        }

        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);

            let stxi_body = loaded
                .get(&stxi_ref)
                .ok_or_else(|| BrokenInvariant::MissingUtxo(stxi_ref.clone()))?;

            let stxi_body = stxi_body.borrow_owner().clone();
            delta.consumed_utxo.insert(stxi_ref, stxi_body);
        }
    }

    Ok(delta)
}

pub fn compute_undo_delta(
    block: &MultiEraBlock,
    context: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<UtxoSetDelta, BrokenInvariant> {
    let mut delta = UtxoSetDelta::default();

    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    for (tx_hash, tx) in txs.iter() {
        for (idx, body) in tx.produces() {
            let utxo_ref = TxoRef(*tx_hash, idx as u32);
            delta.undone_utxo.insert(utxo_ref, Arc::new(body.into()));
        }
    }

    for (_, tx) in txs.iter() {
        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);

            let stxi_body = context
                .get(&stxi_ref)
                .ok_or_else(|| BrokenInvariant::MissingUtxo(stxi_ref.clone()))?;

            let stxi_body = stxi_body.borrow_owner().clone();
            delta.recovered_stxi.insert(stxi_ref, stxi_body);
        }
    }

    Ok(delta)
}

pub fn compute_origin_delta(genesis: &Genesis) -> UtxoSetDelta {
    let mut delta = UtxoSetDelta::default();

    // byron
    {
        let utxos = pallas::ledger::configs::byron::genesis_utxos(&genesis.byron);

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
            delta
                .produced_utxo
                .insert(utxo_ref, Arc::new(utxo_body.into()));
        }
    }
    // shelley
    {
        let utxos = pallas::ledger::configs::shelley::shelley_utxos(&genesis.shelley);

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

            delta
                .produced_utxo
                .insert(utxo_ref, Arc::new(utxo_body.into()));
        }
    }

    delta
}

#[cfg(test)]
mod tests {
    use pallas::{
        crypto::hash::Hash,
        ledger::{addresses::Address, traverse::MultiEraTx},
    };
    use std::str::FromStr;

    use super::*;

    fn fake_slice_for_block(block: &MultiEraBlock) -> HashMap<TxoRef, OwnedMultiEraOutput> {
        let consumed: HashMap<_, _> = block
            .txs()
            .iter()
            .flat_map(MultiEraTx::consumes)
            .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
            .map(|key| {
                (
                    key,
                    OwnedMultiEraOutput::decode(Arc::new(EraCbor(block.era().into(), vec![])))
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
