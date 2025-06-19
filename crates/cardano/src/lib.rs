use pallas::codec::minicbor;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::MultiEraTx;
use std::collections::HashMap;
use std::collections::HashSet;

use dolos_core::*;

pub mod pparams;
//pub mod validate;

pub type Block<'a> = MultiEraBlock<'a>;

pub type UtxoBody<'a> = MultiEraOutput<'a>;

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
    mut ledger: LedgerSlice,
) -> Result<LedgerDelta, BrokenInvariant> {
    let era: u16 = block.era().into();
    let mut delta = LedgerDelta {
        new_position: Some(ChainPoint::Specific(block.slot(), block.hash())),
        new_block: match block {
            MultiEraBlock::Byron(x) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::Conway(x) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::Babbage(x) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::AlonzoCompatible(x, _) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::EpochBoundary(x) => minicbor::to_vec((0_u16, x)).unwrap(),
            _ => todo!(),
        },
        ..Default::default()
    };

    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    for (tx_hash, tx) in txs.iter() {
        delta.seen_txs.insert(*tx_hash);

        for (idx, produced) in tx.produces() {
            let uxto_ref = TxoRef(*tx_hash, idx as u32);

            delta.produced_utxo.insert(uxto_ref, produced.into());
        }

        for consumed in tx.consumes() {
            let stxi_ref = TxoRef(*consumed.hash(), consumed.index() as u32);

            let stxi_body = ledger
                .resolved_inputs
                .remove(&stxi_ref)
                .ok_or_else(|| BrokenInvariant::MissingUtxo(stxi_ref.clone()))?;

            delta.consumed_utxo.insert(stxi_ref, stxi_body);
        }

        if let Some(update) = tx.update() {
            delta
                .new_pparams
                .push(EraCbor(tx.era().into(), update.encode()));
        }
    }

    // check block-level updates (because of f#!@#@ byron)
    if let Some(update) = block.update() {
        delta
            .new_pparams
            .push(EraCbor(block.era().into(), update.encode()));
    }

    Ok(delta)
}

pub fn compute_undo_delta(
    block: &MultiEraBlock,
    mut context: LedgerSlice,
) -> Result<LedgerDelta, BrokenInvariant> {
    let era: u16 = block.era().into();
    let mut delta = LedgerDelta {
        undone_position: Some(ChainPoint::Specific(block.slot(), block.hash())),
        undone_block: match block {
            MultiEraBlock::Byron(x) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::Conway(x) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::Babbage(x) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::AlonzoCompatible(x, _) => minicbor::to_vec((era, x)).unwrap(),
            MultiEraBlock::EpochBoundary(x) => minicbor::to_vec((0_u16, x)).unwrap(),
            _ => todo!(),
        },
        ..Default::default()
    };

    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    for (tx_hash, tx) in txs.iter() {
        delta.unseen_txs.insert(*tx_hash);

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

pub fn compute_origin_delta(genesis: &Genesis) -> LedgerDelta {
    let mut delta = LedgerDelta::default();

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
            delta.produced_utxo.insert(utxo_ref, utxo_body.into());
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
            delta.produced_utxo.insert(utxo_ref, utxo_body.into());
        }
    }

    delta
}

/// Computes the amount of mutable slots in chain.
///
/// Reads the relevant genesis config values and uses the security window
/// guarantee formula from consensus to calculate the latest slot that can be
/// considered immutable.
pub fn mutable_slots(genesis: &Genesis) -> u64 {
    ((3.0 * genesis.byron.protocol_consts.k as f32) / (genesis.shelley.active_slots_coeff.unwrap()))
        as u64
}

/// Computes the latest immutable slot
///
/// Takes the latest known tip, reads the relevant genesis config values and
/// uses the security window guarantee formula from consensus to calculate the
/// latest slot that can be considered immutable. This is used mainly to define
/// which slots can be finalized in the ledger store (aka: compaction).
pub fn lastest_immutable_slot(tip: BlockSlot, genesis: &Genesis) -> BlockSlot {
    tip.saturating_sub(mutable_slots(genesis))
}

pub fn ledger_query_for_block(
    block: &MultiEraBlock,
    unapplied_deltas: &[LedgerDelta],
) -> Result<LedgerQuery, ChainError> {
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

    let required_inputs = consumed
        .into_iter()
        .filter(|x| !consumed_same_block.contains_key(x))
        .filter(|x| !consumed_unapplied_deltas.contains_key(x))
        .collect::<Vec<_>>();

    let extra_inputs = consumed_same_block
        .into_iter()
        .chain(consumed_unapplied_deltas)
        .collect();

    // TODO: include reference scripts and collateral

    Ok(LedgerQuery {
        required_inputs,
        extra_inputs,
    })
}

pub struct ChainLogic;

impl dolos_core::ChainLogic for ChainLogic {
    type Block<'a> = MultiEraBlock<'a>;

    fn decode_block<'a>(block: &'a [u8]) -> Result<Self::Block<'a>, ChainError> {
        MultiEraBlock::decode(block).map_err(ChainError::DecodingError)
    }

    fn lastest_immutable_slot(domain: &impl Domain, tip: BlockSlot) -> BlockSlot {
        lastest_immutable_slot(tip, domain.genesis())
    }

    fn compute_origin_delta<'a>(genesis: &Genesis) -> Result<LedgerDelta, ChainError> {
        let delta = compute_origin_delta(genesis);

        Ok(delta)
    }

    fn compute_apply_delta<'a>(
        ledger: LedgerSlice,
        block: &Self::Block<'a>,
    ) -> Result<LedgerDelta, ChainError> {
        let delta = compute_apply_delta(block, ledger).map_err(ChainError::BrokenInvariant)?;

        Ok(delta)
    }

    fn compute_undo_delta<'a>(
        ledger: LedgerSlice,
        block: &Self::Block<'a>,
    ) -> Result<LedgerDelta, ChainError> {
        let delta = compute_undo_delta(block, ledger).map_err(ChainError::BrokenInvariant)?;

        Ok(delta)
    }

    fn ledger_query_for_block<'a>(
        block: &Self::Block<'a>,
        unapplied_deltas: &[LedgerDelta],
    ) -> Result<LedgerQuery, ChainError> {
        ledger_query_for_block(block, unapplied_deltas)
    }
}

#[cfg(test)]
mod tests {
    use pallas::{
        crypto::hash::Hash,
        ledger::{addresses::Address, traverse::MultiEraTx},
    };
    use std::str::FromStr;

    use super::*;

    fn load_genesis(path: &std::path::Path) -> Genesis {
        let byron = pallas::ledger::configs::byron::from_file(&path.join("byron.json")).unwrap();
        let shelley =
            pallas::ledger::configs::shelley::from_file(&path.join("shelley.json")).unwrap();
        let alonzo = pallas::ledger::configs::alonzo::from_file(&path.join("alonzo.json")).unwrap();
        let conway = pallas::ledger::configs::conway::from_file(&path.join("conway.json")).unwrap();

        Genesis {
            byron,
            shelley,
            alonzo,
            conway,
            force_protocol: None,
        }
    }

    fn fake_slice_for_block(block: &MultiEraBlock) -> LedgerSlice {
        let consumed: HashMap<_, _> = block
            .txs()
            .iter()
            .flat_map(MultiEraTx::consumes)
            .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
            .map(|key| (key, EraCbor(block.era().into(), vec![])))
            .collect();

        LedgerSlice {
            resolved_inputs: consumed,
        }
    }

    fn assert_genesis_utxo_exists(db: &LedgerDelta, tx_hex: &str, addr_base58: &str, amount: u64) {
        let tx = Hash::<32>::from_str(tx_hex).unwrap();

        let utxo_body = db.produced_utxo.get(&TxoRef(tx, 0));

        assert!(utxo_body.is_some(), "utxo not found");
        let utxo_body = MultiEraOutput::try_from(utxo_body.unwrap()).unwrap();

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

        let genesis = load_genesis(&path);

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

        let genesis = load_genesis(&path);

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

        let delta = super::compute_apply_delta(&block, context).unwrap();

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
        let context = fake_slice_for_block(&block);

        let apply = super::compute_apply_delta(&block, context.clone()).unwrap();
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
