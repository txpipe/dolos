use std::borrow::Cow;

use dolos_core::{
    ChainError, ChainPoint, Domain, EraCbor, Genesis, MempoolAwareUtxoStore, MempoolTx,
};

use pallas::codec::minicbor::{self, data::Type, Decoder};
use pallas::ledger::{
    primitives::{NetworkId, TransactionInput},
    traverse::{MultiEraInput, MultiEraOutput, MultiEraTx},
};
use tracing::debug;

/// Maximum nesting depth of arrays, maps and tags in a received transaction.
pub const MAX_TX_CBOR_DEPTH: usize = 256;

/// Rejects a CBOR item that nests arrays, maps and tags deeper than
/// `max_depth`. Reads the first item only and does not recurse.
pub fn check_cbor_depth(cbor: &[u8], max_depth: usize) -> Result<(), minicbor::decode::Error> {
    let mut d = Decoder::new(cbor);

    // Items still to read in each open container; `None` is indefinite.
    let mut open: Vec<Option<u64>> = Vec::new();

    loop {
        match open.last_mut() {
            Some(Some(0)) => {
                open.pop();
                if open.is_empty() {
                    return Ok(());
                }
                continue;
            }
            Some(None) if d.datatype()? == Type::Break => {
                d.set_position(d.position() + 1);
                open.pop();
                if open.is_empty() {
                    return Ok(());
                }
                continue;
            }
            Some(Some(remaining)) => *remaining -= 1,
            _ => (),
        }

        let position = d.position();

        let items = match d.datatype()? {
            Type::Array | Type::ArrayIndef => d.array()?,
            Type::Map | Type::MapIndef => d.map()?.map(|len| len.saturating_mul(2)),
            Type::Tag => {
                d.tag()?;
                Some(1)
            }
            _ => {
                d.skip()?;
                if open.is_empty() {
                    return Ok(());
                }
                continue;
            }
        };

        if open.len() == max_depth {
            return Err(minicbor::decode::Error::message(format!(
                "cbor nests deeper than {max_depth} levels"
            ))
            .at(position));
        }

        open.push(items);
    }
}

pub fn validate_tx<D: Domain>(
    cbor: &[u8],
    utxos: &MempoolAwareUtxoStore<D>,
    tip: Option<ChainPoint>,
    genesis: &Genesis,
) -> Result<MempoolTx, ChainError> {
    check_cbor_depth(cbor, MAX_TX_CBOR_DEPTH)?;

    let tx = MultiEraTx::decode(cbor)?;
    let hash = tx.hash();

    let pparams = crate::load_effective_pparams::<D>(utxos.state())?;
    let pparams = crate::utils::pparams_to_pallas(&pparams);

    let network_id = match genesis.shelley.network_id.as_ref() {
        Some(network) => match network.as_str() {
            "Mainnet" => Some(NetworkId::Mainnet.into()),
            "Testnet" => Some(NetworkId::Testnet.into()),
            _ => None,
        },
        None => None,
    }
    .unwrap();

    let env = pallas::ledger::validate::utils::Environment {
        prot_params: pparams,
        prot_magic: genesis.shelley.network_magic.unwrap(),
        block_slot: tip.clone().unwrap().slot(),
        network_id,
        acnt: Some(pallas::ledger::validate::utils::AccountState::default()),
    };

    let input_refs = tx.requires().iter().map(From::from).collect();

    let utxos_matches = utxos.get_utxos(input_refs)?;

    let mut pallas_utxos = pallas::ledger::validate::utils::UTxOs::new();

    for (txoref, eracbor) in utxos_matches.iter() {
        let tx_in = TransactionInput {
            transaction_id: txoref.0,
            index: txoref.1.into(),
        };

        let input = MultiEraInput::AlonzoCompatible(<Box<Cow<'_, TransactionInput>>>::from(
            Cow::Owned(tx_in),
        ));

        let eracbor = eracbor.as_ref();

        let output = MultiEraOutput::try_from(eracbor)?;

        pallas_utxos.insert(input, output);
    }

    pallas::ledger::validate::phase1::validate_tx(
        &tx,
        0,
        &env,
        &pallas_utxos,
        &mut pallas::ledger::validate::utils::CertState::default(),
    )?;

    let report = evaluate_tx::<D>(cbor, utxos)?;

    for eval in report.iter() {
        if !eval.success {
            return Err(ChainError::Phase2ValidationRejected(eval.logs.clone()));
        }
    }

    debug!(
        phase1 = true,
        phase2 = true,
        redeemer_count = report.len(),
        "tx validated"
    );

    let era = u16::from(tx.era());
    let payload = EraCbor(era, cbor.into());

    let tx = MempoolTx::new(hash, payload, report);

    Ok(tx)
}

pub fn evaluate_tx<D: Domain>(
    cbor: &[u8],
    utxos: &MempoolAwareUtxoStore<D>,
) -> Result<pallas::ledger::validate::phase2::EvalReport, ChainError> {
    check_cbor_depth(cbor, MAX_TX_CBOR_DEPTH)?;

    let tx = MultiEraTx::decode(cbor)?;

    use dolos_core::TxoRef;

    let eras = crate::eras::load_era_summary::<D>(utxos.state())?;

    let pparams = crate::load_effective_pparams::<D>(utxos.state())?;

    let pparams = crate::utils::pparams_to_pallas(&pparams);

    // `eras` keeps slot_length/timestamp in seconds; the Plutus ScriptContext
    // needs POSIXTime in milliseconds. The conversion lives in the helper.
    let slot_config = eras.to_pallas_slot_config();

    let input_refs = tx.requires().iter().map(From::from).collect();

    let utxos: pallas::ledger::validate::utils::UtxoMap = utxos
        .get_utxos(input_refs)?
        .into_iter()
        .map(|(TxoRef(a, b), eracbor)| {
            let era = eracbor.era().try_into().expect("era out of range");

            (
                pallas::ledger::validate::utils::TxoRef::from((a, b)),
                pallas::ledger::validate::utils::EraCbor::from((era, eracbor.cbor().into())),
            )
        })
        .collect();

    let report = pallas::ledger::validate::phase2::evaluate_tx(&tx, &pparams, &utxos, &slot_config)
        .map_err(|e| ChainError::Phase2EvaluationError(e.to_string()))?;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_arrays(depth: usize) -> Vec<u8> {
        let mut cbor = vec![0x81; depth];
        cbor.push(0x00);
        cbor
    }

    #[test]
    fn depth_at_limit_passes() {
        assert!(check_cbor_depth(&nested_arrays(4), 4).is_ok());
    }

    #[test]
    fn depth_over_limit_fails() {
        assert!(check_cbor_depth(&nested_arrays(5), 4).is_err());
    }

    #[test]
    fn leaf_has_no_depth() {
        assert!(check_cbor_depth(&[0x00], 0).is_ok());
        assert!(check_cbor_depth(&[0x81, 0x00], 0).is_err());
    }

    #[test]
    fn tags_count_as_levels() {
        // 121([121([0])])
        let cbor = [0xd8, 0x79, 0x81, 0xd8, 0x79, 0x81, 0x00];
        assert!(check_cbor_depth(&cbor, 4).is_ok());
        assert!(check_cbor_depth(&cbor, 3).is_err());
    }

    #[test]
    fn maps_count_keys_and_values() {
        // {[0]: {0: [0]}}
        let cbor = [0xa1, 0x81, 0x00, 0xa1, 0x00, 0x81, 0x00];
        assert!(check_cbor_depth(&cbor, 3).is_ok());
        assert!(check_cbor_depth(&cbor, 2).is_err());
    }

    #[test]
    fn indefinite_containers_close_on_break() {
        // [_ [_ 0], [_ ]]
        let cbor = [0x9f, 0x9f, 0x00, 0xff, 0x9f, 0xff, 0xff];
        assert!(check_cbor_depth(&cbor, 2).is_ok());
        assert!(check_cbor_depth(&cbor, 1).is_err());

        // {_ 0: [_ ]}
        let cbor = [0xbf, 0x00, 0x9f, 0xff, 0xff];
        assert!(check_cbor_depth(&cbor, 2).is_ok());
    }

    #[test]
    fn sibling_containers_do_not_add_depth() {
        // [[0], [0], [0]]
        let cbor = [0x83, 0x81, 0x00, 0x81, 0x00, 0x81, 0x00];
        assert!(check_cbor_depth(&cbor, 2).is_ok());
    }

    #[test]
    fn empty_containers_close() {
        // [[], {}, 0]
        let cbor = [0x83, 0x80, 0xa0, 0x00];
        assert!(check_cbor_depth(&cbor, 2).is_ok());
    }

    #[test]
    fn truncated_input_fails() {
        assert!(check_cbor_depth(&[0x82, 0x00], 8).is_err());
        assert!(check_cbor_depth(&[0x9f, 0x00], 8).is_err());
        assert!(check_cbor_depth(&[], 8).is_err());
    }

    #[test]
    fn huge_declared_length_fails_fast() {
        // array of 2^64-1 items with one item present
        let cbor = [0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00];
        assert!(check_cbor_depth(&cbor, 8).is_err());
    }

    #[test]
    fn deep_input_does_not_overflow() {
        assert!(check_cbor_depth(&nested_arrays(1_000_000), MAX_TX_CBOR_DEPTH).is_err());
    }
}
