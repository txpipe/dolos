use std::borrow::Cow;

use dolos_core::{
    ChainError, ChainPoint, Domain, EraCbor, Genesis, MempoolAwareUtxoStore, MempoolError,
    MempoolTx, StateStore,
};

use pallas::ledger::{
    primitives::{NetworkId, TransactionInput},
    traverse::{MultiEraInput, MultiEraOutput, MultiEraTx},
};

use crate::pparams;

pub fn validate_tx<D: Domain>(
    cbor: &[u8],
    utxos: &MempoolAwareUtxoStore<D>,
    tip: Option<ChainPoint>,
    genesis: &Genesis,
) -> Result<MempoolTx, ChainError> {
    let tx = MultiEraTx::decode(cbor)?;

    let updates: Vec<_> = utxos
        .state()
        .get_pparams(tip.as_ref().map(|p| p.slot()).unwrap_or_default())?;

    let updates: Vec<_> = updates
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()?;

    let eras = pparams::fold_with_hacks(genesis, &updates, tip.as_ref().unwrap().slot());

    let era = eras.era_for_slot(tip.clone().as_ref().unwrap().slot());

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
        prot_params: era.pparams.clone(),
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

    let report = evaluate_tx::<D>(&cbor, &utxos, tip, genesis)?;

    for eval in report.iter() {
        if !eval.success {
            return Err(ChainError::ValidationExplicitPhase2Error(eval.logs.clone()));
        }
    }

    let hash = tx.hash();
    let era = u16::from(tx.era());
    let payload = EraCbor(era, cbor.into());

    let tx = MempoolTx {
        hash,
        payload,
        confirmed: false,
        report: Some(report),
    };

    Ok(tx)
}

pub fn evaluate_tx<D: Domain>(
    cbor: &[u8],
    utxos: &MempoolAwareUtxoStore<D>,
    tip: Option<ChainPoint>,
    genesis: &Genesis,
) -> Result<pallas::ledger::validate::phase2::EvalReport, ChainError> {
    let tx = MultiEraTx::decode(cbor)?;

    use dolos_core::{EraCbor, StateStore as _, TxoRef};

    let updates: Vec<_> = utxos
        .state()
        .get_pparams(tip.as_ref().map(|p| p.slot()).unwrap_or_default())?;

    let updates: Vec<_> = updates
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()?;

    let eras = pparams::fold_with_hacks(genesis, &updates, tip.as_ref().unwrap().slot());

    let slot_config = pallas::ledger::validate::phase2::script_context::SlotConfig {
        slot_length: eras.edge().pparams.slot_length(),
        zero_slot: eras.edge().start.slot,
        zero_time: eras.edge().start.timestamp.timestamp().try_into().unwrap(),
    };

    let input_refs = tx.requires().iter().map(From::from).collect();

    let utxos: pallas::ledger::validate::utils::UtxoMap = utxos
        .get_utxos(input_refs)?
        .into_iter()
        .map(|(TxoRef(a, b), EraCbor(c, d))| {
            let era = c.try_into().expect("era out of range");

            (
                pallas::ledger::validate::utils::TxoRef::from((a, b)),
                pallas::ledger::validate::utils::EraCbor::from((era, d)),
            )
        })
        .collect();

    let report = pallas::ledger::validate::phase2::evaluate_tx(
        &tx,
        &eras.edge().pparams,
        &utxos,
        &slot_config,
    )?;

    Ok(report)
}
