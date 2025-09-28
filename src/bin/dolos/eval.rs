use itertools::*;
use miette::{Context, IntoDiagnostic};
use pallas::{
    ledger::traverse::{Era, MultiEraInput, MultiEraOutput},
    ledger::validate::utils::{CertState, Environment as ValidationContext, UTxOs},
};
use std::{borrow::Cow, path::PathBuf};

use dolos::{
    adapters::DomainAdapter,
    core::{Domain, EraCbor, StateStore as _, TxoRef},
};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, short)]
    file: PathBuf,

    #[arg(long, short)]
    era: u16,

    #[arg(long, short)]
    epoch: u64,

    #[arg(long, short)]
    block_slot: u64,

    #[arg(long, short)]
    network_id: u8,
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let domain = crate::common::setup_domain(config)?;

    let cbor = std::fs::read_to_string(&args.file)
        .into_diagnostic()
        .context("reading tx from file")?;

    let cbor = hex::decode(cbor)
        .into_diagnostic()
        .context("decoding hex content from file")?;

    let era = Era::try_from(args.era).unwrap();

    let tx = pallas::ledger::traverse::MultiEraTx::decode_for_era(era, &cbor)
        .into_diagnostic()
        .context("decoding tx cbor")?;

    let refs = tx
        .consumes()
        .iter()
        .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
        .collect_vec();

    let resolved = domain
        .state()
        .get_utxos(refs)
        .into_diagnostic()
        .context("resolving utxo")?;

    let mut utxos2 = UTxOs::new();

    for (ref_, body) in resolved.iter() {
        let EraCbor(era, cbor) = body.as_ref();

        let era = (*era)
            .try_into()
            .into_diagnostic()
            .context("era out of range")?;

        let txin = pallas::ledger::primitives::byron::TxIn::Variant0(
            pallas::codec::utils::CborWrap((ref_.0, ref_.1)),
        );

        let key = MultiEraInput::Byron(
            <Box<Cow<'_, pallas::ledger::primitives::byron::TxIn>>>::from(Cow::Owned(txin)),
        );

        let value = MultiEraOutput::decode(era, cbor)
            .into_diagnostic()
            .context("decoding utxo")?;

        utxos2.insert(key, value);
    }

    let pparams =
        dolos_cardano::load_effective_pparams::<DomainAdapter>(domain.state(), args.epoch as u32)
            .into_diagnostic()?;

    let pparams = dolos_cardano::utils::pparams_to_pallas(&pparams);

    let context = ValidationContext {
        block_slot: args.block_slot,
        prot_magic: config.upstream.network_magic().unwrap() as u32,
        network_id: args.network_id,
        prot_params: pparams,
        acnt: None,
    };

    let mut cert_state = CertState::default();

    pallas::ledger::validate::phase1::validate_tx(&tx, 0, &context, &utxos2, &mut cert_state)
        .unwrap();

    Ok(())
}
