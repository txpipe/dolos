use dolos::ledger::{EraCbor, TxoRef};
use itertools::*;
use miette::{Context, IntoDiagnostic};
use pallas::{
    ledger::traverse::{Era, MultiEraInput, MultiEraOutput, MultiEraUpdate},
    ledger::validate::utils::{CertState, Environment as ValidationContext, UTxOs},
};
use std::{borrow::Cow, path::PathBuf, sync::Arc};

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

    let (_, ledger, _) = crate::common::setup_data_stores(config)?;
    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);

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

    let resolved = ledger
        .get_utxos(refs)
        .into_diagnostic()
        .context("resolving utxo")?;

    let mut utxos2 = UTxOs::new();

    for (ref_, body) in resolved.iter() {
        let txin = pallas::ledger::primitives::byron::TxIn::Variant0(
            pallas::codec::utils::CborWrap((ref_.0, ref_.1)),
        );

        let key = MultiEraInput::Byron(
            <Box<Cow<'_, pallas::ledger::primitives::byron::TxIn>>>::from(Cow::Owned(txin)),
        );

        let value = MultiEraOutput::decode(body.0, &body.1)
            .into_diagnostic()
            .context("decoding utxo")?;

        utxos2.insert(key, value);
    }

    let updates = ledger
        .get_pparams(args.epoch)
        .into_diagnostic()
        .context("retrieving pparams")?;

    let updates: Vec<_> = updates
        .iter()
        .map(|EraCbor(era, cbor)| -> miette::Result<MultiEraUpdate> {
            MultiEraUpdate::decode_for_era(*era, cbor).into_diagnostic()
        })
        .try_collect()?;

    let pparams = dolos::ledger::pparams::fold(&genesis, &updates)
        .edge()
        .pparams
        .clone();

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
