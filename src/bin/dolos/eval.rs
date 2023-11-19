use miette::{Context, IntoDiagnostic};
use pallas::{
    applying::{validate, Environment, UTxOs},
    ledger::{
        primitives::byron::TxIn,
        traverse::{Era, MultiEraInput, MultiEraOutput, MultiEraTx},
    },
};
use std::path::PathBuf;

use dolos::storage::applydb::ApplyDB;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, short)]
    file: PathBuf,

    #[arg(long, short)]
    era: u16,

    #[arg(long, short)]
    magic: u32,

    #[arg(long, short)]
    slot: u64,
}

type ResolveInputs = Vec<(TxIn, Vec<u8>)>;

pub fn resolve_inputs(tx: &MultiEraTx<'_>, ledger: &ApplyDB) -> miette::Result<ResolveInputs> {
    let mut set = vec![];

    for input in tx.inputs() {
        let hash = input.hash();
        let idx = input.index();

        let bytes = ledger
            .get_utxo(*hash, idx)
            .into_diagnostic()
            .context("fetching utxo from ledger")?
            .ok_or(miette::miette!("utxo not found"))?;

        //TODO: allow to pass extra utxos manually, to mimic what happens when
        // consuming utxos from the same block;

        let txin = pallas::ledger::primitives::byron::TxIn::Variant0(
            pallas::codec::utils::CborWrap((*hash, idx as u32)),
        );

        set.push((txin, bytes));
    }

    Ok(set)
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (_, _, ledger) = crate::common::open_data_stores(config)?;

    let cbor = std::fs::read_to_string(&args.file)
        .into_diagnostic()
        .context("reading tx from file")?;

    let cbor = hex::decode(&cbor)
        .into_diagnostic()
        .context("decoding hex content from file")?;

    let era = Era::try_from(args.era).unwrap();

    let tx = pallas::ledger::traverse::MultiEraTx::decode_for_era(era, &cbor)
        .into_diagnostic()
        .context("decoding tx cbor")?;

    let mut utxos: UTxOs = UTxOs::new();
    let resolved = resolve_inputs(&tx, &ledger)?;

    for (input, output) in resolved.iter() {
        let key = MultiEraInput::from_byron(&input);

        let value = MultiEraOutput::decode(Era::Byron, &output)
            .into_diagnostic()
            .context("decoding utxo cbor")?;

        utxos.insert(key, value);
    }

    let env: Environment = ApplyDB::mk_environment(args.slot, args.magic)
        .into_diagnostic()
        .context("resolving pparams")?;

    validate(&tx, &utxos, &env).unwrap();

    Ok(())
}
