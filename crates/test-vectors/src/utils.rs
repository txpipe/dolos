use dolos_cardano::pallas_extras;
use miette::{bail, Context, IntoDiagnostic};
use pallas::{codec::minicbor, ledger::addresses::Address};

pub fn account_key(address: &str) -> miette::Result<Vec<u8>> {
    let address = pallas::ledger::addresses::Address::from_bech32(address)
        .into_diagnostic()
        .context("decoding address")?;

    let address = match address {
        Address::Shelley(x) => pallas_extras::shelley_address_to_stake_address(&x),
        Address::Stake(x) => Some(x),
        _ => None,
    };

    let Some(address) = address else {
        bail!("invalid address")
    };

    let stake_cred = dolos_cardano::pallas_extras::stake_address_to_cred(&address);

    minicbor::to_vec(&stake_cred)
        .into_diagnostic()
        .context("encoding address")
}
