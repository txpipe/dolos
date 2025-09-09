use dolos_cardano::{
    mutable_slots,
    nonce::get_nh,
    pparams::ChainSummary,
    utils::{epoch_first_slot, get_first_shelley_slot_and_epoch},
};
use dolos_core::{ArchiveStore, Domain, StateStore as _};
use miette::{bail, Context, IntoDiagnostic};
use pallas::{
    crypto::{
        hash::Hash,
        nonce::{generate_epoch_nonce, generate_rolling_nonce},
    },
    ledger::traverse::{MultiEraBlock, MultiEraUpdate},
};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// epoch for which to calculate nonce
    #[arg(long)]
    pub epoch: u64,
}

pub fn get_summary<D: Domain>(domain: &D) -> miette::Result<ChainSummary> {
    let tip = domain
        .state()
        .cursor()
        .into_diagnostic()
        .context("getting tip")?;

    let slot = tip.map(|t| t.slot()).unwrap_or_default();

    let updates = domain
        .state()
        .get_pparams(slot)
        .into_diagnostic()
        .context("getting pparams")?
        .into_iter()
        .map(|eracbor| {
            MultiEraUpdate::try_from(eracbor)
                .into_diagnostic()
                .context("decoding update era cbor")
        })
        .collect::<miette::Result<Vec<MultiEraUpdate>>>()?;

    Ok(dolos_cardano::pparams::fold_with_hacks(
        domain.genesis(),
        &updates,
        slot,
    ))
}

/// Get rolling nonce from last inmutable block before epoch boundary.
pub fn get_nc<D: Domain>(
    first_shelley_slot: u64,
    epoch: u64,
    domain: &D,
    summary: &ChainSummary,
) -> miette::Result<Hash<32>> {
    let mut eta = domain.genesis().shelley_hash;

    let epoch_first_slot = epoch_first_slot(epoch, summary);

    let mutable_slots = mutable_slots(domain.genesis());
    if mutable_slots > epoch_first_slot {
        bail!("invalid");
    }

    let eta_slot = epoch_first_slot - mutable_slots;
    if eta_slot < first_shelley_slot {
        bail!("epoch before shelley");
    }

    for (_, raw) in domain
        .archive()
        .get_range(Some(first_shelley_slot), Some(eta_slot - 1))
        .into_diagnostic()?
    {
        let block = MultiEraBlock::decode(&raw).expect("failed to decode block");
        eta = generate_rolling_nonce(
            eta,
            &block
                .header()
                .nonce_vrf_output()
                .into_diagnostic()?
                .to_vec(),
        );
    }

    Ok(eta)
}

pub fn compute_nonce<D: Domain>(epoch: u64, domain: &D) -> miette::Result<Hash<32>> {
    let summary = get_summary(domain)?;
    let (first_shelley_slot, first_shelley_epoch) =
        get_first_shelley_slot_and_epoch(&summary).expect("failed to determine shelley boundary");

    if first_shelley_epoch == epoch {
        return Ok(domain.genesis().shelley_hash);
    }

    let nc = get_nc(first_shelley_slot, epoch, domain, &summary)?;
    match get_nh(epoch, domain, &summary).expect("failed to get nh") {
        Some(nh) => Ok(generate_epoch_nonce(nc, nh, None)),
        None => Ok(nc),
    }
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;
    let domain = crate::common::setup_domain(config)?;

    let nonce = compute_nonce(args.epoch, &domain)?;
    println!("{}", hex::encode(nonce.as_slice()));

    Ok(())
}
