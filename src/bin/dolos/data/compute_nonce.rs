use dolos_cardano::{mutable_slots, pparams::ChainSummary, slot_epoch, utils::epoch_first_slot};
use dolos_core::{ArchiveStore, Domain, StateStore as _};
use miette::{bail, Context, IntoDiagnostic};
use pallas::{
    crypto::{
        hash::Hash,
        nonce::{generate_epoch_nonce, generate_rolling_nonce},
    },
    ledger::{
        traverse::{MultiEraBlock, MultiEraUpdate},
        validate::utils::MultiEraProtocolParameters,
    },
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

/// Get first shelley slot
pub fn get_first_shelley_slot_and_epoch(summary: &ChainSummary) -> miette::Result<(u64, u64)> {
    for item in summary.iter_past() {
        if !matches!(item.pparams, MultiEraProtocolParameters::Byron(_)) {
            return Ok((item.start.slot, item.start.epoch));
        }
    }
    bail!("Couldn't find first shelley slot");
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

/// Get the previous block hash of the first block from the previous epoch.
pub fn get_nh<D: Domain>(
    epoch: u64,
    domain: &D,
    summary: &ChainSummary,
) -> miette::Result<Option<Hash<32>>> {
    let slot = epoch_first_slot(epoch - 1, summary);
    let (_, raw) = domain
        .archive()
        .get_range(None, Some(slot - 1))
        .into_diagnostic()?
        .next_back()
        .unwrap();

    let block = MultiEraBlock::decode(&raw).expect("failed to decode block");

    match block {
        MultiEraBlock::Conway(_) => {
            println!("conway");
            Ok(Some(block.hash()))
        }
        _ => Ok(block.header().previous_hash()),
    }
}

pub fn compute_nonce<D: Domain>(epoch: u64, domain: &D) -> miette::Result<Hash<32>> {
    let summary = get_summary(domain)?;
    let (first_shelley_slot, first_shelley_epoch) = get_first_shelley_slot_and_epoch(&summary)?;

    if first_shelley_epoch == epoch {
        return Ok(domain.genesis().shelley_hash);
    }

    let nc = get_nc(first_shelley_slot, epoch, domain, &summary)?;
    match get_nh(epoch, domain, &summary)? {
        Some(nh) => {
            dbg!(nh);
            Ok(generate_epoch_nonce(nc, nh, None))
        }
        None => Ok(nc),
    }
}

pub fn compute_nonces<D: Domain>(domain: &D) {
    let summary = get_summary(domain).expect("getting summary");
    let (first_shelley_slot, _) =
        get_first_shelley_slot_and_epoch(&summary).expect("getting shelley data");

    let mut epoch = 1;
    let mut eta = domain.genesis().shelley_hash;
    let mutable_slots = mutable_slots(domain.genesis());
    for (slot, raw) in domain
        .archive()
        .get_range(Some(first_shelley_slot), None)
        .expect("failed to open iterator")
    {
        let new_epoch = slot_epoch(slot + mutable_slots, &summary).0 as u64;
        if new_epoch > epoch {
            epoch = new_epoch;
            let nonce = match get_nh(new_epoch, domain, &summary).expect("getting nh") {
                Some(nh) => generate_epoch_nonce(eta, nh, None),
                None => eta,
            };
            println!("epoch: {new_epoch}, nonce: {}", hex::encode(nonce));
        }
        let block = MultiEraBlock::decode(&raw).expect("failed to decode block");
        eta = generate_rolling_nonce(eta, &block.header().nonce_vrf_output().unwrap().to_vec());
    }
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;
    let domain = crate::common::setup_domain(config)?;

    //let nonce = compute_nonce(args.epoch, &domain)?;
    //println!("{}", hex::encode(nonce.as_slice()));

    compute_nonces(&domain);

    Ok(())
}
