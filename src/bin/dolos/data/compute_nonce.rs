use dolos_cardano::{load_era_summary, utils::nonce_stability_window, EraSummary, Nonces};
use dolos_core::{ArchiveStore, Domain};
use miette::{bail, Context, IntoDiagnostic};
use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraBlock};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// epoch for which to calculate nonce
    #[arg(long)]
    pub epoch: u64,
}

/// Get the previous block hash of the first block from the previous epoch.
pub fn get_nh<D: Domain>(epoch: u64, domain: &D, summary: &EraSummary) -> miette::Result<Hash<32>> {
    let slot = summary.epoch_start(epoch - 1);
    let (_, raw) = domain
        .archive()
        .get_range(None, Some(slot - 1))
        .into_diagnostic()
        .context("getting block range")?
        .next_back()
        .unwrap();

    let block = MultiEraBlock::decode(&raw)
        .into_diagnostic()
        .context("decoding block")?;
    Ok(block.header().previous_hash().unwrap())
}

pub fn compute_nonce<D: Domain>(epoch: u64, domain: &D) -> miette::Result<Hash<32>> {
    let summary = load_era_summary(domain)
        .into_diagnostic()
        .context("loading era summary")?;

    let (first_shelley_epoch, first_shelley_slot) = summary
        .iter_past_with_protocol()
        .find(|(era, _)| **era == 2)
        .map(|(_, summary)| (summary.start.epoch, summary.start.slot))
        .unwrap_or((0, 0));

    if epoch < first_shelley_epoch {
        bail!("Epoch is before shelley, no nonce.")
    }

    if epoch == first_shelley_epoch {
        return Ok(domain.genesis().shelley_hash);
    }

    let (protocol, era) = summary.protocol_and_era_for_epoch(epoch);
    let largest_stable_slot =
        era.epoch_start(epoch) - nonce_stability_window(*protocol, domain.genesis());

    let mut nonces = Nonces::bootstrap(domain.genesis().shelley_hash);

    for (_, raw) in domain
        .archive()
        .get_range(Some(first_shelley_slot), Some(largest_stable_slot))
        .into_diagnostic()
        .context("failed to query archive")?
    {
        let block = MultiEraBlock::decode(&raw)
            .into_diagnostic()
            .context("failed to decode block")?;

        nonces = nonces.roll(true, &block.header().nonce_vrf_output().unwrap(), None);
    }

    nonces = nonces.sweep(
        if epoch == first_shelley_epoch + 1 {
            None
        } else {
            Some(get_nh(epoch, domain, era).expect("failed to get nh"))
        },
        None,
    );

    Ok(nonces.active)
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;
    let domain = crate::common::setup_domain(config)?;

    let nonce = compute_nonce(args.epoch, &domain)?;
    println!("{}", hex::encode(nonce.as_slice()));

    Ok(())
}
