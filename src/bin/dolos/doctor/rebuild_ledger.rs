use dolos::{
    ledger::{self, pparams::Genesis, LedgerDelta},
    state::LedgerStore,
    wal::{self, LogEntry, LogValue, WalReader as _},
};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::debug;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args;

pub fn run(config: &crate::Config, _args: &Args, feedback: &Feedback) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("rebuilding ledger");

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let wal = crate::common::open_wal(config).context("opening WAL store")?;

    let light = dolos::state::redb::LedgerStore::in_memory_v2_light()
        .into_diagnostic()
        .context("creating in-memory state store")?;

    let light = dolos::state::LedgerStore::Redb(light);

    if light
        .is_empty()
        .into_diagnostic()
        .context("checking empty state")?
    {
        debug!("importing genesis");

        let delta = dolos::ledger::compute_origin_delta(&genesis);

        light
            .apply(&[delta])
            .into_diagnostic()
            .context("applying origin utxos")?;
    }

    let (_, tip) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    match tip {
        wal::ChainPoint::Origin => progress.set_length(0),
        wal::ChainPoint::Specific(slot, _) => progress.set_length(slot),
    }

    let wal_seq = light
        .cursor()
        .into_diagnostic()
        .context("finding ledger cursor")?
        .map(|ledger::ChainPoint(s, h)| wal.assert_point(&wal::ChainPoint::Specific(s, h)))
        .transpose()
        .into_diagnostic()
        .context("locating wal sequence")?;

    let remaining = wal
        .crawl_from(wal_seq)
        .into_diagnostic()
        .context("crawling wal")?;

    for chunk in remaining.chunks(100).into_iter() {
        let logentries = chunk.collect_vec();

        apply_logentry_batch(
            &logentries,
            &light,
            &genesis,
            config.storage.max_ledger_history,
        )?;

        logentries.last().inspect(|(_, logvalue)| {
            match logvalue {
                LogValue::Apply(raw) => {
                    let b = MultiEraBlock::decode(&raw.body).unwrap(); // Safe
                    progress.set_position(b.slot())
                }
                LogValue::Undo(raw) => {
                    let b = MultiEraBlock::decode(&raw.body).unwrap(); // Safe
                    progress.set_position(b.slot())
                }
                LogValue::Mark(_) => {}
            }
        });
    }

    let ledger_path = crate::common::define_ledger_path(config).context("finding ledger path")?;

    let disk = dolos::state::redb::LedgerStore::open_v2_light(ledger_path, None)
        .into_diagnostic()
        .context("opening ledger db")?;

    let disk = dolos::state::LedgerStore::Redb(disk);

    let pb = feedback.indeterminate_progress_bar();
    pb.set_message("copying memory ledger into disc");

    light
        .copy(&disk)
        .into_diagnostic()
        .context("copying from memory db into disc")?;

    pb.abandon_with_message("ledger copy to disk finished");

    let pb = feedback.indeterminate_progress_bar();
    pb.set_message("creating indexes");

    disk.upgrade()
        .into_diagnostic()
        .context("creating indexes")?;

    pb.abandon_with_message("indexes created");

    Ok(())
}

pub fn apply_logentry_batch<'a>(
    logentries: impl IntoIterator<Item = &'a LogEntry>,
    store: &LedgerStore,
    genesis: &Genesis,
    max_ledger_history: Option<u64>,
) -> miette::Result<()> {
    let mut deltas: Vec<LedgerDelta> = vec![];

    for (_, logvalue) in logentries {
        match logvalue {
            LogValue::Apply(raw) => {
                let block = MultiEraBlock::decode(&raw.body)
                    .into_diagnostic()
                    .context("decoding block")?;
                let context = dolos::state::load_slice_for_block(&block, store, &deltas)
                    .into_diagnostic()
                    .context("loading context for deltas")?;
                let delta = dolos::ledger::compute_delta(&block, context).into_diagnostic()?;
                deltas.push(delta);
            }
            LogValue::Undo(raw) => {
                let block = MultiEraBlock::decode(&raw.body)
                    .into_diagnostic()
                    .context("decoding block")?;
                let context = dolos::state::load_slice_for_block(&block, store, &deltas)
                    .into_diagnostic()
                    .context("loading context for deltas")?;
                let delta = dolos::ledger::compute_undo_delta(&block, context).into_diagnostic()?;
                deltas.push(delta);
            }
            LogValue::Mark(_) => {}
        }
    }

    store
        .apply(&deltas)
        .into_diagnostic()
        .context("applying deltas to store")?;

    if let Some(delta) = deltas.last() {
        let tip = match &delta.new_position {
            Some(point) => point.0,
            None => delta.undone_position.as_ref().map(|point| point.0).unwrap(),
        };
        let to_finalize = max_ledger_history
            .map(|x| tip - x)
            .unwrap_or(dolos::ledger::lastest_immutable_slot(tip, genesis));
        store
            .finalize(to_finalize)
            .into_diagnostic()
            .context("finalizing chunk on store")?;
    }

    Ok(())
}
