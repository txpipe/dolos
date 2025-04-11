use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use miette::{bail, IntoDiagnostic as _};
use std::fs::File;
use std::path::PathBuf;
use tar::Builder;

#[derive(Debug, Parser)]
pub struct Args {
    /// the path to export to
    #[arg(short, long)]
    output: PathBuf,

    // Whether to include ledger
    #[arg(long, action)]
    include_ledger: bool,

    // Whether to include chain
    #[arg(long, action)]
    include_chain: bool,
}

fn prepare_wal(
    mut wal: dolos::wal::redb::WalStore,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let db = wal.db_mut().unwrap();

    pb.set_message("compacting wal");
    db.compact().into_diagnostic()?;

    pb.set_message("checking wal integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

fn prepare_ledger(
    ledger: dolos::state::LedgerStore,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let mut ledger = match ledger {
        dolos::state::LedgerStore::Redb(x) => x,
        _ => miette::bail!("Only redb is supported for export"),
    };

    let db = ledger.db_mut().unwrap();
    pb.set_message("compacting ledger");
    db.compact().into_diagnostic()?;

    pb.set_message("checking ledger integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

fn prepare_chain(
    chain: dolos::chain::ChainStore,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let mut chain = match chain {
        dolos::chain::ChainStore::Redb(x) => x,
        _ => miette::bail!("Only redb is supported for export"),
    };

    let db = chain.db_mut().unwrap();
    pb.set_message("compacting chain");
    db.compact().into_diagnostic()?;

    pb.set_message("checking chain integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

pub fn run(
    config: &crate::Config,
    args: &Args,
    feedback: &crate::feedback::Feedback,
) -> miette::Result<()> {
    let pb = feedback.indeterminate_progress_bar();

    let export_file = File::create(&args.output).into_diagnostic()?;
    let encoder = GzEncoder::new(export_file, Compression::default());
    let mut archive = Builder::new(encoder);

    let (wal, ledger, chain) = crate::common::open_data_stores(config)?;

    prepare_wal(wal, &pb)?;

    let Some(root) = &config.storage.path else {
        bail!("storage path is undefined")
    };
    let path = root.join("wal");

    archive
        .append_path_with_name(&path, "wal")
        .into_diagnostic()?;

    if args.include_ledger {
        prepare_ledger(ledger, &pb)?;
        let path = root.join("ledger");

        archive
            .append_path_with_name(&path, "ledger")
            .into_diagnostic()?;
    }

    prepare_chain(chain, &pb)?;

    if args.include_chain {
        let path = root.join("chain");

        archive
            .append_path_with_name(&path, "chain")
            .into_diagnostic()?;

        pb.set_message("creating archive");
    }

    archive.finish().into_diagnostic()?;

    Ok(())
}
