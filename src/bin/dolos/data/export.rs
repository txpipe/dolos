use clap::Parser;
use dolos_core::config::RootConfig;
use flate2::write::GzEncoder;
use flate2::Compression;
use miette::IntoDiagnostic as _;
use std::fs::File;
use std::path::PathBuf;
use tar::Builder;

#[derive(Debug, Parser)]
pub struct Args {
    /// the path to export to
    #[arg(short, long)]
    output: PathBuf,

    // Whether to include chain
    #[arg(long, action)]
    include_chain: bool,

    // Whether to include state
    #[arg(long, action)]
    include_state: bool,
}

fn prepare_wal(
    mut wal: dolos::adapters::WalAdapter,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let db = wal.db_mut().unwrap();

    pb.set_message("compacting wal");
    db.compact().into_diagnostic()?;

    pb.set_message("checking wal integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

fn prepare_chain(
    archive: &mut dolos_redb3::archive::ArchiveStore,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let db = archive.db_mut();
    pb.set_message("compacting chain");
    db.compact().into_diagnostic()?;

    pb.set_message("checking chain integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

fn prepare_state(
    state: &mut dolos_redb3::state::StateStore,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let db = state.db_mut().unwrap();
    pb.set_message("compacting state");
    db.compact().into_diagnostic()?;

    pb.set_message("checking state integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

pub fn run(
    config: &RootConfig,
    args: &Args,
    feedback: &crate::feedback::Feedback,
) -> miette::Result<()> {
    let pb = feedback.indeterminate_progress_bar();

    let export_file = File::create(&args.output).into_diagnostic()?;
    let encoder = GzEncoder::new(export_file, Compression::default());
    let mut archive = Builder::new(encoder);

    let mut stores = crate::common::setup_data_stores(config)?;

    prepare_wal(stores.wal, &pb)?;

    let root = crate::common::ensure_storage_path(config)?;

    let path = root.join("wal");

    archive
        .append_path_with_name(&path, "wal")
        .into_diagnostic()?;

    if args.include_chain {
        prepare_chain(&mut stores.archive, &pb)?;
        let path = root.join("chain");

        archive
            .append_path_with_name(&path, "chain")
            .into_diagnostic()?;

        pb.set_message("creating archive");
    }

    if args.include_state {
        prepare_state(&mut stores.state, &pb)?;
        let path = root.join("state");

        archive
            .append_path_with_name(&path, "state")
            .into_diagnostic()?;

        pb.set_message("creating state");
    }

    archive.finish().into_diagnostic()?;

    Ok(())
}
