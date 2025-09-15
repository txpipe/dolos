use clap::Parser;
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
    chain: dolos::adapters::ArchiveAdapter,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let mut chain = match chain {
        dolos::adapters::ArchiveAdapter::Redb(x) => x,
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

    let stores = crate::common::setup_data_stores(config)?;

    prepare_wal(stores.wal, &pb)?;

    let root = crate::common::ensure_storage_path(config)?;

    let path = root.join("wal");

    archive
        .append_path_with_name(&path, "wal")
        .into_diagnostic()?;

    prepare_chain(stores.archive, &pb)?;

    if args.include_chain {
        let path = root.join("chain");

        archive
            .append_path_with_name(&path, "chain")
            .into_diagnostic()?;

        pb.set_message("creating archive");
    }

    if args.include_state {
        let path = root.join("state");

        archive
            .append_path_with_name(&path, "state")
            .into_diagnostic()?;

        pb.set_message("creating archive");
    }

    archive.finish().into_diagnostic()?;

    Ok(())
}
