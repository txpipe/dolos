use dolos::wal::LogSeq;
use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// wal sequence where to start trimming (inclusive)
    #[arg(long)]
    from: Option<LogSeq>,

    /// wal sequence where to stop trimming (inclusive)
    #[arg(long)]
    to: Option<LogSeq>,
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (mut wal, _) = crate::common::open_data_stores(config).context("opening data stores")?;

    wal.remove_range(args.from, args.to)
        .into_diagnostic()
        .context("removing range from WAL")?;

    println!("wal segment trimmed");

    Ok(())
}
