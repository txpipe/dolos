use dolos::sync::SyncLimit;
use dolos_core::config::RootConfig;
use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Keep following the chain once the upstream tip is reached
    #[arg(long)]
    follow: bool,

    /// Stop once the ledger reaches this epoch, on its first block: the only
    /// state a Stelae snapshot of that epoch is cut from
    #[arg(long, value_name = "N")]
    stop_epoch: Option<u64>,
}

impl Args {
    fn sync_limit(&self) -> SyncLimit {
        if self.follow {
            SyncLimit::NoLimit
        } else {
            SyncLimit::UntilTip
        }
    }
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    let domain = crate::common::setup_domain_with_stop_epoch(config, args.stop_epoch)?;

    let sync = dolos::sync::pipeline(config, args.sync_limit(), domain)
        .into_diagnostic()
        .context("bootstrapping sync pipeline")?;

    gasket::daemon::Daemon::new(sync).block();

    Ok(())
}
