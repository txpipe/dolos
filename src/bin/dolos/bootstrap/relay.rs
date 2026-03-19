use dolos_core::config::RootConfig;
use tracing::info;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args, Default, Clone)]
pub struct Args {}

pub fn run(_config: &RootConfig, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    // Relay bootstrap is intentionally a no-op on storage.
    // The daemon starts as a fresh node and syncs from genesis via chain-sync.
    info!("relay bootstrap selected — daemon will sync from genesis");
    Ok(())
}
