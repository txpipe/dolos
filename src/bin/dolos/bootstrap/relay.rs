use dolos_core::config::RootConfig;
use tracing::info;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args, Default, Clone)]
pub struct Args {}

pub fn run(_config: &RootConfig, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    info!("data initialized to sync from origin");

    Ok(())
}
