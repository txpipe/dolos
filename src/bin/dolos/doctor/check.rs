use dolos_core::config::RootConfig;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &RootConfig, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing_error_only()?;
    crate::common::setup_domain(config)?;

    println!("check ok: config and data are valid");

    Ok(())
}
