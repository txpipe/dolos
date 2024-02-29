#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, _, _) = crate::common::open_data_stores(&config)?;

    dolos::submit::serve(config.submit, wal, true).await?;

    Ok(())
}
