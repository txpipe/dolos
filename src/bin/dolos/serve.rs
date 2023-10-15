use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let (wal, chain, _) = crate::common::open_data_stores(&config)?;

    dolos::serve::serve(config.serve, wal, chain).await?;

    Ok(())
}
