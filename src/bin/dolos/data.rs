use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .unwrap();

    let (wal, chain, ledger) = crate::common::open_data_stores(config)?;

    if let Some((slot, hash)) = wal.find_tip().unwrap() {
        println!("found WAL tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("WAL is empty");
    }

    println!("---");

    if let Some((slot, hash)) = chain.find_tip().unwrap() {
        println!("found chain tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("chain is empty");
    }

    println!("---");

    if let Some((slot, hash)) = ledger.cursor().unwrap() {
        println!("found ledger tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("chain is empty");
    }

    Ok(())
}
