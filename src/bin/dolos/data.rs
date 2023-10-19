#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

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
