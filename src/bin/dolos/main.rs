use clap::Parser;
use miette::{IntoDiagnostic, Result};
mod read;
mod sync;

#[derive(Parser)]
#[clap(name = "Dolos")]
#[clap(bin_name = "dolos")]
#[clap(author, version, about, long_about = None)]
enum Dolos {
    Sync(sync::Args),
    Read(read::Args),
}

fn main() -> Result<()> {
    let args = Dolos::parse();

    match args {
        Dolos::Sync(x) => sync::run(&x).into_diagnostic()?,
        Dolos::Read(x) => read::run(&x).into_diagnostic()?,
    };

    Ok(())
}
