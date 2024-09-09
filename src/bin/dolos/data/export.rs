use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use miette::IntoDiagnostic as _;
use std::fs::File;
use std::path::PathBuf;
use tar::Builder;

#[derive(Debug, Parser)]
pub struct Args {
    /// the path to export to
    #[arg(short, long)]
    output: PathBuf,

    /// exclude wal data
    #[arg(long)]
    exclude_wal: bool,

    /// exclude ledger data
    #[arg(long)]
    exclude_ledger: bool,
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    let export_file = File::create(&args.output).into_diagnostic()?;
    let encoder = GzEncoder::new(export_file, Compression::default());
    let mut archive = Builder::new(encoder);

    let files = match (args.exclude_wal, args.exclude_ledger) {
        (true, false) => vec!["ledger"],
        (false, true) => vec!["wal"],
        (false, false) => vec!["wal", "ledger"],
        (true, true) => miette::bail!("Cannot exclude both wal and ledger"),
    };

    for file in files.iter() {
        let file_path = config.storage.path.join(file);

        archive
            .append_path_with_name(&file_path, file)
            .into_diagnostic()?;
    }

    archive.finish().into_diagnostic()?;

    Ok(())
}
