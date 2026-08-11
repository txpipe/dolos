//! What a published stele contains, without pulling it.
//!
//! The first thing an operator does when a restore behaves oddly, priced
//! accordingly: two small GETs — the manifest and the config blob — and no
//! layer is fetched. Everything a pull verifies is verified on the way in, so
//! what is printed is a stele, not merely a manifest that looked like one.
//!
//! ## `--json` is the document, not a report
//!
//! It emits the canonical inscription verbatim on stdout — the exact bytes the
//! identity is the sha256 of. That makes it both the document a verifier
//! hashes and the thing that pipes into `digest --chain-from`, and it is why
//! nothing is appended to it, a newline included.
//!
//! ## No signers column
//!
//! ADR-004 lists signers among what an inspection reports. There are no
//! signatures to list until Phase 5 delivers `sign`, so the field is simply
//! absent — stated here and in the help, rather than printed as an empty
//! column that reads as "none".

use clap::Parser;
use dolos_core::config::RootConfig;
use dolos_snapshot::registry::{self, Point, Repository};
use miette::{Context as _, IntoDiagnostic as _};

#[derive(Debug, Parser)]
pub struct Args {
    /// OCI repository holding the stele, e.g.
    /// `oci://ghcr.io/txpipe/dolos-mainnet`
    #[arg(long, value_name = "OCI_URL")]
    repo: Repository,

    /// which stele to inspect: `latest`, or `epoch-N` for the stele published
    /// at the end of epoch N
    #[arg(long, value_name = "POINT", default_value = "latest")]
    point: Point,

    /// talk to the repository over plaintext HTTP rather than HTTPS; for a
    /// registry on a loopback address or a mirror inside a cluster, and for
    /// nothing reachable from outside one
    #[arg(long, action)]
    insecure: bool,

    /// emit the canonical inscription verbatim instead of the report: the
    /// exact bytes the identity is the sha256 of, which is what
    /// `digest --chain-from` takes. Signers are not listed anywhere yet —
    /// signatures arrive with phase 5
    #[arg(long, action)]
    json: bool,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let auth = crate::common::stele_registry_auth(&config.stelae)?;

    // An inspection reads two small blobs into memory, so nothing is staged
    // and this directory is never created here.
    let scratch = crate::common::stele_scratch_dir(&config.storage, None);

    let registry = registry::open(&args.repo, args.insecure, auth, scratch)
        .into_diagnostic()
        .context("opening the repository")?;

    let inspected = registry::inspect(&registry, args.point)
        .into_diagnostic()
        .context("reading the stele")?;

    if args.json {
        use std::io::Write as _;

        // The bytes, not a re-encoding: anything pretty-printed or
        // newline-terminated would hash to an identity nobody published and
        // be refused by `digest --chain-from`.
        let canonical = inspected.inscription.canonicalize().into_diagnostic()?;

        // Flushed explicitly: with no trailing newline a line-buffered stdout
        // still holds the tail of the document here, and the flush at process
        // exit discards its error — `inspect --json > stele.json` onto a full
        // disk or a closed pipe would otherwise write a truncated inscription
        // and exit zero.
        let mut stdout = std::io::stdout();

        stdout
            .write_all(&canonical)
            .into_diagnostic()
            .context("writing the inscription to stdout")?;

        stdout
            .flush()
            .into_diagnostic()
            .context("flushing the inscription to stdout")?;

        return Ok(());
    }

    let inscription = &inspected.inscription;

    let position = dolos_snapshot::read_position(&inscription.position)
        .into_diagnostic()
        .context("reading the stele's position")?;

    println!("{} at {}", args.repo, args.point);
    println!("identity: {}", inspected.identity);
    println!(
        "profile:  {} v{}",
        inscription.profile.name, inscription.profile.version
    );
    println!("sequence: {}", inscription.sequence);
    println!(
        "position: {} (magic {}), epoch {}, {}",
        position.network.name(),
        position.network.magic(),
        position.epoch,
        position.point,
    );
    println!(
        "compression: {} level {}",
        inscription.compression.algo, inscription.compression.level
    );

    match (inscription.history.first(), inscription.history.last()) {
        (Some(first), Some(last)) if first.sequence != last.sequence => println!(
            "history:  {} entries, sequence {} ({}) through sequence {} ({})",
            inscription.history.len(),
            first.sequence,
            first.inscription_digest,
            last.sequence,
            last.inscription_digest,
        ),
        (Some(only), _) => println!(
            "history:  1 entry, sequence {} ({})",
            only.sequence, only.inscription_digest,
        ),
        _ => println!("history:  none; this stele starts a chain"),
    }

    println!("layers:   {}", inscription.layers.len());

    let mut records = 0u64;

    for (descriptor, compressed) in inscription.layers.iter().zip(&inspected.compressed) {
        records += descriptor.records;

        // The compressed size is the manifest's claim; a manifest claiming
        // something impossible prints as unknown rather than as a number.
        let compressed = match compressed {
            Some(bytes) => bytes.to_string(),
            None => "?".to_owned(),
        };

        println!(
            "  {:<8} {:>10} records {:>14} bytes {:>14} compressed  {}",
            descriptor.kind,
            descriptor.records,
            descriptor.uncompressed_size,
            compressed,
            descriptor.scope,
        );
    }

    println!(
        "totals:   {} records, {} uncompressed bytes, {} compressed bytes",
        records,
        inscription.uncompressed_size(),
        inspected.total_compressed,
    );

    Ok(())
}
