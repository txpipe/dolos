//! Minimal external host for Dolos snapshot planning, encoding and
//! reproduction.
//!
//! Build without Dolos service features:
//!
//! ```text
//! cargo run --no-default-features --example headless_snapshot -- \
//!   ./dolos.toml ./stele [208,236,290]
//! ```
//!
//! The optional third argument is the publisher's retained state-dump epochs;
//! omitted, the stele retains the tip alone. The destination must not already
//! contain a stele. Progress rendering and destination cleanup remain host
//! responsibilities.
//!
//! The stores must stand on the first block of an epoch, as a
//! `chain.stop_epoch` sync leaves them; any other cut is refused before a
//! layer is written (`crates/snapshot/PROFILE.md`, "Only the boundary cut is
//! published").

use std::{error::Error, path::Path, sync::Arc};

use dolos::engine::ReplayWorkspace;
use dolos_cardano::networks::ResolvedNetwork;
use dolos_snapshot::{
    export::First,
    facade::{Selection, SnapshotSource as _},
    progress::Observer,
};

type AnyError = Box<dyn Error + Send + Sync + 'static>;

fn main() -> Result<(), AnyError> {
    let mut args = std::env::args_os().skip(1);
    let config_path = args.next().ok_or("missing path to dolos.toml")?;
    let destination = args.next().ok_or("missing output directory")?;
    let state_epochs: Vec<u64> = match args.next() {
        Some(list) => list
            .to_str()
            .ok_or("state epochs are not valid UTF-8")?
            .split(',')
            .map(str::parse)
            .collect::<Result<_, _>>()?,
        None => Vec::new(),
    };

    let config = dolos::core::config::load(Some(Path::new(&config_path)))?;
    let genesis = Arc::new(ResolvedNetwork::resolve(&config.chain)?.genesis()?);

    // Opening a workspace does not initialize or advance the ledger. It gives
    // the host an exclusive read-only snapshot view over the committed stores.
    let workspace = ReplayWorkspace::open(&config, genesis.clone())?;

    let snapshot_result = (|| -> Result<(), AnyError> {
        let snapshot = workspace.snapshot();
        let retained = dolos_snapshot::planning::retained_epochs(&state_epochs)?;
        let plan = snapshot.selected_plan(
            u64::from(genesis.network_magic()),
            retained,
            Selection::default(),
        )?;

        let inscription =
            snapshot.publish_directory(Path::new(&destination), &plan, &Observer::silent())?;

        // Reproduction uses the same plan and encoders and writes nothing.
        snapshot.verify_reproduction(&inscription, &plan)?;
        let document = snapshot.digest_document(&plan, &First)?;

        eprintln!("sequence: {}", plan.sequence);
        eprintln!("layers:   {}", document.layers);
        eprintln!("identity: {}", document.identity);
        Ok(())
    })();

    let finish_result = workspace.finish();

    snapshot_result?;
    finish_result?;

    Ok(())
}
