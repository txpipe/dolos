//! Minimal external host for Dolos snapshot planning, encoding and
//! reproduction.
//!
//! Build without Dolos service features:
//!
//! ```text
//! cargo run --no-default-features --example headless_snapshot -- \
//!   ./dolos.toml ./stele
//! ```
//!
//! The destination must not already contain a stele. Configuration precedence,
//! progress rendering and destination cleanup remain host responsibilities.

use std::{error::Error, path::Path, sync::Arc};

use dolos::core::Genesis;
use dolos::engine::ReplayWorkspace;
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

    let config: dolos::core::config::RootConfig = config::Config::builder()
        .add_source(config::File::from(Path::new(&config_path)))
        .build()?
        .try_deserialize()?;

    let genesis = Arc::new(Genesis::from_file_paths(
        &config.genesis.byron_path,
        &config.genesis.shelley_path,
        &config.genesis.alonzo_path,
        &config.genesis.conway_path,
        config.genesis.force_protocol,
    )?);

    // Opening a workspace does not initialize or advance the ledger. It gives
    // the host an exclusive read-only snapshot view over the committed stores.
    let workspace = ReplayWorkspace::open(&config, genesis.clone())?;

    let snapshot_result = (|| -> Result<(), AnyError> {
        let snapshot = workspace.snapshot();
        let retained = dolos_snapshot::planning::retained_epochs(&config)?;
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
