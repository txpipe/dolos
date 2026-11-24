//! Minimal external host for Dolos snapshot planning, encoding and
//! reproduction.
//!
//! Build without Dolos service features:
//!
//! ```text
//! cargo run --no-default-features --example headless_snapshot -- \
//!   ./dolos.toml ./root
//! ```
//!
//! The destination is a network root: the Dolos snapshot goes into its
//! `dolos/` layout and the closed epochs' blocks into `blocks/`, which the
//! Dolos snapshot names in `position.blocks`. An empty destination starts a
//! lineage. One whose latest Dolos snapshot is at the sequence just before
//! this one is appended to: the new inscription names that snapshot as its
//! `previous` and lists its own diff and the tip, not the predecessor's
//! layers. A latest Dolos snapshot at any other sequence is refused before a
//! layer is written. Progress rendering and destination cleanup remain host
//! responsibilities.
//!
//! The stores must stand on the first block of an epoch, with the archive
//! ending there, as a sync stopped at that epoch leaves them; any other cut is
//! refused before a layer is written (`crates/snapshot/PROFILE.md`, "Only the
//! boundary cut is published").

use std::{error::Error, path::Path, sync::Arc};

use dolos::engine::ReplayWorkspace;
use dolos_cardano::networks::ResolvedNetwork;
use dolos_snapshot::{
    facade::{Selection, SnapshotSource as _},
    progress::Observer,
};

type AnyError = Box<dyn Error + Send + Sync + 'static>;

fn main() -> Result<(), AnyError> {
    let mut args = std::env::args_os().skip(1);
    let config_path = args.next().ok_or("missing path to dolos.toml")?;
    let destination = args.next().ok_or("missing output directory")?;
    let config = dolos::core::config::load(Some(Path::new(&config_path)))?;
    let genesis = Arc::new(ResolvedNetwork::resolve(&config.chain)?.genesis()?);

    // Opening a workspace does not initialize or advance the ledger. It gives
    // the host an exclusive read-only snapshot view over the committed stores.
    let workspace = ReplayWorkspace::open(&config, genesis.clone())?;

    let snapshot_result = (|| -> Result<(), AnyError> {
        let snapshot = workspace.snapshot();
        let plan =
            snapshot.selected_plan(u64::from(genesis.network_magic()), Selection::default())?;

        let inscription =
            snapshot.publish_directory(Path::new(&destination), &plan, &Observer::silent())?;

        // Reproduction uses the same plan and encoders and writes nothing.
        snapshot.verify_reproduction(&inscription, &plan)?;

        // The identity is the published inscription's: after an append it
        // names its predecessor, and the layer count is this sequence's diff
        // plus the tip.
        eprintln!("sequence: {}", plan.sequence);
        eprintln!("layers:   {}", inscription.layers().count());
        eprintln!("identity: {}", inscription.digest()?);
        Ok(())
    })();

    let finish_result = workspace.finish();

    snapshot_result?;
    finish_result?;

    Ok(())
}
