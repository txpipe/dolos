//! Minimal external host for Dolos domain construction and bulk replay.
//!
//! Build without Dolos service features:
//!
//! ```text
//! cargo run --no-default-features --example headless_replay -- \
//!   ./dolos.toml ./snapshot/immutable 500
//! ```
//!
//! The final argument is an optional stopping epoch. Source acquisition,
//! progress rendering, publication, and housekeeping intentionally stay in
//! the host application.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use dolos::core::{Genesis, RawBlock};
use dolos::engine::{BulkReplaySession, ReplayProgress};

type AnyError = Box<dyn Error + Send + Sync + 'static>;

fn main() -> Result<(), AnyError> {
    let mut args = std::env::args_os().skip(1);
    let config_path = args.next().ok_or("missing path to dolos.toml")?;
    let immutable_path = args.next().ok_or("missing immutable directory")?;
    let stop_epoch = args
        .next()
        .map(|value| value.to_string_lossy().parse::<u64>())
        .transpose()?;

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

    let session = BulkReplaySession::open(&config, genesis, stop_epoch)?;

    session
        .run(|session| {
            let cursor = session.committed_position()?;
            let point = match cursor {
                Some(cursor) => {
                    let slot = cursor.slot();
                    cursor.try_into().map_err(|()| {
                        format!("state cursor at slot {slot} is not anchored to a block hash")
                    })?
                }
                None => pallas::network::miniprotocols::Point::Origin,
            };

            let mut blocks = pallas::interop::hardano::storage::immutable::read_blocks_from_point(
                Path::new(&immutable_path),
                point.clone(),
            )?;

            if point != pallas::network::miniprotocols::Point::Origin {
                blocks.next();
            }

            let mut batch: Vec<RawBlock> = Vec::with_capacity(100);

            for block in blocks {
                batch.push(Arc::new(block?));

                if batch.len() == 100 && import_batch(session, &mut batch)? {
                    return Ok::<_, AnyError>(());
                }
            }

            if !batch.is_empty() {
                import_batch(session, &mut batch)?;
            }

            Ok(())
        })
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

fn import_batch(
    session: &mut BulkReplaySession,
    batch: &mut Vec<RawBlock>,
) -> Result<bool, AnyError> {
    let progress = session.import_blocks(std::mem::take(batch))?;

    match progress {
        ReplayProgress::Committed { position } => {
            eprintln!("committed through {position}");
            Ok(false)
        }
        ReplayProgress::Boundary { position } => {
            eprintln!("stopping boundary committed at {position}");
            Ok(true)
        }
    }
}
