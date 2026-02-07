use std::{ops::Range, path::Path, sync::Arc};

use dolos_core::RawBlock;

// Replace placeholders with real values when fixtures are available.
pub const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/preview");
pub const KNOWN_TX_HASH: &str =
    "a8fa4293645facb2a0332f4dfc442dff3fc9ca021c95ee908df5d9605e3825be";

// Adjust this range if the fixture does not contain the desired tx in this window.
pub const IMMUTABLE_BLOCK_RANGE: Range<u64> = 0..100;

pub fn load_immutable_blocks(range: Range<u64>) -> Vec<RawBlock> {
    let dir = Path::new(FIXTURE_DIR);
    let mut iter = pallas::interop::hardano::storage::immutable::read_blocks(dir)
        .expect("failed to read immutable db fixture");

    let skip = range.start as usize;
    let take = range.end.saturating_sub(range.start) as usize;

    iter.by_ref()
        .skip(skip)
        .take(take)
        .map(|result| {
            let block = result.expect("failed to read block from fixture");
            Arc::new(block)
        })
        .collect()
}
