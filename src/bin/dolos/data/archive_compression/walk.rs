//! Walks a raw segment as the stream of CBOR items it is. Every stored body
//! is one item, so the boundaries come from the bytes themselves and cover
//! the bodies the archive index no longer points at — a duplicate from a
//! resumed import, a block a rollback displaced — as well as the ones it
//! does. A stream that does not end on an item boundary is refused.

use dolos_fjall::archive::ArchiveStore;
use dolos_fjall::flatfiles::BlockLocation;
use miette::{bail, IntoDiagnostic, WrapErr};
use pallas::codec::minicbor::Decoder;
use pallas::ledger::traverse::MultiEraBlock;

/// How much of the stream is read at a time. An item longer than this is
/// handled by reading on until it is complete.
const WINDOW: u64 = 8 << 20;

/// One body of the stream.
#[derive(Debug, Clone, Copy)]
pub struct Body {
    pub location: BlockLocation,
}

/// Every body of segment `segment_id`, in physical order, covering exactly
/// `logical_len` bytes.
pub fn walk(
    archive: &ArchiveStore,
    segment_id: u32,
    logical_len: u64,
) -> miette::Result<Vec<Body>> {
    let mut bodies = Vec::new();
    let mut buf: Vec<u8> = Vec::new();
    // `buf[0]` sits at logical offset `base`; `end` is the offset after its
    // last byte; `pos` is where the next item starts.
    let mut base = 0u64;
    let mut end = 0u64;
    let mut pos = 0u64;

    while pos < logical_len {
        let start = (pos - base) as usize;
        let mut decoder = Decoder::new(&buf[start..]);
        match decoder.skip() {
            Ok(()) => {
                let length = decoder.position();
                let item = &buf[start..start + length];
                let slot = MultiEraBlock::decode(item)
                    .map(|block| block.slot())
                    .map_err(|e| {
                        miette::miette!(
                            "segment {segment_id:06}: the CBOR item at offset {pos} is not a \
                             block: {e}"
                        )
                    })?;
                let length = u32::try_from(length).into_diagnostic().wrap_err_with(|| {
                    format!("segment {segment_id:06}: the body at offset {pos} is too long")
                })?;
                let home = BlockLocation::segment_for_slot(slot);
                if home != segment_id {
                    bail!(
                        "segment {segment_id:06}: the block at offset {pos} carries slot {slot}, \
                         which belongs to segment {home:06}; refusing to seal a stream that is \
                         not this segment's"
                    );
                }
                bodies.push(Body {
                    location: BlockLocation {
                        segment_id,
                        offset: pos,
                        length,
                    },
                });
                pos += length as u64;
            }
            Err(e) if e.is_end_of_input() => {
                if end >= logical_len {
                    bail!(
                        "segment {segment_id:06}: the {} trailing bytes at offset {pos} are not a \
                         whole CBOR item; the stream is incomplete and cannot be sealed as it is",
                        logical_len - pos
                    );
                }
                let take = WINDOW.min(logical_len - end);
                let chunk = archive
                    .read_location(&BlockLocation {
                        segment_id,
                        offset: end,
                        length: take as u32,
                    })
                    .into_diagnostic()
                    .wrap_err_with(|| {
                        format!("segment {segment_id:06}: reading {take} bytes at offset {end}")
                    })?;
                if start > 0 {
                    buf.drain(..start);
                    base = pos;
                }
                buf.extend_from_slice(&chunk);
                end += take;
            }
            Err(e) => {
                bail!("segment {segment_id:06}: the bytes at offset {pos} are not a CBOR item: {e}")
            }
        }
    }

    Ok(bodies)
}
