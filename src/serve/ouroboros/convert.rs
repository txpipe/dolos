use pallas::network::miniprotocols::chainsync;

use crate::prelude::*;
use crate::wal;
use crate::wal::RawBlock;

pub fn header_cbor_to_chainsync(block: wal::RawBlock) -> Result<chainsync::HeaderContent, Error> {
    let RawBlock { body, .. } = block;

    let block = pallas::ledger::traverse::MultiEraBlock::decode(&body).map_err(Error::parse)?;

    let out = chainsync::HeaderContent {
        variant: block.era() as u8,
        byron_prefix: match block.era() {
            pallas::ledger::traverse::Era::Byron => Some((1, 0)),
            _ => None,
        },
        cbor: block.header().cbor().to_vec(),
    };

    Ok(out)
}
