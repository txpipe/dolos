use pallas::ledger::traverse::Era;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::chainsync;

use crate::prelude::*;

fn era_to_header_variant(era: Era) -> u8 {
    match era {
        Era::Byron => 0,
        Era::Shelley => 1,
        Era::Allegra => 2,
        Era::Mary => 3,
        Era::Alonzo => 4,
        Era::Babbage => 5,
        Era::Conway => 6,
        _ => todo!("don't know how to process era"),
    }
}

fn define_byron_prefix(block: &MultiEraBlock) -> Option<(u8, u64)> {
    match block.era() {
        pallas::ledger::traverse::Era::Byron => {
            if block.header().as_eb().is_some() {
                Some((0, 0))
            } else {
                Some((1, 0))
            }
        }
        _ => None,
    }
}

pub fn header_cbor_to_chainsync(block: RawBlock) -> Result<chainsync::HeaderContent, Error> {
    let RawBlock { body, .. } = block;

    let block = pallas::ledger::traverse::MultiEraBlock::decode(&body).map_err(Error::parse)?;

    let out = chainsync::HeaderContent {
        variant: era_to_header_variant(block.era()),
        byron_prefix: define_byron_prefix(&block),
        cbor: block.header().cbor().to_vec(),
    };

    Ok(out)
}
