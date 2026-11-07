use crate::prelude::*;
use dolos_cardano::EraState as DolosEraState;
use pallas::codec::minicbor::{self, Encoder};
use pallas::codec::utils::AnyCbor;

const PICOSECONDS_PER_SECOND: u128 = 1_000_000_000_000;
const MILLISECONDS_PER_SECOND: u64 = 1_000;

/// Seconds since the system start, in picoseconds. The node writes this as a
/// CBOR integer, which takes a bignum past `u64` (about 213 days of chain).
fn encode_relative_time<W: minicbor::encode::Write>(
    encoder: &mut Encoder<W>,
    timestamp: u64,
    system_start: u64,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    let picos = u128::from(timestamp.saturating_sub(system_start)) * PICOSECONDS_PER_SECOND;

    match u64::try_from(picos) {
        Ok(picos) => {
            encoder.u64(picos)?;
        }
        Err(_) => {
            let bytes = picos.to_be_bytes();
            let first = bytes.iter().take_while(|byte| **byte == 0).count();

            encoder.tag(minicbor::data::IanaTag::PosBignum)?;
            encoder.bytes(&bytes[first..])?;
        }
    }

    Ok(())
}

pub struct EraHistoryResponse<'a> {
    pub eras: &'a [DolosEraState],
    pub system_start: u64,
    /// The safe zone and genesis window of a Byron row: 2k.
    pub byron_safe_zone: u64,
    /// The safe zone and genesis window from Shelley on: 3k/f.
    pub shelley_safe_zone: u64,
}

impl<'a, C> minicbor::Encode<C> for EraHistoryResponse<'a> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        encoder: &mut Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        encoder.array(self.eras.len() as u64)?;

        for era in self.eras {
            encoder.array(3)?;

            encoder.array(3)?;
            encode_relative_time(encoder, era.start.timestamp, self.system_start)?;
            encoder.u64(era.start.slot)?;
            encoder.u64(era.start.epoch)?;

            match &era.end {
                Some(end) => {
                    encoder.array(3)?;
                    encode_relative_time(encoder, end.timestamp, self.system_start)?;
                    encoder.u64(end.slot)?;
                    encoder.u64(end.epoch)?;
                }
                None => {
                    encoder.null()?;
                }
            }

            let safe_zone = if era.protocol < 2 {
                self.byron_safe_zone
            } else {
                self.shelley_safe_zone
            };

            encoder.array(4)?;
            encoder.u64(era.epoch_length)?;
            encoder.u64(era.slot_length.saturating_mul(MILLISECONDS_PER_SECOND))?;

            if era.end.is_none() {
                encoder.array(1)?;
                encoder.u8(1)?;
            } else {
                encoder.array(3)?;
                encoder.u8(0)?;
                encoder.u64(safe_zone)?;

                encoder.array(1)?;
                encoder.u8(0)?;
            }

            encoder.u64(safe_zone)?;
        }
        Ok(())
    }
}

/// The `GetInterpreter` answer: the era table, as the node's era history.
pub fn load_era_history_response<D: Domain>(domain: &D) -> Result<AnyCbor, Error> {
    let genesis = domain.genesis();
    let eras = dolos_cardano::load_era_table(domain.state(), &genesis)
        .map_err(|e| Error::server(format!("failed to load era summary: {e}")))?;

    build_era_history_response(&eras, &genesis)
}

fn build_era_history_response(eras: &[DolosEraState], genesis: &Genesis) -> Result<AnyCbor, Error> {
    if eras.is_empty() {
        return Err(Error::server("era summary is empty"));
    }

    let system_start = genesis
        .shelley
        .system_start
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp() as u64)
        .ok_or_else(|| Error::server("invalid system start"))?;

    // `mutable_slots` needs this value, and a custom genesis may not have it
    if genesis.shelley.active_slots_coeff.is_none() {
        return Err(Error::server("missing active slots coefficient"));
    }

    let resp = EraHistoryResponse {
        eras,
        system_start,
        byron_safe_zone: genesis.byron.protocol_consts.k as u64 * 2,
        shelley_safe_zone: dolos_cardano::mutable_slots(genesis),
    };

    Ok(AnyCbor::from_encode(resp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_genesis_without_active_slots_coeff_is_an_error() {
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.shelley.active_slots_coeff = None;

        let era = DolosEraState {
            start: dolos_cardano::EraBoundary {
                epoch: 0,
                slot: 0,
                timestamp: 1666656000,
            },
            end: None,
            epoch_length: 86400,
            slot_length: 1,
            protocol: 6,
        };

        assert!(build_era_history_response(&[era], &genesis).is_err());
    }
}
