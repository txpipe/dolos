use crate::prelude::*;
use dolos_cardano::EraState as DolosEraState;
use pallas::codec::minicbor::{self, Encoder};
use pallas::codec::utils::AnyCbor;

const PICOSECONDS_PER_SECOND: u128 = 1_000_000_000_000;

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
    pub security_param: u64,
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

            let safe_from_tip = self.security_param * 2;
            let genesis_window = self.security_param * 2;

            encoder.array(4)?;
            encoder.u64(era.epoch_length)?;
            let slot_length_picos = (era.slot_length as u128)
                .saturating_mul(PICOSECONDS_PER_SECOND)
                .min(u64::MAX as u128) as u64;
            encoder.u64(slot_length_picos)?;

            if era.end.is_none() {
                encoder.array(1)?;
                encoder.u8(1)?;
            } else {
                encoder.array(3)?;
                encoder.u8(0)?;
                encoder.u64(safe_from_tip)?;

                encoder.array(1)?;
                encoder.u8(0)?;
            }

            encoder.u64(genesis_window)?;
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

    let security_param = genesis
        .shelley
        .security_param
        .ok_or_else(|| Error::server("missing security param"))?;

    let resp = EraHistoryResponse {
        eras,
        system_start,
        security_param: security_param.into(),
    };

    Ok(AnyCbor::from_encode(resp))
}
