use crate::prelude::*;
use dolos_cardano::EraSummary as DolosEraSummary;
use pallas::codec::minicbor::{self, Encoder};
use pallas::codec::utils::AnyCbor;

pub struct EraHistoryResponse<'a> {
    pub eras: &'a [DolosEraSummary],
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

        const PICOSECONDS_PER_SECOND: u128 = 1_000_000_000_000;

        for era in self.eras {
            encoder.array(3)?;

            encoder.array(3)?;
            let start_relative_time = era.start.timestamp.saturating_sub(self.system_start) as u128;
            let start_relative_picos = start_relative_time
                .saturating_mul(PICOSECONDS_PER_SECOND)
                .min(u64::MAX as u128) as u64;
            encoder.u64(start_relative_picos)?;
            encoder.u64(era.start.slot)?;
            encoder.u64(era.start.epoch)?;

            let era_is_open_ended = match &era.end {
                Some(end) => {
                    let end_relative_time = end.timestamp.saturating_sub(self.system_start) as u128;
                    let end_relative_picos =
                        end_relative_time.saturating_mul(PICOSECONDS_PER_SECOND);
                    if end_relative_picos > u64::MAX as u128 {
                        encoder.null()?;
                        true
                    } else {
                        encoder.array(3)?;
                        encoder.u64(end_relative_picos as u64)?;
                        encoder.u64(end.slot)?;
                        encoder.u64(end.epoch)?;
                        false
                    }
                }
                None => {
                    encoder.null()?;
                    true
                }
            };

            let safe_from_tip = self.security_param * 2;
            let genesis_window = self.security_param * 2;

            encoder.array(4)?;
            encoder.u64(era.epoch_length)?;
            let slot_length_picos = (era.slot_length as u128)
                .saturating_mul(PICOSECONDS_PER_SECOND)
                .min(u64::MAX as u128) as u64;
            encoder.u64(slot_length_picos)?;

            if era_is_open_ended {
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

pub fn build_era_history_response(
    eras: &[DolosEraSummary],
    genesis: &Genesis,
) -> Result<AnyCbor, Error> {
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
