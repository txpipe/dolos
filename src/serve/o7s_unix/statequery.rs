use chrono::{Datelike, Timelike};
use dolos_cardano::{load_era_summary, EraSummary as DolosEraSummary};
use dolos_core::StateStore;
use pallas::codec::minicbor::{self, Encoder};
use pallas::codec::utils::AnyCbor;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::{localstate, localstate::queries_v16 as q16, Point as OPoint};
use tracing::{debug, info, warn};

use crate::prelude::*;

struct EraHistoryResponse<'a> {
    eras: &'a [DolosEraSummary],
    system_start: u64,
    security_param: u64,
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

            // Start Bound
            encoder.array(3)?;
            let start_relative_time = era.start.timestamp.saturating_sub(self.system_start);
            encoder.u64(start_relative_time)?;
            encoder.u64(era.start.slot)?;
            encoder.u64(era.start.epoch)?;

            // EraEnd
            match &era.end {
                Some(end) => {
                    encoder.array(3)?;
                    let end_relative_time = end.timestamp.saturating_sub(self.system_start);
                    encoder.u64(end_relative_time)?;
                    encoder.u64(end.slot)?;
                    encoder.u64(end.epoch)?;
                }
                None => {
                    encoder.null()?;
                }
            }

            // EraParams
            encoder.array(4)?;
            encoder.u64(era.epoch_length)?;
            encoder.u64(era.slot_length * 1000)?;

            let safe_from_tip = self.security_param * 2;

            // SafeZone
            if era.end.is_none() {
                // UnsafeIndefiniteSafeZone: [1, 1]
                encoder.array(1)?;
                encoder.u8(1)?;
            } else {
                // StandardSafeZone: [3, 0, safeFromTip, safeBeforeEpoch]
                // safeFromTip = 2 * k (stability window)
                // safeBeforeEpoch is encoded as [1, 0] for NoLowerBound
                encoder.array(3)?;
                encoder.u8(0)?;

                encoder.u64(safe_from_tip)?;
                // safeBeforeEpoch: NoLowerBound = [1, 0]
                encoder.array(1)?;
                encoder.u8(0)?;
            }

            encoder.u64(safe_from_tip)?;
        }

        Ok(())
    }
}

fn build_era_history_response(
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

pub struct Session<D: Domain> {
    domain: D,
    connection: localstate::Server,
    acquired_point: Option<ChainPoint>,
}

impl<D: Domain> Session<D> {
    fn tip_cursor(&self) -> Result<ChainPoint, Error> {
        let point = self
            .domain
            .state()
            .read_cursor()
            .map_err(Error::server)?
            .unwrap_or(ChainPoint::Origin);

        match point {
            ChainPoint::Slot(slot) => {
                let body = self
                    .domain
                    .archive()
                    .get_block_by_slot(&slot)
                    .map_err(Error::server)?
                    .ok_or_else(|| Error::server("block not found for slot"))?;

                let block =
                    MultiEraBlock::decode(&body).map_err(|e| Error::server(e.to_string()))?;

                Ok(ChainPoint::Specific(slot, block.hash()))
            }
            _ => Ok(point),
        }
    }

    async fn send_acquired(&mut self) -> Result<(), Error> {
        debug!("sending acquired confirmation");
        self.connection
            .send_acquired()
            .await
            .map_err(Error::server)?;
        Ok(())
    }

    async fn send_failure(&mut self, reason: localstate::AcquireFailure) -> Result<(), Error> {
        debug!(?reason, "sending acquire failure");
        self.connection
            .send_failure(reason)
            .await
            .map_err(Error::server)?;
        Ok(())
    }

    async fn handle_acquire(
        &mut self,
        point: Option<pallas::network::miniprotocols::Point>,
    ) -> Result<(), Error> {
        debug!(?point, "handling acquire request");

        let chain_point = match point {
            Some(p) => ChainPoint::from(p),
            None => {
                // None means acquire the latest point
                self.tip_cursor()?
            }
        };

        let exists = match &chain_point {
            ChainPoint::Origin => true,
            ChainPoint::Specific(_slot, hash) => self
                .domain
                .archive()
                .get_block_by_hash(hash.as_slice())
                .map_err(Error::server)?
                .is_some(),
            ChainPoint::Slot(_) => true,
        };

        if exists {
            self.acquired_point = Some(chain_point);
            self.send_acquired().await?;
        } else {
            self.send_failure(localstate::AcquireFailure::PointNotOnChain)
                .await?;
        }

        Ok(())
    }

    async fn handle_query(&mut self, query: AnyCbor) -> Result<(), Error> {
        let req: Result<q16::Request, _> = query.clone().into_decode();

        let response = match req {
            Ok(q16::Request::GetSystemStart) => {
                debug!("GetSystemStart query");
                let genesis = self.domain.genesis();
                let ts = genesis
                    .shelley
                    .system_start
                    .as_ref()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .ok_or_else(|| Error::server("invalid system start"))?;

                let year = ts.year() as i64;
                let day = ts.ordinal() as i64;
                let secs = ts.num_seconds_from_midnight() as i64;
                let nanos = ts.timestamp_subsec_nanos() as i64;
                let picos = secs * 1_000_000_000_000i64 + nanos * 1_000i64;

                let sys_start = q16::SystemStart {
                    year: q16::BigInt::from(year),
                    day_of_year: day,
                    picoseconds_of_day: q16::BigInt::from(picos),
                };

                AnyCbor::from_encode(sys_start)
            }
            Ok(q16::Request::GetChainBlockNo) => {
                debug!("GetChainBlockNo query");
                let number = if let Some((_, raw)) =
                    self.domain.archive().get_tip().map_err(Error::server)?
                {
                    if let Ok(block) = MultiEraBlock::decode(&raw) {
                        block.number()
                    } else {
                        0
                    }
                } else {
                    0
                };

                // Use saturating conversion to avoid silent truncation
                let block_number = u32::try_from(number).unwrap_or(u32::MAX);

                let resp = q16::ChainBlockNumber {
                    slot_timeline: 1, // 1 indicates "At" (not Origin)
                    block_number,
                };

                AnyCbor::from_encode(resp)
            }
            Ok(q16::Request::GetChainPoint) => {
                debug!("GetChainPoint query");
                let point = self.tip_cursor()?;

                AnyCbor::from_encode(match point {
                    ChainPoint::Origin => OPoint::Origin,
                    ChainPoint::Specific(s, h) => OPoint::Specific(s, h.to_vec()),
                    ChainPoint::Slot(_) => OPoint::Origin,
                })
            }
            Ok(q16::Request::LedgerQuery(q16::LedgerQuery::HardForkQuery(
                q16::HardForkQuery::GetInterpreter,
            ))) => {
                debug!("GetInterpreter query");

                let chain_summary = load_era_summary::<D>(self.domain.state())
                    .map_err(|e| Error::server(format!("failed to load era summary: {}", e)))?;

                let eras: Vec<DolosEraSummary> = chain_summary.iter_all().cloned().collect();

                let genesis = self.domain.genesis();
                build_era_history_response(&eras, &genesis)?
            }
            Ok(q16::Request::LedgerQuery(q16::LedgerQuery::HardForkQuery(
                q16::HardForkQuery::GetCurrentEra,
            ))) => {
                debug!("GetCurrentEra query");

                let chain_summary = load_era_summary::<D>(self.domain.state())
                    .map_err(|e| Error::server(format!("failed to load era summary: {}", e)))?;

                let edge = chain_summary.edge();
                let era_index = match edge.protocol {
                    0..=1 => 0, // Byron
                    2 => 1,     // Shelley
                    3 => 2,     // Allegra
                    4 => 3,     // Mary
                    5..=6 => 4, // Alonzo
                    7 => 5,     // Babbage
                    _ => 6,     // Conway
                };

                AnyCbor::from_encode(era_index as u16)
            }
            Ok(q16::Request::LedgerQuery(q16::LedgerQuery::BlockQuery(
                _era,
                q16::BlockQuery::GetLedgerTip,
            ))) => {
                debug!("GetLedgerTip query");
                let point = self.tip_cursor()?;

                let p = match point {
                    ChainPoint::Origin => OPoint::Origin,
                    ChainPoint::Specific(s, h) => OPoint::Specific(s, h.to_vec()),
                    ChainPoint::Slot(_) => OPoint::Origin,
                };
                AnyCbor::from_encode((p,))
            }
            Ok(q16::Request::LedgerQuery(q16::LedgerQuery::BlockQuery(
                _era,
                q16::BlockQuery::GetEpochNo,
            ))) => {
                debug!("GetEpochNo query");
                let chain_summary = load_era_summary::<D>(self.domain.state())
                    .map_err(|e| Error::server(format!("failed to load era summary: {}", e)))?;

                let tip_slot = match self.tip_cursor()? {
                    ChainPoint::Specific(s, _) => s,
                    ChainPoint::Slot(s) => s,
                    ChainPoint::Origin => 0,
                };
                let (epoch, _) = chain_summary.slot_epoch(tip_slot);

                AnyCbor::from_encode((epoch as u32,))
            }
            Ok(req) => {
                warn!(?req, "unhandled known query, returning null");
                AnyCbor::from_encode(())
            }
            Err(e) => {
                warn!(?e, "failed to decode query request, returning null");
                AnyCbor::from_encode(())
            }
        };

        self.connection.send_result(response).await.map_err(|e| {
            warn!(?e, "failed to send query result");
            Error::server(e)
        })?;

        Ok(())
    }

    async fn handle_reacquire(
        &mut self,
        point: Option<pallas::network::miniprotocols::Point>,
    ) -> Result<(), Error> {
        debug!(?point, "handling reacquire request");
        self.handle_acquire(point).await
    }

    async fn handle_release(&mut self) -> Result<(), Error> {
        debug!("handling release request");
        self.acquired_point = None;
        Ok(())
    }

    async fn process_requests(&mut self) -> Result<(), Error> {
        loop {
            if let Some(req) = self
                .connection
                .recv_while_idle()
                .await
                .map_err(Error::server)?
            {
                self.handle_acquire(req.0).await?;
            } else {
                break;
            }

            loop {
                match self
                    .connection
                    .recv_while_acquired()
                    .await
                    .map_err(Error::server)?
                {
                    localstate::ClientQueryRequest::Query(query) => {
                        self.handle_query(query).await?;
                    }
                    localstate::ClientQueryRequest::ReAcquire(point) => {
                        self.handle_reacquire(point).await?;
                        // After reacquire, we stay in acquired state but with new point
                    }
                    localstate::ClientQueryRequest::Release => {
                        self.handle_release().await?;
                        // After release, go back to idle state
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn handle_session<D: Domain, C: CancelToken>(
    domain: D,
    connection: localstate::Server,
    cancel: C,
) -> Result<(), ServeError> {
    let mut session = Session {
        domain,
        connection,
        acquired_point: None,
    };

    info!("statequery session started");

    tokio::select! {
        result = session.process_requests() => {
            if let Err(e) = result {
                warn!(?e, "statequery session error");
                return Err(ServeError::Internal(e.into()));
            }
            info!("statequery client ended protocol");
        },
        _ = cancel.cancelled() => {
            info!("statequery protocol was cancelled");
        }
    }

    Ok(())
}
