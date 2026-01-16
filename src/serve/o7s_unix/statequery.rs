use chrono::{Datelike, Timelike};
use dolos_cardano::{load_era_summary, EraSummary as DolosEraSummary};
use dolos_core::StateStore;
use pallas::codec::utils::AnyCbor;
use pallas::ledger::primitives::Fragment;
use pallas::ledger::traverse::MultiEraBlock;

use pallas::codec::minicbor;
use pallas::network::miniprotocols::{localstate, localstate::queries_v16 as q16, Point as OPoint};
use tracing::{debug, info, warn};

use crate::prelude::*;
use crate::serve::o7s_unix::statequery_utils;
use statequery_utils::{
    build_era_history_response, build_protocol_params, build_utxo_by_address_response,
};

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

    /// Decode cardano-cli tagged query format.
    /// cardano-cli wraps queries like: [0, [0, [era, [era, tag(258), addresses]]]]
    fn decode_cardano_cli_tagged_query(raw_bytes: &[u8]) -> Result<q16::Request, ()> {
        // Look for tag marker (0xd9 = tag with 2-byte value) within array structure
        if raw_bytes.len() >= 10 && raw_bytes[0] == 0x82 {
            if let Some(tag_pos) = raw_bytes.iter().position(|&b| b == 0xd9) {
                if tag_pos + 3 < raw_bytes.len() {
                    // Extract era from position 5 (the first era byte in the structure)
                    let era = raw_bytes.get(5).ok_or(())?.to_owned() as u16;

                    let after_tag = &raw_bytes[tag_pos + 3..];
                    if let Ok(addrs) = minicbor::decode::<q16::Addrs>(after_tag) {
                        return Ok(q16::Request::LedgerQuery(q16::LedgerQuery::BlockQuery(
                            era,
                            q16::BlockQuery::GetUTxOByAddress(addrs),
                        )));
                    }
                }
            }
        }
        Err(())
    }

    fn decode_query_request(query: &AnyCbor) -> Result<q16::Request, minicbor::decode::Error> {
        query.clone().into_decode().or_else(|first_err| {
            let raw_bytes: &[u8] = query.as_ref();

            let decoded = if let Ok(inner) = minicbor::decode::<Vec<u8>>(raw_bytes) {
                minicbor::decode::<q16::Request>(&inner).ok().map(|req| {
                    debug!("decoded query from byte string wrapper");
                    req
                })
            } else {
                None
            }
            .or_else(|| Self::decode_cardano_cli_tagged_query(raw_bytes).ok());

            decoded.ok_or(first_err)
        })
    }

    async fn handle_query(&mut self, query: AnyCbor) -> Result<(), Error> {
        let req = Self::decode_query_request(&query);

        let response = match req {
            Ok(q16::Request::LedgerQuery(q16::LedgerQuery::BlockQuery(
                era,
                q16::BlockQuery::GetCurrentPParams,
            ))) => {
                info!(?era, "GetCurrentPParams query");
                let pparams = build_protocol_params(&self.domain)?;
                dbg!(pparams.clone());
                let a = pparams.encode_fragment().unwrap();
                dbg!(hex::encode(&a));
                dbg!(hex::encode(AnyCbor::from_encode((&pparams,)).raw_bytes()));
                AnyCbor::from_encode((pparams,))
            }
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
                    let block = MultiEraBlock::decode(&raw)
                        .map_err(|e| Error::server(format!("failed to decode tip block: {}", e)))?;
                    block.number()
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
                    0..=1 => 0,  // Byron
                    2 => 1,      // Shelley
                    3 => 2,      // Allegra
                    4 => 3,      // Mary
                    5..=6 => 4,  // Alonzo
                    7..=8 => 5,  // Babbage
                    9..=10 => 6, // Conway
                    _ => 6,      // Unknown/future versions default to Conway
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
            Ok(q16::Request::LedgerQuery(q16::LedgerQuery::BlockQuery(
                _era,
                q16::BlockQuery::GetUTxOByAddress(ref addrs),
            ))) => {
                info!(num_addrs = addrs.len(), "GetUTxOByAddress query");
                build_utxo_by_address_response(&self.domain, addrs)?
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
