use chrono::{Datelike, Timelike};
use dolos_cardano::{load_era_summary, EraSummary as DolosEraSummary};
use dolos_core::StateStore;
use pallas::codec::minicbor::{self, Encoder};
use pallas::codec::utils::{AnyCbor, AnyUInt, KeyValuePairs};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, OriginalHash};
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
            let start_relative_time = era.start.timestamp.saturating_sub(self.system_start) as u128;
            let start_relative_picos = start_relative_time
                .saturating_mul(1_000_000_000_000u128)
                .min(u64::MAX as u128) as u64;
            encoder.u64(start_relative_picos)?;
            encoder.u64(era.start.slot)?;
            encoder.u64(era.start.epoch)?;

            // EraEnd
            let era_is_open_ended = match &era.end {
                Some(end) => {
                    let end_relative_time = end.timestamp.saturating_sub(self.system_start) as u128;
                    let end_relative_picos =
                        end_relative_time.saturating_mul(1_000_000_000_000u128);
                    // If the time would overflow u64, treat this era as open-ended
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

            // EraParams
            encoder.array(4)?;
            encoder.u64(era.epoch_length)?;
            encoder.u64(era.slot_length * 1000)?;

            let safe_from_tip = self.security_param * 2;

            // SafeZone
            if era_is_open_ended {
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

fn convert_output_to_q16(output: &MultiEraOutput) -> Result<q16::TransactionOutput, Error> {
    use pallas::codec::utils::NonEmptyKeyValuePairs;
    use pallas::ledger::primitives::conway::DatumOption;

    let address = output.address().map_err(Error::server)?.to_vec();

    let value_data = output.value();
    let lovelace = AnyUInt::U64(value_data.coin());

    let assets = value_data.assets();
    let has_assets = !assets.is_empty();

    let value = if has_assets {
        let mut policy_map: Vec<(
            pallas::crypto::hash::Hash<28>,
            NonEmptyKeyValuePairs<pallas::codec::utils::Bytes, AnyUInt>,
        )> = vec![];

        for policy_assets in assets {
            let policy_id = *policy_assets.policy();
            let mut asset_entries: Vec<(pallas::codec::utils::Bytes, AnyUInt)> = vec![];

            for asset in policy_assets.assets() {
                let name = asset.name();
                let amount = asset.output_coin().unwrap_or(0);
                asset_entries.push((name.to_vec().into(), AnyUInt::U64(amount)));
            }

            if !asset_entries.is_empty() {
                policy_map.push((policy_id, NonEmptyKeyValuePairs::Def(asset_entries)));
            }
        }

        if policy_map.is_empty() {
            q16::Value::Coin(lovelace)
        } else {
            q16::Value::Multiasset(lovelace, NonEmptyKeyValuePairs::Def(policy_map))
        }
    } else {
        q16::Value::Coin(lovelace)
    };

    let inline_datum = output.datum().and_then(|d| match d {
        DatumOption::Hash(h) => Some(q16::DatumOption::Hash(h)),
        DatumOption::Data(data) => Some(q16::DatumOption::Data(pallas::codec::utils::CborWrap(
            convert_plutus_data(&data.0),
        ))),
    });

    let datum_hash = output.datum().and_then(|d| match d {
        DatumOption::Hash(h) => Some(h),
        DatumOption::Data(data) => Some(data.original_hash()),
    });

    if output.era() >= pallas::ledger::traverse::Era::Alonzo {
        Ok(q16::TransactionOutput::Current(
            q16::PostAlonsoTransactionOutput {
                address: address.into(),
                amount: value,
                inline_datum,
                script_ref: None,
            },
        ))
    } else {
        Ok(q16::TransactionOutput::Legacy(
            q16::LegacyTransactionOutput {
                address: address.into(),
                amount: value,
                datum_hash,
            },
        ))
    }
}

fn convert_plutus_data(data: &pallas::ledger::primitives::PlutusData) -> q16::PlutusData {
    match data {
        pallas::ledger::primitives::PlutusData::Constr(constr) => {
            let fields = constr
                .fields
                .iter()
                .map(convert_plutus_data)
                .collect::<Vec<_>>();
            q16::PlutusData::Constr(q16::Constr {
                tag: constr.tag,
                any_constructor: constr.any_constructor,
                fields: pallas::codec::utils::MaybeIndefArray::Indef(fields),
            })
        }
        pallas::ledger::primitives::PlutusData::Map(kvs) => {
            let mapped = kvs
                .iter()
                .map(|(k, v)| (convert_plutus_data(k), convert_plutus_data(v)))
                .collect::<Vec<_>>();
            q16::PlutusData::Map(KeyValuePairs::Def(mapped))
        }
        pallas::ledger::primitives::PlutusData::BigInt(bi) => match bi {
            pallas::ledger::primitives::BigInt::Int(i) => {
                q16::PlutusData::BigInt(q16::BigInt::Int(*i))
            }
            pallas::ledger::primitives::BigInt::BigUInt(bytes) => {
                let raw: Vec<u8> = bytes.clone().into();
                q16::PlutusData::BigInt(q16::BigInt::BigUInt(raw.into()))
            }
            pallas::ledger::primitives::BigInt::BigNInt(bytes) => {
                let raw: Vec<u8> = bytes.clone().into();
                q16::PlutusData::BigInt(q16::BigInt::BigNInt(raw.into()))
            }
        },
        pallas::ledger::primitives::PlutusData::BoundedBytes(bytes) => {
            let raw: Vec<u8> = bytes.clone().into();
            q16::PlutusData::BoundedBytes(raw.into())
        }
        pallas::ledger::primitives::PlutusData::Array(arr) => {
            let items = arr.iter().map(convert_plutus_data).collect::<Vec<_>>();
            q16::PlutusData::Array(pallas::codec::utils::MaybeIndefArray::Indef(items))
        }
    }
}

fn build_utxo_by_address_response<D: Domain>(
    domain: &D,
    addrs: &q16::Addrs,
) -> Result<AnyCbor, Error> {
    use pallas::ledger::addresses::Address;

    let mut utxo_pairs: Vec<(q16::UTxO, q16::TransactionOutput)> = Vec::new();

    let mut all_refs = std::collections::HashSet::new();
    for addr in addrs.iter() {
        let addr_bytes: &[u8] = addr.as_ref();
        debug!(addr_len = addr_bytes.len(), addr_hex = %hex::encode(addr_bytes), "looking up utxos for address");

        let mut refs = domain
            .state()
            .get_utxo_by_address(addr_bytes)
            .map_err(|e| Error::server(format!("failed to get utxos by address: {}", e)))?;

        debug!(num_refs = refs.len(), "found utxo refs by full address");

        if refs.is_empty() {
            if let Ok(Address::Shelley(shelley_addr)) = Address::from_bytes(addr_bytes) {
                let payment_bytes = shelley_addr.payment().to_vec();
                debug!(payment_hex = %hex::encode(&payment_bytes), "trying payment credential lookup");
                refs = domain
                    .state()
                    .get_utxo_by_payment(&payment_bytes)
                    .map_err(|e| Error::server(format!("failed to get utxos by payment: {}", e)))?;
                debug!(
                    num_refs = refs.len(),
                    "found utxo refs by payment credential"
                );
            }
        }

        all_refs.extend(refs);
    }

    debug!(
        total_refs = all_refs.len(),
        "total unique utxo refs to fetch"
    );

    let refs_vec: Vec<_> = all_refs.into_iter().collect();
    let utxos = domain
        .state()
        .get_utxos(refs_vec.clone())
        .map_err(|e| Error::server(format!("failed to get utxos: {}", e)))?;

    debug!(fetched_utxos = utxos.len(), "fetched utxo data");

    for utxo_ref in refs_vec {
        if let Some(era_cbor) = utxos.get(&utxo_ref) {
            let output = MultiEraOutput::try_from(era_cbor.as_ref())
                .map_err(|e| Error::server(format!("failed to decode utxo: {}", e)))?;
            let q16_utxo = q16::UTxO {
                transaction_id: utxo_ref.0,
                index: AnyUInt::U32(utxo_ref.1),
            };

            let q16_output = convert_output_to_q16(&output)?;
            utxo_pairs.push((q16_utxo, q16_output));
        }
    }

    debug!(num_utxos = utxo_pairs.len(), "returning utxos");

    let response: KeyValuePairs<q16::UTxO, q16::TransactionOutput> = KeyValuePairs::Def(utxo_pairs);

    Ok(AnyCbor::from_encode((response,)))
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
