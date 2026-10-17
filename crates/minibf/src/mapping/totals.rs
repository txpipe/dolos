//! The `/total` bodies, written straight from an `AddressStats` total.
//!
//! The heaviest totals carry millions of units, which the blockfrost-openapi
//! models and `serde_json` take most of a second to encode. These writers
//! produce the same bytes directly, and the tests hold them to the serde path.

use axum::{
    http::{header, HeaderValue},
    response::{IntoResponse, Response},
};
use dolos_cardano::{AddressStats, UnitSums};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// The `AddressContentTotal` body of `/addresses/{address}/total`.
pub fn address_total(address: &str, totals: &AddressStats) -> Vec<u8> {
    write_total("address", address, totals)
}

/// The `AccountAddressesTotal` body of
/// `/accounts/{stake_address}/addresses/total`.
pub fn account_addresses_total(stake_address: &str, totals: &AddressStats) -> Vec<u8> {
    write_total("stake_address", stake_address, totals)
}

/// A written body served as `application/json`, as `Json` serves a model.
pub fn json_response(body: Vec<u8>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}

fn write_total(field: &str, id: &str, totals: &AddressStats) -> Vec<u8> {
    let mut out = Vec::with_capacity(128 + totals.units.len() * 2 * 112);

    out.extend_from_slice(b"{\"");
    out.extend_from_slice(field.as_bytes());
    out.extend_from_slice(b"\":");
    serde_json::to_writer(&mut out, id).expect("writing to a Vec cannot fail");
    out.extend_from_slice(b",\"received_sum\":[");
    write_side(&mut out, totals, |sums| sums.received.0);
    out.extend_from_slice(b"],\"sent_sum\":[");
    write_side(&mut out, totals, |sums| sums.sent.0);
    out.extend_from_slice(b"],\"tx_count\":");
    out.extend_from_slice(
        itoa::Buffer::new()
            .format(totals.tx_count as i32)
            .as_bytes(),
    );
    out.push(b'}');

    out
}

/// One side's amounts: lovelace first, then every asset with a nonzero sum on
/// that side, by unit.
fn write_side(out: &mut Vec<u8>, totals: &AddressStats, pick: fn(&UnitSums) -> u128) {
    let mut num = itoa::Buffer::new();

    let lovelace = totals
        .units
        .get(&Vec::new().into())
        .map(pick)
        .unwrap_or_default();

    out.extend_from_slice(b"{\"unit\":\"lovelace\",\"quantity\":\"");
    out.extend_from_slice(num.format(lovelace).as_bytes());
    out.extend_from_slice(b"\"}");

    for (unit, sums) in &totals.units {
        let quantity = pick(sums);

        if unit.is_empty() || quantity == 0 {
            continue;
        }

        out.extend_from_slice(b",{\"unit\":\"");
        for byte in unit.iter() {
            out.push(HEX[(byte >> 4) as usize]);
            out.push(HEX[(byte & 0xf) as usize]);
        }
        out.extend_from_slice(b"\",\"quantity\":\"");
        out.extend_from_slice(num.format(quantity).as_bytes());
        out.extend_from_slice(b"\"}");
    }
}

#[cfg(test)]
pub(crate) mod oracle {
    //! The serde path the writers replaced, kept as the oracle they are
    //! checked against.

    use blockfrost_openapi::models::{
        account_addresses_total::AccountAddressesTotal,
        account_addresses_total_received_sum_inner::AccountAddressesTotalReceivedSumInner,
        address_content_total::AddressContentTotal,
        tx_content_output_amount_inner::TxContentOutputAmountInner,
    };
    use dolos_cardano::{AddressStats, UnitSums};

    fn stats_amounts(
        stats: &AddressStats,
    ) -> (
        Vec<TxContentOutputAmountInner>,
        Vec<TxContentOutputAmountInner>,
    ) {
        let side = |pick: fn(&UnitSums) -> u128| {
            let lovelace = stats
                .units
                .get(&Vec::new().into())
                .map(pick)
                .unwrap_or_default();

            let assets = stats
                .units
                .iter()
                .filter(|(unit, sums)| !unit.is_empty() && pick(sums) > 0)
                .map(|(unit, sums)| TxContentOutputAmountInner {
                    unit: hex::encode(unit.as_slice()),
                    quantity: pick(sums).to_string(),
                });

            std::iter::once(TxContentOutputAmountInner {
                unit: "lovelace".to_string(),
                quantity: lovelace.to_string(),
            })
            .chain(assets)
            .collect()
        };

        (side(|sums| sums.received.0), side(|sums| sums.sent.0))
    }

    fn account_amounts(
        amounts: Vec<TxContentOutputAmountInner>,
    ) -> Vec<AccountAddressesTotalReceivedSumInner> {
        amounts
            .into_iter()
            .map(|amount| AccountAddressesTotalReceivedSumInner {
                unit: amount.unit,
                quantity: amount.quantity,
            })
            .collect()
    }

    pub fn address_total(address: &str, totals: &AddressStats) -> Vec<u8> {
        let (received_sum, sent_sum) = stats_amounts(totals);

        let model = AddressContentTotal {
            address: address.to_string(),
            received_sum,
            sent_sum,
            tx_count: totals.tx_count as i32,
        };

        serde_json::to_vec(&model).unwrap()
    }

    pub fn account_addresses_total(stake_address: &str, totals: &AddressStats) -> Vec<u8> {
        let (received_sum, sent_sum) = stats_amounts(totals);

        let model = AccountAddressesTotal {
            stake_address: stake_address.to_string(),
            received_sum: account_amounts(received_sum),
            sent_sum: account_amounts(sent_sum),
            tx_count: totals.tx_count as i32,
        };

        serde_json::to_vec(&model).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use dolos_cardano::Sum;
    use pallas::codec::utils::Bytes;

    use super::*;

    const ADDRESS: &str = "addr_test1vpu5vlrf4xkxv2qpwngf6cjhtw542ayty80v8dyr49rf5egfu2p0u";
    const STAKE_ADDRESS: &str = "stake_test1uzpq2pktpnj54e64kfgjkm8nrptdwfj7s7fvhp40e98qsusd9z7ek";

    fn sums(received: u128, sent: u128) -> UnitSums {
        UnitSums {
            received: Sum(received),
            sent: Sum(sent),
        }
    }

    fn asset(policy: u8, name: &[u8]) -> Bytes {
        let mut unit = vec![policy; 28];
        unit.extend_from_slice(name);
        unit.into()
    }

    fn stats(units: impl IntoIterator<Item = (Bytes, UnitSums)>, tx_count: u64) -> AddressStats {
        AddressStats {
            units: units.into_iter().collect(),
            tx_count,
        }
    }

    fn assert_matches_oracle(totals: &AddressStats) {
        assert_eq!(
            address_total(ADDRESS, totals),
            oracle::address_total(ADDRESS, totals),
        );
        assert_eq!(
            account_addresses_total(STAKE_ADDRESS, totals),
            oracle::account_addresses_total(STAKE_ADDRESS, totals),
        );
    }

    #[test]
    fn matches_serde_with_lovelace_and_assets() {
        assert_matches_oracle(&stats(
            [
                (Bytes::from(vec![]), sums(5_000_000, 1_000_000)),
                (asset(0x11, b"tok"), sums(7, 3)),
                (asset(0xab, b""), sums(1, 1)),
            ],
            4,
        ));
    }

    #[test]
    fn matches_serde_without_lovelace() {
        assert_matches_oracle(&stats([(asset(0x11, b"tok"), sums(7, 3))], 1));
    }

    #[test]
    fn matches_serde_with_one_sided_units() {
        assert_matches_oracle(&stats(
            [
                (Bytes::from(vec![]), sums(0, 9)),
                (asset(0x11, b"in"), sums(7, 0)),
                (asset(0x22, b"out"), sums(0, 3)),
                (asset(0x33, b"none"), sums(0, 0)),
            ],
            2,
        ));
    }

    #[test]
    fn matches_serde_at_u128_max() {
        assert_matches_oracle(&stats(
            [
                (Bytes::from(vec![]), sums(u128::MAX, u128::MAX)),
                (asset(0x11, b"tok"), sums(u128::MAX, 1)),
            ],
            1,
        ));
    }

    #[test]
    fn matches_serde_without_assets() {
        assert_matches_oracle(&stats([(Bytes::from(vec![]), sums(42, 0))], 1));
        assert_matches_oracle(&stats([], 0));
    }

    #[test]
    fn matches_serde_past_i32_tx_count() {
        assert_matches_oracle(&stats(
            [(Bytes::from(vec![]), sums(1, 1))],
            i32::MAX as u64 + 7,
        ));
    }

    /// The research's heaviest key carries 1.8 M units; the serde path took
    /// 0.66–0.89 s to encode its body.
    #[test]
    #[cfg_attr(debug_assertions, ignore = "the bound holds for release builds")]
    fn writes_heavy_total_in_time() {
        let units = (0u32..1_800_000).map(|i| {
            let mut unit = vec![0x5a; 28];
            unit.extend_from_slice(b"token");
            unit.extend_from_slice(&i.to_be_bytes());
            (
                unit.into(),
                sums(u64::MAX as u128 + i as u128, i as u128 % 3),
            )
        });

        let totals = stats(units, 2_500_000);

        let start = Instant::now();
        let body = account_addresses_total(STAKE_ADDRESS, &totals);
        let elapsed = start.elapsed();

        eprintln!(
            "wrote {} units, {} bytes in {elapsed:?}",
            totals.units.len(),
            body.len()
        );

        assert!(elapsed <= Duration::from_millis(250), "took {elapsed:?}");
    }
}
