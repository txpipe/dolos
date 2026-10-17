//! The `/total` bodies, written from an `AddressStats` total.

use dolos_cardano::{AddressStats, UnitSums};

use super::direct_json::{write_hex, write_int, write_int_str, write_str, WriteJson};

/// The `AddressContentTotal` body of `/addresses/{address}/total`.
pub struct AddressTotalBody {
    pub address: String,
    pub totals: AddressStats,
}

/// The `AccountAddressesTotal` body of
/// `/accounts/{stake_address}/addresses/total`.
pub struct AccountAddressesTotalBody {
    pub stake_address: String,
    pub totals: AddressStats,
}

impl WriteJson for AddressTotalBody {
    fn size_hint(&self) -> usize {
        size_hint(&self.totals)
    }

    fn write_json(&self, out: &mut Vec<u8>) {
        write_total(out, "address", &self.address, &self.totals);
    }
}

impl WriteJson for AccountAddressesTotalBody {
    fn size_hint(&self) -> usize {
        size_hint(&self.totals)
    }

    fn write_json(&self, out: &mut Vec<u8>) {
        write_total(out, "stake_address", &self.stake_address, &self.totals);
    }
}

fn size_hint(totals: &AddressStats) -> usize {
    128 + totals.units.len() * 2 * 112
}

fn write_total(out: &mut Vec<u8>, field: &str, id: &str, totals: &AddressStats) {
    out.extend_from_slice(b"{\"");
    out.extend_from_slice(field.as_bytes());
    out.extend_from_slice(b"\":");
    write_str(out, id);
    out.extend_from_slice(b",\"received_sum\":[");
    write_side(out, totals, |sums| sums.received.0);
    out.extend_from_slice(b"],\"sent_sum\":[");
    write_side(out, totals, |sums| sums.sent.0);
    out.extend_from_slice(b"],\"tx_count\":");
    write_int(out, totals.tx_count as i32);
    out.push(b'}');
}

/// One side's amounts: lovelace first, then every asset with a nonzero sum on
/// that side, by unit.
fn write_side(out: &mut Vec<u8>, totals: &AddressStats, pick: fn(&UnitSums) -> u128) {
    let lovelace = totals
        .units
        .get(&Vec::new().into())
        .map(pick)
        .unwrap_or_default();

    out.extend_from_slice(b"{\"unit\":\"lovelace\",\"quantity\":");
    write_int_str(out, lovelace);
    out.push(b'}');

    for (unit, sums) in &totals.units {
        let quantity = pick(sums);

        if unit.is_empty() || quantity == 0 {
            continue;
        }

        out.extend_from_slice(b",{\"unit\":");
        write_hex(out, unit);
        out.extend_from_slice(b",\"quantity\":");
        write_int_str(out, quantity);
        out.push(b'}');
    }
}

#[cfg(test)]
pub(crate) mod oracle {
    //! The serde path the bodies replaced, kept as the oracle they are checked
    //! against.

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
        let address = AddressTotalBody {
            address: ADDRESS.to_string(),
            totals: totals.clone(),
        };
        let account = AccountAddressesTotalBody {
            stake_address: STAKE_ADDRESS.to_string(),
            totals: totals.clone(),
        };

        assert_eq!(
            address.to_json_vec(),
            oracle::address_total(ADDRESS, totals),
        );
        assert_eq!(
            account.to_json_vec(),
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

        let account = AccountAddressesTotalBody {
            stake_address: STAKE_ADDRESS.to_string(),
            totals: stats(units, 2_500_000),
        };

        let start = Instant::now();
        let body = account.to_json_vec();
        let elapsed = start.elapsed();

        eprintln!(
            "wrote {} units, {} bytes in {elapsed:?}",
            account.totals.units.len(),
            body.len()
        );

        assert!(elapsed <= Duration::from_millis(250), "took {elapsed:?}");
    }
}
