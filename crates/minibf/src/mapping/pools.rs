//! The Blockfrost models of the pool routes: the metadata response and its
//! fetch errors, the retiring and retired rows, the delegator row, the vote
//! and the db-sync arithmetic behind the pool history.

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    pool_delegators_inner::PoolDelegatorsInner, pool_list_retire_inner::PoolListRetireInner,
    pool_votes_inner, DrepsInnerMetadataError, PoolListExtendedInnerMetadata,
    PoolMetadata as PoolMetadataModel,
};
use dolos_cardano::{PoolEpochLog, PoolHash};
use pallas::ledger::{
    addresses::Network,
    primitives::{conway::Vote, StakeCredential},
};
use serde::Serialize;

use crate::offchain::PoolMetadataLoad;

use super::{bech32_pool, IntoModel};

#[derive(Serialize)]
#[serde(untagged)]
pub enum PoolMetadataResponse {
    Metadata(PoolMetadataModel),
    Empty(EmptyObject),
}

#[derive(Default, Serialize)]
pub struct EmptyObject {}

pub(crate) fn build_pool_extended_metadata(
    onchain: Option<&pallas::ledger::primitives::PoolMetadata>,
    (offchain, error): PoolMetadataLoad,
) -> Option<Box<PoolListExtendedInnerMetadata>> {
    onchain.map(|onchain| {
        Box::new(match offchain {
            Some(offchain) => PoolListExtendedInnerMetadata {
                url: Some(onchain.url.clone()),
                hash: Some(hex::encode(&*onchain.hash)),
                error: None,
                ticker: Some(offchain.ticker),
                name: Some(offchain.name),
                description: Some(offchain.description),
                homepage: Some(offchain.homepage),
            },
            None => PoolListExtendedInnerMetadata {
                url: Some(onchain.url.clone()),
                hash: Some(hex::encode(&*onchain.hash)),
                error: error.map(Box::new),
                ..Default::default()
            },
        })
    })
}

pub(crate) fn build_pool_metadata_response(
    operator: impl AsRef<[u8]>,
    onchain: Option<&pallas::ledger::primitives::PoolMetadata>,
    offchain: Option<crate::mapping::PoolOffchainMetadata>,
    error: Option<DrepsInnerMetadataError>,
) -> Result<PoolMetadataResponse, StatusCode> {
    let operator = operator.as_ref();

    let Some(onchain) = onchain else {
        return Ok(PoolMetadataResponse::Empty(EmptyObject::default()));
    };

    Ok(PoolMetadataResponse::Metadata(PoolMetadataModel {
        pool_id: bech32_pool(operator)?,
        hex: hex::encode(operator),
        url: Some(onchain.url.clone()),
        hash: Some(hex::encode(&*onchain.hash)),
        error: error.map(Box::new),
        ticker: offchain.as_ref().map(|x| x.ticker.clone()),
        name: offchain.as_ref().map(|x| x.name.clone()),
        description: offchain.as_ref().map(|x| x.description.clone()),
        homepage: offchain.as_ref().map(|x| x.homepage.clone()),
    }))
}

pub(crate) fn retire_list_model(
    list: Vec<(u64, PoolHash)>,
) -> Result<Vec<PoolListRetireInner>, StatusCode> {
    list.into_iter()
        .map(|(retiring_epoch, operator)| {
            Ok(PoolListRetireInner {
                pool_id: bech32_pool(operator)?,
                epoch: retiring_epoch as i32,
            })
        })
        .collect()
}

pub(crate) struct PoolDelegatorModelBuilder {
    pub(crate) delegator: StakeCredential,
    pub(crate) account: Option<dolos_cardano::model::AccountState>,
    pub(crate) network: Network,
}

impl IntoModel<PoolDelegatorsInner> for PoolDelegatorModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<PoolDelegatorsInner, StatusCode> {
        let address = crate::mapping::stake_cred_to_address(&self.delegator, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let live_stake = self.account.map(|x| x.live_stake()).unwrap_or_default();

        Ok(PoolDelegatorsInner {
            address,
            live_stake: live_stake.to_string(),
        })
    }
}

/// `numerator / denominator` as Blockfrost reports a pool's `active_size`
/// in its history.
///
/// db-sync divides the two stakes as Postgres `numeric` and casts the
/// quotient to `float8`. The division rounds half up to a scale Postgres
/// picks for at least sixteen significant digits (`select_div_scale`), so the
/// float is the one nearest that decimal rather than the exact ratio, and a
/// plain `f64` division of the stakes misses it by an ulp about a quarter of
/// the time. A zero denominator gives zero.
pub(crate) fn numeric_ratio(numerator: u64, denominator: u64) -> f64 {
    if numerator == 0 || denominator == 0 {
        return 0.0;
    }

    // Postgres stores numerics in base-10000 digits: the weight is the index
    // of the leading digit, counted from the units digit.
    let leading = |mut value: u64| {
        let mut weight = 0i32;
        while value >= 10_000 {
            value /= 10_000;
            weight += 1;
        }
        (weight, value)
    };

    let (weight1, first1) = leading(numerator);
    let (weight2, first2) = leading(denominator);

    let mut qweight = weight1 - weight2;
    if first1 <= first2 {
        qweight -= 1;
    }

    let scale = (16 - qweight * 4).clamp(0, 1000) as usize;

    // Long division to `scale` decimals; the remainder stays below the
    // denominator, so ten times it fits a u128.
    let divisor = denominator as u128;
    let mut integer = numerator as u128 / divisor;
    let mut remainder = numerator as u128 % divisor;
    let mut decimals = Vec::with_capacity(scale);

    for _ in 0..scale {
        remainder *= 10;
        decimals.push((remainder / divisor) as u8);
        remainder %= divisor;
    }

    // Round half up, carrying through the decimals into the integer part.
    if remainder * 2 >= divisor {
        let mut carry = true;
        for digit in decimals.iter_mut().rev() {
            if *digit == 9 {
                *digit = 0;
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            integer += 1;
        }
    }

    let decimals: String = decimals
        .iter()
        .map(|digit| char::from(b'0' + digit))
        .collect();

    format!("{integer}.{decimals}")
        .parse()
        .expect("a decimal literal parses as f64")
}

/// A pool margin as db-sync stores it: a `float8` holding the ratio rounded
/// to the nearest double.
///
/// Two operands up to 2^53 convert exactly, so their quotient is that nearest
/// double. Larger ones go through a decimal expansion, which `parse` rounds
/// correctly as long as the cut leaves the quotient on its side of every
/// midpoint between doubles. 64 decimals do: a quotient that is a midpoint
/// has a power-of-two denominator below 2^64 and ends within 63 decimals, and
/// one that is not lies more than 2^-181 from any midpoint.
pub(crate) fn margin_f64(numerator: u64, denominator: u64) -> f64 {
    const EXACT: u64 = 1 << 53;

    if denominator == 0 {
        return 0.0;
    }

    if numerator <= EXACT && denominator <= EXACT {
        return numerator as f64 / denominator as f64;
    }

    let divisor = denominator as u128;
    let mut remainder = numerator as u128 % divisor;
    let mut text = format!("{}.", numerator as u128 / divisor);

    for _ in 0..64 {
        remainder *= 10;
        text.push(char::from(b'0' + (remainder / divisor) as u8));
        remainder %= divisor;
    }

    text.parse().unwrap_or_default()
}

// HACK: Blockfrost does not derive the operator share. Its SQL reports
// `FLOOR(fee + (rewards - fee) * margin)`, and `rewards` when they fall short
// of the fixed cost. db-sync keeps the margin as a `float8`, so Postgres runs
// the whole expression in `float8`; this does the same, operation for
// operation.
pub(crate) fn bf_compatible_fees(log: &PoolEpochLog) -> u64 {
    let rewards = log.total_rewards;
    let fixed = log.fixed_cost;

    if rewards < fixed {
        return rewards;
    }

    let margin = log
        .margin_cost
        .as_ref()
        .map(|margin| margin_f64(margin.numerator, margin.denominator))
        .unwrap_or_default();

    (fixed as f64 + (rewards - fixed) as f64 * margin).floor() as u64
}

pub(crate) fn pool_vote_model(vote: &Vote) -> pool_votes_inner::Vote {
    match vote {
        Vote::Yes => pool_votes_inner::Vote::Yes,
        Vote::No => pool_votes_inner::Vote::No,
        Vote::Abstain => pool_votes_inner::Vote::Abstain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{
        offchain_hash_mismatch_error as hash_mismatch_error,
        pool_connection_error as connection_error, pool_fetch_error,
        pool_http_response_error as http_response_error, OffchainFetchError,
    };
    use blockfrost_openapi::models::dreps_inner_metadata_error::Code;
    use pallas::ledger::primitives::PoolMetadata;

    #[test]
    fn pool_metadata_response_empty_without_onchain_metadata() {
        let response =
            build_pool_metadata_response([1u8; 28], None, None, None).expect("pool metadata");

        assert!(matches!(response, PoolMetadataResponse::Empty(_)));
    }

    #[test]
    fn pool_metadata_response_includes_onchain_and_offchain_fields() {
        let operator = [1u8; 28];
        let onchain = PoolMetadata {
            url: "https://example.com/pool.json".to_string(),
            hash: vec![2u8; 32].into(),
        };
        let offchain = crate::mapping::PoolOffchainMetadata {
            ticker: "TICK".to_string(),
            name: "Pool Name".to_string(),
            description: "Pool Description".to_string(),
            homepage: "https://example.com".to_string(),
        };

        let response = build_pool_metadata_response(operator, Some(&onchain), Some(offchain), None)
            .expect("pool metadata");

        let PoolMetadataResponse::Metadata(response) = response else {
            panic!("expected metadata response");
        };

        assert_eq!(
            response.pool_id,
            bech32_pool(operator).expect("bech32 pool id")
        );
        let expected_hash = hex::encode([2u8; 32]);
        assert_eq!(response.hex, hex::encode(operator));
        assert_eq!(
            response.url.as_deref(),
            Some("https://example.com/pool.json")
        );
        assert_eq!(response.hash.as_deref(), Some(expected_hash.as_str()));
        assert_eq!(response.ticker.as_deref(), Some("TICK"));
        assert_eq!(response.name.as_deref(), Some("Pool Name"));
        assert_eq!(response.description.as_deref(), Some("Pool Description"));
        assert_eq!(response.homepage.as_deref(), Some("https://example.com"));
        assert!(response.error.is_none());
    }

    #[test]
    fn pool_metadata_response_includes_error_details() {
        let operator = [1u8; 28];
        let onchain = PoolMetadata {
            url: "https://tinyurl.com/39a7pnv5".to_string(),
            hash: vec![
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
                0x89, 0xab, 0xcd, 0xef,
            ]
            .into(),
        };
        let error = hash_mismatch_error(
            &onchain.url,
            onchain.hash.as_slice(),
            &[
                0xb1, 0x23, 0xea, 0x83, 0xd1, 0xf8, 0x7a, 0xfc, 0xb9, 0x5a, 0x76, 0x66, 0x1b, 0xe5,
                0x08, 0xb3, 0x7a, 0x09, 0x57, 0xc7, 0xfa, 0x95, 0xba, 0xa8, 0x83, 0xa8, 0x0d, 0x12,
                0x03, 0xff, 0xcd, 0x94,
            ],
        );

        let response = build_pool_metadata_response(operator, Some(&onchain), None, Some(error))
            .expect("pool metadata");

        let PoolMetadataResponse::Metadata(response) = response else {
            panic!("expected metadata response");
        };

        let error = response.error.expect("expected error");
        assert_eq!(error.code, Code::HashMismatch);
        assert!(error
            .message
            .contains("Hash mismatch when fetching metadata"));
    }

    #[test]
    fn pool_metadata_http_response_error_matches_expected_format() {
        let error =
            http_response_error("https://blockfrost.io/fakemetadata", StatusCode::NOT_FOUND);

        assert_eq!(error.code, Code::HttpResponseError);
        assert_eq!(
            error.message,
            "Error Offchain Pool: HTTP Response error from https://blockfrost.io/fakemetadata resulted in HTTP status code : 404 \"Not Found\""
        );
    }

    #[test]
    fn pool_metadata_connection_error_matches_expected_format() {
        let error =
            connection_error("http://localhost:23009/p/pool_clai_registration_metadata.json");

        assert_eq!(error.code, Code::ConnectionError);
        assert_eq!(
            error.message,
            "Error Offchain Pool: Connection failure error when fetching metadata from http://localhost:23009/p/pool_clai_registration_metadata.json."
        );
    }

    #[test]
    fn pool_fetch_error_preserves_connection_and_http_errors() {
        let url = "https://example.com/pool.json";
        let connection = pool_fetch_error(url, OffchainFetchError::Connection)
            .expect("The connection error is missing.");
        assert_eq!(connection.code, Code::ConnectionError);
        assert_eq!(
            connection.message,
            "Error Offchain Pool: Connection failure error when fetching metadata from https://example.com/pool.json."
        );

        let http = pool_fetch_error(url, OffchainFetchError::HttpResponse(StatusCode::NOT_FOUND))
            .expect("The HTTP response error is missing.");
        assert_eq!(http.code, Code::HttpResponseError);
        assert_eq!(
            http.message,
            "Error Offchain Pool: HTTP Response error from https://example.com/pool.json resulted in HTTP status code : 404 \"Not Found\""
        );
    }

    #[test]
    fn pool_fetch_error_suppresses_client_errors() {
        assert!(
            pool_fetch_error("https://example.com/pool.json", OffchainFetchError::Client).is_none()
        );
    }

    #[test]
    fn pool_fetch_error_maps_body_errors_to_connection_errors() {
        let error = pool_fetch_error("https://example.com/pool.json", OffchainFetchError::Body)
            .expect("The body error is missing.");
        assert_eq!(error, connection_error("https://example.com/pool.json"));
    }

    #[test]
    fn pool_fetch_error_maps_size_exceeded() {
        let error = pool_fetch_error(
            "https://example.com/pool.json",
            OffchainFetchError::SizeExceeded,
        )
        .expect("The size limit error is missing.");

        assert_eq!(error.code, Code::SizeExceeded);
        assert_eq!(
            error.message,
            "Error Offchain Pool: Size error, fetching metadata from https://example.com/pool.json exceeded 512 bytes."
        );
    }

    /// Pool stake, epoch stake and the `active_size` Blockfrost reports, from
    /// mainnet (pool1pu5jlj…, epochs 212, 214, 229) and preview (pool1a7h89…,
    /// epochs 42, 58). A plain `f64` division misses all but the last.
    #[test]
    fn numeric_ratio_matches_blockfrost() {
        let cases = [
            (76778927458702, 12106602864871837, 0.006341905183119658),
            (76957913014111, 13382718156097189, 0.0057505442553946965),
            (13375523282499, 19510155416091834, 0.0006855672339476581),
            (2663056412603, 375970644528781, 0.0070831498452245244),
            (1536346314736, 384837670103602, 0.003992193161138307),
        ];

        for (numerator, denominator, expected) in cases {
            assert_eq!(numeric_ratio(numerator, denominator), expected);
        }

        assert_ne!(76778927458702.0 / 12106602864871837.0, 0.006341905183119658);
    }

    #[test]
    fn numeric_ratio_edges() {
        assert_eq!(numeric_ratio(0, 100), 0.0);
        assert_eq!(numeric_ratio(5, 0), 0.0);
        assert_eq!(numeric_ratio(7, 7), 1.0);
        assert_eq!(numeric_ratio(1, 3), 1.0 / 3.0);
        assert_eq!(numeric_ratio(u64::MAX, u64::MAX), 1.0);
    }

    /// The quotient 0.00323897880448927272|72… is cut at 20 decimals, and the
    /// next digit rounds the cut up to a different double than the truncated
    /// decimal gives. 1273 / 10485760 is 0.00012140274047851562|5, an exact
    /// half at the cut, which rounds up where half down or half to even would
    /// keep a different double. 8529360004794 / 20235083305638617 is
    /// 0.00042151346134647476|4999…, just short of a half, and stays down.
    /// 3000000000014 / 21955336489517329 is 0.00013664103947786739|96…, which
    /// carries through the 9 to …786740.
    #[test]
    fn numeric_ratio_rounds_half_up_at_the_scale() {
        assert_eq!(
            numeric_ratio(71_257_533_698_764, 22_000_000_000_000_000),
            0.003238978804489273
        );
        assert_ne!(
            "0.00323897880448927272".parse::<f64>().unwrap(),
            0.003238978804489273
        );

        assert_eq!(numeric_ratio(1273, 10_485_760), 0.00012140274047851564);
        assert_ne!(
            "0.00012140274047851562".parse::<f64>().unwrap(),
            0.00012140274047851564
        );

        assert_eq!(
            numeric_ratio(8_529_360_004_794, 20_235_083_305_638_617),
            0.00042151346134647474
        );
        assert_ne!(
            "0.00042151346134647477".parse::<f64>().unwrap(),
            0.00042151346134647474
        );

        assert_eq!(
            numeric_ratio(3_000_000_000_014, 21_955_336_489_517_329),
            0.0001366410394778674
        );
        assert_ne!(
            "0.00013664103947786749".parse::<f64>().unwrap(),
            0.0001366410394778674
        );
    }

    /// Both stakes lead with the base-10000 digit 1, so the quotient's weight
    /// drops by one and the scale is 24 decimals, not 20:
    /// 0.000093332914661520271677 against 0.00009333291466152027, which is a
    /// different double. 10^16 + 1 is five digits led by 1, not four led by
    /// 10000, so 1.25e12 over it also takes 24 decimals.
    #[test]
    fn numeric_ratio_scales_past_equal_leading_digits() {
        assert_eq!(
            numeric_ratio(1_129_944_532_028, 12_106_602_864_871_837),
            9.333291466152028e-5
        );
        assert_ne!(
            "0.00009333291466152027".parse::<f64>().unwrap(),
            9.333291466152028e-5
        );

        assert_eq!(
            numeric_ratio(1_250_000_000_000, 10_000_000_000_000_001),
            0.00012499999999999998
        );
        assert_ne!(
            "0.00012499999999999999".parse::<f64>().unwrap(),
            0.00012499999999999998
        );
    }

    fn fee_log(total_rewards: u64, fixed_cost: u64, margin: Option<(u64, u64)>) -> PoolEpochLog {
        PoolEpochLog {
            total_rewards,
            fixed_cost,
            margin_cost: margin.map(|(numerator, denominator)| {
                pallas::ledger::primitives::RationalNumber {
                    numerator,
                    denominator,
                }
            }),
            ..Default::default()
        }
    }

    #[test]
    fn bf_fees_fall_back_to_the_rewards_below_the_fixed_cost() {
        assert_eq!(
            bf_compatible_fees(&fee_log(0, 340_000_000, Some((1, 100)))),
            0
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(1_000, 340_000_000, Some((1, 100)))),
            1_000
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(25_816_995_192, 340_000_000, Some((0, 1)))),
            340_000_000
        );
        assert_eq!(bf_compatible_fees(&fee_log(500, 100, None)), 100);
    }

    /// Blockfrost runs the formula in `float8`, and so floors one lovelace
    /// below the exact `floor(940189878034 * 3 / 11) = 256415421282` here.
    #[test]
    fn bf_fees_round_like_float8() {
        assert_eq!(
            bf_compatible_fees(&fee_log(940_189_878_034, 0, Some((3, 11)))),
            256_415_421_281
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(905_252_471_403, 500_000_000, Some((3, 11)))),
            247_250_674_018
        );
    }

    /// The `Rational64` arithmetic this replaced cast the margin into `i64`, so
    /// a denominator near `u64::MAX` came out negative or overflowed. The
    /// ledger accepts an unreduced margin, and a 100% one past 2^53 keeps its
    /// integer part.
    #[test]
    fn bf_fees_take_any_margin() {
        let fees = bf_compatible_fees(&fee_log(
            10_000_000_000_000,
            340_000_000,
            Some((u64::MAX / 4, u64::MAX - 1)),
        ));
        assert_eq!(fees, 340_000_000 + (10_000_000_000_000 - 340_000_000) / 4);

        assert_eq!(margin_f64(u64::MAX / 4, u64::MAX - 1), 0.25);
        assert_eq!(margin_f64(1, 0), 0.0);

        let full = (1 << 60, 1 << 60);

        assert_eq!(margin_f64(full.0, full.1), 1.0);
        assert_eq!(
            bf_compatible_fees(&fee_log(1_000_000_000_000, 340_000_000, Some(full))),
            1_000_000_000_000
        );
    }

    /// Margins past 2^53 go through the decimal expansion. The first sits
    /// exactly on a midpoint between two doubles, which a 60-decimal cut
    /// rounded down, one lovelace short in `fees`. The second is a double
    /// away from the plain `f64` division of its operands, and so is the
    /// third, whose numerator is within 2^53 and whose odd denominator is just
    /// past it, down to 2^53 + 1.
    #[test]
    fn bf_fees_round_wide_margins_to_the_nearest_double() {
        let midpoint = (12_777_994_273_690_747, 1 << 63);

        assert_eq!(
            margin_f64(midpoint.0, midpoint.1),
            12777994273690747f64 / 9223372036854775808f64
        );
        assert_eq!(
            bf_compatible_fees(&fee_log(446_511_071_931, 340_000_000, Some(midpoint))),
            958_122_242
        );

        let (numerator, denominator) = (3_071_691_459_605_072_764, 15_394_439_036_360_229_302);

        assert_eq!(margin_f64(numerator, denominator), 0.19953253589494388);
        assert_ne!(numerator as f64 / denominator as f64, 0.19953253589494388);

        let narrow = (675_963_839_381_191, 9_007_199_254_746_239);

        assert_eq!(margin_f64(narrow.0, narrow.1), 0.07504706182945822);
        assert_ne!(narrow.0 as f64 / narrow.1 as f64, 0.07504706182945822);
        assert_eq!(
            bf_compatible_fees(&fee_log(73_496_656_964_163, 340_000_000, Some(narrow))),
            5_516_022_643_447
        );

        assert_eq!(margin_f64(narrow.0, (1 << 53) + 1), 0.07504706182950192);
        assert_ne!(
            narrow.0 as f64 / ((1u64 << 53) + 1) as f64,
            0.07504706182950192
        );
    }
}
