use axum::{http::StatusCode, Json};
use itertools::Itertools;
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::ToPrimitive;
use pallas::{
    codec::minicbor,
    crypto::hash::{Hash, Hasher},
    ledger::{
        addresses::{Address, Network, StakeAddress, StakePayload},
        primitives::{
            alonzo,
            conway::{DRep, DatumOption, RedeemerTag, ScriptRef},
            ExUnitPrices, ExUnits, PlutusData, StakeCredential,
        },
        traverse::{MultiEraOutput, MultiEraValue, OriginalHash},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::{Arc, LazyLock},
    time::Duration,
};

use blockfrost_openapi::models::{
    address_content::{AddressContent, Type as AddressType},
    address_content_extended::{AddressContentExtended, Type as AddressExtendedType},
    address_content_extended_amount_inner::AddressContentExtendedAmountInner,
    address_utxo_content_inner::AddressUtxoContentInner,
    dreps_inner_metadata_error::{Code, DrepsInnerMetadataError},
    script_redeemers_inner::Purpose as ScriptRedeemerPurpose,
    script_utxos_inner::ScriptUtxosInner,
    tx_content_output_amount_inner::TxContentOutputAmountInner,
    tx_content_pool_certs_inner_relays_inner::TxContentPoolCertsInnerRelaysInner,
    tx_content_utxo_outputs_inner::TxContentUtxoOutputsInner,
};

use dolos_cardano::pallas_extras;
use dolos_core::{async_query::BlockRefMeta, TxHash, TxoIdx, TxoRef, UtxoValue};

pub mod accounts;
pub mod address_stats;
pub mod assets;
pub mod blocks;
pub mod cost_models;
pub mod direct_json;
pub mod epochs;
pub mod genesis;
pub mod governance;
pub mod network;
pub mod protocol_params;
pub mod txs;

macro_rules! try_into_or_500 {
    ($expr:expr) => {
        $expr.try_into().map_err(|err| {
            tracing::error!(error = ?err, expr = stringify!($expr), "numeric conversion failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    };
}
pub(crate) use try_into_or_500;

/// The `i32` a Blockfrost model types a parameter as, or a 500 when the value
/// does not fit: the chain lets a proposal set fees, sizes and counts anywhere
/// in `u64`, and a wrapped number would read as a valid, negative parameter.
pub fn i32_or_500<T>(value: T) -> Result<i32, StatusCode>
where
    T: TryInto<i32> + Copy + std::fmt::Debug,
{
    value.try_into().map_err(|_| {
        tracing::error!(value = ?value, "parameter does not fit the i32 the model types it as");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

pub fn round_f64<const DECIMALS: u8>(val: f64) -> f64 {
    let multiplier = 10_f64.powi(DECIMALS as i32);
    (val * multiplier).round() / multiplier
}

pub fn parse_datetime_into_timestamp(s: &str) -> Result<i32, StatusCode> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|x| x.timestamp() as i32)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// A ratio as the plain quotient, at the precision `f64` gives it.
pub fn rational_to_f64_unrounded(val: &alonzo::RationalNumber) -> f64 {
    val.numerator as f64 / val.denominator as f64
}

/// A ratio rounded to `DECIMALS` places.
pub fn rational_to_f64<const DECIMALS: u8>(val: &alonzo::RationalNumber) -> f64 {
    round_f64::<DECIMALS>(rational_to_f64_unrounded(val))
}

/// How a protocol parameter model writes its ratios out.
///
/// Blockfrost rounds the parameters in force to the places it shows them
/// with, but serves the change a proposal asks for straight from db-sync's
/// `param_proposal` columns at full `double precision`: a proposal setting
/// tau to 1/6 comes back as 0.16666666666666666 where
/// `/epochs/{n}/parameters` says 0.167. The models that share their field
/// mapping through `protocol_params_model!` name the format as a type, so
/// the places stay written at the field and only whether they apply varies.
pub trait RatioFormat {
    /// `value` as served, `DECIMALS` being the places the field is shown
    /// with whenever it is rounded at all.
    fn to_f64<const DECIMALS: u8>(value: &alonzo::RationalNumber) -> f64;
}

/// Rounded to the places written at the field.
pub struct Rounded;

impl RatioFormat for Rounded {
    fn to_f64<const DECIMALS: u8>(value: &alonzo::RationalNumber) -> f64 {
        rational_to_f64::<DECIMALS>(value)
    }
}

/// The quotient as is, whatever places the field writes.
pub struct Unrounded;

impl RatioFormat for Unrounded {
    fn to_f64<const DECIMALS: u8>(value: &alonzo::RationalNumber) -> f64 {
        rational_to_f64_unrounded(value)
    }
}

pub const DREP_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("drep");
const POOL_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("pool");
const ASSET_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("asset");
const CALIDUS_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("calidus");
const GOV_ACTION_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("gov_action");
const CC_COLD_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("cc_cold");
const CC_HOT_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("cc_hot");

#[inline]
pub fn bech32(hrp: bech32::Hrp, key: impl AsRef<[u8]>) -> Result<String, StatusCode> {
    bech32::encode::<bech32::Bech32>(hrp, key.as_ref())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub fn bech32_drep(drep: &DRep) -> Result<String, StatusCode> {
    let mut payload = [0; 29];

    let key_prefix: [u8; 1] = [0b00100010];
    let script_prefix: [u8; 1] = [0b00100011];

    match drep {
        DRep::Key(key) => {
            payload[..1].copy_from_slice(&key_prefix);
            payload[1..].copy_from_slice(key.as_slice());
        }
        DRep::Script(key) => {
            payload[..1].copy_from_slice(&script_prefix);
            payload[1..].copy_from_slice(key.as_slice());
        }
        DRep::Abstain => return Ok("drep_always_abstain".to_string()),
        DRep::NoConfidence => return Ok("drep_always_no_confidence".to_string()),
    };

    bech32(DREP_HRP, payload)
}

/// The role of a constitutional-committee credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitteeCredentialRole {
    Hot,
    Cold,
}

impl CommitteeCredentialRole {
    /// The high nibble of the CIP-129 header byte.
    const fn nibble(self) -> u8 {
        match self {
            Self::Hot => 0x0,
            Self::Cold => 0x1,
        }
    }

    const fn hrp(self) -> bech32::Hrp {
        match self {
            Self::Hot => CC_HOT_HRP,
            Self::Cold => CC_COLD_HRP,
        }
    }
}

/// The low nibble of the CIP-129 header byte for a key hash.
const CC_KEY_NIBBLE: u8 = 0x2;

/// The low nibble of the CIP-129 header byte for a script hash.
const CC_SCRIPT_NIBBLE: u8 = 0x3;

/// CIP-129 credential ID for the constitutional committee.
///
/// The header byte contains the role in the high nibble (cold `0x1`, hot
/// `0x0`). It contains the credential kind in the low nibble (key `0x2`, script
/// `0x3`). The 28-byte hash follows the header byte. The `cc_cold` or `cc_hot`
/// bech32 payload contains this header. The `..._hex` field that Blockfrost
/// returns is the bare hash.
fn bech32_committee(
    role: CommitteeCredentialRole,
    cred: &StakeCredential,
) -> Result<String, StatusCode> {
    let (hash, kind_nibble): (&[u8], u8) = match cred {
        StakeCredential::AddrKeyhash(key) => (key.as_ref(), CC_KEY_NIBBLE),
        StakeCredential::ScriptHash(key) => (key.as_ref(), CC_SCRIPT_NIBBLE),
    };

    let header = (role.nibble() << 4) | kind_nibble;

    bech32(role.hrp(), [&[header], hash].concat())
}

pub fn bech32_committee_cold(cred: &StakeCredential) -> Result<String, StatusCode> {
    bech32_committee(CommitteeCredentialRole::Cold, cred)
}

pub fn bech32_committee_hot(cred: &StakeCredential) -> Result<String, StatusCode> {
    bech32_committee(CommitteeCredentialRole::Hot, cred)
}

/// This function parses a CIP-129 constitutional-committee credential ID. It
/// is the inverse of `bech32_committee_hot` and `bech32_committee_cold`.
///
/// The Bech32 prefix and the header byte both give the role. If the two roles
/// are not the same, the ID is not valid. Bech32 permits an ID in upper case.
/// The function accepts the prefix in upper case or in lower case.
pub fn parse_committee_id(
    id: &str,
) -> Result<(CommitteeCredentialRole, StakeCredential), StatusCode> {
    let parsed = bech32::primitives::decode::CheckedHrpstring::new::<bech32::Bech32>(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let role = if parsed.hrp() == CC_HOT_HRP {
        CommitteeCredentialRole::Hot
    } else if parsed.hrp() == CC_COLD_HRP {
        CommitteeCredentialRole::Cold
    } else {
        return Err(StatusCode::BAD_REQUEST);
    };

    let payload: Vec<u8> = parsed.byte_iter().collect();
    let (header, hash) = payload.split_first().ok_or(StatusCode::BAD_REQUEST)?;
    let hash: Hash<28> = <[u8; 28]>::try_from(hash)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .into();

    if header >> 4 != role.nibble() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let credential = match header & 0x0f {
        CC_KEY_NIBBLE => StakeCredential::AddrKeyhash(hash),
        CC_SCRIPT_NIBBLE => StakeCredential::ScriptHash(hash),
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    Ok((role, credential))
}

#[cfg(test)]
mod committee_id_tests {
    use super::*;

    fn credentials() -> [StakeCredential; 2] {
        [
            StakeCredential::AddrKeyhash(Hash::from([21u8; 28])),
            StakeCredential::ScriptHash(Hash::from([22u8; 28])),
        ]
    }

    fn encode(hrp: &str, payload: &[u8]) -> String {
        bech32::encode::<bech32::Bech32>(bech32::Hrp::parse_unchecked(hrp), payload)
            .expect("The test payload cannot be encoded.")
    }

    #[test]
    fn parse_committee_id_inverts_the_encoders() {
        for cred in credentials() {
            let hot = bech32_committee_hot(&cred).unwrap();
            assert_eq!(
                parse_committee_id(&hot).unwrap(),
                (CommitteeCredentialRole::Hot, cred.clone())
            );

            let cold = bech32_committee_cold(&cred).unwrap();
            assert_eq!(
                parse_committee_id(&cold).unwrap(),
                (CommitteeCredentialRole::Cold, cred.clone())
            );
        }
    }

    #[test]
    fn parse_committee_id_ignores_the_case() {
        for cred in credentials() {
            let hot = bech32_committee_hot(&cred).unwrap();
            assert_eq!(
                parse_committee_id(&hot.to_ascii_uppercase()).unwrap(),
                (CommitteeCredentialRole::Hot, cred.clone())
            );

            let cold = bech32_committee_cold(&cred).unwrap();
            assert_eq!(
                parse_committee_id(&cold.to_ascii_uppercase()).unwrap(),
                (CommitteeCredentialRole::Cold, cred)
            );
        }
    }

    #[test]
    fn parse_committee_id_rejects_malformed_ids() {
        let hash = [21u8; 28];

        let rejected = [
            // The payload has no header byte.
            encode("cc_hot", &hash),
            // The payload has one byte too many.
            encode("cc_hot", &[&[0x02u8][..], &hash[..], &[0u8][..]].concat()),
            // The low nibble is not a key or a script.
            encode("cc_hot", &[&[0x00u8][..], &hash[..]].concat()),
            encode("cc_hot", &[&[0x01u8][..], &hash[..]].concat()),
            // The high nibble is not a role.
            encode("cc_hot", &[&[0x22u8][..], &hash[..]].concat()),
            // The prefix and the header give different roles.
            encode("cc_hot", &[&[0x12u8][..], &hash[..]].concat()),
            encode("cc_cold", &[&[0x02u8][..], &hash[..]].concat()),
            // The prefix is not a committee prefix.
            encode("drep", &[&[0x22u8][..], &hash[..]].concat()),
            // The text is not Bech32.
            "cc_hot1deadbeef".to_string(),
            hex::encode(hash),
        ];

        for id in rejected {
            assert_eq!(
                parse_committee_id(&id),
                Err(StatusCode::BAD_REQUEST),
                "The parser accepted {id}."
            );
        }
    }
}

pub fn bech32_pool(key: impl AsRef<[u8]>) -> Result<String, StatusCode> {
    bech32(POOL_HRP, key)
}

pub fn bech32_calidus(key: impl AsRef<[u8]>) -> Result<String, StatusCode> {
    bech32(CALIDUS_HRP, key)
}

/// CIP-129 governance action id: the proposing tx hash followed by the index
/// of the action inside that tx.
///
/// Blockfrost writes the index as the shortest big-endian byte string that
/// holds it, so index 0 still costs a byte and an index past 255 costs two.
/// No tx comes close to 256 proposals, but matching the rule is free.
pub fn bech32_gov_action(tx: &Hash<32>, idx: u32) -> Result<String, StatusCode> {
    let bytes = idx.to_be_bytes();
    let first = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len() - 1);

    bech32(GOV_ACTION_HRP, [tx.as_slice(), &bytes[first..]].concat())
}

/// Read a CIP-129 governance action id back into the proposing tx hash and
/// the action index, the inverse of [`bech32_gov_action`].
///
/// The index is whatever big-endian bytes trail the hash, at any length and
/// zero padding included: Blockfrost parses the whole suffix, so a padded
/// `00 01` and the bare 32-byte form are both aliases it serves. An index
/// past `u32` can never name a proposal; it saturates so the lookup misses
/// instead of failing the parse.
///
/// CIP-129 identifiers carry the Bech32 checksum, so this decodes with Bech32
/// and rejects the Bech32m checksum that `bech32::decode` would also accept.
pub fn parse_gov_action_id(id: &str) -> Result<(Hash<32>, u32), StatusCode> {
    let parsed = bech32::primitives::decode::CheckedHrpstring::new::<bech32::Bech32>(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    if parsed.hrp() != GOV_ACTION_HRP {
        return Err(StatusCode::BAD_REQUEST);
    }

    let payload: Vec<u8> = parsed.byte_iter().collect();

    let Some((tx, idx)) = payload.split_at_checked(32) else {
        return Err(StatusCode::BAD_REQUEST);
    };

    let significant = idx.iter().position(|byte| *byte != 0).unwrap_or(idx.len());
    let idx = &idx[significant..];

    let idx = if idx.len() > size_of::<u32>() {
        u32::MAX
    } else {
        idx.iter().fold(0u32, |acc, byte| (acc << 8) | *byte as u32)
    };

    Ok((Hash::from(tx), idx))
}

pub fn asset_fingerprint(subject: &[u8]) -> Result<String, StatusCode> {
    let mut hasher = pallas::crypto::hash::Hasher::<160>::new();
    hasher.input(subject);
    let hash = hasher.finalize();
    bech32(ASSET_HRP, hash.as_ref())
}

pub fn stake_cred_to_address(cred: &StakeCredential, network: Network) -> StakeAddress {
    match cred {
        StakeCredential::AddrKeyhash(key) => StakeAddress::new(network, StakePayload::Stake(*key)),
        StakeCredential::ScriptHash(key) => StakeAddress::new(network, StakePayload::Script(*key)),
    }
}

pub fn vkey_to_stake_address(vkey: Hash<28>, network: Network) -> StakeAddress {
    StakeAddress::new(network, StakePayload::Stake(vkey))
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PoolOffchainMetadata {
    pub name: String,
    pub description: String,
    pub ticker: String,
    pub homepage: String,
}

pub(crate) fn pool_http_response_error(url: &str, status: StatusCode) -> DrepsInnerMetadataError {
    let reason = status.canonical_reason().unwrap_or("Unknown");

    DrepsInnerMetadataError::new(
        Code::HttpResponseError,
        format!(
            "Error Offchain Pool: HTTP Response error from {url} resulted in HTTP status code : {} \"{reason}\"",
            status.as_u16(),
        ),
    )
}

pub(crate) fn pool_connection_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::ConnectionError,
        format!("Error Offchain Pool: Connection failure error when fetching metadata from {url}."),
    )
}

fn pool_size_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::SizeExceeded,
        format!(
            "Error Offchain Pool: Size error, fetching metadata from {url} exceeded {MAX_POOL_METADATA_BYTES} bytes."
        ),
    )
}

pub(crate) fn pool_fetch_error(
    url: &str,
    error: OffchainFetchError,
) -> Option<DrepsInnerMetadataError> {
    match error {
        OffchainFetchError::Client => None,
        OffchainFetchError::Connection | OffchainFetchError::Body => {
            Some(pool_connection_error(url))
        }
        OffchainFetchError::HttpResponse(status) => Some(pool_http_response_error(url, status)),
        OffchainFetchError::SizeExceeded => Some(pool_size_error(url)),
    }
}

pub(crate) async fn pool_offchain_body(url: &str) -> Result<Vec<u8>, OffchainFetchError> {
    let client = offchain_http_client().map_err(|_| OffchainFetchError::Client)?;

    fetch_offchain_body(client, url, Duration::from_secs(5), MAX_POOL_METADATA_BYTES).await
}

#[derive(Debug)]
pub(crate) enum OffchainFetchError {
    Client,
    Connection,
    HttpResponse(StatusCode),
    Body,
    SizeExceeded,
}

impl OffchainFetchError {
    fn into_metadata_error(self, url: &str) -> DrepsInnerMetadataError {
        match self {
            Self::Client => offchain_unknown_error(url),
            Self::Connection | Self::Body => offchain_connection_error(url),
            Self::HttpResponse(status) => offchain_http_response_error(url, status),
            Self::SizeExceeded => offchain_size_error(url),
        }
    }
}

/// This structure contains governance metadata from an anchor.
pub struct AnchorMetadata {
    /// This field contains the JSON body for `json_metadata`.
    pub json: serde_json::Value,

    /// This field contains the raw body in the PostgreSQL `bytea` format.
    /// The value starts with `\x` and then contains lowercase hexadecimal
    /// bytes.
    pub bytes: String,
}

pub(crate) const MAX_POOL_METADATA_BYTES: usize = 512;
const MAX_ANCHOR_METADATA_BYTES: usize = 3_000_000;
const MAX_OFFCHAIN_REDIRECTS: usize = 3;

/// This is the total timeout for one governance-anchor request. It is more than
/// a plain HTTP metadata request needs. An `ipfs://` anchor resolves through a
/// public gateway. A cold content lookup on that gateway can take several
/// seconds.
const ANCHOR_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

struct PublicDnsResolver;

impl reqwest::dns::Resolve for PublicDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();

        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?
                .filter(|address| is_public_ip(address.ip()))
                .collect::<Vec<_>>();

            if addresses.is_empty() {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "the host has no public IP address",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }

            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();

    !(a == 0
        || a == 10
        || (a == 100 && (64..=127).contains(&b))
        || a == 127
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && (c == 0 || c == 2))
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(ip) = ip.to_ipv4() {
        return is_public_ipv4(ip);
    }

    let [a, b, ..] = ip.segments();

    (a & 0xe000) == 0x2000
        && !(a == 0x2001 && b < 0x0200)
        && !(a == 0x2001 && b == 0x0db8)
        && a != 0x2002
        && !(a == 0x3fff && (b & 0xf000) == 0)
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn is_public_http_url(url: &reqwest::Url) -> bool {
    if url.scheme() != "http" && url.scheme() != "https" {
        return false;
    }

    let Some(host) = url.host_str() else {
        return false;
    };

    let host = host.trim_end_matches('.');

    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return false;
    }

    // `host_str` keeps the brackets around an IPv6 literal, so the client never
    // resolves it through the DNS filter. Strip them to classify the literal.
    let literal = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);

    literal.parse::<IpAddr>().map(is_public_ip).unwrap_or(true)
}

fn offchain_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .dns_resolver(Arc::new(PublicDnsResolver))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() > MAX_OFFCHAIN_REDIRECTS {
                attempt.error("The number of redirects is more than the limit.")
            } else if is_public_http_url(attempt.url()) {
                attempt.follow()
            } else {
                attempt.error("The redirect target is not public.")
            }
        }))
        .user_agent("Dolos MiniBF")
}

/// This function returns the HTTP client for metadata outside the blockchain.
///
/// This function creates the client one time and stores the result. All
/// metadata requests use this client and its connection pool. If the builder
/// returns an error, this function stores and returns the same error.
fn offchain_http_client() -> Result<&'static reqwest::Client, &'static reqwest::Error> {
    static CLIENT: LazyLock<Result<reqwest::Client, reqwest::Error>> =
        LazyLock::new(|| offchain_http_client_builder().build());

    CLIENT.as_ref()
}

async fn fetch_offchain_body(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
    max_bytes: usize,
) -> Result<Vec<u8>, OffchainFetchError> {
    let url = match reqwest::Url::parse(url) {
        Ok(url) if is_public_http_url(&url) => url,
        _ => return Err(OffchainFetchError::Connection),
    };

    let response = client
        .get(url)
        .timeout(timeout)
        .send()
        .await
        .map_err(|_| OffchainFetchError::Connection)?;

    if response.status() != StatusCode::OK {
        return Err(OffchainFetchError::HttpResponse(response.status()));
    }

    read_offchain_body(response, max_bytes).await
}

pub(crate) fn offchain_hash_mismatch_error(
    url: &str,
    expected_hash: &[u8],
    actual_hash: &[u8],
) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::HashMismatch,
        format!(
            "Hash mismatch when fetching metadata from {url}. Expected \"{}\" but got \"{}\".",
            hex::encode(expected_hash),
            hex::encode(actual_hash),
        ),
    )
}

fn offchain_http_response_error(url: &str, status: StatusCode) -> DrepsInnerMetadataError {
    let reason = status.canonical_reason().unwrap_or("Unknown");

    DrepsInnerMetadataError::new(
        Code::HttpResponseError,
        format!(
            "The server at {url} returned HTTP status {} \"{reason}\".",
            status.as_u16(),
        ),
    )
}

fn offchain_connection_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::ConnectionError,
        format!("The client cannot connect to {url}."),
    )
}

fn offchain_decode_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::DecodeError,
        format!("The client cannot parse JSON from {url}."),
    )
}

fn offchain_size_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::SizeExceeded,
        format!("The response from {url} is larger than {MAX_ANCHOR_METADATA_BYTES} bytes."),
    )
}

fn offchain_unknown_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::UnknownError,
        format!("The API cannot create an HTTP client for {url}."),
    )
}

fn offchain_no_gateway_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        Code::UnknownError,
        format!("The API cannot resolve {url} without an IPFS gateway."),
    )
}

async fn read_offchain_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, OffchainFetchError> {
    if response
        .content_length()
        .is_some_and(|size| size > max_bytes as u64)
    {
        return Err(OffchainFetchError::SizeExceeded);
    }

    let capacity = response
        .content_length()
        .unwrap_or_default()
        .min(max_bytes as u64) as usize;
    let mut body = Vec::with_capacity(capacity);

    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|_| OffchainFetchError::Body)?;

        let Some(chunk) = chunk else {
            break;
        };

        if chunk.len() > max_bytes - body.len() {
            return Err(OffchainFetchError::SizeExceeded);
        }

        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

/// This function resolves a governance-anchor URL to an ordered list of HTTP(S)
/// URLs.
///
/// An `ipfs://<cid>[/<path>]` URL expands to one
/// `<gateway>/ipfs/<cid>[/<path>]` candidate for each gateway, in gateway
/// order. The caller sends a request to each candidate in order until one
/// serves the content. The function also accepts a redundant `ipfs/` after the
/// scheme.
///
/// The CID keeps its original case. The CID stays in the path, so it does not
/// pass through URL host normalization. That normalization lowercases a base58
/// CIDv0.
///
/// Every other URL gives a single candidate, unchanged. An `ipfs://` URL with
/// no gateways gives no candidates.
fn resolve_anchor_urls(url: &str, ipfs_gateways: &[String]) -> Vec<String> {
    let Some(rest) = url.strip_prefix("ipfs://") else {
        return vec![url.to_string()];
    };

    let rest = rest.strip_prefix("ipfs/").unwrap_or(rest);

    ipfs_gateways
        .iter()
        .map(|gateway| format!("{}/ipfs/{rest}", gateway.trim_end_matches('/')))
        .collect()
}

/// This function fetches the raw body of a governance anchor and makes sure
/// that its hash matches `expected_hash`.
///
/// The function resolves an `ipfs://` URL through `ipfs_gateways`. It sends a
/// request to each gateway in order. It stops at the first gateway that serves
/// content with a hash equal to `expected_hash`. The function fetches every
/// other URL directly.
///
/// If every candidate fails, the function returns a typed error from the last
/// candidate. Every error keeps the original on-chain URL.
pub async fn anchor_offchain_body(
    url: &str,
    expected_hash: &[u8],
    ipfs_gateways: &[String],
) -> Result<Vec<u8>, DrepsInnerMetadataError> {
    let candidates = resolve_anchor_urls(url, ipfs_gateways);

    // The candidate list is empty only for an `ipfs://` URL with no gateway. A
    // direct HTTP(S) URL always gives one candidate.
    if candidates.is_empty() {
        return Err(offchain_no_gateway_error(url));
    }

    let client = offchain_http_client().map_err(|error| {
        tracing::error!(%error, "cannot create the HTTP client for governance metadata");
        offchain_unknown_error(url)
    })?;

    let mut last_error = None;

    for candidate in candidates {
        match fetch_anchor_candidate(client, &candidate, url, expected_hash).await {
            Ok(body) => return Ok(body),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| offchain_connection_error(url)))
}

/// This function decodes a verified anchor body into the JSON value and the
/// raw bytes in the PostgreSQL `bytea` format.
///
/// A body that is not JSON gives a decode error that names `url`.
pub fn anchor_metadata_from_body(
    url: &str,
    body: &[u8],
) -> Result<AnchorMetadata, DrepsInnerMetadataError> {
    serde_json::from_slice(body)
        .map(|json| AnchorMetadata {
            json,
            bytes: format!("\\x{}", hex::encode(body)),
        })
        .map_err(|_| offchain_decode_error(url))
}

/// This function fetches one resolved candidate URL for
/// [`anchor_offchain_body`]. The function makes sure that the body hash
/// matches `expected_hash`.
///
/// `request_url` is the URL that the function fetches. For an `ipfs://` anchor,
/// `request_url` is a gateway URL. `original_url` is the on-chain URL. Every
/// error keeps `original_url`, so the caller reports what the chain recorded.
async fn fetch_anchor_candidate(
    client: &reqwest::Client,
    request_url: &str,
    original_url: &str,
    expected_hash: &[u8],
) -> Result<Vec<u8>, DrepsInnerMetadataError> {
    let body = fetch_offchain_body(
        client,
        request_url,
        ANCHOR_FETCH_TIMEOUT,
        MAX_ANCHOR_METADATA_BYTES,
    )
    .await
    .map_err(|error| error.into_metadata_error(original_url))?;

    let actual_hash = Hasher::<256>::hash(body.as_ref());

    if actual_hash.as_ref() != expected_hash {
        return Err(offchain_hash_mismatch_error(
            original_url,
            expected_hash,
            actual_hash.as_ref(),
        ));
    }

    Ok(body)
}

#[cfg(test)]
mod offchain_tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener},
        thread::{self, JoinHandle},
    };

    struct FixedDnsResolver(SocketAddr);

    impl reqwest::dns::Resolve for FixedDnsResolver {
        fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            let address = self.0;
            let addresses = Box::new(std::iter::once(address)) as reqwest::dns::Addrs;

            Box::pin(async move { Ok(addresses) })
        }
    }

    fn serve_body(body: Vec<u8>, content_length: Option<usize>) -> (String, JoinHandle<()>) {
        let headers = content_length
            .map(|size| format!("Content-Length: {size}\r\n"))
            .unwrap_or_default();

        serve_response(body, "200 OK", &headers)
    }

    fn serve_response(body: Vec<u8>, status: &str, headers: &str) -> (String, JoinHandle<()>) {
        let headers = format!("HTTP/1.1 {status}\r\n{headers}Connection: close\r\n\r\n");
        let listener = TcpListener::bind("127.0.0.1:0").expect("Cannot bind the test server.");
        let address = listener
            .local_addr()
            .expect("Cannot read the test server address.");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("Cannot accept the test request.");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request);

            stream
                .write_all(headers.as_bytes())
                .expect("Cannot write the test response headers.");
            let _ = stream.write_all(&body);
        });

        (format!("http://{address}/metadata"), server)
    }

    fn local_candidate_client(url: &str) -> (reqwest::Client, String) {
        let port = reqwest::Url::parse(url)
            .expect("Cannot parse the test URL.")
            .port()
            .expect("The test URL has no port.");
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let client = offchain_http_client_builder()
            .dns_resolver(Arc::new(FixedDnsResolver(address)))
            .build()
            .expect("Cannot create the test HTTP client.");

        (client, format!("http://anchor.test:{port}/metadata"))
    }

    #[tokio::test]
    async fn anchor_fetch_rejects_loopback_before_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Cannot bind the test server.");
        listener
            .set_nonblocking(true)
            .expect("Cannot configure the test server.");
        let url = format!(
            "http://{}/metadata",
            listener
                .local_addr()
                .expect("Cannot read the test server address.")
        );

        let error = anchor_offchain_body(&url, &[0; 32], &[])
            .await
            .expect_err("The loopback URL was accepted.");
        assert_eq!(error.code, Code::ConnectionError);
        assert_eq!(
            listener
                .accept()
                .expect_err("The client made a connection.")
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn offchain_fetch_rejects_oversized_content_length() {
        for max_bytes in [MAX_POOL_METADATA_BYTES, MAX_ANCHOR_METADATA_BYTES] {
            let (url, server) = serve_body(Vec::new(), Some(max_bytes + 1));
            let (client, request_url) = local_candidate_client(&url);

            let error =
                fetch_offchain_body(&client, &request_url, Duration::from_secs(5), max_bytes)
                    .await
                    .expect_err("The client accepted a response with more bytes than the limit.");

            server.join().expect("The test server did not stop.");
            assert!(matches!(error, OffchainFetchError::SizeExceeded));
        }
    }

    #[tokio::test]
    async fn offchain_fetch_rejects_oversized_stream() {
        for max_bytes in [MAX_POOL_METADATA_BYTES, MAX_ANCHOR_METADATA_BYTES] {
            let body = vec![b'x'; max_bytes + 1];
            let (url, server) = serve_body(body, None);
            let (client, request_url) = local_candidate_client(&url);

            let error =
                fetch_offchain_body(&client, &request_url, Duration::from_secs(5), max_bytes)
                    .await
                    .expect_err("The client accepted a response with more bytes than the limit.");

            server.join().expect("The test server did not stop.");
            assert!(matches!(error, OffchainFetchError::SizeExceeded));
        }
    }

    #[tokio::test]
    async fn offchain_fetch_accepts_size_limit() {
        for max_bytes in [MAX_POOL_METADATA_BYTES, MAX_ANCHOR_METADATA_BYTES] {
            for content_length in [Some(max_bytes), None] {
                let body = vec![b'x'; max_bytes];
                let (url, server) = serve_body(body, content_length);
                let (client, request_url) = local_candidate_client(&url);

                let body =
                    fetch_offchain_body(&client, &request_url, Duration::from_secs(5), max_bytes)
                        .await
                        .expect("The client did not accept the response at the size limit.");

                server.join().expect("The test server did not stop.");
                assert_eq!(body.len(), max_bytes);
            }
        }
    }

    #[tokio::test]
    async fn offchain_client_filters_private_dns_results() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!(
            "http://localhost:{}/metadata",
            listener.local_addr().unwrap().port()
        );

        let result = offchain_http_client()
            .unwrap()
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        assert!(result.is_err());
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn offchain_fetch_blocks_private_redirects_before_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let headers = format!(
            "Location: http://{}/metadata\r\n",
            listener.local_addr().unwrap()
        );
        let (url, server) = serve_response(Vec::new(), "302 Found", &headers);
        let (client, request_url) = local_candidate_client(&url);

        let error = fetch_offchain_body(
            &client,
            &request_url,
            Duration::from_secs(5),
            MAX_POOL_METADATA_BYTES,
        )
        .await
        .expect_err("The client accepted a redirect to a private address.");

        server.join().unwrap();
        assert!(matches!(error, OffchainFetchError::Connection));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn offchain_fetch_enforces_chunked_body_limit() {
        for max_bytes in [MAX_POOL_METADATA_BYTES, MAX_ANCHOR_METADATA_BYTES] {
            for extra in [0, 1] {
                let first = "x".repeat(max_bytes / 2);
                let second = "x".repeat(max_bytes - first.len() + extra);
                let body = format!(
                    "{:x}\r\n{first}\r\n{:x}\r\n{second}\r\n0\r\n\r\n",
                    first.len(),
                    second.len(),
                )
                .into_bytes();
                let (url, server) =
                    serve_response(body, "200 OK", "Transfer-Encoding: chunked\r\n");
                let (client, request_url) = local_candidate_client(&url);

                let result =
                    fetch_offchain_body(&client, &request_url, Duration::from_secs(5), max_bytes)
                        .await;

                server.join().unwrap();
                if extra == 0 {
                    assert_eq!(result.unwrap().len(), max_bytes);
                } else {
                    assert!(matches!(result, Err(OffchainFetchError::SizeExceeded)));
                }
            }
        }
    }

    #[tokio::test]
    async fn offchain_fetch_allows_three_redirects_and_rejects_four() {
        use axum::response::IntoResponse;
        use std::sync::atomic::{AtomicUsize, Ordering};

        for redirects in [3, 4] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/metadata", listener.local_addr().unwrap());
            let requests = Arc::new(AtomicUsize::new(0));
            let calls = requests.clone();
            let app = axum::Router::new().fallback(move || {
                let index = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if index < redirects {
                        (
                            StatusCode::FOUND,
                            [(
                                axum::http::header::LOCATION,
                                format!("/metadata/{}", index + 1),
                            )],
                        )
                            .into_response()
                    } else {
                        "metadata".into_response()
                    }
                }
            });
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let (client, request_url) = local_candidate_client(&url);

            let result = fetch_offchain_body(
                &client,
                &request_url,
                Duration::from_secs(5),
                MAX_POOL_METADATA_BYTES,
            )
            .await;

            server.abort();
            if redirects == 3 {
                assert_eq!(result.unwrap(), b"metadata");
            } else {
                assert!(matches!(result, Err(OffchainFetchError::Connection)));
            }
            assert_eq!(requests.load(Ordering::SeqCst), 4);
        }
    }

    #[tokio::test]
    async fn offchain_fetch_preserves_http_and_body_errors() {
        let (url, server) = serve_response(Vec::new(), "404 Not Found", "");
        let (client, request_url) = local_candidate_client(&url);
        let error = fetch_offchain_body(
            &client,
            &request_url,
            Duration::from_secs(5),
            MAX_POOL_METADATA_BYTES,
        )
        .await
        .unwrap_err();
        server.join().unwrap();
        assert!(matches!(
            error,
            OffchainFetchError::HttpResponse(StatusCode::NOT_FOUND)
        ));

        let (url, server) = serve_body(Vec::new(), Some(1));
        let (client, request_url) = local_candidate_client(&url);
        let error = fetch_offchain_body(
            &client,
            &request_url,
            Duration::from_secs(5),
            MAX_POOL_METADATA_BYTES,
        )
        .await
        .unwrap_err();
        server.join().unwrap();
        assert!(matches!(error, OffchainFetchError::Body));
    }

    #[tokio::test]
    async fn anchor_candidate_keeps_governance_size_limit() {
        assert_eq!(MAX_POOL_METADATA_BYTES, 512);
        assert_eq!(MAX_ANCHOR_METADATA_BYTES, 3_000_000);
        let original_url = "ipfs://bafy-size-limit";

        for size in [
            MAX_POOL_METADATA_BYTES + 1,
            MAX_ANCHOR_METADATA_BYTES,
            MAX_ANCHOR_METADATA_BYTES + 1,
        ] {
            let mut expected = b"{}".to_vec();
            expected.resize(size, b' ');
            let hash = Hasher::<256>::hash(&expected);
            let response_body = if size <= MAX_ANCHOR_METADATA_BYTES {
                expected.clone()
            } else {
                Vec::new()
            };
            let (url, server) = serve_body(response_body, Some(size));
            let (client, request_url) = local_candidate_client(&url);

            let result =
                fetch_anchor_candidate(&client, &request_url, original_url, hash.as_ref()).await;

            server.join().unwrap();
            if size <= MAX_ANCHOR_METADATA_BYTES {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert_eq!(
                    result.err().unwrap(),
                    DrepsInnerMetadataError::new(
                        Code::SizeExceeded,
                        format!("The response from {original_url} is larger than 3000000 bytes."),
                    ),
                );
            }
        }
    }

    #[tokio::test]
    async fn anchor_candidate_rejects_hash_mismatch() {
        let body = br#"{"name":"Dolos"}"#.to_vec();
        let (url, server) = serve_body(body, None);
        let (client, request_url) = local_candidate_client(&url);
        let original_url = "ipfs://bafy-mismatched";

        let error = fetch_anchor_candidate(&client, &request_url, original_url, &[0; 32])
            .await
            .expect_err("The hash mismatch was accepted.");

        server.join().expect("The test server did not stop.");
        assert_eq!(error.code, Code::HashMismatch);
        assert!(error.message.contains(original_url));
    }

    #[tokio::test]
    async fn anchor_candidate_accepts_valid_json_and_hash() {
        let body = br#"{"name":"Dolos"}"#.to_vec();
        let expected_hex = hex::encode(body.as_slice());
        let expected_hash = Hasher::<256>::hash(body.as_ref());
        let (url, server) = serve_body(body, None);
        let (client, request_url) = local_candidate_client(&url);

        let body = fetch_anchor_candidate(
            &client,
            &request_url,
            "ipfs://bafy-valid-json",
            expected_hash.as_ref(),
        )
        .await
        .expect("The valid body was rejected.");

        server.join().expect("The test server did not stop.");

        let metadata = anchor_metadata_from_body("ipfs://bafy-valid-json", &body)
            .expect("The valid metadata was rejected.");
        assert_eq!(metadata.json["name"], "Dolos");
        assert_eq!(metadata.bytes, format!("\\x{expected_hex}"));
    }

    #[tokio::test]
    async fn anchor_candidate_rejects_invalid_json() {
        let body = b"not JSON".to_vec();
        let expected_hash = Hasher::<256>::hash(body.as_ref());
        let (url, server) = serve_body(body, None);
        let (client, request_url) = local_candidate_client(&url);
        let original_url = "ipfs://bafy-invalid-json";

        // the hash matches, so the body comes back; only decoding refuses it
        let body =
            fetch_anchor_candidate(&client, &request_url, original_url, expected_hash.as_ref())
                .await
                .expect("The body with a matching hash was rejected.");

        server.join().expect("The test server did not stop.");

        let error = anchor_metadata_from_body(original_url, &body)
            .err()
            .expect("The invalid JSON was accepted.");
        assert_eq!(error.code, Code::DecodeError);
        assert!(error.message.contains(original_url));
    }

    #[test]
    fn public_url_predicate_blocks_non_public_targets() {
        let public = [
            "https://example.com/metadata",
            "http://1.1.1.1/metadata",
            "https://[2606:4700:4700::1111]/metadata",
        ];
        for url in public {
            let url = reqwest::Url::parse(url).expect("Cannot parse the URL.");
            assert!(is_public_http_url(&url), "{url} is not public");
        }

        let blocked = [
            "ftp://example.com/metadata",
            "https://localhost/metadata",
            "https://service.localhost/metadata",
            "http://127.0.0.1/metadata",
            "http://10.0.0.1/metadata",
            "http://169.254.169.254/metadata",
            "http://[::1]/metadata",
            "http://[::ffff:127.0.0.1]/metadata",
            "http://[fd00::1]/metadata",
        ];
        for url in blocked {
            let url = reqwest::Url::parse(url).expect("Cannot parse the URL.");
            assert!(!is_public_http_url(&url), "{url} is public");
        }
    }

    #[test]
    fn resolve_anchor_urls_expands_ipfs_across_gateways_in_order() {
        assert_eq!(
            resolve_anchor_urls(
                "ipfs://bafkreigmd7xasljkmisbal5pu2xcqolr2fkre4jnlllgqrof4wctadxa7m",
                &[
                    "https://ipfs.io".to_string(),
                    "https://gateway.pinata.cloud".to_string(),
                ],
            ),
            vec![
                "https://ipfs.io/ipfs/bafkreigmd7xasljkmisbal5pu2xcqolr2fkre4jnlllgqrof4wctadxa7m"
                    .to_string(),
                "https://gateway.pinata.cloud/ipfs/bafkreigmd7xasljkmisbal5pu2xcqolr2fkre4jnlllgqrof4wctadxa7m"
                    .to_string(),
            ],
        );
    }

    #[test]
    fn resolve_anchor_urls_keeps_cid_path_and_trims_gateway_slash() {
        assert_eq!(
            resolve_anchor_urls(
                "ipfs://bafyfoo/dir/doc.json",
                &["https://gateway.example/".to_string()]
            ),
            vec!["https://gateway.example/ipfs/bafyfoo/dir/doc.json".to_string()],
        );
    }

    #[test]
    fn resolve_anchor_urls_tolerates_redundant_ipfs_prefix() {
        assert_eq!(
            resolve_anchor_urls("ipfs://ipfs/bafyfoo", &["https://ipfs.io".to_string()]),
            vec!["https://ipfs.io/ipfs/bafyfoo".to_string()],
        );
    }

    #[test]
    fn resolve_anchor_urls_preserves_cidv0_case() {
        // A base58 CIDv0 is case-sensitive. The CID stays in the path, so it
        // does not pass through host normalization. That normalization
        // lowercases the CID and corrupts it.
        assert_eq!(
            resolve_anchor_urls(
                "ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
                &["https://ipfs.io".to_string()],
            ),
            vec!["https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG".to_string()],
        );
    }

    #[test]
    fn resolve_anchor_urls_passes_http_through_as_single_candidate() {
        for url in [
            "https://example.com/metadata.json",
            "http://example.com/a?b=c",
        ] {
            assert_eq!(
                resolve_anchor_urls(url, &["https://ipfs.io".to_string()]),
                vec![url.to_string()],
            );
        }
    }

    #[test]
    fn resolve_anchor_urls_without_gateways_yields_no_candidates() {
        assert!(resolve_anchor_urls("ipfs://bafyfoo", &[]).is_empty());
    }
}

pub trait IntoModel<T>
where
    T: serde::Serialize,
    Self: Sized,
{
    type SortKey: Ord + Clone + Default + Sized;

    fn sort_key(&self) -> Option<Self::SortKey> {
        None
    }

    fn into_model(self) -> Result<T, StatusCode>;

    #[allow(unused)]
    fn into_model_with_sort_key(self) -> Result<(Self::SortKey, T), StatusCode> {
        let sort_key = self.sort_key().unwrap_or_default();
        let model = self.into_model()?;
        Ok((sort_key, model))
    }

    fn into_response(self) -> Result<Json<T>, StatusCode> {
        let tx = self.into_model()?;

        Ok(Json(tx))
    }
}

/// Gives a stable key order to DTOs with `HashMap` fields.
///
/// A `HashMap` serializes its keys in a random order for each instance.
/// `serde_json::Value` sorts object keys while serde_json's `preserve_order`
/// feature stays off.
pub fn sorted_json<T: Serialize>(model: T) -> Result<Json<serde_json::Value>, StatusCode> {
    serde_json::to_value(model)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[derive(Default)]
pub struct AssetTotals {
    lovelace: u128,
    by_asset: HashMap<String, u128>,
}

impl AssetTotals {
    pub fn add_output(&mut self, txout: &MultiEraOutput) {
        let value = txout.value();

        // Add lovelace amount
        self.lovelace += value.coin() as u128;

        // Add other assets
        for ma in value.assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let amount = asset.output_coin().unwrap_or_default() as u128;
                *self.by_asset.entry(unit).or_insert(0) += amount;
            }
        }
    }

    pub fn into_amounts(self) -> Vec<TxContentOutputAmountInner> {
        let lovelace = TxContentOutputAmountInner {
            unit: "lovelace".to_string(),
            quantity: self.lovelace.to_string(),
        };

        let mut assets: Vec<_> = self
            .by_asset
            .into_iter()
            .map(|(unit, quantity)| TxContentOutputAmountInner {
                unit,
                quantity: quantity.to_string(),
            })
            .collect();

        assets.sort_by_key(|a| a.unit.clone());

        std::iter::once(lovelace).chain(assets).collect()
    }
}

pub fn aggregate_assets<'a>(
    txouts: impl Iterator<Item = &'a MultiEraOutput<'a>>,
) -> Vec<TxContentOutputAmountInner> {
    let mut totals = AssetTotals::default();

    for txout in txouts {
        totals.add_output(txout);
    }

    totals.into_amounts()
}

pub fn list_assets<'a>(
    txouts: impl Iterator<Item = &'a MultiEraOutput<'a>>,
) -> Vec<TxContentOutputAmountInner> {
    let mut lovelace = 0;
    let mut assets: Vec<TxContentOutputAmountInner> = vec![];

    for txout in txouts {
        let value = txout.value();

        // Add lovelace amount
        lovelace += value.coin();

        // Add other assets
        for ma in value.assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let amount = asset.output_coin().unwrap_or_default();
                assets.push(TxContentOutputAmountInner {
                    unit,
                    quantity: amount.to_string(),
                });
            }
        }
    }

    let lovelace = TxContentOutputAmountInner {
        unit: "lovelace".to_string(),
        quantity: lovelace.to_string(),
    };

    assets.sort_by_key(|a| a.unit.clone());

    std::iter::once(lovelace).chain(assets).collect()
}

pub struct AssetAggregate {
    pub quantity: u128,
    pub oldest: Option<(u64, usize, TxoIdx)>,
    pub newest: Option<(u64, usize, TxoIdx)>,
}

/// Aggregates asset quantities per unit over a set of utxos, tracking the
/// chain position of the oldest and newest utxo holding each asset —
/// Blockfrost orders account assets by these. The BTreeMap keeps units
/// sorted, providing the tie-break order for assets sharing a position.
pub fn aggregate_account_assets(
    utxos: &HashMap<TxoRef, Arc<UtxoValue>>,
    block_deps: &HashMap<TxHash, BlockRefMeta>,
) -> Result<BTreeMap<String, AssetAggregate>, StatusCode> {
    let mut by_unit: BTreeMap<String, AssetAggregate> = BTreeMap::new();

    for (txo_ref, cbor) in utxos.iter() {
        let output = MultiEraOutput::try_from(cbor.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let position = block_deps
            .get(&txo_ref.0)
            .map(|meta| (meta.slot, meta.tx_index, txo_ref.1));

        for ma in output.value().assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let quantity = asset.output_coin().unwrap_or_default() as u128;

                let agg = by_unit.entry(unit).or_insert(AssetAggregate {
                    quantity: 0,
                    oldest: None,
                    newest: None,
                });

                agg.quantity += quantity;

                if let Some(position) = position {
                    agg.oldest = Some(agg.oldest.map_or(position, |x| x.min(position)));
                    agg.newest = Some(agg.newest.map_or(position, |x| x.max(position)));
                }
            }
        }
    }

    Ok(by_unit)
}

pub enum AddressKind {
    Payment {
        script: bool,
    },
    Shelley {
        stake_address: Option<String>,
        script: bool,
    },
    Byron,
}

impl AddressKind {
    /// Every address model shares these three response fields:
    /// - the stake address that controls the key
    /// - whether the address is from the Byron era
    /// - whether the payment part is a script
    ///
    /// A bare payment credential has no stake part. As a result, it reports no
    /// stake address.
    fn parts(self) -> (Option<String>, bool, bool) {
        match self {
            AddressKind::Payment { script } => (None, false, script),
            AddressKind::Shelley {
                stake_address,
                script,
            } => (stake_address, false, script),
            AddressKind::Byron => (None, true, false),
        }
    }
}

pub struct AddressModelBuilder {
    pub address: String,
    pub amount: Vec<TxContentOutputAmountInner>,
    pub kind: AddressKind,
}

impl IntoModel<AddressContent> for AddressModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<AddressContent, StatusCode> {
        let (stake_address, byron, script) = self.kind.parts();

        Ok(AddressContent {
            address: self.address,
            amount: self.amount,
            stake_address,
            r#type: if byron {
                AddressType::Byron
            } else {
                AddressType::Shelley
            },
            script,
        })
    }
}

pub struct AddressExtendedModelBuilder {
    pub address: String,
    pub amount: Vec<AddressContentExtendedAmountInner>,
    pub kind: AddressKind,
}

impl IntoModel<AddressContentExtended> for AddressExtendedModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<AddressContentExtended, StatusCode> {
        let (stake_address, byron, script) = self.kind.parts();

        Ok(AddressContentExtended {
            address: self.address,
            amount: self.amount,
            stake_address,
            r#type: if byron {
                AddressExtendedType::Byron
            } else {
                AddressExtendedType::Shelley
            },
            script,
        })
    }
}

impl IntoModel<Vec<TxContentOutputAmountInner>> for MultiEraValue<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentOutputAmountInner>, StatusCode> {
        let mut out = vec![];

        out.push(TxContentOutputAmountInner {
            unit: "lovelace".to_string(),
            quantity: self.coin().to_string(),
        });

        for ma in self.assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let amount = asset.output_coin().unwrap_or_default();

                out.push(TxContentOutputAmountInner {
                    unit,
                    quantity: amount.to_string(),
                });
            }
        }

        Ok(out)
    }
}

impl IntoModel<String> for Result<Address, pallas::ledger::addresses::Error> {
    type SortKey = ();

    fn into_model(self) -> Result<String, StatusCode> {
        self.map(|a| a.to_string())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl<'a> IntoModel<String> for ScriptRef<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<String, StatusCode> {
        Ok(pallas_extras::script_ref_hash(&self).to_string())
    }
}

pub struct UtxoOutputModelBuilder<'a> {
    txo_ref: TxoRef,
    output: MultiEraOutput<'a>,
    is_collateral: bool,
    block_data: Option<BlockRefMeta>,
    consumed_by_tx: Option<TxHash>,
}

impl<'a> UtxoOutputModelBuilder<'a> {
    pub fn txo_ref(&self) -> TxoRef {
        self.txo_ref.clone()
    }

    pub fn from_output(tx_hash: TxHash, tx_index: TxoIdx, output: MultiEraOutput<'a>) -> Self {
        Self {
            txo_ref: TxoRef(tx_hash, tx_index),
            output,
            is_collateral: false,
            block_data: None,
            consumed_by_tx: None,
        }
    }

    pub fn from_collateral(
        tx_hash: TxHash,
        output_count: usize,
        collateral_idx: TxoIdx,
        output: MultiEraOutput<'a>,
    ) -> Self {
        Self {
            txo_ref: TxoRef(tx_hash, (output_count + collateral_idx as usize) as u32),
            output,
            is_collateral: true,
            block_data: None,
            consumed_by_tx: None,
        }
    }

    pub fn with_block_data(self, block_data: BlockRefMeta) -> Self {
        Self {
            block_data: Some(block_data),
            ..self
        }
    }

    pub fn with_consumed_by(self, tx: TxHash) -> Self {
        Self {
            consumed_by_tx: Some(tx),
            ..self
        }
    }
}

impl<'a> IntoModel<TxContentUtxoOutputsInner> for UtxoOutputModelBuilder<'a> {
    type SortKey = u64;

    fn into_model(self) -> Result<TxContentUtxoOutputsInner, StatusCode> {
        let out = TxContentUtxoOutputsInner {
            address: self.output.address().into_model()?,
            amount: self
                .output
                .value()
                .into_model()?
                .into_iter()
                .filter(|x| x.unit == "lovelace" || !self.is_collateral)
                .collect(),
            output_index: try_into_or_500!(self.txo_ref.1),
            consumed_by_tx: Some(self.consumed_by_tx.map(|x| x.to_string())),
            data_hash: self.output.datum().map(|x| match x {
                DatumOption::Hash(x) => x.to_string(),
                DatumOption::Data(x) => x.original_hash().to_string(),
            }),
            inline_datum: self
                .output
                .datum()
                .and_then(|x| match x {
                    DatumOption::Hash(_) => None,
                    DatumOption::Data(x) => Some(minicbor::to_vec(&x.0).unwrap()),
                })
                .map(hex::encode),
            collateral: self.is_collateral,
            reference_script_hash: self
                .output
                .script_ref()
                .map(|h| h.into_model())
                .transpose()?,
        };

        Ok(out)
    }
}

impl<'a> IntoModel<AddressUtxoContentInner> for UtxoOutputModelBuilder<'a> {
    type SortKey = (u64, usize, u32);

    fn sort_key(&self) -> Option<Self::SortKey> {
        self.block_data
            .as_ref()
            .map(|data| (data.slot, data.tx_index, self.txo_ref.1))
    }

    fn into_model(self) -> Result<AddressUtxoContentInner, StatusCode> {
        let out = AddressUtxoContentInner {
            address: self.output.address().into_model()?,
            // source the tx_hash from the UTxO's own TxoRef (always present),
            // not the archive block_data lookup which is None for blocks older
            // than `max_history` and would yield an empty hash.
            tx_hash: self.txo_ref.0.to_string(),
            block: self
                .block_data
                .as_ref()
                .map(|b| b.hash.to_string())
                .unwrap_or_default(),
            output_index: try_into_or_500!(self.txo_ref.1),
            amount: self.output.value().into_model()?,
            data_hash: self.output.datum().map(|x| match x {
                DatumOption::Hash(x) => x.to_string(),
                DatumOption::Data(x) => x.original_hash().to_string(),
            }),
            inline_datum: self
                .output
                .datum()
                .and_then(|x| match x {
                    DatumOption::Hash(_) => None,
                    DatumOption::Data(x) => Some(minicbor::to_vec(&x.0).unwrap()),
                })
                .map(hex::encode),
            reference_script_hash: self
                .output
                .script_ref()
                .map(|h| h.into_model())
                .transpose()?,

            // DEPRECATED
            tx_index: try_into_or_500!(self.txo_ref.1),
        };

        Ok(out)
    }
}

impl<'a> IntoModel<ScriptUtxosInner> for UtxoOutputModelBuilder<'a> {
    type SortKey = (u64, usize, u32);

    fn sort_key(&self) -> Option<Self::SortKey> {
        self.block_data
            .as_ref()
            .map(|data| (data.slot, data.tx_index, self.txo_ref.1))
    }

    fn into_model(self) -> Result<ScriptUtxosInner, StatusCode> {
        let out = ScriptUtxosInner {
            address: self.output.address().into_model()?,
            // source the tx_hash from the UTxO's own TxoRef (always present),
            // not the archive block_data lookup which is None for blocks older
            // than `max_history` and would yield an empty hash.
            tx_hash: self.txo_ref.0.to_string(),
            block: self
                .block_data
                .as_ref()
                .map(|b| b.hash.to_string())
                .unwrap_or_default(),
            output_index: try_into_or_500!(self.txo_ref.1),
            amount: self.output.value().into_model()?,
            data_hash: self.output.datum().map(|x| match x {
                DatumOption::Hash(x) => x.to_string(),
                DatumOption::Data(x) => x.original_hash().to_string(),
            }),
            inline_datum: self
                .output
                .datum()
                .and_then(|x| match x {
                    DatumOption::Hash(_) => None,
                    DatumOption::Data(x) => Some(minicbor::to_vec(&x.0).unwrap()),
                })
                .map(hex::encode),
            // the model requires the hash; callers only build this model for
            // outputs that carry a reference script.
            reference_script_hash: self
                .output
                .script_ref()
                .map(|h| h.into_model())
                .transpose()?
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
        };

        Ok(out)
    }
}

impl IntoModel<String> for alonzo::Metadatum {
    type SortKey = ();

    fn into_model(self) -> Result<String, StatusCode> {
        let out = match self {
            alonzo::Metadatum::Int(x) => x.to_string(),
            alonzo::Metadatum::Bytes(x) => format!("0x{}", hex::encode(x.as_slice())),
            alonzo::Metadatum::Text(x) => x.to_string(),
            alonzo::Metadatum::Array(_) => "array".to_string(),
            alonzo::Metadatum::Map(_) => "map".to_string(),
        };

        Ok(out)
    }
}

impl IntoModel<serde_json::Value> for alonzo::Metadatum {
    type SortKey = ();

    fn into_model(self) -> Result<serde_json::Value, StatusCode> {
        let out = match self {
            alonzo::Metadatum::Int(x) => serde_json::Number::from_i128(x.into())
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::String(x.to_string())),
            alonzo::Metadatum::Text(x) => serde_json::Value::String(x.to_string()),
            alonzo::Metadatum::Bytes(x) => {
                let hex_str = format!("0x{}", hex::encode(x.as_slice()));

                serde_json::Value::String(hex_str)
            }
            alonzo::Metadatum::Array(x) => {
                let items: Vec<_> = x.into_iter().map(|x| x.into_model()).try_collect()?;

                serde_json::Value::Array(items)
            }
            alonzo::Metadatum::Map(x) => {
                let items: serde_json::Map<String, serde_json::Value> = x
                    .iter()
                    .map(|(k, v)| Ok((k.clone().into_model()?, v.clone().into_model()?)))
                    .try_collect()
                    .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

                serde_json::Value::Object(items)
            }
        };

        Ok(out)
    }
}

/// The redeemer-to-script matching lives in `dolos_cardano::pallas_extras`:
/// the archive indexer uses it at index time, and this crate uses it at
/// query time.
pub use dolos_cardano::pallas_extras::redeemer_script_hash;

/// The Blockfrost purpose of a script redeemer. Same mapping as
/// [`txs::tx_redeemer_purpose`], distinct generated enum.
pub fn script_redeemer_purpose(tag: RedeemerTag) -> ScriptRedeemerPurpose {
    match tag {
        RedeemerTag::Spend => ScriptRedeemerPurpose::Spend,
        RedeemerTag::Mint => ScriptRedeemerPurpose::Mint,
        RedeemerTag::Cert => ScriptRedeemerPurpose::Cert,
        RedeemerTag::Reward => ScriptRedeemerPurpose::Reward,
        RedeemerTag::Vote => ScriptRedeemerPurpose::Vote,
        RedeemerTag::Propose => ScriptRedeemerPurpose::Propose,
    }
}

/// The fee to run a redeemer. The inputs are the redeemer's execution units
/// and the era's execution prices.
pub fn redeemer_fee(units: &ExUnits, prices: &ExUnitPrices) -> Result<u64, StatusCode> {
    let ExUnitPrices {
        mem_price,
        step_price,
    } = prices;

    // BigRational::new panics on a zero denominator
    if mem_price.denominator == 0 || step_price.denominator == 0 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let unit_mem = BigRational::from_integer(BigInt::from(units.mem));
    let unit_steps = BigRational::from_integer(BigInt::from(units.steps));

    let mem_price = BigRational::new(
        BigInt::from(mem_price.numerator),
        BigInt::from(mem_price.denominator),
    );

    let step_price = BigRational::new(
        BigInt::from(step_price.numerator),
        BigInt::from(step_price.denominator),
    );

    let mem_fee = unit_mem * mem_price;
    let step_fee = unit_steps * step_price;

    let fee = mem_fee + step_fee;

    let fee: u64 = fee
        .ceil()
        .to_integer()
        .try_into()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(fee)
}

impl IntoModel<TxContentPoolCertsInnerRelaysInner> for alonzo::Relay {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentPoolCertsInnerRelaysInner, StatusCode> {
        let out = match self {
            alonzo::Relay::SingleHostAddr(port, ipv4, ipv6) => TxContentPoolCertsInnerRelaysInner {
                ipv4: ipv4.map(|ipv4| {
                    if let Ok(slice) = <[u8; 4]>::try_from(ipv4.as_slice()) {
                        std::net::Ipv4Addr::from(slice).to_string()
                    } else {
                        Default::default()
                    }
                }),
                ipv6: ipv6.map(|ipv6| {
                    if let Ok(mut bytes) = <[u8; 16]>::try_from(ipv6.as_slice()) {
                        // cardano-ledger serializes relay IPv6 addresses as
                        // four 32-bit words in little-endian order; swap each
                        // word back to network order (same as dbsync).
                        for word in bytes.chunks_exact_mut(4) {
                            word.reverse();
                        }
                        std::net::Ipv6Addr::from(bytes).to_string()
                    } else {
                        Default::default()
                    }
                }),
                dns: None,
                dns_srv: None,
                port: port.unwrap_or_default() as i32,
            },
            alonzo::Relay::SingleHostName(port, dns) => TxContentPoolCertsInnerRelaysInner {
                ipv4: None,
                ipv6: None,
                dns: Some(dns.to_string()),
                dns_srv: None,
                port: port.unwrap_or_default() as i32,
            },
            alonzo::Relay::MultiHostName(dns) => TxContentPoolCertsInnerRelaysInner {
                ipv4: None,
                ipv6: None,
                dns: None,
                dns_srv: Some(dns.to_string()),
                port: Default::default(),
            },
        };

        Ok(out)
    }
}

pub struct PlutusDataWrapper(pub PlutusData);
impl PlutusDataWrapper {
    fn as_value(&self) -> Result<serde_json::Value, StatusCode> {
        match &self.0 {
            PlutusData::Constr(x) => {
                let values = x
                    .fields
                    .iter()
                    .map(|d| PlutusDataWrapper(d.clone()).as_value())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                Ok(serde_json::Value::Object(serde_json::Map::from_iter([
                    (
                        "constructor".to_string(),
                        serde_json::Value::Number(x.constr_index().into()),
                    ),
                    ("fields".to_string(), serde_json::Value::Array(values)),
                ])))
            }

            PlutusData::Map(x) => {
                let values = x
                    .iter()
                    .map(|(k, v)| -> Result<serde_json::Value, StatusCode> {
                        let key = PlutusDataWrapper(k.clone()).as_value()?;
                        let value = PlutusDataWrapper(v.clone()).as_value()?;
                        Ok(serde_json::Value::Object(serde_json::Map::from_iter([
                            ("k".to_string(), key),
                            ("v".to_string(), value),
                        ])))
                    })
                    .collect::<Result<Vec<serde_json::Value>, StatusCode>>()?;

                Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                    "map".to_string(),
                    serde_json::Value::Array(values),
                )])))
            }

            PlutusData::Array(x) => {
                let values = x
                    .iter()
                    .map(|d| PlutusDataWrapper(d.clone()).as_value())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                    "list".to_string(),
                    serde_json::Value::Array(values),
                )])))
            }

            PlutusData::BigInt(x) => match x {
                pallas::ledger::primitives::BigInt::Int(int) => {
                    let i = Into::<i128>::into(*int);
                    Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                        "int".to_string(),
                        serde_json::Value::Number((i as i64).into()),
                    )])))
                }
                pallas::ledger::primitives::BigInt::BigUInt(bounded_bytes) => {
                    let bigint = num_bigint::BigUint::from_bytes_be(bounded_bytes.as_slice());
                    let number = serde_json::Number::from_f64(
                        bigint.to_f64().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
                    )
                    .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                    Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                        "int".to_string(),
                        serde_json::Value::Number(number),
                    )])))
                }
                pallas::ledger::primitives::BigInt::BigNInt(bounded_bytes) => {
                    let bigint = num_bigint::BigInt::from_signed_bytes_be(bounded_bytes.as_slice());
                    let number = serde_json::Number::from_f64(
                        bigint.to_f64().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
                    )
                    .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                    Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                        "int".to_string(),
                        serde_json::Value::Number(number),
                    )])))
                }
            },

            PlutusData::BoundedBytes(x) => {
                Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                    "bytes".to_string(),
                    serde_json::Value::String(x.to_string()),
                )])))
            }
        }
    }
}
impl IntoModel<HashMap<String, serde_json::Value>> for PlutusDataWrapper {
    type SortKey = ();

    fn into_model(self) -> Result<HashMap<String, serde_json::Value>, StatusCode> {
        let value = self.as_value()?;
        serde_json::from_value(value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redeemer_fee_charges_ceiled_price() {
        let units = ExUnits {
            mem: 10,
            steps: 100,
        };

        let prices = ExUnitPrices {
            mem_price: alonzo::RationalNumber {
                numerator: 577,
                denominator: 10_000,
            },
            step_price: alonzo::RationalNumber {
                numerator: 721,
                denominator: 10_000_000,
            },
        };

        // 10 * 577/10000 + 100 * 721/10000000 = 0.584... — the fee rounds up
        assert_eq!(redeemer_fee(&units, &prices).expect("fee"), 1);
    }
}
