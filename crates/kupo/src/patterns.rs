use base58::FromBase58;
use pallas::crypto::hash::Hasher;
use pallas::ledger::addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pattern {
    Any,
    Address(AddressPattern),
    Asset(AssetPattern),
    OutputRef(OutputRefPattern),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddressPattern {
    Full(Vec<u8>),
    Credentials {
        payment: CredentialPattern,
        delegation: CredentialPattern,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetPattern {
    policy: Vec<u8>,
    name: AssetNamePattern,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssetNamePattern {
    Any,
    Exact(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputRefPattern {
    index: OutputIndexPattern,
    tx_id: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputIndexPattern {
    Any,
    Exact(u32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialPattern {
    Any,
    KeyHash(Vec<u8>),
    ScriptHash(Vec<u8>),
    AnyHash(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PatternParseError {
    Empty,
    MetadataTagNotQueryable,
    InvalidPattern(String),
}

impl fmt::Display for PatternParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternParseError::Empty => write!(f, "pattern is empty"),
            PatternParseError::MetadataTagNotQueryable => {
                write!(f, "metadata tag patterns are index-only")
            }
            PatternParseError::InvalidPattern(value) => {
                write!(f, "invalid pattern: {value}")
            }
        }
    }
}

impl std::error::Error for PatternParseError {}

impl Pattern {
    pub fn parse(input: &str) -> Result<Self, PatternParseError> {
        let input = input.trim();
        if input.is_empty() {
            return Err(PatternParseError::Empty);
        }

        if input == "*" {
            return Ok(Pattern::Any);
        }

        if input.starts_with('{') && input.ends_with('}') {
            return Err(PatternParseError::MetadataTagNotQueryable);
        }

        if let Some((left, right)) = input.split_once('/') {
            if left.is_empty() || right.is_empty() {
                return Err(PatternParseError::InvalidPattern(input.to_string()));
            }
            let payment = parse_credential(left)?;
            let delegation = parse_credential(right)?;
            return Ok(Pattern::Address(AddressPattern::Credentials {
                payment,
                delegation,
            }));
        }

        if let Some((policy, name)) = input.split_once('.') {
            if policy.is_empty() || name.is_empty() {
                return Err(PatternParseError::InvalidPattern(input.to_string()));
            }
            let policy = parse_hex_exact(policy, 56)?;
            let name = if name == "*" {
                AssetNamePattern::Any
            } else {
                let name = parse_hex_range(name, 2, 64)?;
                AssetNamePattern::Exact(name)
            };
            return Ok(Pattern::Asset(AssetPattern { policy, name }));
        }

        if let Some((index, tx_id)) = input.split_once('@') {
            if index.is_empty() || tx_id.is_empty() {
                return Err(PatternParseError::InvalidPattern(input.to_string()));
            }
            let index = if index == "*" {
                OutputIndexPattern::Any
            } else {
                let parsed = index
                    .parse::<u32>()
                    .map_err(|_| PatternParseError::InvalidPattern(input.to_string()))?;
                OutputIndexPattern::Exact(parsed)
            };

            let tx_id = parse_hex_exact(tx_id, 64)?;
            return Ok(Pattern::OutputRef(OutputRefPattern { index, tx_id }));
        }

        parse_address_pattern(input).map(Pattern::Address)
    }

    pub fn matches_address(&self, address: &Address) -> bool {
        match self {
            Pattern::Any => true,
            Pattern::Address(pattern) => pattern.matches(address),
            _ => false,
        }
    }

    pub fn matches_asset(&self, policy: &[u8], name: &[u8]) -> bool {
        match self {
            Pattern::Any => true,
            Pattern::Asset(pattern) => pattern.matches(policy, name),
            _ => false,
        }
    }

    pub fn matches_output_ref(&self, tx_id: &[u8], index: u32) -> bool {
        match self {
            Pattern::Any => true,
            Pattern::OutputRef(pattern) => pattern.matches(tx_id, index),
            _ => false,
        }
    }
}

impl AddressPattern {
    fn matches(&self, address: &Address) -> bool {
        match self {
            AddressPattern::Full(bytes) => address.to_vec() == *bytes,
            AddressPattern::Credentials {
                payment,
                delegation,
            } => match address {
                Address::Shelley(shelley) => {
                    payment.matches_payment(shelley.payment())
                        && delegation.matches_delegation(shelley.delegation())
                }
                _ => false,
            },
        }
    }
}

impl AssetPattern {
    pub fn policy(&self) -> &[u8] {
        &self.policy
    }

    pub fn name(&self) -> &AssetNamePattern {
        &self.name
    }

    fn matches(&self, policy: &[u8], name: &[u8]) -> bool {
        if self.policy != policy {
            return false;
        }

        match &self.name {
            AssetNamePattern::Any => true,
            AssetNamePattern::Exact(expected) => expected == name,
        }
    }
}

impl OutputRefPattern {
    pub fn tx_id(&self) -> &[u8] {
        &self.tx_id
    }

    pub fn index(&self) -> &OutputIndexPattern {
        &self.index
    }

    fn matches(&self, tx_id: &[u8], index: u32) -> bool {
        if self.tx_id != tx_id {
            return false;
        }

        match self.index {
            OutputIndexPattern::Any => true,
            OutputIndexPattern::Exact(expected) => expected == index,
        }
    }
}

impl CredentialPattern {
    fn matches_payment(&self, payment: &ShelleyPaymentPart) -> bool {
        match (self, payment) {
            (CredentialPattern::Any, _) => true,
            (CredentialPattern::KeyHash(expected), ShelleyPaymentPart::Key(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            (CredentialPattern::ScriptHash(expected), ShelleyPaymentPart::Script(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            (CredentialPattern::AnyHash(expected), ShelleyPaymentPart::Key(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            (CredentialPattern::AnyHash(expected), ShelleyPaymentPart::Script(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            _ => false,
        }
    }

    fn matches_delegation(&self, delegation: &ShelleyDelegationPart) -> bool {
        match (self, delegation) {
            (CredentialPattern::Any, _) => true,
            (CredentialPattern::KeyHash(expected), ShelleyDelegationPart::Key(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            (CredentialPattern::ScriptHash(expected), ShelleyDelegationPart::Script(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            (CredentialPattern::AnyHash(expected), ShelleyDelegationPart::Key(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            (CredentialPattern::AnyHash(expected), ShelleyDelegationPart::Script(hash)) => {
                expected.as_slice() == hash.as_ref()
            }
            _ => false,
        }
    }
}

fn parse_address_pattern(input: &str) -> Result<AddressPattern, PatternParseError> {
    if let Ok(address) = Address::from_bech32(input) {
        return Ok(AddressPattern::Full(address.to_vec()));
    }

    if let Ok(bytes) = hex::decode(input) {
        if let Ok(address) = Address::from_bytes(&bytes) {
            return Ok(AddressPattern::Full(address.to_vec()));
        }
    }

    if let Ok(bytes) = input.from_base58() {
        if let Ok(address) = Address::from_bytes(&bytes) {
            return Ok(AddressPattern::Full(address.to_vec()));
        }
    }

    Err(PatternParseError::InvalidPattern(input.to_string()))
}

fn parse_credential(input: &str) -> Result<CredentialPattern, PatternParseError> {
    if input == "*" {
        return Ok(CredentialPattern::Any);
    }

    if let Ok((hrp, data)) = bech32::decode(input) {
        return parse_bech32_credential(input, hrp.as_str(), &data);
    }

    if input.len() == 56 {
        let bytes = parse_hex_exact(input, 56)?;
        return Ok(CredentialPattern::AnyHash(bytes));
    }

    if input.len() == 64 {
        let bytes = parse_hex_exact(input, 64)?;
        let hash = hash_key(&bytes);
        return Ok(CredentialPattern::KeyHash(hash));
    }

    Err(PatternParseError::InvalidPattern(input.to_string()))
}

fn parse_bech32_credential(
    input: &str,
    hrp: &str,
    payload: &[u8],
) -> Result<CredentialPattern, PatternParseError> {
    match hrp {
        "vk" | "addr_vk" | "stake_vk" => {
            if payload.len() != 32 {
                return Err(PatternParseError::InvalidPattern(input.to_string()));
            }
            Ok(CredentialPattern::KeyHash(hash_key(payload)))
        }
        "vkh" | "addr_vkh" | "stake_vkh" => {
            if payload.len() != 28 {
                return Err(PatternParseError::InvalidPattern(input.to_string()));
            }
            Ok(CredentialPattern::KeyHash(payload.to_vec()))
        }
        "script" => {
            if payload.len() != 28 {
                return Err(PatternParseError::InvalidPattern(input.to_string()));
            }
            Ok(CredentialPattern::ScriptHash(payload.to_vec()))
        }
        _ => Err(PatternParseError::InvalidPattern(input.to_string())),
    }
}

fn parse_hex_exact(input: &str, len: usize) -> Result<Vec<u8>, PatternParseError> {
    if input.len() != len {
        return Err(PatternParseError::InvalidPattern(input.to_string()));
    }
    hex::decode(input).map_err(|_| PatternParseError::InvalidPattern(input.to_string()))
}

fn parse_hex_range(
    input: &str,
    min_len: usize,
    max_len: usize,
) -> Result<Vec<u8>, PatternParseError> {
    let len = input.len();
    if len < min_len || len > max_len || !len.is_multiple_of(2) {
        return Err(PatternParseError::InvalidPattern(input.to_string()));
    }
    hex::decode(input).map_err(|_| PatternParseError::InvalidPattern(input.to_string()))
}

fn hash_key(bytes: &[u8]) -> Vec<u8> {
    let hash = Hasher::<224>::hash(bytes);
    hash.as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas::ledger::addresses::Address;

    const SHELLEY_WITH_STAKE: &str =
        "addr1q9dhugez3ka82k2kgh7r2lg0j7aztr8uell46kydfwu3vk6n8w2cdu8mn2ha278q6q25a9rc6gmpfeekavuargcd32vsvxhl7e";
    const SHELLEY_PAYMENT_ONLY: &str = "addr1vx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzers66hrl8";
    const STAKE_ADDRESS: &str = "stake178phkx6acpnf78fuvxn0mkew3l0fd058hzquvz7w36x4gtcccycj5";
    const BYRON_ADDRESS: &str =
        "37btjrVyb4KDXBNC4haBVPCrro8AQPHwvCMp3RFhhSVWwfFmZ6wwzSK6JK1hY6wHNmtrpTf1kdbva8TCneM2YsiXT7mrzT21EacHnPpz5YyUdj64na";

    #[test]
    fn parses_wildcard() {
        let pattern = Pattern::parse("*").unwrap();
        assert_eq!(pattern, Pattern::Any);
    }

    #[test]
    fn rejects_metadata_tag_patterns() {
        let err = Pattern::parse("{42}").unwrap_err();
        assert_eq!(err, PatternParseError::MetadataTagNotQueryable);
    }

    #[test]
    fn matches_full_addresses() {
        let address = Address::from_bech32(SHELLEY_WITH_STAKE).unwrap();
        let pattern = Pattern::parse(SHELLEY_WITH_STAKE).unwrap();
        assert!(pattern.matches_address(&address));

        let stake = Address::from_bech32(STAKE_ADDRESS).unwrap();
        let stake_pattern = Pattern::parse(STAKE_ADDRESS).unwrap();
        assert!(stake_pattern.matches_address(&stake));

        let byron_bytes = BYRON_ADDRESS.from_base58().unwrap();
        let byron = Address::from_bytes(&byron_bytes).unwrap();
        let byron_pattern = Pattern::parse(BYRON_ADDRESS).unwrap();
        assert!(byron_pattern.matches_address(&byron));
    }

    #[test]
    fn matches_credential_patterns() {
        let address = Address::from_bech32(SHELLEY_WITH_STAKE).unwrap();
        let shelley = match address {
            Address::Shelley(addr) => addr,
            _ => panic!("expected shelley address"),
        };

        let wildcard = Pattern::parse("*/*").unwrap();
        assert!(wildcard.matches_address(&Address::Shelley(shelley.clone())));

        let payment_hex = hex::encode(shelley.payment().to_vec());
        let payment_pattern = Pattern::parse(&format!("{payment_hex}/*")).unwrap();
        assert!(payment_pattern.matches_address(&Address::Shelley(shelley.clone())));

        let delegation_hex = match shelley.delegation() {
            ShelleyDelegationPart::Key(hash) => hex::encode(hash.as_ref()),
            ShelleyDelegationPart::Script(hash) => hex::encode(hash.as_ref()),
            _ => panic!("expected delegation credential"),
        };
        let delegation_pattern = Pattern::parse(&format!("*/{delegation_hex}")).unwrap();
        assert!(delegation_pattern.matches_address(&Address::Shelley(shelley.clone())));

        let payment_only = Address::from_bech32(SHELLEY_PAYMENT_ONLY).unwrap();
        assert!(!delegation_pattern.matches_address(&payment_only));
    }

    #[test]
    fn parses_bech32_credentials() {
        let address = Address::from_bech32(SHELLEY_WITH_STAKE).unwrap();
        let shelley = match address {
            Address::Shelley(addr) => addr,
            _ => panic!("expected shelley address"),
        };

        let delegation_bytes = match shelley.delegation() {
            ShelleyDelegationPart::Key(hash) => hash.as_ref().to_vec(),
            ShelleyDelegationPart::Script(hash) => hash.as_ref().to_vec(),
            _ => panic!("expected delegation credential"),
        };

        let hrp = bech32::Hrp::parse("stake_vkh").unwrap();
        let delegation_bech32 = bech32::encode::<bech32::Bech32>(hrp, &delegation_bytes).unwrap();
        let pattern = Pattern::parse(&format!("*/{delegation_bech32}")).unwrap();
        assert!(pattern.matches_address(&Address::Shelley(shelley.clone())));

        let pubkey = vec![7u8; 32];
        let pub_hrp = bech32::Hrp::parse("vk").unwrap();
        let pub_bech32 = bech32::encode::<bech32::Bech32>(pub_hrp, &pubkey).unwrap();
        let pattern = Pattern::parse(&format!("{pub_bech32}/*")).unwrap();
        let expected = hash_key(&pubkey);
        match pattern {
            Pattern::Address(AddressPattern::Credentials { payment, .. }) => {
                assert_eq!(payment, CredentialPattern::KeyHash(expected));
            }
            _ => panic!("expected address credentials pattern"),
        }
    }

    #[test]
    fn matches_asset_patterns() {
        let policy_hex = "a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235";
        let policy = hex::decode(policy_hex).unwrap();
        let asset_name = b"HOSKY";
        let asset_hex = hex::encode(asset_name);

        let pattern = Pattern::parse(&format!("{policy_hex}.{asset_hex}")).unwrap();
        assert!(pattern.matches_asset(&policy, asset_name));
        assert!(!pattern.matches_asset(&policy, b"SNEK"));

        let any = Pattern::parse(&format!("{policy_hex}.*")).unwrap();
        assert!(any.matches_asset(&policy, b"ANY"));
    }

    #[test]
    fn matches_output_reference_patterns() {
        let tx_id_hex = "35d8340cd6a5d31bf9d09706b92adedf9b1b632e682fdab9fc8865ee3de14e09";
        let tx_id = hex::decode(tx_id_hex).unwrap();

        let pattern = Pattern::parse(&format!("42@{tx_id_hex}")).unwrap();
        assert!(pattern.matches_output_ref(&tx_id, 42));
        assert!(!pattern.matches_output_ref(&tx_id, 41));

        let any = Pattern::parse(&format!("*@{tx_id_hex}")).unwrap();
        assert!(any.matches_output_ref(&tx_id, 0));
    }
}
