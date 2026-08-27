use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::utils_addresses_xpub::UtilsAddressesXpub;
use dolos_core::Domain;
use ed25519_bip32::{DerivationScheme, XPub};
use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart},
};

use crate::{error::Error, Facade};

/// This value is the first index in the hardened range. Public (soft)
/// BIP32-Ed25519 derivation uses only indices that are less than this value.
const HARDENED_OFFSET: u32 = 0x8000_0000;

/// These values identify the CIP-1852 staking role and index. Each derived
/// address uses the stake credential from the `2/0` child of the account key.
/// The payment role and index do not change this credential.
const STAKING_ROLE: u32 = 2;
const STAKING_INDEX: u32 = 0;

/// This function decodes an account xpub from its 64-byte hexadecimal
/// representation. The representation contains a 32-byte public key and a
/// 32-byte chain code.
fn parse_account_xpub(xpub: &str) -> Result<XPub, Error> {
    let bytes = hex::decode(xpub).map_err(|_| Error::InvalidXpub)?;
    let bytes: [u8; 64] = bytes.try_into().map_err(|_| Error::InvalidXpub)?;
    Ok(XPub::from_bytes(bytes))
}

/// This function derives the soft child key at `index`. Then it hashes the
/// Ed25519 public key to make a 28-byte credential.
fn credential(parent: &XPub, role: u32, index: u32) -> Result<Hash<28>, Error> {
    let child = parent
        .derive(DerivationScheme::V2, role)
        .and_then(|role_key| role_key.derive(DerivationScheme::V2, index))
        .map_err(|_| Error::InvalidXpub)?;

    Ok(Hasher::<224>::hash(&child.public_key()))
}

/// This function derives the Shelley base address for `role` and `index` from
/// an account xpub. The payment credential uses the `role/index` child. The
/// stake credential uses the `2/0` child.
fn derive_base_address(
    account_xpub: &XPub,
    role: u32,
    index: u32,
    network: Network,
) -> Result<ShelleyAddress, Error> {
    let payment = credential(account_xpub, role, index)?;
    let stake = credential(account_xpub, STAKING_ROLE, STAKING_INDEX)?;

    Ok(ShelleyAddress::new(
        network,
        ShelleyPaymentPart::key_hash(payment),
        ShelleyDelegationPart::key_hash(stake),
    ))
}

pub async fn xpub_address<D>(
    Path((xpub, role, index)): Path<(String, u32, u32)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<UtilsAddressesXpub>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let account_xpub = parse_account_xpub(&xpub)?;

    if role >= HARDENED_OFFSET {
        return Err(Error::InvalidDerivationRole);
    }

    if index >= HARDENED_OFFSET {
        return Err(Error::InvalidDerivationIndex);
    }

    let network = domain.get_network_id()?;

    let address = derive_base_address(&account_xpub, role, index, network)?
        .to_bech32()
        .map_err(|_| Error::InvalidXpub)?;

    Ok(Json(UtilsAddressesXpub::new(
        xpub,
        role as i32,
        index as i32,
        address,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This pure function derives an address in the same way as the handler.
    /// Thus, the test does not require a domain.
    fn derive(xpub: &str, role: u32, index: u32, network: Network) -> String {
        let account_xpub = parse_account_xpub(xpub).expect("The xpub must be valid.");
        derive_base_address(&account_xpub, role, index, network)
            .expect("The function cannot derive the address.")
            .to_bech32()
            .expect("The function cannot encode the address.")
    }

    // The upstream Blockfrost fixtures contain the account xpub and the
    // expected Mainnet addresses for this endpoint:
    // `/utils/addresses/xpub/{xpub}/{role}/{index}`.
    const XPUB: &str = "7ec9738746cb4708df52a455b43aa3fdee8955abaf37f68ffc79bb84fbf9e1b39d77e2deb9749faf890ff8326d350ed3fd0e4aa271b35cad063692af87102152";

    #[test]
    fn matches_blockfrost_mainnet_fixtures() {
        assert_eq!(
            derive(XPUB, 0, 0, Network::Mainnet),
            "addr1qxykyqgwd577heaunndagj66z0n2z0jgedjcn3qxlrujpjq49ucjdfty5p5qlw5qe28v9k988stffc2g0hx2xx86a2dqnt753m",
        );
        assert_eq!(
            derive(XPUB, 1, 1, Network::Mainnet),
            "addr1qx8dz454rqaxjhrynjhppwq22wwk2dtkz022ngxgcdahflc49ucjdfty5p5qlw5qe28v9k988stffc2g0hx2xx86a2dqq078gh",
        );
        assert_eq!(
            derive(XPUB, 0, 3, Network::Mainnet),
            "addr1qy9ltwrqmtl9vu2y9y24aaxppyfhjhyhrfdgy8usxuu3hdq49ucjdfty5p5qlw5qe28v9k988stffc2g0hx2xx86a2dqjrqcu2",
        );
        assert_eq!(
            derive(XPUB, 1, 3, Network::Mainnet),
            "addr1qytpyxyh5j023fq88xhj862guwrymhwjadt5czqvumpv02s49ucjdfty5p5qlw5qe28v9k988stffc2g0hx2xx86a2dq56wr69",
        );
    }

    #[test]
    fn testnet_shares_credentials_with_mainnet() {
        // The network nibble is the only difference. Both addresses use the
        // `2/0` stake credential.
        let mainnet = derive(XPUB, 0, 0, Network::Mainnet);
        let testnet = derive(XPUB, 0, 0, Network::Testnet);

        assert!(mainnet.starts_with("addr1"));
        assert!(testnet.starts_with("addr_test1"));
    }

    #[test]
    fn rejects_malformed_xpub() {
        assert!(matches!(
            parse_account_xpub("not-hex"),
            Err(Error::InvalidXpub)
        ));
        // This hexadecimal value has 63 bytes instead of 64 bytes.
        assert!(matches!(
            parse_account_xpub(&"ab".repeat(63)),
            Err(Error::InvalidXpub)
        ));
    }
}
