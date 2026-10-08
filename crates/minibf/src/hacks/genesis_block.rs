//! Blockfrost serves a synthetic genesis block per network. Dolos stores none,
//! so it is built here from the genesis config.

use axum::http::StatusCode;
use base64::Engine as _;
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{ArchiveStore as _, Domain, Genesis};
use pallas::{
    crypto::{
        hash::{Hash, Hasher},
        key::ed25519::PublicKey,
    },
    interop::hardano::configs::{byron, shelley},
    ledger::{
        addresses::{byron::AddressPayload, Address, ByronAddress},
        traverse::MultiEraBlock,
    },
};

use crate::{log_and_500, Facade};

use super::{GENESIS_HASH_MAINNET, GENESIS_HASH_PREPROD, GENESIS_HASH_PREVIEW};

pub struct GenesisBlock {
    pub hash: &'static str,
    pub time: i32,
    pub next_block: &'static str,
}

const PREPROD: GenesisBlock = GenesisBlock {
    hash: GENESIS_HASH_PREPROD,
    time: 1654041600,
    next_block: "9ad7ff320c9cf74e0f5ee78d22a85ce42bb0a487d0506bf60cfb5a91ea4497d2",
};

const PREVIEW: GenesisBlock = GenesisBlock {
    hash: GENESIS_HASH_PREVIEW,
    time: 1666656000,
    next_block: "268ae601af8f9214804735910a3301881fbe0eec9936db7d1fb9fc39e93d1e37",
};

const MAINNET: GenesisBlock = GenesisBlock {
    hash: GENESIS_HASH_MAINNET,
    time: 1506203091,
    next_block: "89d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df4",
};

pub fn genesis_for_domain<D: Domain>(domain: &Facade<D>) -> Option<&'static GenesisBlock> {
    match domain.genesis().shelley.network_magic {
        Some(1) => Some(&PREPROD),
        Some(2) => Some(&PREVIEW),
        Some(764824073) => Some(&MAINNET),
        _ => None,
    }
}

pub fn is_genesis_hash<D: Domain>(domain: &Facade<D>, hash: &[u8]) -> Result<bool, StatusCode> {
    let Some(genesis) = genesis_for_domain(domain) else {
        return Ok(false);
    };

    let genesis_hash = hex::decode(genesis.hash).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(hash == genesis_hash.as_slice())
}

/// `None` on a network without a known genesis block.
pub fn genesis_block<D: Domain>(domain: &Facade<D>) -> Result<Option<BlockContent>, StatusCode> {
    let Some(genesis) = genesis_for_domain(domain) else {
        return Ok(None);
    };

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let confirmations = MultiEraBlock::decode(&tip)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .number() as i32;

    let byron_utxos = byron::genesis_utxos(&domain.genesis().byron);
    let shelley_utxos = shelley::shelley_utxos(&domain.genesis().shelley);

    let output = byron_utxos.iter().map(|(_, _, x)| *x).sum::<u64>()
        + shelley_utxos.iter().map(|(_, _, x)| *x).sum::<u64>();

    Ok(Some(BlockContent {
        time: genesis.time,
        height: None,
        hash: genesis.hash.to_string(),
        slot: None,
        epoch: None,
        epoch_slot: None,
        slot_leader: "Genesis slot leader".to_string(),
        size: 0,
        tx_count: (byron_utxos.len() + shelley_utxos.len()) as i32,
        output: Some(output.to_string()),
        fees: Some("0".to_string()),
        block_vrf: None,
        op_cert: None,
        op_cert_counter: None,
        previous_block: None,
        next_block: Some(genesis.next_block.to_string()),
        confirmations,
    }))
}

/// Blockfrost links the first real block back to the genesis block.
pub fn set_genesis_previous_block<D: Domain>(domain: &Facade<D>, block: &mut BlockContent) {
    if block.height.is_some_and(|x| x > 1) {
        return;
    }

    let Some(genesis) = genesis_for_domain(domain) else {
        return;
    };

    if block.hash == genesis.hash {
        return;
    }

    if block.previous_block.is_none() {
        block.previous_block = Some(genesis.hash.to_string());
    }
}

/// A transaction of the genesis block: one genesis output, under the hash the
/// ledger gives it.
pub struct GenesisTx {
    pub hash: Hash<32>,
    pub address: String,
}

/// The genesis block's transactions in the order db-sync stores them, which is
/// the order Blockfrost lists them in.
///
/// db-sync reads Byron's AVVM distribution off a map keyed by redeem key and
/// its other balances off one keyed by address, so neither follows the genesis
/// file. No network has both kinds; AVVM goes first, then the other Byron
/// balances, then Shelley's initial funds.
pub fn genesis_txs(genesis: &Genesis) -> Result<Vec<GenesisTx>, StatusCode> {
    let mut avvm = genesis
        .byron
        .avvm_distr
        .keys()
        .map(|key| {
            let key = base64::engine::general_purpose::URL_SAFE
                .decode(key)
                .map_err(log_and_500("malformed genesis entry"))?;
            let pubkey =
                PublicKey::try_from(&key[..]).map_err(log_and_500("malformed genesis entry"))?;
            let address: ByronAddress = AddressPayload::new_redeem(pubkey, None).into();

            Ok((key, address))
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;
    avvm.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut balances = genesis
        .byron
        .non_avvm_balances
        .keys()
        .map(|address| {
            ByronAddress::from_base58(address).map_err(log_and_500("malformed genesis entry"))
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;
    balances.sort_by_key(|address| address.to_vec());

    let byron = avvm
        .into_iter()
        .map(|(_, address)| address)
        .chain(balances)
        .map(|address| GenesisTx {
            hash: Hasher::<256>::hash_cbor(&address),
            address: address.to_base58(),
        });

    let mut funds = shelley::shelley_utxos(&genesis.shelley)
        .into_iter()
        .map(|(hash, address, _)| (address.to_vec(), hash, address))
        .collect::<Vec<(Vec<u8>, Hash<32>, Address)>>();
    funds.sort_by(|(a, ..), (b, ..)| a.cmp(b));

    let shelley = funds
        .into_iter()
        .map(|(_, hash, address)| -> Result<GenesisTx, StatusCode> {
            Ok(GenesisTx {
                hash,
                address: address
                    .to_bech32()
                    .map_err(log_and_500("malformed genesis entry"))?,
            })
        });

    byron.map(Ok).chain(shelley).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::blocks::touched_addresses_model;

    /// Blake2b-256 of the lines, to compare whole listings with Blockfrost's.
    fn fingerprint<'a>(lines: impl IntoIterator<Item = &'a str>) -> String {
        let joined = lines.into_iter().collect::<Vec<_>>().join("\n");
        Hasher::<256>::hash(joined.as_bytes()).to_string()
    }

    /// Each network's whole `/blocks/{genesis}/txs` and `/addresses` listing,
    /// fingerprinted off live Blockfrost on 2026-10-08.
    #[test]
    fn genesis_txs_match_blockfrost_order() {
        let networks = [
            (
                dolos_cardano::include::preprod::load(),
                8,
                "4a1250d2c09cee92fb1a530a5b75356793f18927e9fcb73aaafe21659474da96",
                "af7db121702b30d3441f723bd5248d091def994b0549185e0ee2224b86de53da",
            ),
            (
                dolos_cardano::include::preview::load(),
                8,
                "825ab0021bde72c04e517ed197ddcfae68ce07f4a2f499159b8da8c58af41b48",
                "8081983d0676ed05ed0eef22d8f013c53067780030877bcd3f5bb3968a751bb5",
            ),
            (
                dolos_cardano::include::mainnet::load(),
                14_505,
                "05d4bb96c5977d982aab0be47b40e2ae7ccb07d14e455b62e24354afa7f52441",
                "cfb71bece3845721255e7b1af8a9202093364eba780374c6f2c3db63e8169b6f",
            ),
        ];

        for (genesis, count, txs, addresses) in networks {
            let genesis_txs = genesis_txs(&genesis).unwrap();
            assert_eq!(genesis_txs.len(), count);

            let hashes: Vec<String> = genesis_txs.iter().map(|tx| tx.hash.to_string()).collect();
            assert_eq!(fingerprint(hashes.iter().map(String::as_str)), txs);

            let touched = genesis_txs
                .into_iter()
                .map(|tx| (tx.hash.to_string(), [tx.address].into()));
            let listed = touched_addresses_model(touched);
            assert_eq!(
                fingerprint(listed.iter().map(|x| x.address.as_str())),
                addresses
            );
        }
    }
}
