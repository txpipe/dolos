use crate::mapping::{bech32, bech32_drep, IntoModel, DREP_HRP};
use axum::http::StatusCode;
use blockfrost_openapi::models::{Drep, DrepsInner};
use dolos_cardano::{
    model::{drep_encoded_bytes, DRepState},
    pallas_extras, ChainSummary, PParamsSet,
};
use dolos_core::BlockSlot;
use pallas::ledger::primitives::{conway::DRep, Epoch};

pub const DREP_ALWAYS_ABSTAIN: &str = "drep_always_abstain";
pub const DREP_ALWAYS_NO_CONFIDENCE: &str = "drep_always_no_confidence";

#[derive(Debug, PartialEq, Eq)]
pub struct ParsedDRep {
    pub drep_id: String,
    pub encoded: Vec<u8>,
    pub is_legacy: bool,
    pub is_special: bool,
}

impl ParsedDRep {
    fn special(drep_id: &str, key: u8) -> Self {
        Self {
            drep_id: drep_id.to_string(),
            encoded: vec![key],
            is_legacy: false,
            is_special: true,
        }
    }

    fn cip129(drep_id: &str, encoded: Vec<u8>) -> Self {
        Self {
            drep_id: drep_id.to_string(),
            encoded,
            is_legacy: false,
            is_special: false,
        }
    }

    fn legacy(drep_id: String, hash: Vec<u8>, prefix: u8) -> Self {
        Self {
            drep_id,
            encoded: [vec![prefix], hash].concat(),
            is_legacy: true,
            is_special: false,
        }
    }
}

pub fn parse_drep_id(drep_id: &str) -> Result<ParsedDRep, StatusCode> {
    match drep_id {
        DREP_ALWAYS_ABSTAIN => Ok(ParsedDRep::special(drep_id, 0)),
        DREP_ALWAYS_NO_CONFIDENCE => Ok(ParsedDRep::special(drep_id, 1)),
        drep_id => {
            let (hrp, payload) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

            match (hrp.as_str(), payload.len()) {
                ("drep", 29) => {
                    let header_byte = payload.first().ok_or(StatusCode::BAD_REQUEST)?;

                    // CIP-129 defines exactly two DRep headers: 0x22 (key
                    // hash) and 0x23 (script hash); credential values 0 and 1
                    // are reserved. Blockfrost rejects everything else.
                    let valid_headers = [
                        pallas_extras::DREP_KEY_PREFIX,
                        pallas_extras::DREP_SCRIPT_PREFIX,
                    ];

                    if !valid_headers.contains(header_byte) {
                        return Err(StatusCode::BAD_REQUEST);
                    }

                    Ok(ParsedDRep::cip129(drep_id, payload))
                }
                ("drep", 28) => Ok(ParsedDRep::legacy(
                    drep_id.to_string(),
                    payload,
                    pallas_extras::DREP_KEY_PREFIX,
                )),
                // Blockfrost accepts only the `drep` and `drep_script`
                // prefixes; `drep_vkh` gets 400 there, so it gets 400 here.
                ("drep_script", 28) => Ok(ParsedDRep::legacy(
                    bech32(DREP_HRP, &payload).map_err(|_| StatusCode::BAD_REQUEST)?,
                    payload,
                    pallas_extras::DREP_SCRIPT_PREFIX,
                )),
                _ => Err(StatusCode::BAD_REQUEST),
            }
        }
    }
}

/// Blockfrost's `retired` flag: the latest lifecycle event is an
/// unregistration. Special DReps never hold a `registered_at`, so they never
/// read as retired.
pub fn drep_is_retired(state: &DRepState) -> bool {
    state.is_unregistered()
}

/// Blockfrost's `expired` flag: a still-registered DRep whose last activity
/// lies more than `drep_activity` epochs behind the tip. The epoch where the
/// two are exactly equal is still active.
pub fn drep_is_expired(
    state: &DRepState,
    chain: &ChainSummary,
    tip: BlockSlot,
    pparams: &PParamsSet,
) -> bool {
    if drep_is_retired(state) {
        return false;
    }

    let last_active_epoch = state.last_active_slot.map(|x| chain.slot_epoch(x).0);
    let inactivity_period = pparams.drep_inactivity_period().unwrap_or_default();
    let expiring_epoch = last_active_epoch.map(|x| x + inactivity_period);
    let (current_epoch, _) = chain.slot_epoch(tip);

    expiring_epoch
        .map(|expiration| expiration < current_epoch)
        .unwrap_or(false)
}

pub struct DrepModelBuilder<'a> {
    pub drep_id: String,
    pub drep_id_encoded: Vec<u8>,
    pub is_legacy: bool,
    pub is_special: bool,
    pub state: Option<DRepState>,
    pub pparams: &'a PParamsSet,
    pub chain: &'a ChainSummary,
    pub tip: BlockSlot,
}

impl<'a> DrepModelBuilder<'a> {
    fn first_active_epoch(&self) -> Option<Epoch> {
        if self.is_special {
            return None;
        }

        if self
            .state
            .as_ref()
            .map(|x| x.is_unregistered())
            .unwrap_or(true)
        {
            return None;
        }

        self.state
            .as_ref()?
            .registered_at
            .map(|x| self.chain.slot_epoch(x.0).0)
    }

    fn last_active_epoch(&self) -> Option<Epoch> {
        if self.is_special {
            return None;
        }

        self.state
            .as_ref()?
            .last_active_slot
            .map(|x| self.chain.slot_epoch(x).0)
    }

    fn is_drep_expired(&self) -> bool {
        if self.is_special {
            return false;
        }

        self.state
            .as_ref()
            .map(|state| drep_is_expired(state, self.chain, self.tip, self.pparams))
            .unwrap_or(false)
    }

    fn is_drep_retired(&self) -> bool {
        if self.is_special {
            return false;
        }

        self.state.as_ref().map(drep_is_retired).unwrap_or(false)
    }

    fn is_drep_active(&self) -> bool {
        !self.is_drep_retired()
    }

    fn hex_value(&self) -> String {
        if self.is_special {
            "".to_string()
        } else if self.is_legacy {
            hex::encode(&self.drep_id_encoded[1..])
        } else {
            hex::encode(&self.drep_id_encoded)
        }
    }

    /// The boundary pass stores the ledger-exact `drep_distr` snapshot in
    /// `DRepState.voting_power`; serving it keeps the API on the corrected
    /// value instead of a live re-aggregation.
    fn amount(&self) -> String {
        self.state
            .as_ref()
            .map(|x| x.voting_power)
            .unwrap_or_default()
            .to_string()
    }
}

impl<'a> IntoModel<Drep> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Drep, StatusCode> {
        let out = Drep {
            drep_id: self.drep_id.clone(),
            hex: self.hex_value(),
            amount: self.amount(),
            active: self.is_drep_active(),
            active_epoch: self.first_active_epoch().map(|x| x as i32),
            has_script: pallas_extras::drep_id_is_script(&self.drep_id_encoded),
            retired: self.is_drep_retired(),
            expired: self.is_drep_expired(),
            last_active_epoch: self.last_active_epoch().map(|x| x as i32),
        };

        Ok(out)
    }
}

impl<'a> IntoModel<DrepsInner> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<DrepsInner, StatusCode> {
        let out = DrepsInner {
            drep_id: self.drep_id.clone(),
            hex: self.hex_value(),
            amount: self.amount(),
            has_script: pallas_extras::drep_id_is_script(&self.drep_id_encoded),
            retired: self.is_drep_retired(),
            expired: self.is_drep_expired(),
            last_active_epoch: self.last_active_epoch().map(|x| x as i32),
            // off-chain metadata is fetched and attached by the caller
            metadata: None,
        };

        Ok(out)
    }
}

pub fn drep_list_item(
    state: DRepState,
    pparams: &PParamsSet,
    chain: &ChainSummary,
    tip: BlockSlot,
) -> Result<DrepsInner, StatusCode> {
    let drep_id = bech32_drep(&state.identifier)?;
    let drep_id_encoded = drep_encoded_bytes(&state.identifier);
    let is_special = matches!(state.identifier, DRep::Abstain | DRep::NoConfidence);

    let builder = DrepModelBuilder {
        drep_id,
        drep_id_encoded,
        is_legacy: false,
        is_special,
        state: Some(state),
        pparams,
        chain,
        tip,
    };

    builder.into_model()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bech32::{Bech32, Hrp};

    fn encode_id(hrp: &str, payload: &[u8]) -> String {
        let hrp = Hrp::parse_unchecked(hrp);
        bech32::encode::<Bech32>(hrp, payload).expect("failed to encode bech32 id")
    }

    #[test]
    fn parse_drep_id_special_cases() {
        assert_eq!(
            parse_drep_id(DREP_ALWAYS_ABSTAIN),
            Ok(ParsedDRep::special(DREP_ALWAYS_ABSTAIN, 0))
        );

        assert_eq!(
            parse_drep_id(DREP_ALWAYS_NO_CONFIDENCE),
            Ok(ParsedDRep::special(DREP_ALWAYS_NO_CONFIDENCE, 1))
        );
    }

    #[test]
    fn parse_drep_id_cip105_key() {
        let hash = vec![7u8; 28];
        let drep_id = encode_id("drep", &hash);

        assert_eq!(
            parse_drep_id(&drep_id),
            Ok(ParsedDRep::legacy(
                drep_id.clone(),
                hash,
                pallas_extras::DREP_KEY_PREFIX,
            ))
        );
    }

    #[test]
    fn parse_drep_id_normalizes_script() {
        let hash = vec![7u8; 28];
        let cip105 = encode_id("drep", &hash);

        assert_eq!(
            parse_drep_id(&encode_id("drep_script", &hash)),
            Ok(ParsedDRep::legacy(
                cip105,
                hash,
                pallas_extras::DREP_SCRIPT_PREFIX,
            ))
        );
    }

    #[test]
    fn parse_drep_id_cip129_accepts_only_key_and_script_headers() {
        let hash = vec![7u8; 28];

        for header in [
            pallas_extras::DREP_KEY_PREFIX,
            pallas_extras::DREP_SCRIPT_PREFIX,
        ] {
            let payload = [vec![header], hash.clone()].concat();
            assert!(parse_drep_id(&encode_id("drep", &payload)).is_ok());
        }

        // upper nibble matches, credential nibble is reserved or invalid
        for header in [0x20u8, 0x21, 0x24, 0x2f] {
            let payload = [vec![header], hash.clone()].concat();
            assert_eq!(
                parse_drep_id(&encode_id("drep", &payload)),
                Err(StatusCode::BAD_REQUEST)
            );
        }
    }

    #[test]
    fn parse_drep_id_rejects_malformed_ids() {
        // not bech32
        assert!(parse_drep_id("not-a-drep").is_err());
        // wrong hrp
        assert!(parse_drep_id(&encode_id("pool", &[7u8; 28])).is_err());
        // Blockfrost does not accept the drep_vkh prefix
        assert!(parse_drep_id(&encode_id("drep_vkh", &[7u8; 28])).is_err());
        // wrong payload
        assert!(parse_drep_id(&encode_id("drep", &[7u8; 27])).is_err());
        assert!(parse_drep_id(&encode_id("drep", &[7u8; 30])).is_err());
        assert!(parse_drep_id(&encode_id("drep_script", &[7u8; 29])).is_err());
    }
}
