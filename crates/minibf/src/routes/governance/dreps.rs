use crate::mapping::{bech32, IntoModel, DREP_HRP};
use axum::http::StatusCode;
use blockfrost_openapi::models::{Drep, DrepsInner};
use dolos_cardano::{model::DRepState, pallas_extras, ChainSummary, PParamsSet};
use dolos_core::BlockSlot;
use pallas::ledger::primitives::{conway::DRep, Epoch};

pub fn parse_drep_id(drep_id: &str) -> Result<(String, Vec<u8>, bool, bool), StatusCode> {
    match drep_id {
        "drep_always_abstain" => Ok((drep_id.to_string(), vec![0], false, true)),
        "drep_always_no_confidence" => Ok((drep_id.to_string(), vec![1], false, true)),
        drep_id => {
            let (hrp, payload) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

            match (hrp.as_str(), payload.len()) {
                ("drep", 29) => {
                    let header_byte = payload.first().ok_or(StatusCode::BAD_REQUEST)?;

                    // first 4 bits need to be equal to 0010
                    if header_byte & 0b11110000 != 0b00100000 {
                        return Err(StatusCode::BAD_REQUEST);
                    }

                    Ok((drep_id.to_string(), payload, false, false))
                }
                ("drep", 28) => Ok((
                    drep_id.to_string(),
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_vkh", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_script", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_SCRIPT_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                _ => Err(StatusCode::BAD_REQUEST),
            }
        }
    }
}

pub struct DrepModelBuilder<'a> {
    pub drep_id: String,
    pub drep_id_encoded: Vec<u8>,
    pub is_legacy: bool,
    pub state: Option<DRepState>,
    pub pparams: PParamsSet,
    pub chain: &'a ChainSummary,
    pub tip: BlockSlot,
}

impl<'a> DrepModelBuilder<'a> {
    fn is_special_case(&self) -> bool {
        ["drep_always_abstain", "drep_always_no_confidence"].contains(&self.drep_id.as_str())
    }

    fn first_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
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
        if self.is_special_case() {
            return None;
        }

        self.state
            .as_ref()?
            .last_active_slot
            .map(|x| self.chain.slot_epoch(x).0)
    }

    fn is_drep_expired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }

        if self.is_drep_retired() {
            return false;
        }

        let last_active_epoch = self.last_active_epoch();
        let inactivity_period = self.pparams.drep_inactivity_period().unwrap_or_default();
        let expiring_epoch = last_active_epoch.map(|x| x + inactivity_period);
        let (current_epoch, _) = self.chain.slot_epoch(self.tip);

        expiring_epoch
            .map(|expiration| expiration <= current_epoch)
            .unwrap_or(false)
    }

    fn is_drep_retired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }

        let Some(state) = self.state.as_ref() else {
            return false;
        };

        match (state.registered_at, state.unregistered_at) {
            (Some(registered), Some(unregistered)) => unregistered > registered,
            (Some(_), None) => false,
            _ => false,
        }
    }

    fn is_drep_active(&self) -> bool {
        !self.is_drep_retired()
    }

    fn hex_value(&self) -> String {
        if self.is_special_case() {
            "".to_string()
        } else if self.is_legacy {
            hex::encode(&self.drep_id_encoded[1..])
        } else {
            hex::encode(&self.drep_id_encoded)
        }
    }

    fn amount(&self) -> String {
        self.state
            .as_ref()
            .map(|x| x.voting_power.to_string())
            .unwrap_or_default()
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
    pparams: PParamsSet,
    chain: &ChainSummary,
    tip: BlockSlot,
) -> Result<DrepsInner, StatusCode> {
    let (drep_id, drep_id_encoded, is_legacy) = match &state.identifier {
        DRep::Key(hash) => (
            bech32(DREP_HRP, hash)?,
            [vec![pallas_extras::DREP_KEY_PREFIX], hash.to_vec()].concat(),
            true,
        ),
        DRep::Script(hash) => (
            bech32(DREP_HRP, hash)?,
            [vec![pallas_extras::DREP_SCRIPT_PREFIX], hash.to_vec()].concat(),
            true,
        ),
        DRep::Abstain => ("drep_always_abstain".to_string(), vec![0], false),
        DRep::NoConfidence => ("drep_always_no_confidence".to_string(), vec![1], false),
    };

    let builder = DrepModelBuilder {
        drep_id,
        drep_id_encoded,
        is_legacy,
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
            parse_drep_id("drep_always_abstain"),
            Ok(("drep_always_abstain".to_string(), vec![0], false, true))
        );

        assert_eq!(
            parse_drep_id("drep_always_no_confidence"),
            Ok((
                "drep_always_no_confidence".to_string(),
                vec![1],
                false,
                true
            ))
        );
    }

    #[test]
    fn parse_drep_id_cip105_key() {
        let hash = vec![7u8; 28];
        let drep_id = encode_id("drep", &hash);

        assert_eq!(
            parse_drep_id(&drep_id),
            Ok((
                drep_id.clone(),
                [vec![pallas_extras::DREP_KEY_PREFIX], hash].concat(),
                true,
                false,
            ))
        );
    }

    #[test]
    fn parse_drep_id_normalizes_vkh_and_script() {
        let hash = vec![7u8; 28];
        let cip105 = encode_id("drep", &hash);

        assert_eq!(
            parse_drep_id(&encode_id("drep_vkh", &hash)),
            Ok((
                cip105.clone(),
                [vec![pallas_extras::DREP_KEY_PREFIX], hash.clone()].concat(),
                true,
                false,
            ))
        );

        assert_eq!(
            parse_drep_id(&encode_id("drep_script", &hash)),
            Ok((
                cip105,
                [vec![pallas_extras::DREP_SCRIPT_PREFIX], hash].concat(),
                true,
                false,
            ))
        );
    }

    #[test]
    fn parse_drep_id_rejects_malformed_ids() {
        // not bech32
        assert!(parse_drep_id("not-a-drep").is_err());
        // wrong hrp
        assert!(parse_drep_id(&encode_id("pool", &[7u8; 28])).is_err());
        // wrong payload
        assert!(parse_drep_id(&encode_id("drep", &[7u8; 27])).is_err());
        assert!(parse_drep_id(&encode_id("drep", &[7u8; 30])).is_err());
        assert!(parse_drep_id(&encode_id("drep_vkh", &[7u8; 29])).is_err());
        assert!(parse_drep_id(&encode_id("drep_script", &[7u8; 29])).is_err());
    }
}
