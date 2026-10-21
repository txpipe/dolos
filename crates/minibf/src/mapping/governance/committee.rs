//! The Blockfrost model of a committee member and its hot-key authorization.

use std::collections::BTreeMap;

use axum::http::StatusCode;
use blockfrost_openapi::models::committee_members_inner::{CommitteeMembersInner, Status};
use dolos_cardano::model::gov::CommitteeAuthorization;
use pallas::ledger::primitives::{Epoch, StakeCredential};

use crate::mapping::{bech32_committee_cold, bech32_committee_hot, i32_or_500};

/// This function returns the 28-byte credential hash as hex and a script flag.
/// Blockfrost serves this pair (`cc_*_hex` and `cc_*_has_script`) beside each
/// CIP-129 committee ID.
fn committee_credential_parts(cred: &StakeCredential) -> (String, bool) {
    match cred {
        StakeCredential::AddrKeyhash(key) => (hex::encode(key), false),
        StakeCredential::ScriptHash(key) => (hex::encode(key), true),
    }
}

/// This function resolves one committee member against its hot-key
/// authorization history.
///
/// The code uses the last event in the authorization history to select the
/// status. There are three possible events:
///
/// - No event: the member never authorized a hot key (`not_authorized`).
/// - A resignation: the hot key is gone (`resigned`).
/// - A hot credential: the member can vote now (`authorized`).
///
/// The `cc_hot_*` fields carry the hot credential only for the `authorized`
/// status. For the other two statuses, these fields are null. This behavior is
/// the same as Blockfrost.
fn committee_member(
    cold: &StakeCredential,
    expiry: Epoch,
    auth: Option<&CommitteeAuthorization>,
) -> Result<CommitteeMembersInner, StatusCode> {
    let (cc_cold_hex, cc_cold_has_script) = committee_credential_parts(cold);

    let (status, hot) = match auth {
        None => (Status::NotAuthorized, None),
        Some(CommitteeAuthorization::Resigned(_)) => (Status::Resigned, None),
        Some(CommitteeAuthorization::HotCredential(hot)) => (Status::Authorized, Some(hot)),
    };

    let (cc_hot_id, cc_hot_hex, cc_hot_has_script) = match hot {
        Some(hot) => {
            let (hex, is_script) = committee_credential_parts(hot);
            (Some(bech32_committee_hot(hot)?), Some(hex), Some(is_script))
        }
        None => (None, None, None),
    };

    Ok(CommitteeMembersInner {
        cc_cold_id: bech32_committee_cold(cold)?,
        cc_cold_hex,
        cc_cold_has_script,
        cc_hot_id,
        cc_hot_hex,
        cc_hot_has_script,
        status,
        expiration_epoch: i32_or_500(expiry)?,
    })
}

/// This function resolves all committee members against their authorization
/// history, read through `auth`. It orders the members by the raw cold-hash
/// hex, as Blockfrost does.
pub fn committee_members<'a>(
    members: &BTreeMap<StakeCredential, Epoch>,
    auth: impl Fn(&StakeCredential) -> Option<&'a CommitteeAuthorization>,
) -> Result<Vec<CommitteeMembersInner>, StatusCode> {
    let mut rows = members
        .iter()
        .map(|(cold, expiry)| committee_member(cold, *expiry, auth(cold)))
        .collect::<Result<Vec<_>, _>>()?;

    rows.sort_by(|a, b| a.cc_cold_hex.cmp(&b.cc_cold_hex));

    Ok(rows)
}
