use dolos_core::{RelationContext, RelationRecord, RelationSource};
use stelae::{
    codec::{close, fixed, open, uint},
    frame::{self, CanonicalCbor},
};

use crate::Error;

pub const KIND: &str = "archive-context-committee";
pub const DIMENSION: &str = "committee_authorizations";
pub const BACKFILL: &str = "committee_authorizations_backfill";
pub const ROLLBACK: &str = "committee_authorizations_rollback";

pub fn reset_backfill() -> RelationContext {
    RelationContext {
        version: RelationContext::VERSION,
        complete: false,
        initialized_at: 0,
    }
}

pub fn reset_rollback() -> RelationContext {
    RelationContext {
        version: RelationContext::VERSION,
        complete: true,
        initialized_at: 0,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Context(RelationContext),
    Backfill(RelationContext),
    Rollback(RelationContext),
    Witness(RelationRecord),
}

pub fn validate(record: &Record, tip: u64) -> Result<(), Error> {
    match record {
        Record::Context(context) | Record::Backfill(context) | Record::Rollback(context) => {
            if context.version != RelationContext::VERSION {
                return Err(Error::malformed(
                    KIND,
                    "The reader cannot decode this context version.",
                ));
            }
            if matches!(record, Record::Rollback(_)) && !context.complete {
                return Err(Error::malformed(
                    KIND,
                    "The committee rollback is not finished.",
                ));
            }
            if matches!(record, Record::Backfill(_))
                && !context.complete
                && context.initialized_at != 0
            {
                return Err(Error::malformed(
                    KIND,
                    "If the backfill is not finished, start it again at slot zero.",
                ));
            }
        }
        Record::Witness(record) => {
            if record.source.dimension != DIMENSION {
                return Err(Error::malformed(
                    KIND,
                    "The relation dimension is not correct.",
                ));
            }
            for (key, prefixes, name) in [
                (&record.source.subject, [0x12, 0x13], "cold credential"),
                (&record.source.object, [0x02, 0x03], "hot credential"),
            ] {
                if key.len() != 29 || !prefixes.contains(&key[0]) {
                    return Err(Error::malformed(
                        KIND,
                        format!("The CIP-129 {name} is not correct."),
                    ));
                }
            }
            if record.source.source_slot > tip {
                return Err(Error::malformed(
                    KIND,
                    "The source slot is more than the slot at the snapshot position.",
                ));
            }
        }
    }
    Ok(())
}

pub fn encode(record: &Record) -> Result<CanonicalCbor, Error> {
    validate(record, u64::MAX)?;
    Ok(frame::encode(|e| {
        match record {
            Record::Context(context) | Record::Backfill(context) | Record::Rollback(context) => {
                let tag = match record {
                    Record::Context(_) => 0,
                    Record::Backfill(_) => 1,
                    Record::Rollback(_) => 2,
                    Record::Witness(_) => unreachable!(),
                };
                e.array(4)?
                    .u64(tag)?
                    .u16(context.version)?
                    .bool(context.complete)?
                    .u64(context.initialized_at)?;
            }
            Record::Witness(record) => {
                e.array(6)?
                    .u64(3)?
                    .bytes(&record.source.subject)?
                    .bytes(&record.source.object)?
                    .u64(record.source.source_slot)?;
                match record.observed_at {
                    Some(slot) => {
                        e.u64(slot)?;
                    }
                    None => {
                        e.null()?;
                    }
                }
                e.bool(record.active)?;
            }
        }
        Ok(())
    })?)
}

pub fn decode(bytes: &[u8]) -> Result<Record, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);
    let length = decoder
        .array()
        .map_err(|e| Error::malformed(KIND, e.to_string()))?;
    let tag = uint(KIND, "tag", &mut decoder)?;
    let boolean = |decoder: &mut minicbor::Decoder<'_>| {
        decoder
            .bool()
            .map_err(|e| Error::malformed(KIND, e.to_string()))
    };
    let record = match (tag, length) {
        (0..=2, Some(4)) => {
            let context = RelationContext {
                version: u16::try_from(uint(KIND, "version", &mut decoder)?)
                    .map_err(|_| Error::malformed(KIND, "The version is too large for u16."))?,
                complete: boolean(&mut decoder)?,
                initialized_at: uint(KIND, "initialized_at", &mut decoder)?,
            };
            match tag {
                0 => Record::Context(context),
                1 => Record::Backfill(context),
                _ => Record::Rollback(context),
            }
        }
        (3, Some(6)) => {
            let subject: [u8; 29] = fixed(KIND, "subject", &mut decoder)?;
            let object: [u8; 29] = fixed(KIND, "object", &mut decoder)?;
            let source_slot = uint(KIND, "source_slot", &mut decoder)?;
            let observed_at = if decoder
                .datatype()
                .map_err(|e| Error::malformed(KIND, e.to_string()))?
                == minicbor::data::Type::Null
            {
                decoder
                    .null()
                    .map_err(|e| Error::malformed(KIND, e.to_string()))?;
                None
            } else {
                Some(uint(KIND, "observed_at", &mut decoder)?)
            };
            Record::Witness(RelationRecord {
                source: RelationSource {
                    dimension: DIMENSION.to_owned(),
                    subject: subject.to_vec(),
                    object: object.to_vec(),
                    source_slot,
                },
                observed_at,
                active: boolean(&mut decoder)?,
            })
        }
        _ => {
            return Err(Error::malformed(
                KIND,
                "The tag is unknown or the length is wrong.",
            ))
        }
    };
    close(KIND, &decoder, bytes)?;
    validate(&record, u64::MAX)?;
    Ok(record)
}

#[derive(Debug, Default)]
pub struct OrderCheck {
    metadata: u8,
    last: Option<RelationSource>,
}

impl OrderCheck {
    pub fn check(&mut self, record: &Record, tip: u64) -> Result<(), Error> {
        validate(record, tip)?;
        match record {
            Record::Context(_) | Record::Backfill(_) | Record::Rollback(_) => {
                let expected = match record {
                    Record::Context(_) => 0,
                    Record::Backfill(_) => 1,
                    Record::Rollback(_) => 2,
                    Record::Witness(_) => unreachable!(),
                };
                if self.metadata != expected || self.last.is_some() {
                    return Err(Error::out_of_order(
                        KIND,
                        "The metadata occurs more than once or is not in the correct order.",
                    ));
                }
                self.metadata += 1;
            }
            Record::Witness(record) => {
                if self.metadata != 3 {
                    return Err(Error::out_of_order(
                        KIND,
                        "The witness occurs before the context metadata.",
                    ));
                }
                if self
                    .last
                    .as_ref()
                    .is_some_and(|last| record.source <= *last)
                {
                    return Err(Error::out_of_order(
                        KIND,
                        "The witness occurs more than once or is not in increasing order.",
                    ));
                }
                self.last = Some(record.source.clone());
            }
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<(), Error> {
        if self.metadata != 3 {
            return Err(Error::malformed(
                KIND,
                "The context metadata is not all present.",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitteeScope {
    pub network_magic: u64,
    pub point: dolos_core::ChainPoint,
}

impl crate::Scope for CommitteeScope {
    fn header(&self) -> Result<CanonicalCbor, Error> {
        let hash = self
            .point
            .hash()
            .ok_or_else(|| Error::UnanchoredPoint(self.point.to_string()))?;
        Ok(frame::encode(|e| {
            e.array(3)?
                .u64(self.network_magic)?
                .u64(self.point.slot())?
                .bytes(hash.as_ref())?;
            Ok(())
        })?)
    }

    fn descriptor(&self) -> serde_json::Value {
        serde_json::json!({
            "required": true,
            "point": {"slot": self.point.slot(), "hash": self.point.hash().map(hex::encode)},
        })
    }

    fn kinds(&self) -> &'static [&'static str] {
        &[KIND]
    }

    fn shape(&self) -> &'static str {
        "a scope for the committee context in the archive"
    }
}

pub fn check_header(bytes: &[u8], point: &serde_json::Value) -> Result<(), Error> {
    let mut decoder = minicbor::Decoder::new(bytes);
    open(KIND, &mut decoder, 3)?;
    uint(KIND, "network_magic", &mut decoder)?;
    let slot = uint(KIND, "slot", &mut decoder)?;
    let hash: [u8; 32] = fixed(KIND, "hash", &mut decoder)?;
    if point != &serde_json::json!({"slot": slot, "hash": hex::encode(hash)}) {
        return Err(Error::malformed(
            KIND,
            "The header position is not the same as the snapshot position.",
        ));
    }
    close(KIND, &decoder, bytes)?;
    Ok(())
}
