use std::ops::Bound;

use dolos_core::{ArchiveError, RelationContext, RelationRecord, RelationSource};
use fjall::{Keyspace, Readable, Snapshot};

use super::fjall_err;

fn malformed(message: &str) -> ArchiveError {
    ArchiveError::InternalError(format!("The archive relation has {message}."))
}

fn component(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        if byte == 0 {
            out.extend_from_slice(&[0, 255]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

fn read_component(input: &mut &[u8]) -> Result<Vec<u8>, ArchiveError> {
    let mut out = Vec::new();
    while let Some((&byte, rest)) = input.split_first() {
        *input = rest;
        if byte != 0 {
            out.push(byte);
            continue;
        }
        let (&escape, rest) = input
            .split_first()
            .ok_or_else(|| malformed("an error in a key escape"))?;
        *input = rest;
        match escape {
            0 => return Ok(out),
            255 => out.push(0),
            _ => return Err(malformed("an error in a key escape")),
        }
    }
    Err(malformed("no key component terminator"))
}

pub(super) fn prefix(dimension: &str, subject: Option<&[u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    component(&mut out, dimension.as_bytes());
    if let Some(subject) = subject {
        component(&mut out, subject);
    }
    out
}

pub(super) fn key(source: &RelationSource) -> Result<Vec<u8>, ArchiveError> {
    let mut out = prefix(&source.dimension, Some(&source.subject));
    component(&mut out, &source.object);
    out.extend_from_slice(&source.source_slot.to_be_bytes());
    if out.len() > u16::MAX as usize {
        return Err(malformed("a key with more than 65535 bytes"));
    }
    Ok(out)
}

pub(super) fn source(mut key: &[u8]) -> Result<RelationSource, ArchiveError> {
    let dimension = String::from_utf8(read_component(&mut key)?)
        .map_err(|_| malformed("a dimension with a UTF-8 error"))?;
    let subject = read_component(&mut key)?;
    let object = read_component(&mut key)?;
    let slot = key
        .try_into()
        .map_err(|_| malformed("a source slot with an incorrect length"))?;
    Ok(RelationSource {
        dimension,
        subject,
        object,
        source_slot: u64::from_be_bytes(slot),
    })
}

pub(super) fn value(record: &RelationRecord) -> Vec<u8> {
    let mut out = vec![
        1,
        u8::from(record.active),
        u8::from(record.observed_at.is_some()),
    ];
    if let Some(slot) = record.observed_at {
        out.extend_from_slice(&slot.to_be_bytes());
    }
    out
}

pub(super) fn record(key: &[u8], value: &[u8]) -> Result<RelationRecord, ArchiveError> {
    let source = source(key)?;
    let (active, observed_at) = match value {
        [1, active @ 0..=1, 0] => (*active == 1, None),
        [1, active @ 0..=1, 1, slot @ ..] if slot.len() == 8 => (
            *active == 1,
            Some(u64::from_be_bytes(slot.try_into().unwrap())),
        ),
        _ => return Err(malformed("a value format error")),
    };
    Ok(RelationRecord {
        source,
        observed_at,
        active,
    })
}

pub(super) fn context_value(context: &RelationContext) -> Result<Vec<u8>, ArchiveError> {
    if context.version != RelationContext::VERSION {
        return Err(ArchiveError::InvalidStoreVersion);
    }
    let mut out = context.version.to_be_bytes().to_vec();
    out.push(u8::from(context.complete));
    out.extend_from_slice(&context.initialized_at.to_be_bytes());
    Ok(out)
}

pub(super) fn context(value: &[u8]) -> Result<RelationContext, ArchiveError> {
    if value.len() != 11 || value[2] > 1 {
        return Err(malformed("a context format error"));
    }
    let version = u16::from_be_bytes(value[..2].try_into().unwrap());
    if version != RelationContext::VERSION {
        return Err(ArchiveError::InvalidStoreVersion);
    }
    Ok(RelationContext {
        version,
        complete: value[2] == 1,
        initialized_at: u64::from_be_bytes(value[3..].try_into().unwrap()),
    })
}

pub(super) struct RelationIter {
    inner: fjall::Iter,
    done: bool,
}

impl RelationIter {
    pub(super) fn from(
        snapshot: &Snapshot,
        keyspace: &Keyspace,
        dimension: &str,
        after: Option<&RelationSource>,
    ) -> Result<Self, ArchiveError> {
        if after.is_some_and(|source| source.dimension != dimension) {
            return Err(ArchiveError::InternalError(
                "The dimension of the relation cursor is not the requested dimension.".into(),
            ));
        }
        let prefix = prefix(dimension, None);
        let lower = match after {
            Some(source) => Bound::Excluded(key(source)?),
            None => Bound::Included(prefix.clone()),
        };
        let mut upper = prefix;
        let last = upper.last_mut().unwrap();
        *last += 1;
        Ok(Self {
            inner: snapshot.range(keyspace, (lower, Bound::Excluded(upper))),
            done: false,
        })
    }

    pub(super) fn new(snapshot: &Snapshot, keyspace: &Keyspace, prefix: &[u8]) -> Self {
        Self {
            inner: snapshot.prefix(keyspace, prefix),
            done: false,
        }
    }
}

impl Iterator for RelationIter {
    type Item = Result<RelationRecord, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let Some(guard) = self.inner.next() else {
            self.done = true;
            return None;
        };
        let result = guard
            .into_inner()
            .map_err(fjall_err)
            .and_then(|(key, value)| record(&key, &value));
        if result.is_err() {
            self.done = true;
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relation_encoding_is_lossless_and_ordered() {
        let subjects = [
            vec![],
            vec![0],
            vec![0, 0],
            vec![0, 255],
            vec![1],
            vec![255],
        ];
        let sources: Vec<_> = subjects
            .into_iter()
            .map(|subject| RelationSource {
                dimension: "dim\0ension".into(),
                subject,
                object: vec![255, 0, 1],
                source_slot: u64::MAX,
            })
            .collect();
        let keys: Vec<_> = sources.iter().map(|s| key(s).unwrap()).collect();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for (expected, key) in sources.iter().zip(keys) {
            assert_eq!(&source(&key).unwrap(), expected);
            let expected = RelationRecord {
                source: expected.clone(),
                observed_at: Some(0),
                active: true,
            };
            assert_eq!(record(&key, &value(&expected)).unwrap(), expected);
        }
    }

    #[test]
    fn malformed_relations_are_errors() {
        for key in [&[][..], &[0][..], &[0, 1][..], &[255, 0, 0, 0, 0, 0, 0][..]] {
            assert!(source(key).is_err());
        }
        let source = RelationSource {
            dimension: "d".into(),
            subject: vec![],
            object: vec![],
            source_slot: 1,
        };
        let key = key(&source).unwrap();
        for value in [
            &[][..],
            &[1, 2, 0][..],
            &[1, 0, 2][..],
            &[1, 0, 1][..],
            &[1, 0, 0, 0][..],
            &[2, 0, 0][..],
        ] {
            assert!(record(&key, value).is_err());
        }
        assert!(context(&[0; 11]).is_err());
        assert!(context(&[0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
        let huge = RelationSource {
            object: vec![0; 65535],
            ..source
        };
        assert!(super::key(&huge).is_err());
    }
}
