//! A zstd dictionary under evaluation, with the content identity its
//! provenance is recorded by.

use std::fmt;
use std::sync::Arc;

use sha2::{Digest, Sha256};

/// Content identity of a dictionary: the SHA-256 of its bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DictionaryId([u8; 32]);

impl DictionaryId {
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for DictionaryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for DictionaryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DictionaryId({self})")
    }
}

/// A zstd dictionary and its identity. Cheap to clone.
#[derive(Clone)]
pub struct Dictionary {
    bytes: Arc<[u8]>,
    id: DictionaryId,
}

impl Dictionary {
    /// Adopt `bytes` as a dictionary. Any bytes are accepted: zstd treats a
    /// buffer without its dictionary magic as raw content.
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        let bytes = bytes.into();
        let id = DictionaryId::of(&bytes);
        Self { bytes, id }
    }

    /// The dictionary the production store compresses with.
    pub fn bundled() -> Self {
        Self::new(dolos_flatfiles::BUNDLED_DICTIONARY)
    }

    pub fn id(&self) -> DictionaryId {
        self.id
    }

    /// The id zstd stamps into frames using this dictionary; `0` for raw
    /// content.
    pub fn zstd_id(&self) -> u32 {
        zstd::zstd_safe::get_dict_id_from_dict(&self.bytes).map_or(0, |id| id.get())
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for Dictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dictionary")
            .field("id", &self.id)
            .field("len", &self.bytes.len())
            .finish()
    }
}
