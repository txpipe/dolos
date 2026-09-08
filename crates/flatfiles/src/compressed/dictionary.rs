//! Dictionaries as durable storage: content-addressed, immutable, supplied by
//! the caller rather than discovered on disk.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use zstd::zstd_safe::DDict;

/// Content identity of a dictionary: the SHA-256 of its bytes.
///
/// This is what a segment records and what a reader asks a
/// [`DictionarySource`] for. It never changes for a given dictionary, unlike
/// the 32-bit id zstd derives for trained dictionaries (and leaves at zero for
/// raw-content ones).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DictionaryId([u8; 32]);

impl DictionaryId {
    /// Identity of `bytes` as a dictionary.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Parse the lowercase or uppercase hex form `Display` produces.
    pub fn from_hex(hex: &str) -> Option<Self> {
        let hex = hex.as_bytes();
        if hex.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, pair) in hex.chunks_exact(2).enumerate() {
            let nibble = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
            out[i] = nibble(pair[0])? << 4 | nibble(pair[1])?;
        }
        Some(Self(out))
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
    zstd_id: u32,
}

impl Dictionary {
    /// Adopt `bytes` as a dictionary. Any bytes are accepted: zstd treats a
    /// buffer without its dictionary magic as raw content.
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        let bytes = bytes.into();
        let id = DictionaryId::of(&bytes);
        let zstd_id = zstd::zstd_safe::get_dict_id_from_dict(&bytes).map_or(0, |id| id.get());
        Self { bytes, id, zstd_id }
    }

    /// Adopt `bytes` only if they hash to `expected`.
    pub fn verified(bytes: impl Into<Arc<[u8]>>, expected: DictionaryId) -> io::Result<Self> {
        let dictionary = Self::new(bytes);
        if dictionary.id != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "dictionary content hashes to {} but {expected} was expected",
                    dictionary.id
                ),
            ));
        }
        Ok(dictionary)
    }

    pub fn id(&self) -> DictionaryId {
        self.id
    }

    /// The id zstd stamps into frames using this dictionary; `0` for raw
    /// content.
    pub fn zstd_id(&self) -> u32 {
        self.zstd_id
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Digest the dictionary once for repeated decoding, returning an error
    /// if zstd cannot prepare its contents.
    pub fn prepare(&self) -> io::Result<PreparedDictionary> {
        let decoder = DDict::try_create(&self.bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cannot prepare dictionary {}: invalid dictionary or allocation failure",
                    self.id
                ),
            )
        })?;
        Ok(PreparedDictionary {
            dictionary: self.clone(),
            decoder,
        })
    }
}

impl fmt::Debug for Dictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dictionary")
            .field("id", &self.id)
            .field("zstd_id", &self.zstd_id)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// A dictionary digested into zstd's decoder form.
pub struct PreparedDictionary {
    dictionary: Dictionary,
    decoder: DDict<'static>,
}

impl PreparedDictionary {
    pub fn dictionary(&self) -> &Dictionary {
        &self.dictionary
    }

    pub fn id(&self) -> DictionaryId {
        self.dictionary.id
    }

    pub fn decoder(&self) -> &DDict<'static> {
        &self.decoder
    }

    /// Retained bytes, including the source and zstd's copied decoder data.
    /// Cache bookkeeping and Arc headers are bounded separately by entries.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.dictionary.bytes.len())
            .saturating_add(self.decoder.sizeof())
    }
}

/// Where a reader obtains the dictionary a segment names.
///
/// The codec never looks for dictionaries on its own: the store decides where
/// they live and hands them over through this trait. Returning `Ok(None)`
/// makes the read fail with a `NotFound` error naming the identity.
pub trait DictionarySource: Send + Sync {
    fn dictionary(&self, id: &DictionaryId) -> io::Result<Option<Dictionary>>;
}

/// A source that holds no dictionaries.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoDictionaries;

impl DictionarySource for NoDictionaries {
    fn dictionary(&self, _id: &DictionaryId) -> io::Result<Option<Dictionary>> {
        Ok(None)
    }
}

/// An in-memory set of dictionaries keyed by identity.
#[derive(Debug, Default, Clone)]
pub struct DictionarySet {
    dictionaries: HashMap<DictionaryId, Dictionary>,
}

impl DictionarySet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, dictionary: Dictionary) -> DictionaryId {
        let id = dictionary.id();
        self.dictionaries.insert(id, dictionary);
        id
    }

    pub fn with(mut self, dictionary: Dictionary) -> Self {
        self.insert(dictionary);
        self
    }
}

impl DictionarySource for DictionarySet {
    fn dictionary(&self, id: &DictionaryId) -> io::Result<Option<Dictionary>> {
        Ok(self.dictionaries.get(id).cloned())
    }
}

impl<T: DictionarySource + ?Sized> DictionarySource for &T {
    fn dictionary(&self, id: &DictionaryId) -> io::Result<Option<Dictionary>> {
        (**self).dictionary(id)
    }
}

impl<T: DictionarySource + ?Sized> DictionarySource for Arc<T> {
    fn dictionary(&self, id: &DictionaryId) -> io::Result<Option<Dictionary>> {
        (**self).dictionary(id)
    }
}

/// Dictionaries installed as files in one directory, named by identity:
/// `<dir>/<64 hex digits>.dict`.
///
/// This is where a store keeps the dictionaries its segments name. A file is
/// read on demand and its content hash checked against the name, so a
/// renamed or edited file is refused rather than trusted; a missing file is
/// `Ok(None)`, which a reader reports as a missing dictionary.
#[derive(Debug, Clone)]
pub struct DictionaryDir {
    dir: PathBuf,
}

impl DictionaryDir {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Where `id` is expected to live.
    pub fn file(&self, id: &DictionaryId) -> PathBuf {
        self.dir.join(format!("{id}.dict"))
    }

    /// Write `dictionary` under its identity, durably. Installing the same
    /// dictionary twice is a no-op; a dictionary is never overwritten with
    /// different bytes because its name is its content hash.
    pub fn install(&self, dictionary: &Dictionary) -> io::Result<PathBuf> {
        fs::create_dir_all(&self.dir)?;
        let target = self.file(&dictionary.id());
        if let Some(existing) = self.dictionary(&dictionary.id())? {
            debug_assert_eq!(existing.id(), dictionary.id());
            return Ok(target);
        }
        let staging = self.dir.join(format!("{}.dict.tmp", dictionary.id()));
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&staging)?;
        file.write_all(dictionary.bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&staging, &target)?;
        if cfg!(unix) {
            fs::File::open(&self.dir)?.sync_all()?;
        }
        Ok(target)
    }
}

impl DictionarySource for DictionaryDir {
    fn dictionary(&self, id: &DictionaryId) -> io::Result<Option<Dictionary>> {
        let path = self.file(id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("cannot read dictionary {}: {e}", path.display()),
                ))
            }
        };
        Dictionary::verified(bytes, *id)
            .map(Some)
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))
    }
}

/// The dictionary shipped with this crate, trained offline on a recorded
/// sample of Cardano blocks; `dictionary/PROVENANCE.md` beside the crate
/// says which. Every build carries it, so a fresh instance needs no corpus
/// or download.
pub fn bundled_dictionary() -> Dictionary {
    Dictionary::new(BUNDLED_DICTIONARY)
}

static BUNDLED_DICTIONARY: &[u8] = include_bytes!("../../dictionary/cardano.dict");
