//! What the cache commands know of each namespace's entries.
//!
//! A [`CacheUse`] carries the chain knowledge `warm` and `verify` need, which
//! stays out of `dolos-core`: how an operator names a key, how to build an
//! entry without the cache, and how far an entry reaches.

use dolos_cardano::{
    eras::load_era_summary,
    periods::stats::{
        build_totals_entry, totals_candidates, totals_watermark, TotalsEntry, TotalsKey, TOTALS,
    },
    ChainSummary,
};
use dolos_core::{cache::FillError, CacheEntry, Domain, StateStore as _};

/// How far an entry reaches, in its use's unit (an epoch for `totals`).
pub type Watermark = u64;

/// One namespace's entries, as the commands build and check them.
pub trait CacheUse<D: Domain>: Send + Sync + 'static {
    fn namespace(&self) -> &'static str;

    /// A key from its text form, one line of a `warm --keys` file.
    fn parse_key(&self, text: &str) -> Result<Vec<u8>, FillError>;

    /// The watermark a build reaches now, none while nothing can be cached.
    fn current_watermark(&self, domain: &D) -> Result<Option<Watermark>, FillError>;

    /// The entry of `key` up to `watermark`, built without the cache.
    fn build(&self, domain: &D, key: &[u8], watermark: Watermark) -> Result<CacheEntry, FillError>;

    /// The watermark of a stored entry.
    fn watermark(&self, value: &[u8]) -> Option<Watermark>;

    /// The keys worth warming.
    fn candidates(&self, domain: &D) -> Result<Vec<Vec<u8>>, FillError>;
}

/// The lifetime totals of a credential, keyed by its text form: an address, a
/// payment credential or a stake address.
pub struct Totals {
    chain: ChainSummary,
}

impl Totals {
    pub fn new<D: Domain>(domain: &D) -> Result<Self, FillError> {
        let chain = load_era_summary::<D>(domain.state())?;
        Ok(Self { chain })
    }
}

impl<D: Domain> CacheUse<D> for Totals {
    fn namespace(&self) -> &'static str {
        TOTALS.name
    }

    fn parse_key(&self, text: &str) -> Result<Vec<u8>, FillError> {
        Ok(TotalsKey::parse(text, &self.chain)?.to_bytes())
    }

    fn current_watermark(&self, domain: &D) -> Result<Option<Watermark>, FillError> {
        let Some(tip) = domain.state().read_cursor()? else {
            return Ok(None);
        };

        let (tip, _) = self.chain.slot_epoch(tip.slot());

        Ok(totals_watermark(domain, &self.chain, tip)?)
    }

    fn build(&self, domain: &D, key: &[u8], watermark: Watermark) -> Result<CacheEntry, FillError> {
        let key = TotalsKey::from_bytes(key)?;
        Ok(build_totals_entry(domain, &self.chain, &key, watermark)?)
    }

    fn watermark(&self, value: &[u8]) -> Option<Watermark> {
        TotalsEntry::watermark_of(value)
    }

    fn candidates(&self, domain: &D) -> Result<Vec<Vec<u8>>, FillError> {
        let keys = totals_candidates(domain, &self.chain)?;
        Ok(keys.iter().map(TotalsKey::to_bytes).collect())
    }
}
