//! What the cache commands know of each namespace's entries.
//!
//! A [`CacheUse`] carries the chain knowledge `warm` and `verify` need, which
//! stays out of `dolos-core`: how an operator names a key, how to build an
//! entry without the cache, and how far an entry reaches.

use dolos_cardano::{
    eras::load_era_summary,
    periods::{
        assets::{
            assets_watermark, build_asset_entry, first_mint_slot, sealed_subjects, AssetEntry,
            ASSETS,
        },
        stats::{
            build_totals_entry, totals_candidates, totals_watermark, TotalsEntry, TotalsKey, TOTALS,
        },
    },
    ChainSummary,
};
use dolos_core::{
    cache::{Cost, FillError},
    CacheEntry, Domain, StateStore as _,
};

/// How far an entry reaches, in its use's unit (an epoch for `totals`).
pub type Watermark = u64;

/// One namespace's entries, as the commands build and check them.
pub trait CacheUse<D: Domain>: Send + Sync + 'static {
    fn namespace(&self) -> &'static str;

    /// The namespace's own admission threshold, which the config may override.
    fn admission(&self) -> Cost;

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

    fn admission(&self) -> Cost {
        TOTALS.admission
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

/// The fold of an asset's mints and burns, keyed by its subject in hex: its
/// policy id followed by its name, as `/assets/{subject}` takes it.
pub struct Assets {
    chain: ChainSummary,
}

impl Assets {
    pub fn new<D: Domain>(domain: &D) -> Result<Self, FillError> {
        let chain = load_era_summary::<D>(domain.state())?;
        Ok(Self { chain })
    }

    /// The epoch of the node's tip, none before its first block.
    fn tip<D: Domain>(&self, domain: &D) -> Result<Option<u64>, FillError> {
        let Some(tip) = domain.state().read_cursor()? else {
            return Ok(None);
        };

        Ok(Some(self.chain.slot_epoch(tip.slot()).0))
    }
}

impl<D: Domain> CacheUse<D> for Assets {
    fn namespace(&self) -> &'static str {
        ASSETS.name
    }

    fn admission(&self) -> Cost {
        ASSETS.admission
    }

    fn parse_key(&self, text: &str) -> Result<Vec<u8>, FillError> {
        let subject = hex::decode(text)?;

        if !(28..=60).contains(&subject.len()) {
            return Err(format!("not an asset subject, a policy id and a name: {text}").into());
        }

        Ok(subject)
    }

    fn current_watermark(&self, domain: &D) -> Result<Option<Watermark>, FillError> {
        let Some(tip) = self.tip(domain)? else {
            return Ok(None);
        };

        Ok(assets_watermark(domain, &self.chain, tip))
    }

    fn build(&self, domain: &D, key: &[u8], watermark: Watermark) -> Result<CacheEntry, FillError> {
        let tip = self.tip(domain)?.ok_or("the node holds no block")?;

        let first = first_mint_slot(domain, &self.chain, tip, key)?
            .ok_or_else(|| format!("asset {} was never minted", hex::encode(key)))?;

        let (start, _) = self.chain.slot_epoch(first);

        Ok(build_asset_entry(
            domain,
            &self.chain,
            key,
            start,
            watermark,
        )?)
    }

    fn watermark(&self, value: &[u8]) -> Option<Watermark> {
        AssetEntry::watermark_of(value)
    }

    fn candidates(&self, domain: &D) -> Result<Vec<Vec<u8>>, FillError> {
        Ok(sealed_subjects(domain)?)
    }
}
