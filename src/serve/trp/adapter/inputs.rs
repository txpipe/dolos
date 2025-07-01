use std::collections::{HashMap, HashSet};
use tracing::debug;

use tx3_cardano::pallas::ledger::traverse::{Era, MultiEraOutput};

use dolos_core::{Domain, EraCbor, StateStore as _, TxoRef};

enum Subset {
    All,
    Specific(HashSet<TxoRef>),
}

impl Subset {
    #[allow(dead_code)]
    fn union(a: Self, b: Self) -> Self {
        match (a, b) {
            (Self::All, _) => Self::All,
            (_, Self::All) => Self::All,
            (Self::Specific(s1), Self::Specific(s2)) => {
                Self::Specific(s1.union(&s2).cloned().collect())
            }
        }
    }

    fn intersection(a: Self, b: Self) -> Self {
        match (a, b) {
            (Self::All, x) => x,
            (x, Self::All) => x,
            (Self::Specific(s1), Self::Specific(s2)) => {
                Self::Specific(s1.intersection(&s2).cloned().collect())
            }
        }
    }

    fn intersection_of_all<const N: usize>(subsets: [Self; N]) -> Self {
        let mut result = Subset::All;

        for subset in subsets {
            result = Self::intersection(result, subset);
        }

        result
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::All => false,
            Self::Specific(s) => s.is_empty(),
        }
    }
}

impl From<HashSet<TxoRef>> for Subset {
    fn from(value: HashSet<TxoRef>) -> Self {
        Self::Specific(value)
    }
}

fn utxo_includes_custom_asset(
    utxo: &MultiEraOutput<'_>,
    expected: &tx3_lang::ir::AssetExpr,
) -> Result<bool, tx3_cardano::Error> {
    let policy = tx3_cardano::coercion::expr_into_bytes(&expected.policy)?;

    let value = utxo.value();

    let mas = value.assets();

    let ma = mas.iter().find(|ma| ma.policy() == policy.as_slice());

    let ma = match ma {
        Some(ma) => ma,
        None => return Ok(false),
    };

    let name = tx3_cardano::coercion::expr_into_bytes(&expected.asset_name)?;

    let assets = ma.assets();

    let asset = assets.iter().find(|a| a.name() == name.as_slice());

    let asset = match asset {
        Some(asset) => asset,
        None => return Ok(false),
    };

    let amount = tx3_cardano::coercion::expr_into_number(&expected.amount)?;

    Ok(asset.output_coin().unwrap_or_default() as i128 >= amount)
}

fn utxo_includes_lovelace_amount(
    utxo: &MultiEraOutput<'_>,
    amount: &tx3_lang::ir::Expression,
) -> Result<bool, tx3_cardano::Error> {
    let expected = tx3_cardano::coercion::expr_into_number(amount)?;
    Ok(utxo.value().coin() as i128 >= expected)
}

fn utxo_matches_min_amount(
    utxo: &MultiEraOutput<'_>,
    min_amount: &tx3_lang::ir::Expression,
) -> Result<bool, tx3_cardano::Error> {
    let expected = tx3_cardano::coercion::expr_into_assets(min_amount)?;

    let lovelace_ok = expected
        .iter()
        .filter(|x| x.policy.is_none())
        .map(|asset| utxo_includes_lovelace_amount(utxo, &asset.amount))
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .all(|x| *x);

    let custom_ok = expected
        .iter()
        .filter(|x| !x.policy.is_none())
        .map(|asset| utxo_includes_custom_asset(utxo, asset))
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .all(|x| *x);

    Ok(lovelace_ok && custom_ok)
}

fn utxo_matches(
    utxo: &MultiEraOutput<'_>,
    criteria: &tx3_lang::ir::InputQuery,
) -> Result<bool, tx3_cardano::Error> {
    let min_amount_check = if let min_amount = &criteria.min_amount {
        utxo_matches_min_amount(utxo, min_amount)?
    } else {
        // if there is no min amount requirement, then the utxo matches
        true
    };

    Ok(min_amount_check)
}

fn pick_first_utxo_match(
    utxos: HashMap<TxoRef, EraCbor>,
    criteria: &tx3_lang::ir::InputQuery,
) -> Result<Option<tx3_lang::Utxo>, tx3_cardano::Error> {
    for (txoref, EraCbor(era, cbor)) in utxos {
        let era = Era::try_from(era).expect("era out of range");
        let parsed = MultiEraOutput::decode(era, &cbor)
            .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

        if utxo_matches(&parsed, criteria)? {
            let mapped = super::utxos::into_tx3_utxo(&txoref, &parsed)?;
            return Ok(Some(mapped));
        }
    }

    Ok(None)
}

const MAX_SEARCH_SPACE_SIZE: usize = 50;

struct InputSelector<'a, D: Domain> {
    ledger: &'a D::State,
    network: tx3_cardano::Network,
}

impl<'a, D: Domain> InputSelector<'a, D> {
    pub fn new(ledger: &'a D::State, network: tx3_cardano::Network) -> Self {
        Self { ledger, network }
    }

    fn narrow_by_address(
        &self,
        expr: &tx3_lang::ir::Expression,
    ) -> Result<Subset, tx3_cardano::Error> {
        let address = tx3_cardano::coercion::expr_into_address(expr, self.network)?.to_vec();

        let utxos = self
            .ledger
            .get_utxo_by_address(&address)
            .map_err(|e| tx3_cardano::Error::LedgerInternalError(e.to_string()))?;

        Ok(Subset::Specific(utxos.into_iter().collect()))
    }

    fn narrow_by_asset_presence(
        &self,
        expr: &tx3_lang::ir::AssetExpr,
    ) -> Result<Subset, tx3_cardano::Error> {
        let amount = tx3_cardano::coercion::expr_into_number(&expr.amount)?;

        // skip filtering if required amount is 0 since it's not adding any constraints
        if amount == 0 {
            return Ok(Subset::All);
        }

        // skip filtering lovelace since it's not an custom asset
        if expr.policy.is_none() {
            return Ok(Subset::All);
        }

        let policy_bytes = tx3_cardano::coercion::expr_into_bytes(&expr.policy)?;
        let name_bytes = tx3_cardano::coercion::expr_into_bytes(&expr.asset_name)?;

        let subject = [policy_bytes.as_slice(), name_bytes.as_slice()].concat();

        let utxos = self
            .ledger
            .get_utxo_by_asset(&subject)
            .map_err(|e| tx3_cardano::Error::LedgerInternalError(e.to_string()))?;

        Ok(Subset::Specific(utxos.into_iter().collect()))
    }

    fn narrow_by_multi_asset_presence(
        &self,
        expr: &tx3_lang::ir::Expression,
    ) -> Result<Subset, tx3_cardano::Error> {
        let assets = tx3_cardano::coercion::expr_into_assets(expr)?;

        let mut matches = Subset::All;

        for asset in assets {
            let next = self.narrow_by_asset_presence(&asset)?;
            matches = Subset::intersection(matches, next);
        }

        Ok(matches)
    }

    fn narrow_by_ref(&self, expr: &tx3_lang::ir::Expression) -> Result<Subset, tx3_cardano::Error> {
        let refs = tx3_cardano::coercion::expr_into_utxo_refs(expr)?;

        let mapped = refs
            .iter()
            .map(|r| TxoRef(r.txid.as_slice().into(), r.index))
            .collect();

        Ok(Subset::Specific(mapped))
    }

    fn narrow_search_space(
        &self,
        criteria: &tx3_lang::ir::InputQuery,
    ) -> Result<Subset, tx3_cardano::Error> {
        let matching_address = if let address = &criteria.address {
            self.narrow_by_address(address)?
        } else {
            Subset::All
        };

        if matching_address.is_empty() {
            debug!("matching address is empty");
        }

        let matching_assets = if let min_amount = &criteria.min_amount {
            self.narrow_by_multi_asset_presence(min_amount)?
        } else {
            Subset::All
        };

        if matching_assets.is_empty() {
            debug!("matching assets is empty");
        }

        let matching_refs = if let refs = &criteria.r#ref {
            self.narrow_by_ref(refs)?
        } else {
            Subset::All
        };

        if matching_refs.is_empty() {
            debug!("matching refs is empty");
        }

        Ok(Subset::intersection_of_all([
            matching_address,
            matching_assets,
            matching_refs,
        ]))
    }

    pub fn select(
        &self,
        criteria: &tx3_lang::ir::InputQuery,
        resolve_context: &tx3_cardano::ResolveContext,
    ) -> Result<tx3_lang::UtxoSet, tx3_cardano::Error> {
        let search_space = self.narrow_search_space(criteria)?;

        let refs = match search_space {
            Subset::Specific(refs) if refs.len() <= MAX_SEARCH_SPACE_SIZE => refs,
            Subset::Specific(_) => return Err(tx3_cardano::Error::InputQueryTooBroad),
            Subset::All => return Err(tx3_cardano::Error::InputQueryTooBroad),
        };

        let refs = refs
            .into_iter()
            .filter(|TxoRef(hash, index)| {
                let utxo_ref = tx3_lang::UtxoRef {
                    txid: hash.to_vec(),
                    index: *index,
                };
                !resolve_context.ignore.contains(&utxo_ref)
            })
            .collect::<Vec<_>>();

        let utxos = self
            .ledger
            .get_utxos(refs.into_iter().collect())
            .map_err(|e| tx3_cardano::Error::LedgerInternalError(e.to_string()))?;

        let matched = pick_first_utxo_match(utxos, criteria)?;

        if let Some(utxo) = matched {
            Ok(vec![utxo].into_iter().collect())
        } else {
            Ok(tx3_lang::UtxoSet::new())
        }
    }
}

pub fn resolve<D: Domain>(
    ledger: &D::State,
    network: tx3_cardano::Network,
    criteria: &tx3_lang::ir::InputQuery,
    resolve_context: &tx3_cardano::ResolveContext,
) -> Result<tx3_lang::UtxoSet, tx3_cardano::Error> {
    InputSelector::<D>::new(ledger, network).select(criteria, &resolve_context)
}
