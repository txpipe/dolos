use std::collections::{HashMap, HashSet};
use tracing::debug;

use tx3_cardano::pallas::ledger::traverse::{Era, MultiEraOutput};

use dolos_core::{EraCbor, StateStore, TxoRef};

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
    let min_amount_check = if let Some(min_amount) = &criteria.min_amount.as_option() {
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

struct InputSelector<'a, S: StateStore> {
    ledger: &'a S,
    network: tx3_cardano::Network,
}

impl<'a, S: StateStore> InputSelector<'a, S> {
    pub fn new(ledger: &'a S, network: tx3_cardano::Network) -> Self {
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
        let matching_address = if let Some(address) = &criteria.address.as_option() {
            self.narrow_by_address(address)?
        } else {
            Subset::All
        };

        if matching_address.is_empty() {
            debug!("matching address is empty");
        }

        let matching_assets = if let Some(min_amount) = &criteria.min_amount.as_option() {
            self.narrow_by_multi_asset_presence(min_amount)?
        } else {
            Subset::All
        };

        if matching_assets.is_empty() {
            debug!("matching assets is empty");
        }

        let matching_refs = if let Some(refs) = &criteria.r#ref.as_option() {
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
        resolve_context: &tx3_cardano::resolve::ResolveContext,
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

        let utxos = StateStore::get_utxos(self.ledger, refs)
            .map_err(|e| tx3_cardano::Error::LedgerInternalError(e.to_string()))?;

        let matched = pick_first_utxo_match(utxos, criteria)?;

        if let Some(utxo) = matched {
            Ok(vec![utxo].into_iter().collect())
        } else {
            Ok(tx3_lang::UtxoSet::new())
        }
    }
}

pub fn resolve<S: StateStore>(
    ledger: &S,
    network: tx3_cardano::Network,
    criteria: &tx3_lang::ir::InputQuery,
    resolve_context: &tx3_cardano::resolve::ResolveContext,
) -> Result<tx3_lang::UtxoSet, tx3_cardano::Error> {
    InputSelector::<S>::new(ledger, network).select(criteria, &resolve_context)
}

#[cfg(test)]
mod tests {
    use dolos_testing::toy_domain::seed_random_memory_store;

    use super::*;

    fn new_input_query(
        address: &dolos_testing::TestAddress,
        naked_amount: Option<u64>,
        other_assets: Vec<(dolos_testing::TestAsset, u64)>,
    ) -> tx3_lang::ir::InputQuery {
        let naked_asset = naked_amount.map(|x| tx3_lang::ir::AssetExpr {
            policy: tx3_lang::ir::Expression::None,
            asset_name: tx3_lang::ir::Expression::None,
            amount: tx3_lang::ir::Expression::Number(x as i128),
        });

        let other_assets: Vec<tx3_lang::ir::AssetExpr> = other_assets
            .into_iter()
            .map(|(asset, amount)| tx3_lang::ir::AssetExpr {
                policy: tx3_lang::ir::Expression::Bytes(asset.policy().as_slice().to_vec()),
                asset_name: tx3_lang::ir::Expression::Bytes(asset.name().unwrap().to_vec()),
                amount: tx3_lang::ir::Expression::Number(amount as i128),
            })
            .collect();

        let all_assets = naked_asset.into_iter().chain(other_assets).collect();

        tx3_lang::ir::InputQuery {
            address: tx3_lang::ir::Expression::Address(address.to_bytes()),
            min_amount: tx3_lang::ir::Expression::Assets(all_assets),
            r#ref: tx3_lang::ir::Expression::None,
        }
    }

    #[test]
    fn test_select_by_address() {
        let network = tx3_cardano::Network::Testnet;

        let store = seed_random_memory_store(|x: &dolos_testing::TestAddress| {
            dolos_testing::utxo_with_random_amount(x, 4_000_000..5_000_000)
        });

        for subject in dolos_testing::TestAddress::everyone() {
            let criteria = new_input_query(&subject, None, vec![]);

            let utxos = resolve(
                &store,
                network,
                &criteria,
                &tx3_cardano::resolve::ResolveContext::default(),
            )
            .unwrap();

            assert_eq!(utxos.len(), 1);

            for utxo in utxos {
                assert_eq!(utxo.address, subject.to_bytes());
            }
        }
    }

    #[test]
    fn test_select_by_naked_amount() {
        let network = tx3_cardano::Network::Testnet;

        let store = seed_random_memory_store(|x: &dolos_testing::TestAddress| {
            dolos_testing::utxo_with_random_amount(x, 4_000_000..5_000_000)
        });

        let criteria = new_input_query(&dolos_testing::TestAddress::Alice, Some(6_000_000), vec![]);

        let utxos = resolve(
            &store,
            network,
            &criteria,
            &tx3_cardano::resolve::ResolveContext::default(),
        )
        .unwrap();
        assert!(utxos.is_empty());

        let criteria = new_input_query(&dolos_testing::TestAddress::Alice, Some(4_000_000), vec![]);

        let utxos = resolve(
            &store,
            network,
            &criteria,
            &tx3_cardano::resolve::ResolveContext::default(),
        )
        .unwrap();

        let match_count = dbg!(utxos.len());
        assert_eq!(match_count, 1);
    }

    #[test]
    fn test_select_by_asset_amount() {
        let network = tx3_cardano::Network::Testnet;

        let store = seed_random_memory_store(|x: &dolos_testing::TestAddress| {
            dolos_testing::utxo_with_random_asset(x, dolos_testing::TestAsset::Hosky, 500..1000)
        });

        for address in dolos_testing::TestAddress::everyone() {
            // test negative case where we ask more than available amount

            let criteria = new_input_query(
                &address,
                None,
                vec![(dolos_testing::TestAsset::Hosky, 2000)],
            );

            let utxos = resolve(
                &store,
                network,
                &criteria,
                &tx3_cardano::resolve::ResolveContext::default(),
            )
            .unwrap();
            assert!(utxos.is_empty());

            // test negative case where we ask for a different asset

            let criteria =
                new_input_query(&address, None, vec![(dolos_testing::TestAsset::Snek, 500)]);

            let utxos = resolve(
                &store,
                network,
                &criteria,
                &tx3_cardano::resolve::ResolveContext::default(),
            )
            .unwrap();
            assert!(utxos.is_empty());

            // test positive case where we ask for the present asset and amount within range

            let criteria =
                new_input_query(&address, None, vec![(dolos_testing::TestAsset::Hosky, 500)]);

            let utxos = resolve(
                &store,
                network,
                &criteria,
                &tx3_cardano::resolve::ResolveContext::default(),
            )
            .unwrap();
            assert_eq!(utxos.len(), 1);
        }
    }
}
