use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};

use super::balius::odk::driver::EventPattern;

type WorkerId = String;
type ChannelId = u32;
type Method = String;
type AddressBytes = Vec<u8>;

#[derive(Hash, PartialEq, Eq)]
enum MatchKey {
    RequestMethod(WorkerId, Method),
    UtxoAddress(AddressBytes),
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Target {
    pub channel: u32,
    pub worker: String,
}

fn infer_match_keys(worker: &str, pattern: &EventPattern) -> Vec<MatchKey> {
    match pattern {
        EventPattern::Request(x) => vec![MatchKey::RequestMethod(worker.to_owned(), x.to_owned())],
        EventPattern::Utxo(_) => todo!(),
        EventPattern::UtxoUndo(_) => todo!(),
        EventPattern::Timer(_) => todo!(),
        EventPattern::Message(_) => todo!(),
    }
}

type Routes = HashMap<MatchKey, HashSet<Target>>;

#[derive(Default, Clone)]
pub struct Router {
    routes: Arc<RwLock<Routes>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(Default::default())),
        }
    }

    pub fn register_channel(&mut self, worker: &str, channel: u32, pattern: &EventPattern) {
        let keys = infer_match_keys(worker, pattern);
        let mut routes = self.routes.write().unwrap();

        for key in keys {
            let targets = routes.entry(key).or_default();

            targets.insert(Target {
                worker: worker.to_string(),
                channel,
            });
        }
    }

    pub fn find_utxo_targets(&self, utxo: &MultiEraOutput) -> Result<Vec<Target>, super::Error> {
        let routes = self.routes.read().unwrap();

        let key = MatchKey::UtxoAddress(utxo.address()?.to_vec());
        let targets = routes
            .get(&key)
            .iter()
            .flat_map(|x| x.iter())
            .cloned()
            .collect();

        // TODO: match by policy / asset

        Ok(targets)
    }

    pub fn find_request_target(&self, worker: &str, method: &str) -> Result<Target, super::Error> {
        let key = MatchKey::RequestMethod(worker.to_owned(), method.to_owned());
        let routes = self.routes.read().unwrap();

        let targets = routes.get(&key).ok_or(super::Error::NoTarget)?;

        if targets.is_empty() {
            return Err(super::Error::NoTarget);
        }

        if targets.len() > 1 {
            return Err(super::Error::AmbiguousTarget);
        }

        let target = targets.iter().next().unwrap();

        Ok(target.clone())
    }
}
