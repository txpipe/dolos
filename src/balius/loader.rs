use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

use wasmtime::{component::Component, component::Linker, Engine, Store};

use super::{
    adapter::Adapter,
    balius::odk::driver::{Event, Response},
    router::Router,
};

struct LoadedWorker {
    store: Store<Adapter>,
    instance: super::Worker,
}

type WorkerMap = HashMap<String, LoadedWorker>;

#[derive(Clone)]
pub struct Loader {
    engine: Engine,
    linker: Linker<Adapter>,
    router: Router,
    loaded: Arc<Mutex<WorkerMap>>,
}

impl Loader {
    pub fn new(router: Router) -> Result<Self, super::Error> {
        let engine = Default::default();

        let mut linker = Linker::new(&engine);
        super::balius::odk::driver::add_to_linker(&mut linker, |state: &mut Adapter| state)?;
        super::balius::odk::kv::add_to_linker(&mut linker, |state: &mut Adapter| state)?;
        super::balius::odk::submit::add_to_linker(&mut linker, |state: &mut Adapter| state)?;

        Ok(Self {
            engine,
            loaded: Default::default(),
            linker,
            router,
        })
    }

    pub fn register_worker(
        &mut self,
        id: &str,
        wasm_path: impl AsRef<Path>,
    ) -> wasmtime::Result<()> {
        let component = Component::from_file(&self.engine, wasm_path)?;

        let mut store = Store::new(
            &self.engine,
            Adapter::new(id.to_owned(), self.router.clone()),
        );

        let instance = super::Worker::instantiate(&mut store, &component, &self.linker)?;
        instance.call_init(&mut store, &vec![])?;

        self.loaded
            .lock()
            .unwrap()
            .insert(id.to_owned(), LoadedWorker { store, instance });

        Ok(())
    }

    pub fn dispatch_event(
        &self,
        worker: &str,
        channel: u32,
        event: &Event,
    ) -> Result<Response, super::Error> {
        let mut lock = self.loaded.lock().unwrap();

        let worker = lock
            .get_mut(worker)
            .ok_or(super::Error::WorkerNotFound(worker.to_string()))?;

        let result = worker
            .instance
            .call_handle(&mut worker.store, channel, event)?;

        let response = result.map_err(|code| super::Error::Handle(code))?;

        Ok(response)
    }
}
