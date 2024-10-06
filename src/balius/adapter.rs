use super::{
    balius::odk::{driver, kv, submit},
    router::Router,
};

#[derive(Clone)]
pub struct Adapter {
    worker_id: String,
    router: Router,
}

impl Adapter {
    pub fn new(worker_id: String, router: Router) -> Self {
        Self { worker_id, router }
    }
}

impl kv::Host for Adapter {
    fn get_value(&mut self, key: String) -> Result<kv::Payload, kv::KvError> {
        todo!()
    }

    fn set_value(&mut self, key: String, value: kv::Payload) -> Result<(), kv::KvError> {
        println!("{}:{}", key, hex::encode(value));

        Ok(())
    }

    fn list_values(&mut self, prefix: String) -> Result<Vec<String>, kv::KvError> {
        todo!()
    }
}

impl submit::Host for Adapter {
    fn submit_tx(&mut self, tx: submit::Cbor) -> Result<(), submit::SubmitError> {
        println!("{}", hex::encode(tx));

        Ok(())
    }
}

impl driver::Host for Adapter {
    fn register_channel(&mut self, id: u32, pattern: driver::EventPattern) -> () {
        self.router.register_channel(&self.worker_id, id, &pattern);
    }
}
