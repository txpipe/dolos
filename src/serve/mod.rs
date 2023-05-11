use serde::{Deserialize, Serialize};

pub mod grpc;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub grpc: grpc::Config,
}
