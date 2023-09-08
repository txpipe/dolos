use serde::{Deserialize, Serialize};

pub mod grpc;
pub mod ouroboros;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub grpc: Option<grpc::Config>,
    pub ouroboros: Option<ouroboros::Config>,
}
