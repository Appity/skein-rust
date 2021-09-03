use std::error::Error;

use async_trait::async_trait;
use serde_json::Value;

use super::rpc;

#[async_trait]
pub trait Responder : Send + Sized + Sync + 'static {
    async fn respond(&mut self, request: &rpc::Request) -> Result<Value,Box<dyn Error>>;

    fn terminated(&self) -> bool {
        false
    }
}
