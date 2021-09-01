use async_trait::async_trait;

use serde_json::Value;

use super::rpc::Error;

#[async_trait]
pub trait Responder {
    async fn respond(&self, id: &str, method: &str, params: &Value) -> Result<Value,Error>;
}
