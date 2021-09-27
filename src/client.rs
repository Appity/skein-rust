use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait Client {
    async fn rpc_request(&self, method: impl ToString + Send + 'async_trait, params: Option<Value>) -> Result<Value, Box<dyn std::error::Error>>;
    async fn rpc_request_serialize<T>(&self, method: impl ToString + Send + 'async_trait, params: Option<impl Into<Value> + Send + 'async_trait>) -> Result<T, Box<dyn std::error::Error>> where T : From<Value> + Send + 'async_trait;
    async fn rpc_request_inject(&self, method: impl ToString + Send + 'async_trait, params: Option<Value>) -> Result<(), Box<dyn std::error::Error>>;
}
