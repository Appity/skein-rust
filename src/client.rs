use async_trait::async_trait;
use serde_json::Value;

use crate::AsyncResult;

#[async_trait]
pub trait Client {
    async fn rpc_request(&self, method: impl ToString + Send + 'async_trait, params: Option<Value>) -> AsyncResult<Value>;
    async fn rpc_request_serialize<T>(&self, method: impl ToString + Send + 'async_trait, params: Option<impl Into<Value> + Send + 'async_trait>) -> AsyncResult<T> where T : From<Value> + Send + 'async_trait;
    async fn rpc_request_inject(&self, method: impl ToString + Send + 'async_trait, params: Option<Value>) -> AsyncResult<String>;
}
