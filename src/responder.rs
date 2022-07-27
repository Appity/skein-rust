use async_trait::async_trait;
use serde_json::Value;

use super::rpc;

use crate::AsyncResult;

#[async_trait]
pub trait Responder : Send + Sized + Sync + 'static {
    async fn respond(&mut self, request: &rpc::Request) -> AsyncResult<Value>;
}
