use async_trait::async_trait;

use serde_json::Value;

#[async_trait]
pub trait Responder {
    async fn respond<C>(&self, context: &C, method: &str) -> Value where C : Send + Sync + 'static;
}
