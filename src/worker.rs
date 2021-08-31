use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::env;
use std::sync::Arc;

use amq_protocol_types::ShortString;
use lapin::{
    BasicProperties,
    options::*,
    types::FieldTable,
    message::Delivery,
    Channel,
    Connection,
    ConnectionProperties,
    Result
};
use serde_json::json;
use serde::ser::Serialize;
use serde_json::Value;
use tokio::task::JoinHandle;

use super::Responder;

pub struct Worker<C> where C : Send + Sized + Sync + 'static {
    context: C,
    channel: Arc<Channel>,
    queue_name: String
}

impl<C> Worker<C> where C : Send + Sized + Sync + 'static  {
    pub async fn new<S1,S2>(context: C, amqp_addr: S1, queue_name: S2) -> Result<Worker<C>> where S1 : ToString, S2 : ToString {
        let connection = Connection::connect(
            amqp_addr.to_string().as_str(),
            ConnectionProperties::default().with_default_executor(8),
        ).await?;

        let channel = connection.create_channel().await?;

        channel.queue_declare(
            queue_name.to_string().as_str(),
            QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: true
            },
            FieldTable::default()
        ).await?;

        let queue_name = queue_name.to_string();

        Ok(
            Worker {
                context,
                channel: Arc::new(channel),
                queue_name
            }
        )
    }

    pub fn queue_name(&self) -> &str {
        self.queue_name.as_str()
    }

    pub fn run<R>(self) -> JoinHandle<Result<Self>> where R : Responder {
        tokio::spawn(async move {
            let channel = self.channel.clone();
            let queue_name = self.queue_name.clone();

            let mut consumer = channel.basic_consume(
                queue_name.as_str(),
                "",
                BasicConsumeOptions::default(),
                FieldTable::default()
            ).await?;

            loop {
                tokio::select!(
                    incoming = consumer.next() => {
                        match incoming {
                            Some(Ok((channel, delivery))) => {
                                self.handle_rpc_delivery(&channel, &delivery).await;
                            },
                            Some(Err(err)) => {
                                log::error!("Error: {:?}", err);
                            },
                            None => {
                                break;
                            }
                        }
                    }
                );
            }

            Ok(self)
        })
    }

    async fn handle_json_rpc(&self, value: Value) -> Value {
        json!("blah")
    }

    fn json_rpc_error_reply<S>(code: i32, message: S, data: Option<Value>) -> Value where S : Serialize {
        if let Some(data) = data {
            json!({
                "jsonrpc": "2.0",
                "code": code,
                "message": message,
                "data": data
            })
        }
        else {
            json!({
                "jsonrpc": "2.0",
                "code": code,
                "message": message
            })
        }
    }

    async fn try_reply_to(&self, channel: &Channel, delivery: &Delivery, reply: Value) {
        if let Some(reply_to) = delivery.properties.reply_to() {
            let reply_to = reply_to.as_str();

            if reply_to.len() > 0 {
                let payload = reply.to_string().as_bytes().to_vec();

                channel.basic_publish(
                    "",
                    reply_to,
                    Default::default(),
                    payload,
                    BasicProperties::default().with_content_type(ShortString::from("application/json"))
                ).await.ok();
            }
        }
    }

    async fn handle_rpc_delivery(&self, channel: &Channel, delivery: &Delivery) {
        match std::str::from_utf8(&delivery.data) {
            Ok(s) => {
                match serde_json::from_str::<Value>(s) {
                    Ok(v) => {
                        match &v["jsonrpc"] {
                            Value::String(ver) => {
                                match ver.as_str() {
                                    "2.0" => {
                                        let reply = self.handle_json_rpc(v).await;

                                        self.try_reply_to(channel, delivery, reply).await;
                                    },
                                    ver => {
                                        log::warn!("Error: Mismatched JSON-RPC version {:?}", ver);
                                    }
                                }
                            },
                            Value::Null => {
                                log::warn!("Error: \"jsonrpc\" attribute missing");
                            },
                            _ => {
                                log::warn!("Error: \"jsonrpc\" attribute is not a string");
                            }
                        }
                    },
                    Err(e) => {
                        log::warn!("Error: Invalid JSON in message ({})", e);
                    }
                }
            },
            Err(e) => {
                log::warn!("Error: Invalid UTF-8 in message ({})", e);
            }
        }

        channel.basic_ack(
            delivery.delivery_tag,
            BasicAckOptions::default()
        ).map(|_| ()).await;
    }
}

#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;

    struct ContextExample {
        id: u32
    }

    struct ResponderExample {
    }

    #[async_trait]
    impl Responder for ResponderExample {
        async fn respond<C>(&self, context: &ContextExample, method: &str) -> Value where C : ContextExample {
            json!("Example")
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads=2)]
    async fn test_new() {
        let context = ContextExample {
            id: 1
        };

        // /context: C, amqp_addr: S, queue_name: S)

        let worker = Worker::new(
            context,
            env::var("AMQP_URL").unwrap_or("amqp://localhost:5672/%2f".to_string()),
            "test"
        ).await.unwrap();

        worker.run::<ResponderExample>().await.ok();
    }
}
