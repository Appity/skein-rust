use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::convert::TryFrom;

use lapin::{
    BasicProperties,
    options::*,
    types::FieldTable,
    message::Delivery,
    Channel,
    Connection,
    ConnectionProperties,
    Result as LapinResult
};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;

use crate::Responder;
use crate::rpc;

#[derive(Clone,Debug)]
pub struct WorkerConfig {
    amqp_addr: String,
    queue_name: String,
    timeout_warning: Duration,
    timeout_terminate: Duration
}

impl WorkerConfig {
    pub fn new(amqp_addr: impl ToString, queue_name: impl ToString, timeout_warning: Option<Duration>, timeout_terminate: Option<Duration>) -> Self {
        let amqp_addr = amqp_addr.to_string();
        let queue_name = queue_name.to_string();

        Self {
            amqp_addr,
            queue_name,
            timeout_warning: timeout_warning.unwrap_or_else(|| Duration::from_secs(30)),
            timeout_terminate: timeout_terminate.unwrap_or_else(|| Duration::from_secs(300))
        }
    }

    async fn channel(&self) -> LapinResult<Channel> {
        let connection = Connection::connect(
            self.amqp_addr.as_str(),
            ConnectionProperties::default(),
        ).await?;

        let channel = connection.create_channel().await?;

        channel.queue_declare(
            self.queue_name.as_str(),
            QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: true
            },
            FieldTable::default()
        ).await?;

        Ok(channel)
    }
}

pub struct Worker<C> where C : Responder {
    context: C,
    config: WorkerConfig
}

impl<C> Worker<C> where C : Responder {
    pub fn new(context: C, amqp_addr: impl ToString, queue_name: impl ToString, timeout_warning: Option<Duration>, timeout_terminate: Option<Duration>) -> LapinResult<Worker<C>> {
        Ok(
            Worker {
                context,
                config: WorkerConfig::new(amqp_addr, queue_name, timeout_warning, timeout_terminate)
            }
        )
    }

    pub fn queue_name(&self) -> &str {
        self.config.queue_name.as_str()
    }

    pub async fn handle_with_timeout(&mut self, channel: &Channel, delivery: &Delivery) {
        // let mut warning = interval(self.config.timeout_warning);
        let now = Instant::now();

        match timeout(self.config.timeout_terminate, self.handle_rpc_delivery(&delivery)).await {
            Ok(response) => {
                self.try_reply_to(&channel, &delivery, &response).await;

                channel.basic_ack(
                    delivery.delivery_tag,
                    BasicAckOptions::default()
                ).map(|_| ()).await;

                log::debug!("RPC call processed in {:.2}s", now.elapsed().as_secs_f32());
            },
            Err(err) => {
                log::error!("Timeout error when processing RPC call: {}", err);

                channel.basic_nack(
                    delivery.delivery_tag,
                    BasicNackOptions::default()
                ).map(|_| ()).await;

                log::debug!("RPC call failed in {:.2}s", now.elapsed().as_secs_f32());
            }
        }
    }

    pub fn run(mut self) -> JoinHandle<LapinResult<Self>> {
        let config = self.config.clone();
        let queue_name = self.config.queue_name.clone();

        tokio::spawn(async move {
            loop {
                match config.channel().await {
                    Ok(channel) => {
                        match channel.basic_consume(
                            queue_name.as_str(),
                            "",
                            BasicConsumeOptions::default(),
                            FieldTable::default()
                        ).await {
                            Ok(mut consumer) => {
                                loop {
                                    match consumer.next().await {
                                        Some(Ok(delivery)) => {
                                            log::debug!("Dispatching RPC call");

                                            self.handle_with_timeout(&channel, &delivery).await;
                                        },
                                        Some(Err(err)) => {
                                            log::error!("Error: {:?}", err);
                                            log::warn!("AMQP consumer reconnecting.");

                                            break;
                                        },
                                        None => {
                                            log::warn!("AMQP consumer ran dry? Reconnecting.");

                                            break;
                                        }
                                    }

                                    if self.context.terminated() {
                                        log::debug!("Worker terminated by request");

                                        return Ok(self);
                                    }
                                }
                            },
                            Err(err) => {
                                log::error!("Error connecting consumer: {}", err);

                                sleep(Duration::from_secs(5)).await;

                                log::warn!("AMQP consumer reconnecting.");
                            }
                        }
                    },
                    Err(err) => {
                        log::error!("Error connecting channel: {}", err);

                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        })
    }

    async fn try_reply_to(&self, channel: &Channel, delivery: &Delivery, response: &rpc::Response) {
        if let Some(reply_to) = delivery.properties.reply_to() {
            let reply_to = reply_to.as_str();

            if !reply_to.is_empty() {
                match serde_json::to_string(response) {
                    Ok(str) => {
                        let payload = str.as_bytes().to_vec();

                        // FIX: Warn on transmission error
                        if let Err(err) = channel.basic_publish(
                            "",
                            reply_to,
                            Default::default(),
                            payload,
                            BasicProperties::default().with_content_type("application/json".into())
                        ).await {
                            log::warn!("Error: Could not publish reply {:?}", err);
                        }
                    },
                    Err(err) => {
                        log::warn!("Error: Internal processing error when replying {:?}", err);
                    }
                }
            }
        }
        else {
            log::debug!("No reply-to header for request");
        }
    }

    async fn handle_rpc_delivery(&mut self, delivery: &Delivery) -> rpc::Response {
        match rpc::Request::try_from(delivery) {
            Ok(request) => {
                log::debug!("Request received: {}", request.id());

                match self.context.respond(&request).await {
                    Ok(result) => {
                        rpc::Response::result_for(&request, result)
                    },
                    Err(err) => {
                        log::warn!("Error: Internal processing error {:?}", err);

                        rpc::Response::error_for(&request, -32603, "Internal processing error", None)
                    }
                }
            },
            Err(response) => response
        }
    }
}

#[cfg(test)]
mod test {
    use std::env;

    use async_trait::async_trait;
    use serde_json::json;
    use serde_json::Value;

    use super::*;

    struct ContextExample {
        id: u32,
        terminated: bool
    }

    #[async_trait]
    impl Responder for ContextExample {
        async fn respond(&mut self, _request: &rpc::Request) -> Result<Value,Box<dyn std::error::Error>> {
            self.terminated = true;

            Ok(json!(format!("Example {}", &self.id)))
        }

        fn terminated(&self) -> bool {
            self.terminated
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads=2)]
    async fn test_new() {
        let context = ContextExample {
            id: 1,
            terminated: false
        };

        // /context: C, amqp_addr: S, queue_name: S)

        let worker = Worker::new(
            context,
            env::var("AMQP_URL").unwrap_or("amqp://localhost:5672/%2f".to_string()),
            "test",
            None,
            None
        ).unwrap();

        worker.run().abort();
    }
}
