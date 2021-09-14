use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use async_trait::async_trait;
use gethostname::gethostname;
use lapin::{
    BasicProperties,
    options::*,
    types::FieldTable,
    Connection,
    ConnectionProperties,
    Result as LapinResult
};
use serde_json::Value;
use tokio::sync::mpsc::{unbounded_channel,UnboundedSender};
use tokio::sync::oneshot::{channel as oneshot_channel,Sender as OneshotSender};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use crate::rpc;
use crate::Client as ClientTrait;

#[derive(Clone,Debug)]
pub struct ClientOptions {
    pub amqp_url: String,
    pub queue_name: String,
    pub ident: String,
    pub timeout: Duration,
    pub threads: usize
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            amqp_url: "amqp://localhost:5672/%2f".to_string(),
            queue_name: "skein_rpc".to_string(),
            ident: "skein".to_string(),
            timeout: Duration::from_secs(30),
            threads: 8
        }
    }
}

impl ClientOptions {
    pub fn new(amqp_url: impl ToString, queue_name: impl ToString, ident: impl ToString) -> Self {
        Self {
            amqp_url: amqp_url.to_string(),
            queue_name: queue_name.to_string(),
            ident: ident.to_string(),
            timeout: Duration::from_secs(30),
            threads: 8
        }
    }

    pub fn with_amqp_url(mut self, amqp_url: impl ToString) -> Self {
        self.amqp_url = amqp_url.to_string();

        self
    }

    pub fn with_queue_name(mut self, queue_name: impl ToString) -> Self {
        self.queue_name = queue_name.to_string();

        self
    }

    pub fn with_ident(mut self, ident: impl ToString) -> Self {
        self.ident = ident.to_string();

        self
    }

    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = duration;

        self
    }

    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;

        self
    }
}

#[derive(Debug)]
pub struct Client {
    rpc: UnboundedSender<(rpc::Request, Option<OneshotSender<rpc::Response>>)>,
    options: ClientOptions,
    handle: JoinHandle<()>
}

impl Client {
    pub async fn new(options: ClientOptions) -> LapinResult<Client> {
        let connection = Connection::connect(
            options.amqp_url.as_str(),
            ConnectionProperties::default(),
        ).await?;

        let channel = connection.create_channel().await?;
        let queue_name = options.queue_name.to_string();

        channel.queue_declare(
            queue_name.as_str(),
            QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: true
            },
            FieldTable::default()
        ).await?;

        let ident = format!(
            "{}-{}@{}",
            options.ident.to_string(),
            Uuid::new_v4().to_string(),
            gethostname().into_string().unwrap()
        );

        channel.queue_declare(
            ident.as_str(),
            QueueDeclareOptions {
                passive: false,
                durable: false,
                exclusive: false,
                auto_delete: true,
                nowait: true
            },
            FieldTable::default()
        ).await?;

        let mut consumer = channel.basic_consume(
            ident.as_str(),
            "",
            BasicConsumeOptions::default(),
            FieldTable::default()
        ).await?;

        let (tx, mut rx) = unbounded_channel::<(rpc::Request, Option<OneshotSender<rpc::Response>>)>();

        let rpc_queue_name = queue_name.clone();
        let reply_to = ident.clone();

        let handle = tokio::spawn(async move {
            let rpc_queue_name = rpc_queue_name.as_str();
            let properties = BasicProperties::default().with_content_type("application/json".into());

            let mut requests = HashMap::<String,OneshotSender<rpc::Response>>::new();

            loop {
                tokio::select!(
                    r = rx.recv() => {
                        match r {
                            Some((request,reply)) => {
                                match serde_json::to_string(&request) {
                                    Ok(str) => {
                                        let payload = str.as_bytes().to_vec();
                                        let properties = if request.reply_to() {
                                            properties.clone().with_reply_to(reply_to.as_str().into())
                                        }
                                        else {
                                            properties.clone()
                                        };

                                        match channel.basic_publish(
                                            "", // FUTURE: Allow specifying exchange
                                            rpc_queue_name,
                                            Default::default(),
                                            payload,
                                            properties
                                        ).await {
                                            Ok(_) => {
                                                if let Some(reply) = reply {
                                                    requests.insert(request.id().clone(), reply);
                                                }
                                            },
                                            Err(err) => {
                                                if let Some(reply) = reply {
                                                    reply.send(
                                                        rpc::Response::error_for(
                                                            &request,
                                                            -32603,
                                                            format!("Could not send request: {}", err),
                                                            None
                                                        )
                                                    ).ok();
                                                }
                                            }
                                        }
                                    },
                                    Err(err) => {
                                        log::error!("Error serializing request: {}", err);
                                    }
                                }
                            },
                            None => break
                        }
                    },
                    incoming = consumer.next() => {
                        match incoming {
                            Some(Ok(delivery)) => {
                                match rpc::Response::try_from(&delivery) {
                                    Ok(response) => {
                                        match response.id() {
                                            Some(id) => {
                                                match requests.remove(id) {
                                                    Some(reply) => {
                                                        reply.send(response).ok();
                                                    },
                                                    None => {
                                                        // Unknown request.
                                                        log::warn!("Warning: Received response for unknown request {}", id);
                                                    }
                                                }
                                            },
                                            None => {
                                                log::error!("Error: {:?}", response);
                                            }
                                        }
                                    },
                                    Err(err) => {
                                        log::error!("Error: {:?}", err);
                                    }
                                }

                                channel.basic_ack(
                                    delivery.delivery_tag,
                                    BasicAckOptions::default()
                                ).map(|_| ()).await;
                            },
                            Some(Err(err)) => {
                                log::error!("Error: {:?}", err);
                            },
                            None => {
                                // break;
                            }
                        }
                    }
                )
            }
        });

        Ok(
            Client {
                rpc: tx,
                options,
                handle
            }
        )
    }

    pub fn abort(self) {
        self.handle.abort()
    }
}

#[async_trait]
impl ClientTrait for Client {
    async fn rpc_request_serialize<T>(&self, method: impl ToString + Send + 'async_trait, params: Option<impl Into<Value> + Send + 'async_trait>) -> Result<T, Box<dyn std::error::Error>> where T : From<Value> + Send + 'async_trait {
        let (reply, responder) = oneshot_channel::<rpc::Response>();
        let method = method.to_string();

        let request = rpc::Request::new_serialize(Uuid::new_v4().to_string(), &method, params);

        log::debug!("{}> RPC Request: {} (sent)", request.id(), &method);

        self.rpc.send((request, Some(reply)))?;

        match timeout(self.options.timeout, responder).await?? {
            rpc::Response::Result { result, .. } => {
                Ok(result.into())
            },
            rpc::Response::Error { error, .. } => {
                Err(Box::new(error))
            }
        }
    }

    async fn rpc_request(&self, method: impl ToString + Send + 'async_trait, params: Option<Value>) -> Result<Value, Box<dyn std::error::Error>> {
        let (reply, responder) = oneshot_channel::<rpc::Response>();
        let method = method.to_string();

        let request = rpc::Request::new(Uuid::new_v4().to_string(), &method, params);

        log::debug!("{}> RPC Request: {} (sent)", request.id(), &method);

        self.rpc.send((request,Some(reply)))?;

        match timeout(self.options.timeout, responder).await?? {
            rpc::Response::Result { result, .. } => {
                Ok(result)
            },
            rpc::Response::Error { error, .. } => {
                Err(Box::new(error))
            }
        }
    }

    async fn rpc_request_noreply(&self, method: impl ToString + Send + 'async_trait, params: Option<Value>) -> Result<(), Box<dyn std::error::Error>> {
        let method = method.to_string();

        let request = rpc::Request::new_noreply(Uuid::new_v4().to_string(), &method, params);

        log::debug!("{}> RPC Request: {} (sent)", request.id(), &method);

        self.rpc.send((request,None))?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // use super::*;

    // #[test]

}
