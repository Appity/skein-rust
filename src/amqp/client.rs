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
    Channel,
    Connection,
    ConnectionProperties,
    Consumer,
    Result as LapinResult
};
use serde_json::Value;
use tokio_amqp::LapinTokioExt;
use tokio::sync::mpsc::{unbounded_channel,UnboundedReceiver,UnboundedSender};
use tokio::sync::oneshot::{channel as oneshot_channel,Receiver as OneshotReceiver,Sender as OneshotSender};
// use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::time::sleep;
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

async fn declare_queues(options: &ClientOptions, channel: &Channel, ident: &String) -> LapinResult<()> {
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

    channel.queue_declare(
        ident.as_str(),
        QueueDeclareOptions {
            passive: false,
            durable: false,
            exclusive: true,
            auto_delete: true,
            nowait: true
        },
        FieldTable::default()
    ).await?;

    Ok(())
}

async fn create_consumer(loop_context: &ClientLoopContext) -> LapinResult<(Channel,Consumer)> {
    let connection = connect(&loop_context.options).await?;
    let channel = connection.create_channel().await?;

    declare_queues(&loop_context.options, &channel, &loop_context.ident).await?;

    let consumer = channel.basic_consume(
        loop_context.ident.as_str(),
        "",
        BasicConsumeOptions::default(),
        FieldTable::default()
    ).await?;

    Ok((channel, consumer))
}

async fn client_consumer_loop(channel: Channel, mut consumer: Consumer, loop_context: &mut ClientLoopContext) -> LapinResult<()> {
    let properties = BasicProperties::default().with_content_type("application/json".into());
    let rpc_queue_name = loop_context.options.queue_name.to_string();
    let reply_to = loop_context.ident.clone();

    loop {
        if let Ok(_) = loop_context.interrupted.try_recv() {
            log::debug!("Client loop interrupted");

            break;
        }

        log::debug!("Client loop begins, connected to {}", rpc_queue_name);

        tokio::select!(
            r = loop_context.rx.recv() => {
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
                                    rpc_queue_name.as_str(),
                                    Default::default(),
                                    payload,
                                    properties
                                ).await {
                                    Ok(confirm) => {
                                        confirm.await?;

                                        log::trace!("Delivery published to {}", &rpc_queue_name);

                                        if let Some(reply) = reply {
                                            loop_context.requests.insert(request.id().clone(), reply);
                                        }
                                    },
                                    Err(err) => {
                                        log::error!("Error sending request: {}", err);

                                        // FIX: Don't reply like this until it's a lost cause
                                        // if let Some(reply) = reply {
                                        //     reply.send(
                                        //         rpc::Response::error_for(
                                        //             &request,
                                        //             -32603,
                                        //             format!("Could not send request: {}", err),
                                        //             None
                                        //         )
                                        //     ).ok();
                                        // }

                                        Err(err)?
                                    }
                                }
                            },
                            Err(err) => {
                                log::error!("Error serializing request: {}", err);
                            }
                        }
                    },
                    None => {
                        log::debug!("Cannel closed, exiting client loop");

                        break;
                    }
                }
            },
            incoming = consumer.next() => {
                match incoming {
                    Some(Ok(delivery)) => {
                        match rpc::Response::try_from(&delivery) {
                            Ok(response) => {
                                match response.id() {
                                    Some(id) => {
                                        match loop_context.requests.remove(id) {
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

    Ok(())
}

struct ClientLoopContext {
    ident: String,
    options: ClientOptions,
    interrupted: OneshotReceiver<()>,
    rx: UnboundedReceiver<(rpc::Request, Option<OneshotSender<rpc::Response>>)>,
    requests: HashMap::<String,OneshotSender<rpc::Response>>
}

async fn client_handle(mut loop_context: ClientLoopContext) -> LapinResult<JoinHandle<()>> {
    Ok(tokio::spawn(async move {
        loop {
            match create_consumer(&loop_context).await {
                Ok((channel, consumer)) => {
                    match client_consumer_loop(channel, consumer, &mut loop_context).await {
                        Ok(_) => break,
                        Err(err) => {
                            log::error!("Error in consumer loop: {}", err);
                        }
                    }
                },
                Err(err) => {
                    log::error!("Error creating consumer: {}", err);
                }
            }

            sleep(Duration::from_secs(5)).await;
        }
    }))
}

async fn connect(options: &ClientOptions) -> LapinResult<Connection> {
    Connection::connect(
        options.amqp_url.as_str(),
        ConnectionProperties::default().with_tokio(),
    ).await
}

#[derive(Debug)]
pub struct Client {
    rpc: UnboundedSender<(rpc::Request, Option<OneshotSender<rpc::Response>>)>,
    interrupt: OneshotSender<()>,
    options: ClientOptions,
    handle: JoinHandle<()>
}

impl Client {
    pub async fn new(options: ClientOptions) -> LapinResult<Client> {
        let ident = format!(
            "{}-{}@{}",
            options.ident.to_string(),
            Uuid::new_v4().to_string(),
            gethostname().into_string().unwrap()
        );

        let (tx, rx) = unbounded_channel::<(rpc::Request, Option<OneshotSender<rpc::Response>>)>();

        let (interrupt, interrupted) = oneshot_channel::<()>();

        let loop_context = ClientLoopContext {
            ident,
            options: options.clone(),
            interrupted,
            rx,
            requests: HashMap::new()
        };

        Ok(
            Client {
                rpc: tx,
                interrupt,
                options,
                handle: client_handle(loop_context).await?
            }
        )
    }

    pub fn into_handle(self) -> JoinHandle<()> {
        self.handle
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
