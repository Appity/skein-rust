use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use async_trait::async_trait;
use gethostname::gethostname;
use lapin::{
    options::*,
    types::FieldTable,
    Channel,
    Connection,
    ConnectionProperties,
    Consumer,
    publisher_confirm::PublisherConfirm,
    Result as LapinResult
};
use serde_json::Value;
use tokio_amqp::LapinTokioExt;
use tokio::sync::mpsc::{unbounded_channel,UnboundedReceiver,UnboundedSender};
use tokio::sync::oneshot::{channel as oneshot_channel,Sender as OneshotSender};
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
    let rpc_queue_name = loop_context.options.queue_name.to_string();
    let reply_to = loop_context.ident.clone();

    loop {
        tokio::select!(
            c = loop_context.confirm_rx.recv() => {
                if let Some((confirm,command)) = c {
                    if let Err(err) = confirm.await {
                        if let Err(err) = loop_context.tx.send(command) {
                            log::error!("Error requeueing message: {}", err);
                        }
                        else {
                            loop_context.retried += 1;
                        }

                        return Err(err);
                    }

                    loop_context.confirmations += 1;

                    match command {
                        ClientCommand::Request(request, reply) => {
                            log::trace!("{}> Delivery {} published to {}", request.id(), &loop_context.confirmations, &rpc_queue_name);

                            loop_context.requests.insert(request.id().clone(), reply);
                        },
                        ClientCommand::Inject(request, reply) => {
                            log::trace!("{}> Delivery {} published to {}", request.id(), &loop_context.confirmations, &rpc_queue_name);

                            if let Err(_) = reply.send(request.id().clone()) {
                                log::error!("{}> Error sending reply", request.id());
                            }
                        },
                        ClientCommand::Terminate => {
                            // Not sure how this would get here.
                        }
                    }
                }
            },
            r = loop_context.rx.recv() => {
                match r {
                    Some(ClientCommand::Request(request,reply)) => {
                        match serde_json::to_string(&request) {
                            Ok(str) => {
                                log::trace!("{}> Publishing", request.id());

                                match channel.basic_publish(
                                    "", // FUTURE: Allow specifying exchange
                                    rpc_queue_name.as_str(),
                                    Default::default(),
                                    str.as_bytes().to_vec(),
                                    request.properties(reply_to.as_str())
                                ).await {
                                    Ok(confirm) => {
                                        if let Err(_) = loop_context.confirm_tx.send((confirm, ClientCommand::Request(request,reply))) {
                                            log::error!("Error pushing to confirmation queue");
                                        }
                                    },
                                    Err(err) => {
                                        if let Err(err) = loop_context.tx.send(ClientCommand::Request(request,reply)) {
                                            log::error!("Error requeueing message: {}", err);
                                        }
                                        else {
                                            loop_context.retried += 1;
                                        }

                                        return Err(err);
                                    }
                                }
                            },
                            Err(err) => {
                                log::error!("Error serializing request: {}", err);
                            }
                        }
                    },
                    Some(ClientCommand::Inject(request,reply)) => {
                        log::trace!("{}> Request injection", request.id());

                        match serde_json::to_string(&request) {
                            Ok(str) => {
                                match channel.basic_publish(
                                    "", // FUTURE: Allow specifying exchange
                                    rpc_queue_name.as_str(),
                                    Default::default(),
                                    str.as_bytes().to_vec(),
                                    request.properties("")
                                ).await {
                                    Ok(confirm) => {
                                        if let Err(_) = loop_context.confirm_tx.send((confirm, ClientCommand::Inject(request,reply))) {
                                            log::error!("Error pushing to confirmation queue");
                                        }
                                    },
                                    Err(err) => {
                                        if let Err(err) = loop_context.tx.send(ClientCommand::Inject(request,reply)) {
                                            log::error!("Error requeueing message: {}", err);
                                        }
                                        else {
                                            loop_context.retried += 1;
                                        }

                                        return Err(err);
                                    }
                                }
                            },
                            Err(err) => {
                                log::error!("Error serializing request: {}", err);
                            }
                        }
                    },
                    Some(ClientCommand::Terminate) => {
                        log::trace!("Client terminated, exiting client loop");

                        return Ok(());
                    },
                    None => {
                        log::trace!("Channel closed, exiting client loop");

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
                                        log::error!("Missing ID error: {:?}", response);
                                    }
                                }
                            },
                            Err(err) => {
                                log::error!("Error creating Response from Delivery: {:?}", err);
                            }
                        }

                        channel.basic_ack(
                            delivery.delivery_tag,
                            BasicAckOptions::default()
                        ).map(|_| ()).await;
                    },
                    Some(Err(err)) => {
                        return Err(err);
                    },
                    None => {
                        log::debug!("Consumer channel closed");
                        // Consumer has been closed, but don't break out of
                        // loop until all of the backlog has cleared.

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
    connections: usize,
    confirmations: usize,
    retried: usize,
    confirm_tx: UnboundedSender<(PublisherConfirm,ClientCommand)>,
    confirm_rx: UnboundedReceiver<(PublisherConfirm,ClientCommand)>,
    tx: UnboundedSender<ClientCommand>,
    rx: UnboundedReceiver<ClientCommand>,
    requests: HashMap::<String,OneshotSender<rpc::Response>>
}

impl ClientLoopContext {
    fn report(&self) -> ClientReport {
        ClientReport {
            connections: self.connections,
            confirmations: self.confirmations,
            retried: self.retried,
            pending: self.requests.len()
        }
    }
}

#[derive(Clone,Debug)]
pub struct ClientReport {
    pub connections: usize,
    pub confirmations: usize,
    pub retried: usize,
    pub pending: usize
}

async fn client_handle(mut loop_context: ClientLoopContext) -> LapinResult<JoinHandle<ClientReport>> {
    Ok(tokio::spawn(async move {
        loop {
            log::trace!("Creating connection and consumer");

            match create_consumer(&loop_context).await {
                Ok((channel, consumer)) => {
                    loop_context.connections += 1;

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

            sleep(Duration::from_secs(1)).await;
        }

        loop_context.report()
    }))
}

async fn connect(options: &ClientOptions) -> LapinResult<Connection> {
    Connection::connect(
        options.amqp_url.as_str(),
        ConnectionProperties::default().with_tokio(),
    ).await
}

#[derive(Debug)]
enum ClientCommand {
    Request(rpc::Request,OneshotSender<rpc::Response>),
    Inject(rpc::Request,OneshotSender<String>),
    Terminate
}

#[derive(Debug)]
pub struct Client {
    rpc: UnboundedSender<ClientCommand>,
    options: ClientOptions,
    handle: JoinHandle<ClientReport>
}

impl Client {
    pub async fn new(options: ClientOptions) -> LapinResult<Client> {
        let ident = format!(
            "{}-{}@{}",
            options.ident.to_string(),
            Uuid::new_v4().to_string(),
            gethostname().into_string().unwrap()
        );

        let (confirm_tx, confirm_rx) = unbounded_channel::<(PublisherConfirm,ClientCommand)>();
        let (tx, rx) = unbounded_channel::<ClientCommand>();

        let loop_context = ClientLoopContext {
            ident,
            options: options.clone(),
            connections: 0,
            confirmations: 0,
            retried: 0,
            confirm_tx,
            confirm_rx,
            tx: tx.clone(),
            rx,
            requests: HashMap::new()
        };

        Ok(
            Client {
                rpc: tx,
                options,
                handle: client_handle(loop_context).await?
            }
        )
    }

    pub fn into_handle(self) -> JoinHandle<ClientReport> {
        self.handle
    }

    pub fn abort(self) {
        self.handle.abort()
    }

    pub fn close(&self) -> bool {
        self.rpc.send(ClientCommand::Terminate).is_ok()
    }
}

#[async_trait]
impl ClientTrait for Client {
    async fn rpc_request_serialize<T>(&self, method: impl ToString + Send + 'async_trait, params: Option<impl Into<Value> + Send + 'async_trait>) -> Result<T, Box<dyn std::error::Error>> where T : From<Value> + Send + 'async_trait {
        let (reply, responder) = oneshot_channel::<rpc::Response>();
        let method = method.to_string();

        let request = rpc::Request::new_serialize(Uuid::new_v4().to_string(), &method, params);

        log::trace!("{}> RPC Request: {} (confirmations)", request.id(), &method);

        self.rpc.send(ClientCommand::Request(request, reply))?;

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

        log::trace!("{}> RPC Request: {} (confirmations)", request.id(), &method);

        self.rpc.send(ClientCommand::Request(request, reply))?;

        match timeout(self.options.timeout, responder).await?? {
            rpc::Response::Result { result, .. } => {
                Ok(result)
            },
            rpc::Response::Error { error, .. } => {
                Err(Box::new(error))
            }
        }
    }

    async fn rpc_request_inject(&self, method: impl ToString + Send + 'async_trait, params: Option<Value>) -> Result<String, Box<dyn std::error::Error>> {
        let method = method.to_string();

        let request = rpc::Request::new_noreply(Uuid::new_v4().to_string(), &method, params);

        log::trace!("{}> RPC Request: {} (confirmations)", request.id(), &method);

        let (reply, responder) = oneshot_channel::<String>();

        self.rpc.send(ClientCommand::Inject(request,reply))?;

        Ok(responder.await?)
    }
}

#[cfg(test)]
mod test {
    // use super::*;

    // #[test]

}
