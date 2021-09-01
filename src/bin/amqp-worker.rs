use std::env;
use std::error::Error;

use async_trait::async_trait;
use clap::Clap;
use log::LevelFilter;
use serde_json::json;
use serde_json::Value;
use simple_logger::SimpleLogger;

use skein_rpc::amqp::Worker;
use skein_rpc::Responder;
use skein_rpc::rpc;

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
struct Program {
    #[clap(short,long)]
    verbose : bool,
    #[clap(short,long)]
    amqp_url : Option<String>,
    #[clap(short,long,default_value="skein_test")]
    queue : String
}

struct WorkerContext {
    terminated : bool
}

impl Default for WorkerContext {
    fn default() -> Self {
        Self {
            terminated: false
        }
    }
}

#[async_trait]
impl Responder for WorkerContext {
    async fn respond(&mut self, request: &rpc::Request) -> Result<Value,Box<dyn Error>> {
        match request.method().as_str() {
            "echo" => {
                match request.params() {
                    Some(v) => Ok(v.clone()),
                    None => Ok(json!(null))
                }
            },
            _ => {
                Err(Box::new(rpc::ErrorResponse::new(-32601, "Method not found", None)))
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let program = Program::parse();

    SimpleLogger::new().with_level(
        if program.verbose {
            LevelFilter::Debug
        }
        else {
            LevelFilter::Info
        }
    ).init().unwrap();

    let amqp_url = program.amqp_url.unwrap_or(env::var("AMQP_URL").unwrap_or("amqp://localhost:5672/%2f".to_string()));
    let queue = program.queue;

    let context = WorkerContext::default();
    let worker = Worker::new(context, amqp_url, queue).await?;

    worker.run().await?;

    Ok(())
}
