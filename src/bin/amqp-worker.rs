use std::env;
use std::error::Error;
use std::num::ParseIntError;

use async_trait::async_trait;
use clap::Clap;
use dotenv::dotenv;
use log::LevelFilter;
use serde_json::json;
use serde_json::Value;
use simple_logger::SimpleLogger;
use tokio::time::sleep;
use tokio::time::Duration;

use skein_rpc::amqp::Worker;
use skein_rpc::Responder;
use skein_rpc::rpc;

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
struct Program {
    #[clap(short,long)]
    verbose : bool,
    #[clap(short,long)]
    env_file : Option<String>,
    #[clap(short,long)]
    amqp_url : Option<String>,
    #[clap(long,parse(try_from_str=Self::try_into_duration))]
    timeout_warning : Option<Duration>,
    #[clap(long,parse(try_from_str=Self::try_into_duration))]
    timeout_terminate : Option<Duration>,
    #[clap(short,long)]
    queue : Option<String>
}

impl Program {
    fn try_into_duration(s: &str) -> Result<Duration, ParseIntError> {
        s.parse().map(|seconds| Duration::from_secs(seconds))
    }
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
                Ok(request.params().cloned().unwrap_or(json!(null)))
            },
            "stall" => {
                sleep(Duration::from_secs(10)).await;

                Ok(json!(false))
            },
            _ => {
                Err(Box::new(rpc::ErrorResponse::new(-32601, "Method not found", None)))
            }
        }
    }

    fn terminated(&self) -> bool {
        self.terminated
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let program = Program::parse();

    match program.env_file {
        Some(ref path) => {
            dotenv::from_filename(path).ok();
        },
        None => {
            dotenv().ok();
        }
    }

    SimpleLogger::new().with_level(
        if program.verbose {
            LevelFilter::Debug
        }
        else {
            LevelFilter::Info
        }
    ).init().unwrap();

    let amqp_url = program.amqp_url.unwrap_or_else(|| env::var("AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672/%2f".to_string()));
    let queue = program.queue.unwrap_or_else(|| env::var("AMQP_QUEUE").unwrap_or_else(|_| "skein_test".to_string()));

    let context = WorkerContext::default();
    let worker = Worker::new(
        context,
        amqp_url,
        queue,
        program.timeout_warning,
        program.timeout_terminate
    )?;

    worker.run().await??;

    Ok(())
}
