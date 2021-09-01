use std::env;
use std::error::Error;
use std::num::ParseIntError;
use std::time::Duration;
use std::time::Instant;

use clap::Clap;
use log::LevelFilter;
use serde_json::json;
use simple_logger::SimpleLogger;

use skein_rpc::Client;
use skein_rpc::amqp::Client as AMQPClient;
use skein_rpc::amqp::ClientOptions as AMQPClientOptions;

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
struct Program {
    #[clap(short,long)]
    verbose : bool,
    #[clap(short,long)]
    amqp_url : Option<String>,
    #[clap(short,long,default_value="skein_test")]
    queue : String,
    #[clap(short,long,default_value="1")]
    repeat: usize,
    #[clap(short,long,default_value="10",parse(try_from_str=Self::try_into_duration))]
    timeout : Duration,
    method : Option<String>,
    #[clap(multiple=true)]
    args : Vec<String>
}

impl Program {
    fn try_into_duration(s: &str) -> Result<Duration, ParseIntError> {
        s.parse().map(|seconds| Duration::from_secs(seconds))
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

    let options = AMQPClientOptions::new(
        amqp_url, queue, "amqp-client"
    );

    let client = AMQPClient::new(options).await?;
    let method = program.method.unwrap_or("echo".to_string());
    let params = Some(json!(program.args));

    let now = Instant::now();

    for _ in 0..program.repeat {
        let response = client.rpc_request(method.as_str(), params.clone()).await?;

        if program.verbose {
            println!("{:?}", response);
        }
    }

    let elapsed = now.elapsed().as_secs_f64();

    log::info!("Completed {} request(s) in {:.2}s ({:.1}RPS)", program.repeat, elapsed, program.repeat as f64/elapsed);

    Ok(())
}
