use std::env;
use std::error::Error;
use std::num::ParseIntError;
use std::time::Duration;
use std::time::Instant;

use clap::Clap;
use dotenv::dotenv;
use serde_json::json;

use skein_rpc::Client;
use skein_rpc::amqp::Client as AMQPClient;
use skein_rpc::amqp::ClientOptions as AMQPClientOptions;
use skein_rpc::logging;

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
struct Program {
    #[clap(short, long, parse(from_occurrences))]
    verbose: usize,
    #[clap(short,long)]
    env_file : Option<String>,
    #[clap(short,long)]
    amqp_url : Option<String>,
    #[clap(short,long)]
    queue : Option<String>,
    #[clap(short,long)]
    silent: bool,
    #[clap(short='t',long)]
    report: bool,
    #[clap(short,long,default_value="1")]
    repeat: usize,
    #[clap(long)]
    ident: Option<String>,
    #[clap(short,long,default_value="10",parse(try_from_str=Self::try_into_duration))]
    timeout : Duration,
    #[clap(long)]
    noreply : bool,
    method : String,
    #[clap(multiple=true)]
    args : Vec<String>
}

impl Program {
    fn try_into_duration(s: &str) -> Result<Duration, ParseIntError> {
        s.parse().map(Duration::from_secs)
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

    logging::setup(program.verbose);

    let options = AMQPClientOptions::new(
        program.amqp_url.unwrap_or_else(|| env::var("AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672/%2f".to_string())),
        program.queue.unwrap_or_else(|| env::var("AMQP_QUEUE").unwrap_or_else(|_| "skein_test".to_string())),
        program.ident.unwrap_or_else(|| "amqp-client".to_string())
    ).with_timeout(program.timeout);

    // skein_test

    let client = AMQPClient::new(options).await?;
    let method = program.method;
    let params = Some(json!(program.args));

    let now = Instant::now();

    if program.noreply {
        for _ in 0..program.repeat {
            match client.rpc_request_noreply(method.as_str(), params.clone()).await {
                Ok(()) => { },
                Err(err) => {
                    log::error!("Error: {}", err);
                }
            }
        }
    }
    else {
        for _ in 0..program.repeat {
            match client.rpc_request(method.as_str(), params.clone()).await {
                Ok(response) => {
                    if !program.silent {
                        println!("{}", response.to_string());
                    }
                },
                Err(err) => {
                    log::error!("Error: {}", err);
                }
            }
        }
    }

    client.into_handle().await.ok();

    if program.report {
        let elapsed = now.elapsed().as_secs_f64();

        log::info!("Completed {} request(s) in {:.2}s ({:.1}RPS)", program.repeat, elapsed, program.repeat as f64/elapsed);
    }

    Ok(())
}
