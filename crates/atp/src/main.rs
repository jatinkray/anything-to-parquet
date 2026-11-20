use std::process::ExitCode;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cli;
mod ui;

#[tokio::main]
async fn main() -> ExitCode {
    let filter =
        EnvFilter::try_from_env("ATP_LOG").unwrap_or_else(|_| EnvFilter::new("warn,atp_core=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .without_time()
        .init();

    let args = cli::Cli::parse();
    match cli::run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("atp: {err}");
            if let Some(source) = std::error::Error::source(&err) {
                eprintln!("  caused by: {source}");
            }
            ExitCode::from(err.exit_code() as u8)
        }
    }
}
