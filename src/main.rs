use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::Parser;
use clokwerk::AsyncScheduler;
use config::Strategy;
use log::LevelFilter;

mod symbol;
mod data_structure;
mod client;
pub mod order_types;
pub mod error_types;
pub mod strategy;
mod config;
mod logger;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    name: String,
}

static LOGGER: logger::BotLogger = logger::BotLogger;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))?;


    let config = config::load_config()?;
    let strategy = config.get(&args.name).context(anyhow!("{} is not found in config", args.name))?;
    match strategy {
        Strategy::Shannon(strategy_config) => {
            let mut scheduler = AsyncScheduler::new();
            strategy::shannon_gmo::start_shannon_gmo(&mut scheduler, strategy_config.symbol.clone());
            run_forever(scheduler).await;
        }
    }
    Ok(())
}

async fn run_forever(mut scheduler: AsyncScheduler) {
    loop {
        tokio::spawn(scheduler.run_pending());
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}