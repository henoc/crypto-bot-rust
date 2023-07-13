use std::{collections::HashMap, env};

use anyhow::{Context, anyhow};
use clap::Parser;
use config::{Strategy, CrawlerConfig};
use log::LevelFilter;
use once_cell::sync::Lazy;
use symbol::Exchange;

mod symbol;
mod data_structure;
mod client;
pub mod order_types;
pub mod error_types;
pub mod strategy;
mod config;
mod logger;
mod utils;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    name: String,
    #[clap(short, long)]
    check: bool,
}

static LOGGER: logger::BotLogger = logger::BotLogger;
static CONFIG: Lazy<HashMap<String,Strategy>> = Lazy::new(|| {
    config::load_config().unwrap()
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))?;

    env::set_var("NAME", &args.name);

    let strategy = CONFIG.get(&args.name).context(anyhow!("{} is not found in config", args.name))?;
    match strategy {
        Strategy::Shannon(strategy_config) => {
            strategy::shannon_gmo::start_shannon_gmo(strategy_config).await;
        },
        Strategy::TracingMm(strategy_config) => {
            strategy::tracingmm_bitflyer::start_tracingmm_bitflyer(strategy_config, args.check).await;
        },
        Strategy::Crawler(strategy_config) => {
            match strategy_config.symbol.exc {
                Exchange::Coincheck => {
                    strategy::crawler_coincheck::start_crawler_coincheck().await;
                },
                Exchange::Bitflyer => {
                    strategy::crawler_bitflyer::start_crawler_bitflyer(strategy_config, args.check).await;
                },
                Exchange::Binance => {
                    strategy::crawler_binance::start_crawler_binance(strategy_config, args.check).await;
                },
                _ => {
                    anyhow::bail!("{} is not supported", strategy_config.symbol.exc);
                }
            }
        },
    }
    Ok(())
}
