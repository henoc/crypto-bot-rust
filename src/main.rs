use anyhow::{Context, anyhow};
use clap::Parser;
use config::{Strategy, CrawlerConfig};
use log::LevelFilter;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))?;


    let config = config::load_config()?;
    let strategy = config.get(&args.name).context(anyhow!("{} is not found in config", args.name))?;
    match strategy {
        Strategy::Shannon(strategy_config) => {
            strategy::shannon_gmo::start_shannon_gmo(strategy_config).await;
        },
        Strategy::Crawler(strategy_config) => {
            match strategy_config.symbol.exc {
                Exchange::Coincheck => {
                    strategy::crawler_coincheck::start_crawler_coincheck().await;
                },
                Exchange::Bitflyer => {
                    strategy::crawler_bitflyer::start_crawler_bitflyer(strategy_config, args.check).await;
                },
                _ => {
                    anyhow::bail!("{} is not supported", strategy_config.symbol.exc);
                }
            }
        },
    }
    Ok(())
}
