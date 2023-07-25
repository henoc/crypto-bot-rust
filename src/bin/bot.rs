use std::{collections::HashMap, env};

use anyhow::{Context, anyhow};
use clap::Parser;
use bot::{config::{Strategy, self}, logger, global_vars::DEBUG};
use log::LevelFilter;
use once_cell::sync::Lazy;
use bot::symbol::Exchange;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    name: String,
    #[clap(short, long)]
    debug: bool,
}

static LOGGER: logger::BotLogger = logger::BotLogger;
static CONFIG: Lazy<HashMap<String,Strategy>> = Lazy::new(|| {
    config::load_config().unwrap()
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !args.debug {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Info))?;
    }

    env::set_var("NAME", &args.name);
    DEBUG.set(args.debug).unwrap();

    let strategy = CONFIG.get(&args.name).context(anyhow!("{} is not found in config", args.name))?;
    match strategy {
        Strategy::Shannon(strategy_config) => {
            bot::strategy::shannon_gmo::start_shannon_gmo(strategy_config).await;
        },
        Strategy::TracingMm(strategy_config) => {
            bot::strategy::tracingmm_bitflyer::start_tracingmm_bitflyer(strategy_config).await;
        },
        Strategy::Crawler(strategy_config) => {
            #[allow(unreachable_patterns)]
            match strategy_config.symbols[0].exc {
                Exchange::Coincheck => {
                    bot::strategy::crawler_coincheck::start_crawler_coincheck().await;
                },
                Exchange::Bitflyer => {
                    bot::strategy::crawler_bitflyer::start_crawler_bitflyer(strategy_config).await;
                },
                Exchange::Binance => {
                    bot::strategy::crawler_binance::start_crawler_binance(strategy_config).await;
                },
                Exchange::Gmo => {
                    bot::strategy::crawler_gmo::start_crawler_gmo(strategy_config).await;
                }
                _ => {
                    anyhow::bail!("{} is not supported", strategy_config.symbols[0].exc);
                }
            }
        },
    }
    Ok(())
}
