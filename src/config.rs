use std::collections::HashMap;

use anyhow::Result;
use serde::Deserialize;

use crate::symbol::{Symbol, Exchange};

pub type Config = HashMap<String, Strategy>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "strategy")]
pub enum Strategy {
    Shannon(ShannonConfig),
    Crawler(CrawlerConfig),
}

#[derive(Debug, Deserialize)]
pub struct ShannonConfig {
    pub symbol: Symbol,
    pub virtual_amount: VirtualAmount,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VirtualAmount {
    pub base: f64,
    pub quote: f64,
}

pub fn load_config() -> Result<Config> {
    let config = std::fs::read_to_string("config.bot.yaml").unwrap();
    let config: Config = serde_yaml::from_str(&config).unwrap();
    Ok(config)
}

#[derive(Debug, Deserialize)]
pub struct CrawlerConfig {
    pub exc: Exchange,
}