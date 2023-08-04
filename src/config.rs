use std::collections::HashMap;

use anyhow::Result;
use chrono::Duration;
use serde::{Deserialize, Deserializer};

use crate::{symbol::{Symbol}, utils::tracingmm_utils::PriceInOut};

pub type Config = HashMap<String, Strategy>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "strategy")]
pub enum Strategy {
    Shannon(ShannonConfig),
    TracingMm(TracingMMConfig),
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
    pub symbols: Vec<Symbol>,
    pub kline_builder: Vec<KLineBuilderConfig>
}

#[derive(Debug, Deserialize, Clone)]
pub struct KLineBuilderConfig {
    pub timeframe: Timeframe,
    pub len: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct Timeframe(pub Duration);

impl<'de> Deserialize<'de> for Timeframe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // "\d+s" or "\d+m" or "\d+h"
        let s = String::deserialize(deserializer)?;
        let duration = match s.chars().last().unwrap() {
            's' => Duration::seconds(s[0..s.len()-1].parse::<i64>().unwrap()),
            'm' => Duration::minutes(s[0..s.len()-1].parse::<i64>().unwrap()),
            'h' => Duration::hours(s[0..s.len()-1].parse::<i64>().unwrap()),
            _ => panic!("invalid timeframe"),
        };
        Ok(Timeframe(duration))
    }
}

impl Into<Duration> for Timeframe {
    fn into(self) -> Duration {
        self.0
    }
}

#[derive(Debug, Deserialize)]
pub struct TracingMMConfig {
    pub symbol: Symbol,
    pub timeframe: Timeframe,
    pub leverage: f64,

    #[serde(default = "max_side_positions_default")]
    pub max_side_positions: i64,

    pub ref_symbol: Symbol,
    pub atr_period: i64,
    pub beta: PriceInOut,
    pub gamma: PriceInOut,
    pub losscut_rate: Option<f64>,
    /// timeframeで何フレームか
    pub exit_mean_frame: i32,
}

fn max_side_positions_default() -> i64 {
    3
}