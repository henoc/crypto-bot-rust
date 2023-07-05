use std::collections::HashMap;

use chrono::{Duration, DateTime, Utc};
use hyper::HeaderMap;
use maplit::hashmap;
use serde::{Deserialize, Deserializer};

use crate::{symbol::{Symbol, SymbolType, Exchange, Currency}, utils::time::KLinesTimeUnit, order_types::Side};

use super::{method::{get, ToQuery}, types::{KLines, TradeRecord}};

pub struct CoincheckClient {
    client: reqwest::Client,
    endpoint: String,
}

impl CoincheckClient {
    pub fn new() -> CoincheckClient {
        CoincheckClient {
            client: reqwest::Client::new(),
            endpoint: "https://coincheck.com".to_string(),
        }
    }

    pub async fn get<S: ToQuery,T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: S,
    ) -> anyhow::Result<T> {
        get(&self.client, &self.endpoint, path, HeaderMap::new(), query).await
    }
}

pub struct KLineRequest {
    pub symbol: Symbol,
    pub timeframe: Duration,
    pub limit: i64, 
}

impl ToQuery for KLineRequest {
    fn to_query(&self) -> HashMap<String, String> {
        hashmap! {
            "pair".to_string() => self.symbol.to_native(),
            "unit".to_string() => format!("{}", self.timeframe.num_seconds()),
            "market".to_string() => "coincheck".to_string(),
            "limit".to_string() => format!("{}", self.limit),
            "v2".to_string() => "true".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KLineResponse(pub Vec<Vec<Option<f64>>>);

impl<'de> Deserialize<'de> for KLineResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Vec::<Vec<Option<f64>>>::deserialize(deserializer)?;
        Ok(KLineResponse(v))
    }
}

impl KLineResponse {
    ///
    /// [1685932020,null,null,null,null,0.0], の場合がある
    pub fn to_klines(&self, until: DateTime<Utc>, timeframe: Duration) -> anyhow::Result<KLines> {
        let mut ret = KLines::new_options(&self.0, KLinesTimeUnit::Second)?;
        ret.sort()?;
        ret.reindex(until, timeframe)?;
        Ok(ret)
    }
}

// [[unixtime_sec, id, pair, price, volume, taker_side, taker_id, maker_id]]
// [['1687419859', '246596088', 'btc_jpy', '4262466.0', '0.0198', 'sell', '5634315383', '5634315372'],
//  ['1687419859', '246596087', 'btc_jpy', '4262616.0', '0.005', 'sell', '5634315383', '5634315362']]

pub struct WsTradesResponse(pub Vec<Vec<String>>);

impl<'de> Deserialize<'de> for WsTradesResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    { 
        let v = Vec::<Vec<String>>::deserialize(deserializer)?;
        Ok(WsTradesResponse(v))
    }
}

impl WsTradesResponse {
    pub fn to_trade_records(&self,) -> anyhow::Result<Vec<TradeRecord>> {
        let mut ret = vec![];
        for item in &self.0 {
            ret.push(TradeRecord::new(
                Symbol::new(Currency::BTC, Currency::JPY, SymbolType::Spot, Exchange::Coincheck),
                item[0].parse::<i64>()? * 1000,
                item[3].parse::<f64>()?,
                item[4].parse::<f64>()?,
                if item[5] == "sell" { Side::Sell } else { Side::Buy },
            ))
        }
        Ok(ret)
    }
}