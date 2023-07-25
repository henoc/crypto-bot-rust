use std::collections::{HashMap, BTreeMap};

use chrono::{Duration, DateTime, Utc};
use hyper::HeaderMap;
use maplit::hashmap;
use serde::{Deserialize, Deserializer};

use crate::{symbol::{Symbol, SymbolType, Exchange, Currency}, utils::time::{UnixTimeUnit, datetime_utc_from_timestamp}, order_types::Side, data_structure::float_exp::FloatExp};

use super::{method::{get, GetRequest, HasPath}, types::{KLines, TradeRecord}};

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

    pub async fn get<S: GetRequest + HasPath>(
        &self,
        query: S,
    ) -> anyhow::Result<S::Response> {
        get(&self.client, &self.endpoint, S::PATH, HeaderMap::new(), query).await
            .map(|x| x.1)
    }
}

pub struct KLineRequest {
    pub symbol: Symbol,
    pub timeframe: Duration,
    pub limit: i64, 
}

impl HasPath for KLineRequest {
    const PATH: &'static str = "/api/charts/candle_rates";
    type Response = KLineResponse;
}

impl GetRequest for KLineRequest {
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
        let ret = KLines::new_options(&self.0, UnixTimeUnit::Second)?;
        Ok(ret.sorted()?.reindex(until, timeframe)?)
    }
}

pub struct OrderbookRequest;

impl HasPath for OrderbookRequest {
    const PATH: &'static str = "/api/order_books";
    type Response = OrderbookResponse;
}

impl GetRequest for OrderbookRequest {
    fn to_query(&self) -> HashMap<String, String> {
        hashmap! {}
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderbookResponse {
    pub asks: Vec<PriceSizePair>,
    pub bids: Vec<PriceSizePair>,
}

impl OrderbookResponse {
    pub fn by_side(&self, side: Side) -> &Vec<PriceSizePair> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsResponse {
    Orderbook(WsOrderbookResponse),
    Trade(WsTradesResponse),
}

// [[unixtime_sec, id, pair, price, volume, taker_side, taker_id, maker_id]]
// [['1687419859', '246596088', 'btc_jpy', '4262466.0', '0.0198', 'sell', '5634315383', '5634315372'],
//  ['1687419859', '246596087', 'btc_jpy', '4262616.0', '0.005', 'sell', '5634315383', '5634315362']]

#[derive(Debug)]
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

// ['btc_jpy', {'bids': [['4246651.0', '0'], ['4246654.0', '0.05'], ['4245433.0', '0.0114406']], 'asks': [['4255238.0', '0'], ['4255236.0', '0.1']], 'last_update_at': '1690096140'}]

#[derive(Debug, Clone)]
pub struct WsOrderbookResponse {
    pub symbol: String,
    pub bids: Vec<PriceSizePair>,
    pub asks: Vec<PriceSizePair>,
    pub last_update_at: DateTime<Utc>,
}

impl WsOrderbookResponse {
    pub fn by_side(&self, side: Side) -> &Vec<PriceSizePair> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }
}

impl <'de> Deserialize<'de> for WsOrderbookResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    { 
        let (symbol, obj) = <(String, WsOrderbookObject)>::deserialize(deserializer)?;
        Ok(WsOrderbookResponse {
            symbol,
            bids: obj.bids,
            asks: obj.asks,
            last_update_at: obj.last_update_at,
        })
    }
}

#[derive(Debug, Deserialize, Clone)]
struct WsOrderbookObject {
    bids: Vec<PriceSizePair>,
    asks: Vec<PriceSizePair>,
    #[serde(deserialize_with = "deserialize_unixtime_sec")]
    last_update_at: DateTime<Utc>,
}

fn deserialize_unixtime_sec<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{ 
    let v = String::deserialize(deserializer)?;
    let v = v.parse::<i64>().map_err(serde::de::Error::custom)?;
    Ok(datetime_utc_from_timestamp(v, UnixTimeUnit::Second))
}

#[derive(Debug, Clone)]
pub struct PriceSizePair {
    pub price: f64,
    pub size: f64,
}

impl <'de> Deserialize<'de> for PriceSizePair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    { 
        let (price, size) = <(String, String)>::deserialize(deserializer)?;
        let (price, size) = (price.parse::<f64>().map_err(serde::de::Error::custom)?, size.parse::<f64>().map_err(serde::de::Error::custom)?);
        Ok(PriceSizePair { price, size })
    }
}

#[test]
fn test_deserialize_ws_orderbook() {
    let s = r#"["btc_jpy",{"bids":[["4246651.0","0"],["4246654.0","0.05"],["4245433.0","0.0114406"]],"asks":[["4255238.0","0"],["4255236.0","0.1"]],"last_update_at":"1690096140"}]"#;
    let obj: WsOrderbookResponse = serde_json::from_str(s).unwrap();
    assert_eq!(obj.symbol, "btc_jpy");
    assert_eq!(obj.bids.len(), 3);
    assert_eq!(obj.asks.len(), 2);
}