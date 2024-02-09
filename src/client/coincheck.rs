use std::{collections::HashMap, str::FromStr, sync::{atomic::AtomicI64, Arc}};
use std::time::Duration as StdDuration;

use anyhow;
use labo::export::{chrono::{Duration, DateTime, Utc, self}};
use hyper::{HeaderMap, header::CONTENT_TYPE, http::HeaderName};
use log::info;
use maplit::hashmap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use labo::export::serde_json::Value;
use tokio::sync::Mutex;

use crate::{symbol::{Symbol, SymbolType, Exchange, Currency}, utils::{time::{UnixTimeUnit, datetime_utc_from_timestamp, deserialize_rfc3339}, serde::deserialize_f64_from_str, useful_traits::{HashMapToHeaderMap, ResultFlatten}}, order_types::Side, data_structure::float_exp::FloatExp};

use super::{method::{get, GetRequest, HasPath, EmptyQueryRequest, post, delete}, types::{KLines, TradeRecord}, credentials::ApiCredentials, auth::coincheck_auth};

static PREV_NONCE: Lazy<Mutex<i64>> = Lazy::new(|| Mutex::new(0));

const NONCE_INTERVAL_MS: i64 = 50;

/// こちらでnonceを被らないように値を足してもサーバー側での到着が逆になればエラーになるので、
/// sleepを入れて順序をつけている
async fn get_nonce() -> i64 {
    let mut nonce = chrono::Utc::now().timestamp_millis();
    let mut prev_nonce = PREV_NONCE.lock().await;
    let curr = nonce;
    if *prev_nonce + NONCE_INTERVAL_MS > nonce {
        nonce = *prev_nonce + NONCE_INTERVAL_MS;
    }
    *prev_nonce = nonce;
    tokio::time::sleep(StdDuration::from_millis((nonce - curr) as u64)).await;
    info!("nonce: {}", nonce);
    nonce
}

pub struct CoincheckClient {
    client: reqwest::Client,
    endpoint: String,
    api_credentials: Option<ApiCredentials>,
}

impl CoincheckClient {
    pub fn new(api_credentials: Option<ApiCredentials>) -> CoincheckClient {
        CoincheckClient {
            client: reqwest::Client::new(),
            endpoint: "https://coincheck.com".to_string(),
            api_credentials,
        }
    }

    pub async fn get_public<S: GetRequest + HasPath>(
        &self,
        query: S,
    ) -> anyhow::Result<S::Response> {
        get(&self.client, &self.endpoint, S::PATH, HeaderMap::new(), query).await
            .map(|x: (_, RestResponse<S::Response>)| x.1.into_result()).flatten_()
    }

    pub async fn get_private<S: GetRequest + HasPath>(&self, query: S) -> anyhow::Result<S::Response> {
        let header = coincheck_auth::<Value>(S::PATH, None, self.api_credentials.as_ref().unwrap(), get_nonce().await)?;
        let res: (_, RestResponse<S::Response>) = get(&self.client, &self.endpoint, S::PATH, header.to_header_map()?, query).await?;
        res.1.into_result()
    }

    pub async fn post<S: Serialize + HasPath>(&self, body: &S) -> anyhow::Result<S::Response> {
        let header = coincheck_auth(S::PATH, Some(body), self.api_credentials.as_ref().unwrap(), get_nonce().await)?;
        let res: (_, RestResponse<S::Response>) = post(&self.client, &self.endpoint, S::PATH, header.to_header_map()?, body).await?;
        res.1.into_result()
    }

    /// pathに引数をもつ特殊APIなので直に実装
    pub async fn cancel_order(&self, id: i64) -> anyhow::Result<CancelOrderResponse> {
        let path = cancel_order_path(id);
        let header = coincheck_auth::<Value>(&path, None, self.api_credentials.as_ref().unwrap(), get_nonce().await)?;
        let res: (_, RestResponse<CancelOrderResponse>) = delete(&self.client, &self.endpoint, &path, header.to_header_map()?).await?;
        res.1.into_result()
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum RestResponse<T> {
    Ok(T),
    Err(RestErrResponse)
}

impl<T> RestResponse<T> {
    pub fn into_result(self) -> anyhow::Result<T> {
        match self {
            RestResponse::Ok(x) => Ok(x),
            RestResponse::Err(x) => Err(anyhow::anyhow!("Received error message: {}", x.error)),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RestErrResponse {
    pub error: String,
    pub success: bool,
}

impl RestErrResponse {
    pub fn is_price_range_error(&self) -> bool {
        self.error.contains("Rate deviates from actual price")
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

pub struct OpenOrderRequest;

impl HasPath for OpenOrderRequest {
    const PATH: &'static str = "/api/exchange/orders/opens";
    type Response = OpenOrderResponse;
}

impl GetRequest for OpenOrderRequest {
    fn to_query(&self) -> HashMap<String, String> {
        hashmap! {}
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrderResponse {
    pub success: bool,
    pub orders: Vec<OpenOrderItem>,
}

// {'success': True, 'orders': [{'id': 5710599665, 'order_type': 'sell', 'rate': '4200000.0', 'pair': 'btc_jpy', 'pending_amount': '0.005', 'pending_market_buy_amount': None, 'stop_loss_rate': None, 'created_at': '2023-07-29T14:23:31.000Z'}]}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrderItem {
    pub id: i64,
    pub order_type: String,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub rate: f64,
    #[serde(deserialize_with = "deserialize_coincheck_pair")]
    pub pair: Symbol,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub pending_amount: f64,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub created_at: DateTime<Utc>,
}

const CANCEL_ORDER_PATH: &str = "/api/exchange/orders/{id}";

pub fn cancel_order_path(id: i64) -> String {
    CANCEL_ORDER_PATH.replace("{id}", &format!("{}", id))
}

#[derive(Debug, Clone, Deserialize)]
pub struct CancelOrderResponse {
    pub success: bool,
    pub id: i64,
}

pub struct BalanceRequest;

impl HasPath for BalanceRequest {
    const PATH: &'static str = "/api/accounts/balance";
    type Response = BalanceResponse;
}

impl EmptyQueryRequest for BalanceRequest {}

#[derive(Debug, Clone, Deserialize)]
pub struct BalanceResponse {
    pub success: bool,
    /// free
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub jpy: f64,
    /// free
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub btc: f64,
    /// used
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub jpy_reserved: f64,
    /// used
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub btc_reserved: f64,
}

fn deserialize_coincheck_pair<'de, D>(deserializer: D) -> Result<Symbol, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let s = s.to_uppercase();
    let cs = s.split("_").collect::<Vec<_>>();
    if cs.len() != 2 {
        return Err(serde::de::Error::custom(format!("invalid pair: {}", s)));
    }
    let (base, quote) = (cs[0], cs[1]);
    let base = Currency::from_str(base).map_err(serde::de::Error::custom)?;
    let quote = Currency::from_str(quote).map_err(serde::de::Error::custom)?;
    Ok(Symbol::new(base, quote, SymbolType::Spot, Exchange::Coincheck))
}

pub struct TransactionsRequest;

impl HasPath for TransactionsRequest {
    const PATH: &'static str = "/api/exchange/orders/transactions";
    type Response = TransactionsResponse;
}

impl EmptyQueryRequest for TransactionsRequest {
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransactionsResponse {
    pub success: bool,
    pub transactions: Vec<TransactionItem>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransactionItem {
    pub id: i64,
    pub order_id: i64,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub created_at: DateTime<Utc>,
    pub funds: TransactionFunds,
    #[serde(deserialize_with = "deserialize_coincheck_pair")]
    pub pair: Symbol,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub rate: f64,
    pub fee_currency: Option<Currency>,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub fee: f64,
    pub liquidity: String,
    pub side: String,
}

/// 減るときは負になっている
#[derive(Debug, Clone, Deserialize)]
pub struct TransactionFunds {
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub btc: f64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub jpy: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "order_type", rename_all = "snake_case")]
pub enum OrderRequest {
    Buy(LimitOrderRequest),
    Sell(LimitOrderRequest),
    MarketBuy(MarketBuyOrderRequest),
    MarketSell(MarketSellOrderRequest),
}

impl OrderRequest {
    pub fn limit_order(side: Side, pair: Symbol, rate: FloatExp, amount: FloatExp, time_in_force: Option<TimeInForce>) -> Self {
        match side {
            Side::Buy => OrderRequest::Buy(LimitOrderRequest {
                pair,
                rate,
                amount,
                time_in_force,
            }),
            Side::Sell => OrderRequest::Sell(LimitOrderRequest {
                pair,
                rate,
                amount,
                time_in_force,
            }),
        }
    }
    /// amountはbuyならJPY, sellならBTC
    pub fn market_order(side: Side, pair: Symbol, amount: FloatExp, time_in_force: Option<TimeInForce>) -> Self {
        match side {
            Side::Buy => OrderRequest::MarketBuy(MarketBuyOrderRequest {
                pair,
                market_buy_amount: amount,
                time_in_force,
            }),
            Side::Sell => OrderRequest::MarketSell(MarketSellOrderRequest {
                pair,
                amount,
                time_in_force,
            }),
        }
    }
}

impl HasPath for OrderRequest {
    const PATH: &'static str = "/api/exchange/orders";
    type Response = RestResponse<OrderResponse>;
}

#[derive(Debug, Clone, Serialize)]
pub struct LimitOrderRequest {
    pub pair: Symbol,
    pub rate: FloatExp,
    pub amount: FloatExp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<TimeInForce>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketBuyOrderRequest {
    pub pair: Symbol,
    /// JPY
    pub market_buy_amount: FloatExp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<TimeInForce>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketSellOrderRequest {
    pub pair: Symbol,
    pub amount: FloatExp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<TimeInForce>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    GoodTillCancelled,
    PostOnly,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    pub success: bool,
    pub id: i64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub rate: f64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub amount: f64,
    pub order_type: String,
    pub time_in_force: String,
    pub stop_loss_rate: Option<String>,
    pub pair: String,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TickerRequest {
    pub pair: Symbol,
}

impl HasPath for TickerRequest {
    const PATH: &'static str = "/api/ticker";
    type Response = TickerResponse;
}

impl GetRequest for TickerRequest {
    fn to_query(&self) -> HashMap<String, String> {
        hashmap! {
            "pair".to_string() => self.pair.to_native(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TickerResponse {
    pub last: f64,
    pub bid: f64,
    pub ask: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub timestamp: i64,
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
    use labo::export::serde_json;
    let s = r#"["btc_jpy",{"bids":[["4246651.0","0"],["4246654.0","0.05"],["4245433.0","0.0114406"]],"asks":[["4255238.0","0"],["4255236.0","0.1"]],"last_update_at":"1690096140"}]"#;
    let obj: WsOrderbookResponse = serde_json::from_str(s).unwrap();
    assert_eq!(obj.symbol, "btc_jpy");
    assert_eq!(obj.bids.len(), 3);
    assert_eq!(obj.asks.len(), 2);
}

#[test]
fn test_deserialize_open_orders() {
    use labo::export::serde_json;
    let s = r#"{"success": true, "orders": [{"id": 5710599665, "order_type": "sell", "rate": "4200000.0", "pair": "btc_jpy", "pending_amount": "0.005", "pending_market_buy_amount": null, "stop_loss_rate": null, "created_at": "2023-07-29T14:23:31.000Z"}]}"#;
    let obj: OpenOrderResponse = serde_json::from_str(s).unwrap();
    assert_eq!(obj.success, true);
    assert_eq!(obj.orders.len(), 1);
    assert_eq!(obj.orders[0].id, 5710599665);
}

#[tokio::test]
async fn test_open_orders() {
    let client = CoincheckClient::new(Some(crate::client::credentials::CREDENTIALS.coincheck.clone()));
    let res: OpenOrderResponse = client.get_private(OpenOrderRequest).await.unwrap();
    println!("{:?}", res);
}

#[tokio::test]
async fn test_balance() {
    let client = CoincheckClient::new(Some(crate::client::credentials::CREDENTIALS.coincheck.clone()));
    let res = client.get_private(BalanceRequest).await.unwrap();
    println!("{:?}", res);
}

#[tokio::test]
async fn test_get_transactions() {
    let client = CoincheckClient::new(Some(crate::client::credentials::CREDENTIALS.coincheck.clone()));
    let _res: TransactionsResponse = client.get_private(TransactionsRequest).await.unwrap();
}

#[tokio::test]
async fn test_post_order() {
    let client = CoincheckClient::new(Some(crate::client::credentials::CREDENTIALS.coincheck.clone()));
    let res = client.post(&OrderRequest::Buy(LimitOrderRequest {
        pair: Symbol::new(Currency::BTC, Currency::JPY, SymbolType::Spot, Exchange::Coincheck),
        rate: FloatExp::from_f64(1000000.0, 0),
        amount: FloatExp::from_f64(0.005, -3),
        time_in_force: None,
    })).await.unwrap();
    println!("{:?}", res);
}