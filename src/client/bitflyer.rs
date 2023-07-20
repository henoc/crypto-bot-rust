use anyhow::bail;
use chrono::{DateTime, Utc};
use hyper::{Method, HeaderMap, StatusCode};
use maplit::hashmap;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::Value;
use url::Url;

use crate::{order_types::Side, symbol::Symbol, error_types::BotError, data_structure::float_exp::FloatExp, utils::time::datetime_utc};

use super::{credentials::ApiCredentials, method::{make_header, GetRequest, get, post, HasPath, post_no_parse}, auth::bitflyer_auth, types::TradeRecord};

#[derive(Debug, Clone)]
pub struct BitflyerClient {
    client: reqwest::Client,
    endpoint: String,
    api_credentials: Option<ApiCredentials>,
}

impl BitflyerClient {
    pub fn new(api_credentials: Option<ApiCredentials>) -> BitflyerClient {
        BitflyerClient {
            client: reqwest::Client::new(),
            endpoint: "https://api.bitflyer.com".to_string(),
            api_credentials,
        }
    }

    fn make_header<T: serde::Serialize>(&self, method: Method, path: &str, body: Option<&T>) -> anyhow::Result<HeaderMap> {
        let api_credentials = match &self.api_credentials {
            Some(x) => x,
            None => bail!("api_credentials is None"),
        };
        Ok(make_header(bitflyer_auth(method, path, body, api_credentials)?))
    }

    pub async fn get_public<S: GetRequest + HasPath>(
        &self,
        query: S,
    ) -> anyhow::Result<S::Response> {
        let req = query.to_json();
        let res = get(&self.client, &self.endpoint, S::PATH, HeaderMap::new(), query).await;
        catch_response(res, &req)
    }

    pub async fn get_private<S: GetRequest + HasPath>(
        &self,
        query: S,
    ) -> anyhow::Result<S::Response> {
        let url = Url::parse_with_params(format!("{}{}", &self.endpoint, S::PATH).as_str(), query.to_query()).unwrap();
        let header_path = if query.to_query().len() > 0 {
            url.path().to_string() + "?" + url.query().unwrap()
        } else {
            url.path().to_string()
        };
        let req = query.to_json();
        let res = get(&self.client, &self.endpoint, S::PATH, self.make_header::<Value>(Method::GET, &header_path, None)?, query).await;
        catch_response(res, &req)
    }

    pub async fn post<S: serde::Serialize + HasPath>(
        &self,
        body: &S,
    ) -> anyhow::Result<S::Response> {
        let res = post(&self.client, &self.endpoint, S::PATH, self.make_header(Method::POST, S::PATH, Some(&body))?, body).await;
        catch_response(res, &body)
    }

    pub async fn post_no_parse<S: serde::Serialize + HasPath>(
        &self,
        body: &S,
    ) -> anyhow::Result<()> {
        post_no_parse(&self.client, &self.endpoint, S::PATH, self.make_header(Method::POST, S::PATH, Some(&body))?, body).await.map(|_| ())
    }
}

fn catch_response<S: serde::Serialize ,T: DeserializeOwned>(res: anyhow::Result<(StatusCode, Value)>, req: &S) -> anyhow::Result<T> {
    match res {
        Ok((status, value)) => {
            if let Some(message) = value["error_message"].as_str() {
                // margin insufficiency
                if message.contains("Margin amount is insufficient") {
                    bail!(BotError::MarginInsufficiency)
                }
                if message.contains("Market state is closed") {
                    bail!(BotError::Maintenance)
                }
                bail!(BotError::BitflyerClientMessage { status, message: message.to_string(), reqest: serde_json::to_string(req).unwrap_or_default() })
            } else {
                Ok(serde_json::from_value(value)?)
            }
        }
        Err(e) => Err(e),
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct WsResponse {
    pub jsonrpc: String,
    pub method: String,
    pub params: WsResponseParams,
}

#[derive(Deserialize, Debug, Clone)]
pub struct WsResponseParams {
    pub channel: String,
    /// channelが lightning_executionsのときはVec<ExecutionItem>
    pub message: Value,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ExecutionItem {
    pub id: i64,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub exec_date: DateTime<Utc>,
    pub buy_child_order_acceptance_id: String,
    pub sell_child_order_acceptance_id: String,
}

impl ExecutionItem {
    pub fn to_trade_record(&self, symbol: Symbol) -> TradeRecord {
        TradeRecord::new(
            symbol,
            self.exec_date.timestamp_millis(),
            self.price,
            self.size,
            self.side,
        )
    }
}

fn deserialize_rfc3339<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let datetime = DateTime::parse_from_rfc3339(&s)
            .map_err(serde::de::Error::custom)?
            .with_timezone(&Utc);
        Ok(datetime)
    }

#[derive(Deserialize, Debug, Clone)]
pub struct BoardResult {
    pub mid_price: f64,
    pub bids: Vec<PriceSizePair>,
    pub asks: Vec<PriceSizePair>,
}

impl BoardResult {
    pub fn by_side(&self, side: Side) -> &Vec<PriceSizePair> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct PriceSizePair {
    pub price: f64,
    pub size: f64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TickerResult {
    pub product_code: String,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub timestamp: DateTime<Utc>,
    pub tick_id: i64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub best_bid_size: f64,
    pub best_ask_size: f64,
    pub total_bid_depth: f64,
    pub total_ask_depth: f64,
    pub ltp: f64,
    pub volume: f64,
    pub volume_by_product: f64,
}

pub struct GetPositionRequest {
    pub product_code: String,
}

impl HasPath for GetPositionRequest {
    const PATH: &'static str = "/v1/me/getpositions";
    type Response = GetPositionResponse;
}

impl GetRequest for GetPositionRequest {
    fn to_query(&self) -> std::collections::HashMap<String, String> {
        hashmap! {
            "product_code".to_string() => self.product_code.clone(),
        }
    }
}

/// /v1/me/getpositions
pub type GetPositionResponse = Vec<PositionDetail>;

#[derive(Deserialize, Debug, Clone)]
pub struct PositionDetail {
    pub product_code: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub commission: f64,
    pub open_date: String,
    pub swap_point_accumulate: f64,
    pub require_collateral: f64,
    pub leverage: f64,
    pub pnl: f64,
    pub sfd: f64,
}

/// /v1/me/sendchildorder
#[derive(Deserialize, Debug)]
pub struct ChildOrderResponse {
    pub child_order_acceptance_id: String,
}

#[derive(Serialize, Debug, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum ChildOrderType {
    Limit,
    Market,
}

/// /v1/me/sendchildorder
#[derive(Serialize, Debug)]
pub struct ChildOrderRequest {
    pub product_code: String,
    pub child_order_type: ChildOrderType,
    pub side: Side,
    pub price: Option<FloatExp>,
    pub size: FloatExp,
    pub minute_to_expire: Option<u32>,
}

impl HasPath for ChildOrderRequest {
    const PATH: &'static str = "/v1/me/sendchildorder";
    type Response = ChildOrderResponse;
}

/// /v1/me/cancelallchildorders
#[derive(Serialize, Debug)]
pub struct CancelAllOrdersRequest {
    pub product_code: String,
}

impl HasPath for CancelAllOrdersRequest {
    const PATH: &'static str = "/v1/me/cancelallchildorders";
    type Response = ();
}

#[derive(Serialize, Debug)]
pub struct CancelChildOrderRequest {
    pub product_code: String,
    pub child_order_acceptance_id: String,
}

impl HasPath for CancelChildOrderRequest {
    const PATH: &'static str = "/v1/me/cancelchildorder";
    type Response = ();
}

#[derive(Serialize, Debug)]
pub struct GetCollateralRequest;

impl GetRequest for GetCollateralRequest {
    fn to_query(&self) -> std::collections::HashMap<String, String> {
        hashmap! {}
    }
}

impl HasPath for GetCollateralRequest {
    const PATH: &'static str = "/v1/me/getcollateral";
    type Response = GetCollateralResponse;
}

#[derive(Deserialize, Debug)]
pub struct GetCollateralResponse {
    pub collateral: f64,
    pub open_position_pnl: f64,
    pub require_collateral: f64,
    pub keep_rate: f64,
}

#[derive(Serialize, Debug)]
pub struct TickerRequest {
    pub product_code: String,
}

impl GetRequest for TickerRequest {
    fn to_query(&self) -> std::collections::HashMap<String, String> {
        hashmap! {
            "product_code".to_string() => self.product_code.clone(),
        }
    }
}

impl HasPath for TickerRequest {
    const PATH: &'static str = "/v1/getticker";
    type Response = TickerResponse;
}

#[derive(Deserialize, Debug)]
pub struct TickerResponse {
    pub product_code: String,
    pub timestamp: String,
    pub tick_id: i64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub best_bid_size: f64,
    pub best_ask_size: f64,
    pub total_bid_depth: f64,
    pub total_ask_depth: f64,
    pub ltp: f64,
    pub volume: f64,
    pub volume_by_product: f64,
}

#[test]
fn test_ticker_result() {
    use chrono::Datelike;
    let obj = serde_json::json!({
        "product_code": "BTC_JPY",
        "timestamp": "2019-04-11T05:14:12.3739915Z",
        "state": "RUNNING",
        "tick_id": 25965446,
        "best_bid": 580006,
        "best_ask": 580771,
        "best_bid_size": 2.00000013,
        "best_ask_size": 0.4,
        "total_bid_depth": 1581.64414981,
        "total_ask_depth": 1415.32079982,
        "market_bid_size": 0,
        "market_ask_size": 0,
        "ltp": 580790,
        "volume": 6703.96837634,
        "volume_by_product": 6703.96837634
      });
    let res: TickerResult = serde_json::from_value(obj).unwrap();
    assert_eq!(res.product_code, "BTC_JPY");
    assert_eq!(res.timestamp.year(), 2019);
}