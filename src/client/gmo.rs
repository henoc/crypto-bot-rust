use std::str::FromStr;

use anyhow::{bail, self};
use labo::export::chrono::{DateTime, Utc};
use hyper::{Method, HeaderMap};
use serde::{Deserialize, Serialize, Deserializer};
use labo::export::serde_json::{Value};

use crate::{symbol::{Symbol, Currency, SymbolType, Exchange}, order_types::{Side, OrderType}, error_types::BotError, utils::{time::deserialize_rfc3339, serde::deserialize_f64_from_str}};

use super::{method::{make_header, get, post, GetRequest, EmptyQueryRequest, HasPath}, credentials::ApiCredentials, auth::gmo_coin_auth};

#[derive(Debug, Clone)]
pub struct GmoClient {
    client: reqwest::Client,
    public_endpoint: String,
    private_endpoint: String,
    api_credentials: Option<ApiCredentials>,
}

impl GmoClient {
    pub fn new(api_credentials: Option<ApiCredentials>) -> GmoClient {
        GmoClient {
            client: reqwest::Client::new(),
            public_endpoint: "https://api.coin.z.com/public".to_string(),
            private_endpoint: "https://api.coin.z.com/private".to_string(),
            api_credentials,
        }
    }

    fn make_header<T: serde::Serialize>(&self, method: Method, path: &str, body: Option<&T>) -> anyhow::Result<HeaderMap> {
        let api_credentials = match &self.api_credentials {
            Some(x) => x,
            None => bail!("api_credentials is None"),
        };
        Ok(make_header(gmo_coin_auth(method, path, body, api_credentials)?))
    }

    pub async fn get_public<S: GetRequest, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: S,
    ) -> anyhow::Result<T> {
        get(&self.client, &self.public_endpoint, path, HeaderMap::new(), query).await
            .map(|x| x.1)
    }

    pub async fn get_private<S: GetRequest, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: S,
    ) -> anyhow::Result<T> {
        get(&self.client, &self.private_endpoint, path, self.make_header::<Value>(Method::GET, path, None)?, query).await
            .map(|x| x.1)
    }

    pub async fn post<S: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &S,
    ) -> anyhow::Result<T> {
        post(&self.client, &self.private_endpoint, path, self.make_header(Method::POST, path, Some(&body))?, body).await
            .map(|x| x.1)
    }
}

#[derive(Debug, Deserialize)]
pub struct GmoClientResponse<T> {
    pub status: u16,
    pub data: Option<T>,
    pub messages: Option<Vec<GmoClientResponseMessage>>,
    pub responsetime: Option<String>,
}

impl <T> GmoClientResponse<T> {
    pub fn into_result(self) -> anyhow::Result<T> {
        match self.data {
            Some(data) => Ok(data),
            _ => {
                match self.messages {
                    Some(messages) if messages[0].message_string.to_lowercase().contains("maintenance") => Err(BotError::Maintenance.into()),
                    Some(messages) => Err(BotError::GmoClientMessage { code: messages[0].message_code.clone(), message: messages[0].message_string.clone() }.into()),
                    _ => Err(BotError::GmoClientMessage { code: "unknown".to_string(), message: "unknown".to_string() }.into()),
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct GmoClientResponseMessage {
    pub message_code: String,
    pub message_string: String,
}

pub struct AccountAssetsRequest;

impl EmptyQueryRequest for AccountAssetsRequest {
}

impl HasPath for AccountAssetsRequest {
    const PATH: &'static str = "/v1/account/assets";
    type Response = AccountAssets;
}

pub type AccountAssets = Vec<AccountAsset>;

#[derive(Debug, Deserialize)]
pub struct AccountAsset {
    pub amount: String,
    pub available: String,
    pub symbol: String,
}

pub type Tickers = Vec<Ticker>;

#[derive(Debug, Deserialize)]
pub struct Ticker {
    pub last: String
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GmoTimeInForce {
    /**
     * post-only
     */
    SOK,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    pub symbol: Symbol,
    pub side: Side,
    pub execution_type: OrderType,
    pub size: String,
    pub price: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<GmoTimeInForce>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsResponse {
    Ok(WsOkResponse),
    Err(WsErrResponse),
}

#[derive(Debug, Deserialize)]
/// {"error":"ERR-5003 Request too many."}
pub struct WsErrResponse {
    pub error: String,
}

impl WsErrResponse {
    pub fn is_too_many_request(&self) -> bool {
        self.error.contains("Request too many")
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "channel", rename_all = "camelCase")]
pub enum WsOkResponse {
    Orderbooks(OrderbooksResult),
}

#[derive(Debug, Deserialize)]
pub struct OrderbooksResult {
    pub asks: Vec<PriceSizePair>,
    pub bids: Vec<PriceSizePair>,
    #[serde(deserialize_with = "deserialize_gmo_symbol")]
    pub symbol: Symbol,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct PriceSizePair {
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub price: f64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub size: f64,
}

fn deserialize_gmo_symbol<'de, D>(deserializer: D) -> Result<Symbol, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let cs = s.split("_").collect::<Vec<_>>();
        if cs.len() >= 3 || cs.len() == 0 {
            return Err(serde::de::Error::custom("invalid symbol"));
        }
        let (base, quote, r#type) = if cs.len() == 1 {
            (Currency::from_str(cs[0]).map_err(serde::de::Error::custom)?,
                Currency::JPY,
                SymbolType::Spot)
        } else {
            (Currency::from_str(cs[0]).map_err(serde::de::Error::custom)?,
                Currency::from_str(cs[1]).map_err(serde::de::Error::custom)?,
                SymbolType::Perp)
        };
        Ok(Symbol::new(base, quote, r#type, Exchange::Gmo))
    }

#[tokio::test]
async fn test_gmo_client_account_assets() {
    use super::credentials::CREDENTIALS;
    let gmo = GmoClient::new(Some(CREDENTIALS.gmo.clone()));
    let res: GmoClientResponse<AccountAssets> = gmo.get_private("/v1/account/assets", AccountAssetsRequest {}).await.unwrap();
    println!("{:?}", res);
    println!("{:?}", res.into_result());
}

#[tokio::test]
async fn test_gmo_client_order() {
    use crate::symbol::SymbolType;
    use crate::symbol::Exchange;
    use crate::symbol::Currency;
    use super::credentials::CREDENTIALS;
    let gmo = GmoClient::new(Some(CREDENTIALS.gmo.clone()));
    let symbol = Symbol::new(Currency::BTC, Currency::JPY, SymbolType::Spot, Exchange::Gmo);
    let res: Value = gmo.post("/v1/order", &CreateOrderRequest {
        symbol,
        side: Side::Buy,
        execution_type: OrderType::Limit,
        size: "0.001".to_string(),
        price: "2000000".to_string(),
        time_in_force: None,
    }).await.unwrap();
    println!("{:?}", res);
}

#[test]
fn test_ws_response() {
    use labo::export::serde_json;
    let s = r#"{"channel":"orderbooks","symbol":"BTC_JPY","timestamp":"2021-08-01T12:00:00.000Z","bids":[{"price":"1000000","size":"0.1"},{"price":"2000000","size":"0.2"}],"asks":[{"price":"3000000","size":"0.3"},{"price":"4000000","size":"0.4"}]}"#;
    let parsed: WsResponse = serde_json::from_str(s).unwrap();
    assert!(matches!(parsed, WsResponse::Ok(_)));
    let s = r#"{"error":"ERR-5003 Request too many."}"#;
    let parsed: WsResponse = serde_json::from_str(s).unwrap();
    assert!(matches!(parsed, WsResponse::Err(_)));
}