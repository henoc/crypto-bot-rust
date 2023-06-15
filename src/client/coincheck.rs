use std::collections::HashMap;

use hyper::HeaderMap;
use maplit::hashmap;

use crate::symbol::Symbol;

use super::{method::{get, ToQuery}, types::KLines};

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
    pub timeframe_sec: i64,
    pub limit: i64, 
}

impl ToQuery for KLineRequest {
    fn to_query(&self) -> HashMap<String, String> {
        hashmap! {
            "pair".to_string() => self.symbol.to_native(),
            "unit".to_string() => format!("{}", self.timeframe_sec),
            "market".to_string() => "coincheck".to_string(),
            "limit".to_string() => format!("{}", self.limit),
            "v2".to_string() => "true".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct KLineResponse(pub Vec<Vec<Option<f64>>>);

impl KLineResponse {
    ///
    /// [1685932020,null,null,null,null,0.0], の場合がある
    pub fn to_klines(&self) -> anyhow::Result<KLines> {
        let mut klines = KLines::empty();
        // for row in &self.0 {
        //     klines.push(row[0].unwrap() as i64, row[1], row[2], row[3], row[4], row[5], TimeUnit::Second);
        // }
        Ok(klines)
    }
}

