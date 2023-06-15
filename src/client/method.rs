use std::collections::HashMap;

use hyper::{header::CONTENT_TYPE, http::HeaderName, HeaderMap};
use reqwest::{self, Url};



pub async fn get<S: ToQuery, T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    header: HeaderMap,
    query: S,
) -> anyhow::Result<T> {
    let url_str = format!("{}{}", endpoint, path);
    let url = Url::parse_with_params(&url_str, query.to_query())?;
    client.get(url)
        .headers(header)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .map_err(|e| e.into())
}

pub async fn post<S: serde::Serialize, T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    header: HeaderMap,
    body: &S,
) -> anyhow::Result<T> {
    let url_str = format!("{}{}", endpoint, path);
    let url = Url::parse(&url_str)?;
    client.post(url)
        .headers(header)
        .json(body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .map_err(|e| e.into())
}

pub fn make_header(auth: HashMap<String, String>) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    for (k, v) in auth {
        headers.insert(k.parse::<HeaderName>().unwrap(), v.parse().unwrap());
    }
    headers
}

pub trait ToQuery {
    fn to_query(&self) -> HashMap<String, String>;
}

impl ToQuery for HashMap<String, String> {
    fn to_query(&self) -> HashMap<String, String> {
        self.clone()
    }
}

pub struct EmptyQuery;

impl ToQuery for EmptyQuery {
    fn to_query(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

