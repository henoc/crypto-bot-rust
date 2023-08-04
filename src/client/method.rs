use std::collections::HashMap;

use hyper::{header::CONTENT_TYPE, http::HeaderName, HeaderMap, StatusCode};
use log::info;
use reqwest::{self, Url, Response};
use serde_json::Value;


pub async fn get<S: GetRequest, T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    header: HeaderMap,
    query: S,
) -> anyhow::Result<(StatusCode, T)> {
    let url_str = format!("{}{}", endpoint, path);
    let url = Url::parse_with_params(&url_str, query.to_query())?;
    let res = client.get(url)
        .headers(header)
        .send()
        .await?;
    let res = error_for_server(res)?;
    let status = res.status();
    let body = res.json().await?;
    Ok((status, body))
}

pub async fn post<S: serde::Serialize, T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    header: HeaderMap,
    body: &S,
) -> anyhow::Result<(StatusCode, T)> {
    let url_str = format!("{}{}", endpoint, path);
    let url = Url::parse(&url_str)?;
    let res = client.post(url)
        .headers(header)
        .json(body)
        .send()
        .await?;
    let res = error_for_server(res)?;
    let status = res.status();
    let body = res.json().await?;
    Ok((status, body))
}

pub async fn post_no_parse<S: serde::Serialize>(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    header: HeaderMap,
    body: &S,
) -> anyhow::Result<StatusCode> {
    let url_str = format!("{}{}", endpoint, path);
    let url = Url::parse(&url_str)?;
    let res = client.post(url)
        .headers(header)
        .json(body)
        .send()
        .await?;
    let res = error_for_server(res)?;
    let status = res.status();
    Ok(status)
}

pub async fn delete<T: serde::de::DeserializeOwned>(client: &reqwest::Client, endpoint: &str, path: &str, header: HeaderMap) -> anyhow::Result<(StatusCode, T)> {
    let url_str = format!("{}{}", endpoint, path);
    let url = Url::parse(&url_str)?;
    let res = client.delete(url)
        .headers(header)
        .send()
        .await?;
    let res = error_for_server(res)?;
    let status = res.status();
    let body = res.json().await?;
    Ok((status, body))
}

pub fn make_header(auth: HashMap<String, String>) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    for (k, v) in auth {
        headers.insert(k.parse::<HeaderName>().unwrap(), v.parse().unwrap());
    }
    headers
}

/// サーバーエラーのときだけErrを投げる
fn error_for_server(response: Response) -> reqwest::Result<Response> {
    if response.status().is_server_error() {
        response.error_for_status()
    } else {
        Ok(response)
    }
}

pub trait GetRequest {
    fn to_query(&self) -> HashMap<String, String>;
    fn to_json(&self) -> Value {
        serde_json::to_value(self.to_query()).unwrap()
    }
}

impl GetRequest for HashMap<String, String> {
    fn to_query(&self) -> HashMap<String, String> {
        self.clone()
    }
}

pub trait EmptyQueryRequest {}

impl<T: EmptyQueryRequest> GetRequest for T {
    fn to_query(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

pub trait HasPath {
    const PATH: &'static str;
    type Response: serde::de::DeserializeOwned;
}
