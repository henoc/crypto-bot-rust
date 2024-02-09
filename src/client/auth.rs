use std::collections::HashMap;

use anyhow;
use labo::export::{chrono, serde_json};
use maplit::hashmap;
use hyper::Method;
use ring::hmac;

use super::credentials::ApiCredentials;

// pybotters > auth.py を見ると参考になる

/**
 * path: /v1/path
 */
pub fn gmo_coin_auth<T: serde::Serialize>(method: Method, path: &str, body: Option<&T>, api_key_secret: &ApiCredentials) -> anyhow::Result<HashMap<String, String>> {
    let timestamp = chrono::Utc::now().timestamp_millis().to_string();
    let text = if body.is_none() {format!("{}{}{}", timestamp, method, path)} else {format!("{}{}{}{}", timestamp, method, path, serde_json::to_string(body.unwrap())?)};
    let api_secret = &api_key_secret.api_secret;
    let signed_key = hmac::Key::new(hmac::HMAC_SHA256, api_secret.as_bytes());
    let signature = hex::encode(hmac::sign(&signed_key, text.as_bytes()).as_ref());
    
    let mut headers = HashMap::new();
    let api_key = &api_key_secret.api_key;
    headers.insert("API-KEY".to_string(), api_key.clone());
    headers.insert("API-TIMESTAMP".to_string(), timestamp);
    headers.insert("API-SIGN".to_string(), signature);
    Ok(headers)
}

pub fn bitflyer_auth<T: serde::Serialize>(method: Method, path: &str, body: Option<&T>, api_key_secret: &ApiCredentials) -> anyhow::Result<HashMap<String, String>> {
    let timestamp = chrono::Utc::now().timestamp().to_string();
    let data = if body.is_none() {format!("{}{}{}", timestamp, method, path)} else {format!("{}{}{}{}", timestamp, method, path, serde_json::to_string(body.unwrap())?)};
    let key = hmac::Key::new(hmac::HMAC_SHA256, api_key_secret.api_secret.as_bytes());
    let signature = hmac::sign(&key, data.as_bytes());
    let sign = hex::encode(signature.as_ref());
    let mut headers = HashMap::new();
    headers.insert("ACCESS-KEY".to_string(), api_key_secret.api_key.clone());
    headers.insert("ACCESS-TIMESTAMP".to_string(), timestamp);
    headers.insert("ACCESS-SIGN".to_string(), sign);
    Ok(headers)
}

const COINCHECK_BASE_URL: &str = "https://coincheck.com";

pub fn coincheck_auth<T: serde::Serialize>(path: &str, body: Option<&T>, api_key_secret: &ApiCredentials, nonce: i64) -> anyhow::Result<HashMap<String, String>> {
    // get body form-data string from body
    let body = match body {
        Some(x) => serde_json::to_string(x)?,
        None => "".to_string(),
    };
    let message = format!("{nonce}{COINCHECK_BASE_URL}{path}{body}");
    let signature = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, api_key_secret.api_secret.as_bytes()), message.as_bytes());
    Ok(hashmap! {
        "ACCESS-KEY".to_string() => api_key_secret.api_key.clone(),
        "ACCESS-NONCE".to_string() => nonce.to_string(),
        "ACCESS-SIGNATURE".to_string() => hex::encode(signature.as_ref()),
    })
}