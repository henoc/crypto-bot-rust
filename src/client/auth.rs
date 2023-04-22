use std::collections::HashMap;

use hyper::Method;
use ring::hmac;

use super::{credentials::ApiCredentials};

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