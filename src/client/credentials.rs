use anyhow::{Context, self};
use once_cell::sync::Lazy;
use serde::Deserialize;


#[derive(Debug, Deserialize)]
pub struct Credentials {
    pub gmo: ApiCredentials,
    pub bitflyer: ApiCredentials,
    pub coincheck: ApiCredentials,
    pub tachibana: TachibanaCredentials,
    pub mail: MailCredentials,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiCredentials {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MailCredentials {
    pub user: String,
    pub password: String,
    pub sendto: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TachibanaCredentials {
    pub user_id: String,
    pub password1: String,
    pub password2: String,
}

pub static CREDENTIALS: Lazy<Credentials> = Lazy::new(|| {
    (|| {
        let config = std::fs::read_to_string("config.yaml")?;
        let config = serde_yaml::from_str::<Credentials>(&config).context("failed to parse config.yaml")?;
        anyhow::Result::<Credentials>::Ok(config)
    })().unwrap()
});

#[test]
fn test_load_config() {
    assert!(CREDENTIALS.gmo.api_key.is_ascii());
}