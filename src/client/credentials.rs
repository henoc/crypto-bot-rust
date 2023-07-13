use once_cell::sync::Lazy;
use serde::Deserialize;


#[derive(Debug, Deserialize)]
pub struct Credentials {
    pub gmo: ApiCredentials,
    pub bitflyer: ApiCredentials,
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

pub static CREDENTIALS: Lazy<Credentials> = Lazy::new(|| {
    let config = std::fs::read_to_string("config.yaml").unwrap();
    let config: Credentials = serde_yaml::from_str(&config).unwrap();
    config
});

#[test]
fn test_load_config() {
    assert!(CREDENTIALS.gmo.api_key.is_ascii());
}