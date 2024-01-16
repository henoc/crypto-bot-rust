use crate::{config::AbcdfConfig, client::{tachibana::TachibanaClient, credentials::CREDENTIALS}};


pub async fn start_abcdf(_config: &'static AbcdfConfig) {
    let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
}