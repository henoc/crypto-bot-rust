use std::sync::atomic::AtomicU32;

use chrono::{DateTime, Utc, FixedOffset};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{json, Value, Map};
use url::Url;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use encoding_rs::SHIFT_JIS;

use crate::{utils::{time::JST, serde::{deserialize_f64_from_str, serialize_u32_to_str}, useful_traits::StaticVarExt, json_utils::object_update}, symbol::Currency};

use super::credentials::TachibanaCredentials;

static SESSION_ID: OnceCell<RwLock<String>> = OnceCell::new();
static PREV_NONCE: AtomicU32 = AtomicU32::new(1);

#[cfg(test)]
const ENDPOINT: &str = "https://demo-kabuka.e-shiten.jp/e_api_v4r5/";
#[cfg(not(test))]
const ENDPOINT: &str = "https://kabuka.e-shiten.jp/e_api_v4r5/";

pub struct TachibanaClient {
    client: reqwest::Client,
    api_credentials: TachibanaCredentials,
}

impl TachibanaClient {
    pub fn new(api_credentials: TachibanaCredentials) -> TachibanaClient {
        TachibanaClient {
            client: reqwest::Client::new(),
            api_credentials,
        }
    }

    pub async fn login(&self) -> anyhow::Result<()> {
        let params = LoginRequest {
            s_user_id: self.api_credentials.user_id.clone(),
            s_password: self.api_credentials.password1.clone()
        }.to_json();
        let mut url = Url::parse(ENDPOINT)?.join( LoginRequest::PATH)?;
        let query = utf8_percent_encode(params.to_string().as_str(), NON_ALPHANUMERIC).to_string();
        url.set_query(Some(&query));
        let res = self.client.get(url).send().await?;
        let body = res.bytes().await?;
        let body = SHIFT_JIS.decode(&body).0.into_owned();
        let res = serde_json::from_str::<RestResponse<LoginResponse>>(body.as_str())?;
        let session_id = Url::parse(&res.0.s_url_request)?.path_segments().unwrap().nth(2).unwrap().to_string();
        SESSION_ID.get_or_init(|| RwLock::new(session_id));
        Ok(())
    }

    pub async fn logout(&self) -> anyhow::Result<()> {
        self.send(LogoutRequest).await.map(|_| ())
    }

    pub async fn send<S: TachibanaRequest>(&self, query: S) -> anyhow::Result<S::Response> {
        let params = query.to_json();
        let mut url = Url::parse(ENDPOINT)?.join( S::PATH)?.join(format!("{}/", SESSION_ID.read()).as_str())?;
        let query = utf8_percent_encode(params.to_string().as_str(), NON_ALPHANUMERIC).to_string();
        url.set_query(Some(&query));
        let res = self.client.get(url).send().await?;
        let body = res.bytes().await?;
        let body = SHIFT_JIS.decode(&body).0.into_owned();
        let res = serde_json::from_str::<RestResponse<S::Response>>(body.as_str())?;
        Ok(res.0)
    }
}

pub trait TachibanaRequest: Serialize + Sized {
    /// 後ろに/が必要
    const PATH: &'static str;
    const CLMID: &'static str;
    type Response: serde::de::DeserializeOwned;
    fn to_json(self) -> Value {
        let mut j = serde_json::to_value(self).unwrap();
        if j == Value::Null {
            j = json!({});
        }
        let common = json!(
            {
                "p_no": PREV_NONCE.fetch_add(1, std::sync::atomic::Ordering::SeqCst).to_string(),
                "p_sd_date": format!("{}", Utc::now().with_timezone(&JST()).format("%Y.%m.%d-%H:%M:%S%.3f")),
                "sCLMID": Self::CLMID,
                "sJsonOfmt": "5",
            }
        );
        object_update(&mut j, common).unwrap();
        j
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    pub s_user_id: String,
    pub s_password: String,
}

impl TachibanaRequest for LoginRequest {
    const PATH: &'static str = "auth/";
    const CLMID: &'static str = "CLMAuthLoginRequest";
    type Response = LoginResponse;
}

#[derive(Debug, Serialize)]
pub struct LogoutRequest;

impl TachibanaRequest for LogoutRequest {
    const PATH: &'static str = "request/";
    const CLMID: &'static str = "CLMAuthLogoutRequest";
    type Response = SimpleResponse;
}

macro_rules! impl_serialize {
    ($t:ident, $($a:ident => $b:expr),+) => {
        impl Serialize for $t {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: serde::Serializer {
                match self {
                    $(
                        $t::$a => serializer.serialize_str($b),
                    )+
                }
            }
        }
    };
}

#[derive(Debug)]
pub enum TaxAccountType {
    Specific    // 特定口座
}

#[derive(Debug)]
pub enum StockMarket {
    Tsc,    // 東証
}

#[derive(Debug)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug)]
pub enum OrderTime {
    None,               // 指定なし
    Opening,                // 寄付
    Closing,                // 引け
    MarketIfNotExecuted,    // 不成
}

#[derive(Debug)]
pub enum OrderPrice {
    None,               // 指定なし（逆指値、現引、現渡）
    Market,             // 成行
    Limit(u64),         // 指値
}

impl Serialize for OrderPrice {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        match self {
            OrderPrice::None => serializer.serialize_str("*"),
            OrderPrice::Market => serializer.serialize_str("0"),
            OrderPrice::Limit(x) => serializer.serialize_str(&x.to_string()),
        }
    }
}

impl_serialize!(TaxAccountType, Specific => "1");
impl_serialize!(StockMarket, Tsc => "00");
impl_serialize!(OrderSide, Buy => "3", Sell => "1");
impl_serialize!(OrderTime, None => "0", Opening => "2", Closing => "4", MarketIfNotExecuted => "6");


#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    s_zyoutoeki_kazei_c: TaxAccountType,
    s_issue_code: Currency,
    s_sizyou_c: StockMarket,
    s_baibai_kubun: OrderSide,
    s_condition: OrderTime,
    s_order_price: OrderPrice,
}

impl TachibanaRequest for OrderRequest {
    const PATH: &'static str = "request/";
    const CLMID: &'static str = "CLMKabuNewOrder";
    type Response = ();
}


#[derive(Debug)]
pub struct RestResponse<T>(pub T);


impl<'de, T: DeserializeOwned> Deserialize<'de> for RestResponse<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de> {
        let j = Map::<String, Value>::deserialize(deserializer)?;
        if j.get("p_errno").and_then(|x| x.as_str()).unwrap_or("0") == "0" {
            let res = serde_json::from_value::<T>(Value::Object(j)).map_err(serde::de::Error::custom)?;
            Ok(Self(res))
        } else {
            /*
            {
                "p_no":"4",
                "p_sd_date":"2024.01.14-22:44:53.336",
                "p_rv_date":"2024.01.14-22:44:53.327",
                "p_errno":"-1",
                "p_err":"引数（p_no:[4]）エラー。",
                "sCLMID":"CLMAuthLoginRequest"
            }
            */
            Err(serde::de::Error::custom(
                format!(
                    "{}: {}",
                    j.get("sCLMID").and_then(|x| x.as_str()).unwrap_or("unknown sCLMID"),
                    j.get("p_err").and_then(|x| x.as_str()).unwrap_or("unknown p_err")
            )))
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
pub struct SimpleResponse {
    pub sCLMID: String,
}

/*
{
    "p_sd_date":"2024.01.13-11:59:37.938",
    "p_no":"1",
    "p_rv_date":"2024.01.13-11:59:37.799",
    "p_errno":"0",
    "p_err":"",
    "sCLMID":"CLMAuthLoginAck",
    "sResultCode":"0",
    "sResultText":"",
    "sZyoutoekiKazeiC":"1",
    "sSecondPasswordOmit":"0",
    "sLastLoginDate":"20240113115842",
    "sSogoKouzaKubun":"1",
    "sHogoAdukariKouzaKubun":"1",
    "sFurikaeKouzaKubun":"1",
    "sGaikokuKouzaKubun":"1",
    "sMRFKouzaKubun":"0",
    "sTokuteiKouzaKubunGenbutu":"1",
    "sTokuteiKouzaKubunSinyou":"1",
    "sTokuteiKouzaKubunTousin":"0",
    "sTokuteiHaitouKouzaKubun":"1",
    "sTokuteiKanriKouzaKubun":"0",
    "sSinyouKouzaKubun":"1",
    "sSakopKouzaKubun":"0",
    "sMMFKouzaKubun":"0",
    "sTyukokufKouzaKubun":"0",
    "sKawaseKouzaKubun":"0",
    "sHikazeiKouzaKubun":"1",
    "sKinsyouhouMidokuFlg":"0",
    "sUrlRequest":"https://demo-kabuka.e-shiten.jp/e_api_v4r5/request/Nzk5Mzc1OTExMTMwMS0xMTYtNjU0ODI=/",
    "sUrlMaster":"https://demo-kabuka.e-shiten.jp/e_api_v4r5/master/Nzk5Mzc1OTExMTMwMS0xMTYtNjU0ODI=/",
    "sUrlPrice":"https://demo-kabuka.e-shiten.jp/e_api_v4r5/price/Nzk5Mzc1OTExMTMwMS0xMTYtNjU0ODI=/",
    "sUrlEvent":"https://demo-kabuka.e-shiten.jp/e_api_v4r5/event/Nzk5Mzc1OTExMTMwMS0xMTYtNjU0ODI=/"
}
 */

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    pub s_url_request: String,
    pub s_url_master: String,
    pub s_url_price: String,
    pub s_url_event: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginBalanceResponse {
    s_hituke: String,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    s_azukari_kin: f64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    s_sinyou_sinkidate_kanougaku: f64,
}

#[tokio::test]
async fn test_tachibana_client() {
    let client = TachibanaClient::new(crate::client::credentials::CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
    assert!(SESSION_ID.get().is_some());
    client.logout().await.unwrap();
}