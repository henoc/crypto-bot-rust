use std::sync::OnceLock;
use std::{sync::atomic::AtomicU32, collections::HashMap};

use anyhow;
use labo::export::chrono::{DateTime, Utc, FixedOffset, NaiveDate, Timelike};
use labo::export::serde_json;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize, de::DeserializeOwned, Deserializer};
use serde_json::{json, Value, Map};
use url::Url;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use encoding_rs::SHIFT_JIS;

use crate::data_structure::float_exp::FloatExp;
use crate::order_types::Side;
use crate::{utils::{time::JST, serde::{deserialize_f64_from_str, serialize_u32_to_str, deserialize_i64_from_str, deserialize_f64_opt_from_str}, useful_traits::StaticVarExt, json_utils::object_update}, symbol::{Currency, Symbol}};

use super::credentials::TachibanaCredentials;

static SESSION_ID: OnceLock<RwLock<String>> = OnceLock::new();
static PREV_NONCE: AtomicU32 = AtomicU32::new(1);

const DEMO_ENDPOINT: &str = "https://demo-kabuka.e-shiten.jp/e_api_v4r5/";

const ENDPOINT: &str = "https://kabuka.e-shiten.jp/e_api_v4r5/";

pub struct TachibanaClient {
    client: reqwest::Client,
    api_credentials: TachibanaCredentials,
    demo: bool,
}

impl TachibanaClient {
    pub fn new(api_credentials: TachibanaCredentials) -> TachibanaClient {
        TachibanaClient {
            client: reqwest::Client::new(),
            api_credentials,
            demo: false,
        }
    }

    pub fn new_demo(api_credentials: TachibanaCredentials) -> TachibanaClient {
        TachibanaClient {
            client: reqwest::Client::new(),
            api_credentials,
            demo: true,
        }
    }

    const fn endpoint(&self) -> &'static str {
        if self.demo {
            DEMO_ENDPOINT
        } else {
            ENDPOINT
        }
    }

    pub async fn login(&self) -> anyhow::Result<()> {
        let params = LoginRequest {
            s_user_id: self.api_credentials.user_id.clone(),
            s_password: self.api_credentials.password1.clone()
        }.to_json();
        let mut url = Url::parse(self.endpoint())?.join( LoginRequest::PATH)?;
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

    pub async fn send<S: TachibanaRequest>(&self, mut query: S) -> anyhow::Result<S::Response> {
        query.set_password2(self.api_credentials.password2.clone());
        query.validation()?;
        let params = query.to_json();
        let mut url = Url::parse(self.endpoint())?.join( S::PATH)?.join(format!("{}/", SESSION_ID.read()?).as_str())?;
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
    fn set_password2(&mut self, _password2: String) {
    }
    fn validation(&self) -> anyhow::Result<()> {
        Ok(())
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

#[derive(Debug, Serialize)]
pub enum TaxAccountType {
    #[serde(rename = "1")]
    Specific    // 特定口座
}

#[derive(Debug, Serialize)]
pub enum StockMarket {
    #[serde(rename = "00")]
    Tsc,    // 東証
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OrderSide {
    #[serde(rename = "3")]
    Buy,
    #[serde(rename = "1")]
    Sell,
}

impl From<Side> for OrderSide {
    fn from(side: Side) -> Self {
        match side {
            Side::Buy => OrderSide::Buy,
            Side::Sell => OrderSide::Sell,
        }
    }
}

#[derive(Debug, Serialize)]
pub enum OrderTime {
    #[serde(rename = "0")]
    None,               // 指定なし
    #[serde(rename = "2")]
    Opening,                // 寄付
    #[serde(rename = "4")]
    Closing,                // 引け
    #[serde(rename = "6")]
    MarketIfNotExecuted,    // 不成
}

#[derive(Debug, Serialize)]
pub enum OrderPrice {
    #[serde(rename = "*")]
    None,               // 指定なし（逆指値、現引、現渡）
    #[serde(rename = "0")]
    Market,             // 成行
    // #[serde(serialize_with = "serialize_limit_price")]
    // /// TODO: 呼び値単位
    // Limit(f64),         // 指値
}

// fn serialize_limit_price<S>(x: &f64, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         serializer.serialize_str(&x.to_string())
//     }

#[derive(Debug, Serialize, Clone, Copy)]
pub enum TradingType {
    #[serde(rename = "0")]
    /// 現物
    Spot,
    #[serde(rename = "2")]
    /// 制度信用新規
    OpenSystemMargin,
    #[serde(rename = "4")]
    /// 制度信用返済
    CloseSystemMargin,
    #[serde(rename = "6")]
    /// 一般信用新規
    OpenGeneralMargin,
    #[serde(rename = "8")]
    /// 一般信用返済
    CloseGeneralMargin,
}

#[derive(Debug, Serialize)]
pub enum TimeInForce {
    #[serde(rename = "0")]
    Interday,
}

#[derive(Debug, Serialize)]
pub enum StopOrderType {
    #[serde(rename = "0")]
    None,
}

#[derive(Debug, Serialize)]
pub enum StopOrderTriggerPrice {
    #[serde(rename = "0")]
    None,
}

#[derive(Debug, Serialize)]
pub enum StopOrderPrice {
    #[serde(rename = "*")]
    None,
}

/// 信用建玉返済順序
#[derive(Debug, Serialize)]
pub enum CloseMarginOrder {
    #[serde(rename = "*")]
    /// 現物または信用新規
    None,
    #[serde(rename = "2")]
    /// 建日順
    DateTime,
}

/// 現引、現渡時のポジション税区分
#[derive(Debug, Serialize)]
pub enum PositionTaxAccountType {
    #[serde(rename = "*")]
    /// 現引、現渡以外
    None,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    s_zyoutoeki_kazei_c: TaxAccountType,
    s_issue_code: Currency,
    s_sizyou_c: StockMarket,
    s_baibai_kubun: OrderSide,
    s_condition: OrderTime,
    s_order_price: OrderPrice,
    s_order_suryou: FloatExp,
    s_genkin_shinyou_kubun: TradingType,
    s_order_expire_day: TimeInForce,
    s_gyakusasi_order_type: StopOrderType,
    s_gyakusasi_zyouken: StopOrderTriggerPrice,
    s_gyakusasi_price: StopOrderPrice,
    s_tatebi_type: CloseMarginOrder,
    s_tategyoku_zyoutoeki_kazei_c: PositionTaxAccountType,
    s_second_password: String,
}

impl OrderRequest {
    pub fn new(base: Currency, side: OrderSide, order_time: OrderTime, price: OrderPrice, amount: FloatExp, trading_type: TradingType) -> Self {
        Self {
            s_zyoutoeki_kazei_c: TaxAccountType::Specific,
            s_issue_code: base,
            s_sizyou_c: StockMarket::Tsc,
            s_baibai_kubun: side,
            s_condition: order_time,
            s_order_price: price,
            s_order_suryou: amount,
            s_genkin_shinyou_kubun: trading_type,
            s_order_expire_day: TimeInForce::Interday,
            s_gyakusasi_order_type: StopOrderType::None,
            s_gyakusasi_zyouken: StopOrderTriggerPrice::None,
            s_gyakusasi_price: StopOrderPrice::None,
            s_tatebi_type: match trading_type {
                TradingType::CloseSystemMargin | TradingType::CloseGeneralMargin => CloseMarginOrder::DateTime,
                _ => CloseMarginOrder::None,
            },
            s_tategyoku_zyoutoeki_kazei_c: PositionTaxAccountType::None,
            s_second_password: "".to_string(),
        }
    }
}

impl TachibanaRequest for OrderRequest {
    const PATH: &'static str = "request/";
    const CLMID: &'static str = "CLMKabuNewOrder";
    type Response = OrderResponse;
    fn set_password2(&mut self, password2: String) {
        self.s_second_password = password2;
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginBalanceRequest {
    #[serde(serialize_with = "serialize_u32_to_str")]
    pub s_hituke_index: u32,
}

impl TachibanaRequest for MarginBalanceRequest {
    const PATH: &'static str = "request/";
    const CLMID: &'static str = "CLMZanKaiSinyouSinkidateSyousai";
    type Response = MarginBalanceResponse;
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginPositionRequest {
    pub s_issue_code: MarginPositionRequestBase,
}

impl TachibanaRequest for MarginPositionRequest {
    const PATH: &'static str = "request/";
    const CLMID: &'static str = "CLMShinyouTategyokuList";
    type Response = MarginPositionResponse;
}

#[derive(Debug, Serialize)]
pub enum MarginPositionRequestBase {
    #[serde(serialize_with = "serialize_currency")]
    Currency(Currency),
    #[serde(rename = "")]
    All,
}

fn serialize_currency<S>(x: &Currency, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&x.to_string())
    }

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceHistoryRequest {
    pub s_issue_code: Currency,
    pub s_sizyou_c: StockMarket,
}

impl TachibanaRequest for PriceHistoryRequest {
    const PATH: &'static str = "price/";
    const CLMID: &'static str = "CLMMfdsGetMarketPriceHistory";
    type Response = PriceHistoryResponse;
    fn validation(&self) -> anyhow::Result<()> {
        let now = Utc::now().with_timezone(&JST());
        if now.hour() >= 8 && now.hour() < 15 {
            anyhow::bail!("validation failed for accessing PriceHistory during business hours");
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceRequest {
    #[serde(serialize_with = "serialize_vec_to_str")]
    pub s_target_issue_code: Vec<Currency>,
    #[serde(serialize_with = "serialize_vec_to_str")]
    pub s_target_column: Vec<PriceType>,
}

/**
 * 日曜日:
    {
      "": "",
      "pDPP": "187.1",
      "pED": "",
      "pPRP": "188.3",
      "sIssueCode": "9432",
      "tDPP:T": "15:00"
    }
 月曜日:
  {
      "": "",
      "pDPP": "190.1",
      "pED": "",
      "pPRP": "187.1",
      "sIssueCode": "9432",
      "tDPP:T": "11:29"
    },
 */

#[derive(Debug, Serialize)]
pub enum PriceType {
    /// 前日終値。営業日の営業時間後でも前日の終値。
    #[serde(rename = "pPRP")]
    PreviousClose,
    #[serde(rename = "pDPP")]
    LastPrice,
    #[serde(rename = "tDPP:T")]
    LastPriceTime,
}

impl TachibanaRequest for PriceRequest {
    const PATH: &'static str = "price/";
    const CLMID: &'static str = "CLMMfdsGetMarketPrice";
    type Response = PriceResponse;
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemDataRequest {
    #[serde(rename = "sTargetCLMID", serialize_with = "serialize_vec_to_str")]
    pub s_target_clmid: Vec<SystemDataCommand>,
    pub s_target_column: String,
}

impl SystemDataRequest {
    pub fn new(commands: Vec<SystemDataCommand>) -> Self {
        let mut props = Vec::new();
        for command in &commands {
            match command {
                SystemDataCommand::BusinessDate => {
                    props.extend_from_slice(&["sDayKey","sTheDay","sMaeEigyouDay_3","sYokuEigyouDay_1","sKabuUkewatasiDay","sKabuKariUkewatasiDay"]);
                },
            }
        }
        Self {
            s_target_clmid: commands,
            s_target_column: props.join(","),
        }
    
    }
}

fn serialize_vec_to_str<S, T>(x: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: Serialize,
    {
        let mut s = String::new();
        for c in x {
            s.push_str(&serde_json::to_string(c).unwrap().replace("\"", ""));
            s.push(',');
        }
        serializer.serialize_str(&s)
    }

impl TachibanaRequest for SystemDataRequest {
    const PATH: &'static str = "master/";
    const CLMID: &'static str = "CLMMfdsGetMasterData";
    type Response = SystemDataResponse;
}

#[derive(Debug, Serialize)]
pub enum SystemDataCommand {
    #[serde(rename = "CLMDateZyouhou")]
    BusinessDate,
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
pub struct OrderResponse {
    pub s_result_code: String,
    pub s_result_text: String,
    pub s_warning_code: String,
    pub s_warning_text: String,
    pub s_order_number: String,
    pub s_order_date: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginBalanceResponse {
    pub s_hituke: String,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_azukari_kin: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_sinyou_sinkidate_kanougaku: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginPositionResponse {
    pub a_shinyou_tategyoku_list: Vec<MarginPositionItem>,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_hyouka_soneki_goukei_kaidate: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_hyouka_soneki_goukei_uridate: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_tokutei_hyouka_soneki_goukei: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_kaitate_daikin: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_uritate_daikin: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_total_daikin: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginPositionItem {
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_tategyoku_number: i64,
    pub s_order_issue_code: CodeResponse,
    pub s_order_baibai_kubun: OrderSide,
    pub s_order_bensai_kubun: MarginPositionType,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_tategyoku_suryou: i64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub s_order_tategyoku_tanka: f64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub s_order_hyouka_tanka: f64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub s_order_gaisan_hyouka_soneki: f64,
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub s_order_gaisan_hyouka_soneki_ritu: f64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_tategyoku_daikin: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_tate_tesuryou: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_gyakuhibu: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_zyun_hibu: i64,
    #[serde(deserialize_with = "deserialize_date_from_str")]
    pub s_order_tategyoku_day: NaiveDate,
    #[serde(deserialize_with = "deserialize_date_from_str")]
    pub s_order_tategyoku_kizitu_day: NaiveDate,
    /// s_order_tategyoku_suryouとどう違うのか不明
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_tategyoku_suryou: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_yakuzyou_hensai_kabusu: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_genbiki_genwatasi_kabusu: i64,
    /// 注文中の数量
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_order_suryou: i64,
    #[serde(deserialize_with = "deserialize_i64_from_str")]
    pub s_order_hensai_kanou_suryou: i64,
}

fn deserialize_date_from_str<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let d = NaiveDate::parse_from_str(&s, "%Y%m%d").map_err(serde::de::Error::custom)?;
        Ok(d)
    }

#[derive(Debug, Deserialize, Hash, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum CodeResponse {
    Defined(Currency),
    Raw(String),
}

#[derive(Debug, Deserialize)]
pub enum MarginPositionType {
    /// なし
    #[serde(rename = "00")]
    None,
    #[serde(rename = "26")]
    SystemHalfYear,
    #[serde(rename = "29")]
    SystemUnlimited,
    #[serde(rename = "36")]
    GeneralHalfYear,
    #[serde(rename = "39")]
    GeneralUnlimited,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceHistoryResponse {
    #[serde(rename = "aCLMMfdsMarketPriceHistory")]
    pub a_clm_mfds_market_price_history: Vec<PriceHistoryItem>,
}

#[derive(Debug, Deserialize)]
pub struct PriceHistoryItem {
    #[serde(rename = "sDate", deserialize_with = "deserialize_date_from_str")]
    pub s_date: NaiveDate,
    // #[serde(rename = "sDOP", deserialize_with = "deserialize_f64_from_str")]
    // pub open: f64,
    // #[serde(rename = "sDHP", deserialize_with = "deserialize_f64_from_str")]
    // pub high: f64,
    // #[serde(rename = "sDLP", deserialize_with = "deserialize_f64_from_str")]
    // pub low: f64,
    // #[serde(rename = "sDPP", deserialize_with = "deserialize_f64_from_str")]
    // pub close: f64,
    // #[serde(rename = "sDV", deserialize_with = "deserialize_f64_from_str")]
    // pub volume: f64,
    #[serde(rename = "pDOPxK", deserialize_with = "deserialize_f64_from_str")]
    pub open_adj: f64,
    #[serde(rename = "pDHPxK", deserialize_with = "deserialize_f64_from_str")]
    pub high_adj: f64,
    #[serde(rename = "pDLPxK", deserialize_with = "deserialize_f64_from_str")]
    pub low_adj: f64,
    #[serde(rename = "pDPPxK", deserialize_with = "deserialize_f64_from_str")]
    pub close_adj: f64,
    #[serde(rename = "pDVxK", deserialize_with = "deserialize_f64_from_str")]
    pub volume_adj: f64,
}

#[derive(Debug, Deserialize)]
pub struct PriceResponse {
    #[serde(rename = "aCLMMfdsMarketPrice", deserialize_with = "deserialize_from_vec_to_hashmap")]
    pub a_clm_mfds_market_price: HashMap<CodeResponse, PriceItem>,
}

#[derive(Debug, Deserialize)]
pub struct PriceItem {
    #[serde(rename = "sIssueCode")]
    pub s_issue_code: CodeResponse,
    #[serde(rename = "pPRP", deserialize_with = "deserialize_f64_opt_from_str", default)]
    pub previous_close: Option<f64>,
    #[serde(rename = "pDPP", deserialize_with = "deserialize_f64_opt_from_str", default)]
    pub last_price: Option<f64>,
    #[serde(rename = "tDPP:T", default)]
    pub last_price_time: Option<String>,
}

impl VecToMapDeserializable for PriceItem {
    type K = CodeResponse;
    fn key(&self) -> Self::K {
        self.s_issue_code.clone()
    }
}

#[derive(Debug, Deserialize)]
pub struct SystemDataResponse {
    #[serde(rename = "CLMDateZyouhou", deserialize_with = "deserialize_from_vec_to_hashmap_opt", default)]
    pub clm_date_zyouhou: Option<HashMap<DayFlag, BusinessDateResponseItem>>
}

trait VecToMapDeserializable {
    type K: std::hash::Hash + Eq;
    fn key(&self) -> Self::K;
}

fn deserialize_from_vec_to_hashmap<'de, D, T>(deserializer: D) -> Result<HashMap<T::K, T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de> + VecToMapDeserializable,
    {
        let v = Vec::<T>::deserialize(deserializer)?;
        let mut m = HashMap::new();
        for item in v {
            m.insert(item.key(), item);
        }
        Ok(m)
    }

fn deserialize_from_vec_to_hashmap_opt<'de, D, T>(deserializer: D) -> Result<Option<HashMap<T::K, T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de> + VecToMapDeserializable,
    {
        let ret = deserialize_from_vec_to_hashmap(deserializer)?;
        Ok(Some(ret))
    }

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BusinessDateResponseItem {
    pub s_day_key: DayFlag,
    /// 休日は最初の営業日になる気がする
    #[serde(deserialize_with = "deserialize_date_from_str")]
    pub s_the_day: NaiveDate,
    /// 一日前
    #[serde(rename = "sMaeEigyouDay_3", deserialize_with = "deserialize_date_from_str")]
    pub s_mae_eigyou_day_3: NaiveDate,
    #[serde(rename = "sYokuEigyouDay_1", deserialize_with = "deserialize_date_from_str")]
    pub s_yoku_eigyou_day_1: NaiveDate,
    #[serde(deserialize_with = "deserialize_date_from_str")]
    pub s_kabu_ukewatasi_day: NaiveDate,
    #[serde(deserialize_with = "deserialize_date_from_str")]
    pub s_kabu_kari_ukewatasi_day: NaiveDate,
}

impl VecToMapDeserializable for BusinessDateResponseItem {
    type K = DayFlag;
    fn key(&self) -> Self::K {
        self.s_day_key.clone()
    }
}

#[derive(Debug, Deserialize, Hash, PartialEq, Eq, Clone)]
pub enum DayFlag {
    #[serde(rename = "001")]
    Today,
    #[serde(rename = "002")]
    Tomorrow,
}

#[tokio::test]
async fn test_tachibana_client() {
    let client = TachibanaClient::new(crate::client::credentials::CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
    assert!(SESSION_ID.get().is_some());
    client.logout().await.unwrap();
}

#[tokio::test]
async fn test_tachibana_order() {
    let client = TachibanaClient::new_demo(crate::client::credentials::CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
    let res = client.send(OrderRequest::new(
        Currency::T9432,
        OrderSide::Buy,
        OrderTime::None,
        OrderPrice::Market,
        FloatExp::from_f64(100., 2),
        TradingType::OpenSystemMargin,
    )).await.unwrap();
    assert_eq!(res.s_result_code, "0");
}

#[tokio::test]
async fn test_tachibana_margin_position() {
    let client = TachibanaClient::new_demo(crate::client::credentials::CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
    let res = client.send(MarginPositionRequest {
        s_issue_code: MarginPositionRequestBase::All,
    }).await.unwrap();
    assert_eq!(res.s_total_daikin, 14300000);
}

#[tokio::test]
async fn test_tachibana_system_data() {
    let client = TachibanaClient::new(crate::client::credentials::CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
    let res = client.send(SystemDataRequest::new(vec![SystemDataCommand::BusinessDate])).await.unwrap();
    println!("{:?}", res);
    assert!(res.clm_date_zyouhou.is_some());
}

#[tokio::test]
async fn test_tachibana_get_price() {
    let client = TachibanaClient::new(crate::client::credentials::CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
    let res = client.send(PriceRequest {
        s_target_issue_code: vec![Currency::T9432],
        s_target_column: vec![PriceType::PreviousClose, PriceType::LastPrice, PriceType::LastPriceTime],
    }).await.unwrap();
    println!("{:?}", res);
}

#[tokio::test]
async fn test_tachibana_get_price_history() {
    let client = TachibanaClient::new(crate::client::credentials::CREDENTIALS.tachibana.clone());
    client.login().await.unwrap();
    let res = client.send(PriceHistoryRequest {
        s_issue_code: Currency::T9432,
        s_sizyou_c: StockMarket::Tsc,
    }).await.unwrap();
    assert!(res.a_clm_mfds_market_price_history.len() > 0);
}