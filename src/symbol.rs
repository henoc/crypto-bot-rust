use std::fmt::Display;

use labo::export::serde_json;
use serde::{Deserialize, Serialize};
use strum::EnumString;

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    Bitflyer,
    Binance,
    Gmo,
    Coincheck,
    Tachibana,
}

impl Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap().replace('\"', ""))
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SymbolType {
    Perp,
    Spot,
    Margin,
}

impl Display for SymbolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap().replace('\"', ""))
    }
}

#[derive(Deserialize, Serialize, EnumString, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Currency {
    BTC,
    XRP,
    JPY,
    USDT,
    /// トヨタ
    #[serde(rename = "7203")]
    T7203,
    /// ソニー
    #[serde(rename = "6758")]
    T6758,
    /// NTT
    #[serde(rename = "9432")]
    T9432,
    #[serde(rename = "6861")]
    T6861,
    #[serde(rename = "8306")]
    T8306,
    #[serde(rename = "8035")]
    T8035,
    #[serde(rename = "9983")]
    T9983,
    #[serde(rename = "4063")]
    T4063,
}

impl Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap().replace('\"', ""))
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct Symbol {
    pub base: Currency,
    pub quote: Currency,
    /// 決済通貨
    pub settlement: Currency,
    pub r#type: SymbolType,
    pub exc: Exchange,
}

impl Symbol {
    pub const fn new(base: Currency, quote: Currency, r#type: SymbolType, exc: Exchange) -> Self {
        Self {
            base,
            quote,
            settlement: quote,
            r#type,
            exc,
        }
    }

    pub fn to_native(&self) -> String {
        match self.exc {
            Exchange::Gmo => match self.r#type {
                SymbolType::Perp => format!("{}_{}", self.base, self.quote),
                SymbolType::Spot => format!("{}", self.base),
                _ => panic!("not implemented"),
            },
            Exchange::Coincheck => format!("{}_{}", self.base.to_string().to_lowercase(), self.quote.to_string().to_lowercase()),
            Exchange::Binance => format!("{}{}", self.base, self.quote),
            Exchange::Bitflyer => match self.r#type {
                SymbolType::Perp => format!("FX_{}_{}", self.base, self.quote),
                SymbolType::Spot => format!("{}_{}", self.base, self.quote),
                _ => panic!("not implemented"),
            },
            Exchange::Tachibana => format!("{}", self.base),
        }
    }

    pub fn to_file_form(&self) -> String {
        format!("{}-{}-{}-{}", self.exc, self.base, self.quote, self.r#type)
    }

    #[inline]
    pub const fn settlement_precision(&self) -> i32 {
        match self.settlement {
            Currency::JPY => 0,
            _ => panic!("not implemented"),
        }
    }

    #[inline]
    pub const fn amount_precision(&self) -> i32 {
        match self.exc {
            Exchange::Gmo => match (self.base, self.r#type) {
                (Currency::BTC, SymbolType::Perp) => -2,
                (Currency::BTC, SymbolType::Spot) => -4,
                (Currency::XRP, SymbolType::Perp) => 1,
                (Currency::XRP, SymbolType::Spot) => 0,
                _ => panic!("not implemented"),
            },
            Exchange::Bitflyer => -8,
            Exchange::Coincheck => -8,
            Exchange::Tachibana => 2,
            _ => panic!("not implemented"),
        }
    }

    #[inline]
    pub const fn price_precision(&self) -> i32 {
        match self.exc {
            Exchange::Gmo => match self.base {
                Currency::BTC => 0,
                Currency::XRP => -3,
                _ => panic!("not implemented"),
            },
            Exchange::Bitflyer => 0,
            Exchange::Coincheck => 0,
            Exchange::Tachibana => -1,
            _ => panic!("not implemented"),
        }
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_native())
    }
}
