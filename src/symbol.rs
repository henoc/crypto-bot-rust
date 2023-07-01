use std::fmt::Display;

use easy_ext::ext;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    Bitflyer,
    Binance,
    Gmo,
    Coincheck,
}

impl Exchange {
    pub fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap().replace("\"", "")
    }
}

impl Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SymbolType {
    Perp,
    Spot,
}

impl SymbolType {
    pub fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap().replace("\"", "")
    }
}

impl Display for SymbolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Currency {
    BTC,
    JPY
}

impl Currency {
    pub fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap().replace("\"", "")
    }
}

impl Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct Symbol {
    pub base: Currency,
    pub quote: Currency,
    pub settlement: Currency,
    pub r#type: SymbolType,
    pub exc: Exchange,
}

impl Symbol {
    pub fn new(base: Currency, quote: Currency, r#type: SymbolType, exc: Exchange) -> Self {
        Self {
            base,
            quote,
            settlement: quote,
            r#type: r#type,
            exc,
        }
    }

    pub fn to_native(&self) -> String {
        match self.exc {
            Exchange::Gmo => match self.r#type {
                SymbolType::Perp => format!("{}_{}", self.base, self.quote),
                SymbolType::Spot => format!("{}", self.base),
            },
            Exchange::Coincheck => format!("{}_{}", self.base.to_string().to_lowercase(), self.quote.to_string().to_lowercase()),
            Exchange::Binance => format!("{}{}", self.base, self.quote),
            _ => panic!("not implemented"),
        }
    }

    pub fn to_file_form(&self) -> String {
        format!("{}-{}-{}-{}", self.exc, self.base, self.quote, self.r#type)
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

#[ext(SymbolPrecision)]
pub impl Symbol {
    fn amount_precision(&self) -> i32 {
        match self.exc {
            Exchange::Gmo => match self.r#type {
                SymbolType::Perp => -2,
                SymbolType::Spot => -4,
            },
            _ => panic!("not implemented"),
        }
    }

    fn price_precision(&self) -> i32 {
        match self.exc {
            Exchange::Gmo => match self.r#type {
                SymbolType::Perp => 0,
                SymbolType::Spot => 0,
            },
            _ => panic!("not implemented"),
        }
    }
}
