use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    pub fn inv(&self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    pub fn to_pos(&self) -> PosSide {
        match self {
            Side::Buy => PosSide::Long,
            Side::Sell => PosSide::Short,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum PosSide {
    Long = 0,
    Short = 1,
}

impl PosSide {
    pub fn to_side(&self) -> Side {
        match self {
            PosSide::Long => Side::Buy,
            PosSide::Short => Side::Sell,
        }
    }

    pub fn sign(&self) -> i64 {
        match self {
            PosSide::Long => 1,
            PosSide::Short => -1,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType {
    Limit,
    Market,
    Stop,
    StopLimit,
}

impl OrderType {
    #[inline]
    pub const fn is_stoploss(&self) -> bool {
        match self {
            OrderType::Stop | OrderType::StopLimit => true,
            _ => false,
        }
    }
}