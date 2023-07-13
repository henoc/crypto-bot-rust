use std::{hash::Hash, collections::HashMap};

use log::info;
use maplit::hashmap;
use uuid::Uuid;

use crate::{order_types::{Side, PosSide}, data_structure::float_exp::FloatExp, client::types::TradeRecord};

#[derive(Debug, Clone, Eq)]
pub struct ReservedOrder {
    pub id: Uuid,
    pub side: Side,
    pub pos_side: PosSide,
    pub price: FloatExp,
    pub amount: FloatExp,
    pub is_stoploss: bool,
    pub pair_order_id: Option<String>,

    pub is_ordered: bool,
}

impl PartialEq for ReservedOrder {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for ReservedOrder {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ReservedOrder {
    pub fn new(side: Side, pos_side: PosSide, price: FloatExp, amount: FloatExp, is_stoploss: bool) -> Self {
        Self {
            id: Uuid::new_v4(),
            side,
            pos_side,
            price,
            amount,
            is_stoploss,
            pair_order_id: None,
            is_ordered: false,
        }
    }

    pub fn is_fire(&self, prev_price: FloatExp, curr_price: FloatExp) -> bool {
        if self.is_ordered {
            return false;
        }
        let mut side = self.side;
        if self.is_stoploss {
            side = side.inv();
        }
        match side {
            Side::Buy => {
                prev_price > self.price && curr_price <= self.price
            },
            Side::Sell => {
                prev_price < self.price && curr_price >= self.price
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReservedOrdersManager {
    pub reserved_orders: HashMap<Uuid, ReservedOrder>,
    prev_price: Option<FloatExp>,
    price_exp: i32,
}

impl ReservedOrdersManager {
    pub fn new(price_exp: i32) -> Self {
        Self {
            reserved_orders: hashmap! {},
            prev_price: None,
            price_exp,
        }
    }

    pub fn cancel_all_orders(&mut self) {
        self.reserved_orders.clear();
    }

    pub fn add_reserved_order(&mut self, side: Side, pos_side: PosSide, price: FloatExp, amount: FloatExp, is_stoploss: bool, pair_order_id: Option<String>) {
        let mut reserved_order = ReservedOrder::new(side, pos_side, price, amount, is_stoploss);
        reserved_order.pair_order_id = pair_order_id;
        info!("add_reserved_order: {:?}", reserved_order);
        self.reserved_orders.insert(reserved_order.id, reserved_order);
    }

    /// 発火する注文を返す
    pub fn trades_handler(&mut self, trades: &Vec<TradeRecord>) -> Vec<ReservedOrder> {
        let mut reserved_orders = vec![];
        for trade in trades {
            let trade_price = FloatExp::from_f64(trade.price, self.price_exp);
            for (_, reserved_order) in &mut self.reserved_orders {
                match self.prev_price {
                    Some(prev_price) => {
                        if reserved_order.is_fire(prev_price, trade_price) {
                            reserved_order.is_ordered = true;
                            reserved_orders.push(reserved_order.clone());
                        }
                    },
                    None => {
                    }
                }
            }
            self.prev_price = Some(trade_price);
        }
        reserved_orders
    }
}