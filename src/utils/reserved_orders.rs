use std::{hash::Hash, collections::HashMap};

use log::info;
use maplit::hashmap;
use uuid::Uuid;

use crate::{order_types::{Side, PosSide, OrderType}, data_structure::float_exp::FloatExp, client::types::TradeRecord};

#[derive(Debug, Clone, Eq)]
pub struct ReservedOrder {
    pub id: Uuid,
    pub order_type: OrderType,
    pub side: Side,
    pub pos_side: PosSide,
    pub price: FloatExp,
    pub amount: FloatExp,
    pub pair_order_id: Option<String>,
    pub pair_rsv_order_id: Option<Uuid>,
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
    pub fn new(order_type: OrderType, side: Side, pos_side: PosSide, price: FloatExp, amount: FloatExp) -> Self {
        Self {
            id: Uuid::new_v4(),
            order_type,
            side,
            pos_side,
            price,
            amount,
            pair_order_id: None,
            pair_rsv_order_id: None,
            is_ordered: false,
        }
    }

    pub fn is_fire(&self, prev_price: Option<FloatExp>, curr_price: FloatExp) -> bool {
        if self.is_ordered {
            return false;
        }
        let mut side = self.side;
        if self.order_type.is_stoploss() {
            side = side.inv();
        }
        match side {
            Side::Buy => {
                (prev_price.is_none() || prev_price.unwrap() > self.price) && curr_price <= self.price
            },
            Side::Sell => {
                (prev_price.is_none() || prev_price.unwrap() < self.price) && curr_price >= self.price
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

    pub fn add_reserved_order(&mut self, order_type: OrderType, side: Side, pos_side: PosSide, price: FloatExp, amount: FloatExp, pair_order_id: Option<String>) -> Uuid {
        let mut reserved_order = ReservedOrder::new(order_type, side, pos_side, price, amount);
        let ret = reserved_order.id;
        reserved_order.pair_order_id = pair_order_id;
        info!("add_reserved_order: {:?}", reserved_order);
        self.reserved_orders.insert(reserved_order.id, reserved_order);
        ret
    }

    pub fn get_mut(&mut self, id: &Uuid) -> Option<&mut ReservedOrder> {
        self.reserved_orders.get_mut(id)
    }

    pub fn remove(&mut self, id: &Uuid) -> Option<ReservedOrder> {
        self.reserved_orders.remove(id)
    }

    /// 発火する注文を返す
    pub fn trades_handler(&mut self, trades: &Vec<TradeRecord>) -> Vec<ReservedOrder> {
        let mut reserved_orders = vec![];
        for trade in trades {
            let trade_price = FloatExp::from_f64(trade.price, self.price_exp);
            for (_, reserved_order) in &mut self.reserved_orders {
                match self.prev_price {
                    Some(prev_price) => {
                        if reserved_order.is_fire(Some(prev_price), trade_price) {
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

    /// orderbookの任意番目のbid, askを超えていたら発火する注文を返す
    /// 指値注文用
    pub fn orderbook_handler(&mut self, bidask: [(f64, f64); 2]) -> Vec<ReservedOrder> {
        let mut reserved_orders = vec![];
        for (_, reserved_order) in &mut self.reserved_orders {
            // buyはbid, sellはaskを超えていたら発火
            if reserved_order.is_fire(None, FloatExp::from_f64(bidask[reserved_order.side as usize].0, self.price_exp)) {
                reserved_order.is_ordered = true;
                reserved_orders.push(reserved_order.clone());
            }
        }
        reserved_orders
    }
}