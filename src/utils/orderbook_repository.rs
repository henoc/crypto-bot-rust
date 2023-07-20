use std::collections::BTreeMap;

use chrono::{DateTime, Utc, Duration};
use serde::Serialize;

use crate::{data_structure::float_exp::FloatExp, order_types::Side};

use super::time::{floor_time, floor_time_sec};

#[derive(Debug)]
pub struct OrderbookBest {
    pub timestamp: DateTime<Utc>,
    pub snapshot: [[(f64,f64);5];2],
}

impl Serialize for OrderbookBest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: serde::Serializer {
        (self.timestamp.timestamp_millis(), self.snapshot).serialize(serializer)
    }
}

/// mmapで配信することも想定
#[derive(Debug)]
pub struct OrderbookRepository {
    /// [buy, sell] BTreeMapはkeyの昇順
    pub state: Vec<BTreeMap<FloatExp, FloatExp>>,
    prev_time: DateTime<Utc>,
    /// snapshotを取る間隔。second以上
    timeframe: Duration,
}

impl OrderbookRepository {
    pub fn new(timeframe: Duration) -> Self {
        Self {
            state: vec![BTreeMap::new(), BTreeMap::new()],
            prev_time: Utc::now(),
            timeframe,
        }
    }

    pub fn replace_state(&mut self, snapshot: Vec<BTreeMap<FloatExp, FloatExp>>) {
        self.state = snapshot;
    }

    /// サーバー時刻を更新し、opentimeを跨いでいればsnapshotを取得する。
    /// insertやremoveの前に呼び出す必要がある
    pub fn snapshot_on_update(&mut self, server_time: DateTime<Utc>) -> Option<OrderbookBest> {
        let mut snapshot = None;
        if floor_time_sec(self.prev_time, self.timeframe, 0) != floor_time_sec(server_time, self.timeframe, 0) {
            snapshot = Some(OrderbookBest {
                timestamp: floor_time(server_time, self.timeframe, 0),    // self.timeframeのopentime
                snapshot: self.get_best(),
            });
        }
        self.prev_time = server_time;
        snapshot
    }

    /// 差分更新
    pub fn insert(&mut self, side: Side, price: FloatExp, amount: FloatExp) {
        self.state[side as usize].insert(price, amount);
    }

    /// 差分更新
    pub fn remove(&mut self, side: Side, price: FloatExp) {
        self.state[side as usize].remove(&price);
    }

    /// mid_priceに合わないものを捨てる
    /// pybottersのbitflyerの処理に準拠
    /// 
    /// return: 削除した数
    pub fn arrange(&mut self, mid_price: FloatExp) -> usize {
        let len1 = self.state[0].len() + self.state[1].len();
        self.state[0].retain(|price, _| *price <= mid_price);   // buy
        self.state[1].retain(|price, _| *price > mid_price);    // sell
        let len2 = self.state[0].len() + self.state[1].len();
        len1 - len2
    }

    /// 板のベストbid/askをN個ずつ取得
    pub fn get_best<const N: usize>(&self) -> [[(f64,f64);N];2] {
        let mut buy = [(0.0,0.0);N];
        // buyはpriceの降順
        for (i, (price, amount)) in self.state[0].iter().rev().enumerate().take(N) {
            buy[i] = (price.to_f64(), amount.to_f64());
        }
        let mut sell = [(0.0,0.0);N];
        for (i, (price, amount)) in self.state[1].iter().enumerate().take(N) {
            sell[i] = (price.to_f64(), amount.to_f64());
        }
        [buy,sell]
    }
}