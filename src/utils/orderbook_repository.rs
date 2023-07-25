use std::collections::BTreeMap;

use chrono::{DateTime, Utc, Duration};
use ordered_float::OrderedFloat;
use serde::Serialize;

use crate::order_types::Side;

use super::time::{floor_time, floor_time_sec};

#[derive(Debug)]
pub struct OrderbookBest {
    pub timestamp: DateTime<Utc>,
    pub snapshot: [[(f64,f64);5];2],
}

pub fn orderbook_best_time_fn(value: &OrderbookBest) -> Option<DateTime<Utc>> {
    Some(value.timestamp)
}

impl OrderbookBest {
    pub fn new(timestamp: DateTime<Utc>, snapshot: [[(f64,f64);5];2]) -> Self {
        Self {
            timestamp,
            snapshot,
        }
    }
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
    pub state: Vec<BTreeMap<OrderedFloat<f64>, OrderedFloat<f64>>>,
    prev_time: DateTime<Utc>,
    /// snapshotを取る間隔。second以上
    timeframe: Duration,
}

impl OrderbookRepository {
    #[inline]
    pub fn new(timeframe: Duration) -> Self {
        Self {
            state: vec![BTreeMap::new(), BTreeMap::new()],
            prev_time: Utc::now(),
            timeframe,
        }
    }

    #[inline]
    pub fn new_with_state(timeframe: Duration, state: Vec<BTreeMap<OrderedFloat<f64>, OrderedFloat<f64>>>) -> Self {
        Self {
            state,
            prev_time: Utc::now(),
            timeframe,
        }
    }

    #[inline]
    pub fn replace_state(&mut self, snapshot: Vec<BTreeMap<OrderedFloat<f64>, OrderedFloat<f64>>>) {
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
    pub fn insert(&mut self, side: Side, price: f64, amount: f64) {
        self.state[side as usize].insert(price.into(), amount.into());
    }

    /// 差分更新
    pub fn remove(&mut self, side: Side, price: f64) {
        self.state[side as usize].remove(&price.into());
    }

    /// mid_priceに合わないものを捨てる
    /// pybottersのbitflyerの処理に準拠
    /// 
    /// return: 削除した数
    pub fn arrange(&mut self, mid_price: f64) -> usize {
        let len1 = self.state[0].len() + self.state[1].len();
        self.state[0].retain(|price, _| price.0 <= mid_price);   // buy
        self.state[1].retain(|price, _| price.0 > mid_price);    // sell
        let len2 = self.state[0].len() + self.state[1].len();
        len1 - len2
    }

    /// 板のベストbid/askをN個ずつ取得
    pub fn get_best<const N: usize>(&self) -> [[(f64,f64);N];2] {
        let mut buy = [(0.0,0.0);N];
        // buyはpriceの降順
        for (i, (price, amount)) in self.state[0].iter().rev().enumerate().take(N) {
            buy[i] = (price.0, amount.0);
        }
        let mut sell = [(0.0,0.0);N];
        for (i, (price, amount)) in self.state[1].iter().enumerate().take(N) {
            sell[i] = (price.0, amount.0);
        }
        [buy,sell]
    }
}

/// 差分を一度に更新する
pub fn apply_diff_once<I: IntoIterator<Item = (OrderedFloat<f64>, OrderedFloat<f64>)>>(mut snapshot: BTreeMap<OrderedFloat<f64>, OrderedFloat<f64>>, diff: I) -> BTreeMap<OrderedFloat<f64>, OrderedFloat<f64>> {
    for (price, amount) in diff {
        if amount == 0. {
            snapshot.remove(&price);
        } else {
            snapshot.insert(price, amount);
        }
    }
    snapshot
}
