use std::{fs::File, collections::HashSet, io::{Write, BufWriter}};

use anyhow::Context;
use labo::export::{chrono::{DateTime, Utc, NaiveDate}, serde_json, rmp_serde};
use labo::export::anyhow;
use serde::Serialize;
use labo::export::serde_json::Value;

use crate::{symbol::{Symbol, Currency}, client::types::TradeRecord, utils::time::datetime_utc};

use super::time::{JST, datetime_utc_from_timestamp, parse_format_time_utc};


pub enum SerializerType {
    Json,
    Msgpack
}

pub struct SerialRecordWriter<S> {
    pub name: String,
    pub symbol: Symbol,
    pub ext: String,
    pub time_fn: Box<dyn Fn(&S) -> Option<DateTime<Utc>> + std::marker::Send + std::marker::Sync>,
    pub que: Vec<S>,
}

impl <S> std::fmt::Debug for SerialRecordWriter<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerialRecordWriter")
            .field("name", &self.name)
            .field("symbol", &self.symbol)
            .field("ext", &self.ext)
            .field("que", &self.que.len())
            .finish()
    }
}

impl<S: Serialize + std::fmt::Debug> SerialRecordWriter<S> {
    pub fn new(name: &str, symbol: &Symbol, ext: &str, time_fn: Box<dyn Fn(&S) -> Option<DateTime<Utc>> + std::marker::Send + std::marker::Sync>) -> SerialRecordWriter<S> {
        SerialRecordWriter {
            name: name.to_string(),
            symbol: symbol.clone(),
            ext: ext.to_string(),
            time_fn,
            que: Vec::new(),
        }
    }

    pub fn file_name(&self, day: NaiveDate) -> String {
        format!("{}_{}_{}.{}", self.name, self.symbol.to_file_form(), day.format("%Y%m%d"), self.ext)
    }

    fn jst_date(&self, item: &S) -> anyhow::Result<NaiveDate> {
        Ok((self.time_fn)(item).context(format!("failed at time_fn. value: {:?}", item))?.with_timezone(&JST()).date_naive())
    }

    pub fn write_msgpack(&self, data: &Vec<S>) -> anyhow::Result<()> {
        self.write(data, SerializerType::Msgpack)
    }

    pub fn write(&self, data: &Vec<S>, serializer_type: SerializerType) -> anyhow::Result<()> {
        let mut days = HashSet::new();
        for x in data.iter().map(|item| self.jst_date(item)) {
            days.insert(x?);
        }
        for day in days {
            let file_name = self.file_name(day);
            let mut file = BufWriter::new(File::options().append(true).create(true).open(format!("market/{}", file_name))?);
            for item in data.iter() {
                if self.jst_date(item)? != day {
                    continue;
                }
                match serializer_type {
                    SerializerType::Json => {
                        serde_json::to_writer(&mut file, item)?;
                        file.write_all(b"\n")?;
                    },
                    SerializerType::Msgpack => rmp_serde::encode::write(&mut file, item)?,
                }
            }
        }

        Ok(())
    }

    // 一時的にためる。ファイル書き出しのRwLockを待つことになるので非推奨
    // pub fn push(&mut self, item: Vec<S>) {
    //     self.que.extend(item);
    // }

    // 一時的にためたものを書き出す
    // pub fn flush(&mut self, serializer_type: SerializerType) -> anyhow::Result<()> {
    //     let mut que = Vec::new();
    //     std::mem::swap(&mut self.que, &mut que);
    //     self.write(&que, serializer_type)
    // }
}

impl SerialRecordWriter<Value> {
    pub fn write_json(&self, data: &Value) -> anyhow::Result<()> {
        self.write(data.as_array().context("data is not array")?, SerializerType::Json)
    }
}

#[test]
fn test_record_writer() {
    let record_writer = SerialRecordWriter::<Value>::new("test", &Symbol::new(Currency::BTC, Currency::JPY, crate::symbol::SymbolType::Spot, crate::symbol::Exchange::Coincheck), "json", Box::new(|item| {
        parse_format_time_utc(item["timestamp"].as_str().unwrap()).ok()
    }));
    let data = serde_json::json!([
        {
            "timestamp": "2021-01-01T00:00:00+09:00",
            "price": 1000.0,
            "amount": 1.0,
        },
        {
            "timestamp": "2021-01-01T01:00:00+09:00",
            "price": 1100.0,
            "amount": 1.0,
        },
        {
            "timestamp": "2021-01-02T00:00:00+09:00",
            "price": 1200.0,
            "amount": 1.0,
        },
        {
            "timestamp": "2021-01-02T01:00:00+09:00",
            "price": 1300.0,
            "amount": 1.0,
        },
    ]);
    record_writer.write_json(&data).unwrap();
}

#[test]
fn test_mpack_record_writer() {
    use crate::client::types::{TradeRecord, MpackTradeRecord};
    use crate::order_types::Side;
    let symbol = Symbol::new(Currency::BTC, Currency::JPY, crate::symbol::SymbolType::Spot, crate::symbol::Exchange::Coincheck);
    let rw = SerialRecordWriter::<MpackTradeRecord>::new(
        "mpacktest",
        &symbol,
        "msgpack",
        Box::new(|item| Some(datetime_utc_from_timestamp(item.0.timestamp, crate::utils::time::UnixTimeUnit::MilliSecond)))
    );
    let data = vec![
        TradeRecord::new(
            symbol, datetime_utc(2023, 1, 1, 0, 0, 0).timestamp_millis(),
            100.,
            100.,
            Side::Buy
        ).mpack(),
        TradeRecord::new(
            symbol, datetime_utc(2023, 1, 1, 1, 0, 0).timestamp_millis(),
            150.,
            100.,
            Side::Sell
        ).mpack(),
        TradeRecord::new(
            symbol, datetime_utc(2023, 1, 2, 0, 0, 0).timestamp_millis(),
            200.,
            100.,
            Side::Buy
        ).mpack(),
    ];
    rw.write_msgpack(&data).unwrap();
}