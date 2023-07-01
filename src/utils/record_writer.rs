use std::{fs::File, collections::HashSet, io::Write};

use anyhow::Context;
use chrono::{DateTime, Utc, NaiveDate};
use serde_json::Value;

use crate::symbol::{Symbol, Currency};

use super::time::{JST, datetime_utc_from_timestamp, parse_format_time_utc};

pub struct RecordWriter {
    pub name: String,
    pub symbol: Symbol,
    pub ext: String,
    pub time_fn: Box<dyn Fn(&Value) -> Option<DateTime<Utc>>>,
}

impl RecordWriter {
    pub fn new(name: &str, symbol: &Symbol, ext: &str, time_fn: Box<dyn Fn(&Value) -> Option<DateTime<Utc>>>) -> RecordWriter {
        RecordWriter {
            name: name.to_string(),
            symbol: symbol.clone(),
            ext: ext.to_string(),
            time_fn,
        }
    }

    pub fn file_name(&self, day: NaiveDate) -> String {
        format!("{}_{}_{}.{}", self.name, self.symbol.to_file_form(), day.format("%Y%m%d"), self.ext)
    }

    fn jst_date(&self, item: &Value) -> anyhow::Result<NaiveDate> {
        Ok((self.time_fn)(item).context(format!("failed at time_fn. value: {:?}", item))?.with_timezone(&JST()).date_naive())
    }

    pub fn write(&self, data: &Value) -> anyhow::Result<()> {
        let mut days = HashSet::new();
        for x in data.as_array().context("data is not array")?.iter().map(|item| self.jst_date(item)) {
            days.insert(x?);
        }
        for day in days {
            let file_name = self.file_name(day);
            let mut file = File::options().append(true).create(true).open(format!("market/{}", file_name))?;
            for item in data.as_array().context("data is not array")?.iter() {
                if self.jst_date(item)? != day {
                    continue;
                }
                serde_json::to_writer(&mut file, item)?;
                file.write_all(b"\n")?;
            }
        }

        Ok(())
    }
}

#[test]
fn test_record_writer() {
    let record_writer = RecordWriter::new("test", &Symbol::new(Currency::BTC, Currency::JPY, crate::symbol::SymbolType::Spot, crate::symbol::Exchange::Coincheck), "json", Box::new(|item| {
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
    record_writer.write(&data).unwrap();
}