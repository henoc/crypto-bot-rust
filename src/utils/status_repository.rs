use std::{collections::HashMap, fs::File, path::Path};

use chrono::Duration;
use serde_json::{Value, json};

use crate::symbol::{Exchange, Symbol, Currency};

use super::json_utils::object_update;


#[derive(Debug)]
pub struct StatusRepository {
    /// strategy name
    pub name: String,
    pub data: HashMap<Symbol, Value>
}

impl StatusRepository {
    pub fn new(name: &str) -> StatusRepository {
        StatusRepository {
            name: name.to_string(),
            data: HashMap::new(),
        }
    }

    fn file_name(&self, symbol: &Symbol) -> String {
        format!(".status_{}_{}.json", self.name, symbol.to_file_form())
    }

    pub fn get(&mut self, symbol: &Symbol, expire_td: Option<Duration>)->anyhow::Result<Value> {
        if let Some(data) = self.data.get(symbol) {
            return Ok(data.clone());
        }
        let file_name = self.file_name(symbol);
        if !Path::new(&file_name).exists() {
            return Ok(Value::Null);
        }
        let file = File::open(file_name)?;
        let data: Value = serde_json::from_reader(file)?;
        if let Some(etd) = expire_td {
            if data["updated"].as_i64().unwrap_or(0) + etd.num_seconds() < chrono::Utc::now().timestamp() {
                return Ok(Value::Null);
            }
        }
        self.data.insert(symbol.clone(), data.clone());
        Ok(data)
    }

    pub fn update(&mut self, symbol: Symbol, mut diff: Value)->anyhow::Result<()> {
        let file_name = self.file_name(&symbol);
        let mut file = File::create(file_name)?;
        diff["updated"] = Value::from(chrono::Utc::now().timestamp());
        if !self.data.contains_key(&symbol) {
            self.data.insert(symbol.clone(), json!({}));
        }
        let mut next = self.data[&symbol].clone();
        object_update(&mut next, diff);
        serde_json::to_writer_pretty(&mut file, &next)?;
        self.data.insert(symbol, next);
        Ok(())
    }
}

#[test]
fn test_status() {
    let mut status = StatusRepository::new("test");
    let symbol = Symbol::new(Currency::BTC, Currency::JPY, crate::symbol::SymbolType::Spot, Exchange::Coincheck);
    let data = status.get(&symbol, Some(Duration::seconds(0))).unwrap();
    assert_eq!(data, Value::Null);
    let diff = json!({
        "a": 1,
        "b": 2,
    });
    status.update(symbol.clone(), diff.clone()).unwrap();

    let mut status = StatusRepository::new("test");
    let data = status.get(&symbol, Some(Duration::seconds(60))).unwrap();
    assert_eq!(data["a"].as_i64(), diff["a"].as_i64());
    assert_eq!(data["b"].as_i64(), diff["b"].as_i64());
    assert_eq!(data["updated"].as_i64().is_some(), true);
}