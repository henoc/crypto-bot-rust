use std::{collections::HashMap, fs::File, path::Path, ops::Index};

use anyhow;
use labo::export::{serde_json, chrono};
use labo::export::chrono::Duration;
use labo::export::serde_json::{Value, json};

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

    pub fn new_init(name: &str, symbol: &Symbol, expire_td: Option<Duration>)->anyhow::Result<StatusRepository> {
        let mut sr = StatusRepository::new(name);
        sr.init(symbol, expire_td)?;
        Ok(sr)
    }

    fn file_name(&self, symbol: &Symbol) -> String {
        format!(".status_{}_{}.json", self.name, symbol.to_file_form())
    }

    pub fn init(&mut self, symbol: &Symbol, expire_td: Option<Duration>)->anyhow::Result<()> {
        let file_name = self.file_name(symbol);
        let mut data = json!({});
        if Path::new(&file_name).exists() {
            let file = File::open(file_name)?;
            data = serde_json::from_reader(file)?;
            if let Some(etd) = expire_td {
                if data["updated"].as_i64().unwrap_or(0) + etd.num_seconds() < chrono::Utc::now().timestamp() {
                    data = json!({});
                }
            }
        }
        self.data.insert(symbol.clone(), data.clone());
        Ok(())
    }

    /// symbolをinitしていなければエラー
    pub fn get(&self, symbol: &Symbol)-> &Value {
        &self.data[symbol]
    }

    pub fn update(&mut self, symbol: Symbol, mut diff: Value)->anyhow::Result<()> {
        let file_name = self.file_name(&symbol);
        let mut file = File::create(file_name)?;
        diff["updated"] = Value::from(chrono::Utc::now().timestamp());
        if !self.data.contains_key(&symbol) {
            self.data.insert(symbol.clone(), json!({}));
        }
        let mut next = self.data[&symbol].clone();
        object_update(&mut next, diff)?;
        serde_json::to_writer_pretty(&mut file, &next)?;
        self.data.insert(symbol, next);
        Ok(())
    }
}

impl Index<&Symbol> for StatusRepository {
    type Output = Value;

    /// symbolをinitしていなければエラー
    #[inline]
    fn index(&self, index: &Symbol) -> &Self::Output {
        &self.data[index]
    }
}

#[test]
fn test_status() {
    let mut status = StatusRepository::new("test");
    let symbol = Symbol::new(Currency::BTC, Currency::JPY, crate::symbol::SymbolType::Spot, Exchange::Coincheck);
    status.init(&symbol, Some(Duration::seconds(0))).unwrap();
    let data = status.get(&symbol);
    assert_eq!(data, &json!({}));
    let diff = json!({
        "a": 1,
        "b": 2,
    });
    status.update(symbol.clone(), diff.clone()).unwrap();

    let mut status = StatusRepository::new("test");
    status.init(&symbol, Some(Duration::seconds(60))).unwrap();
    let data = status.get(&symbol);
    assert_eq!(data["a"].as_i64(), diff["a"].as_i64());
    assert_eq!(data["b"].as_i64(), diff["b"].as_i64());
    assert_eq!(data["updated"].as_i64().is_some(), true);
}