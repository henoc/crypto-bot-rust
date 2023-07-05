use std::{io::{Write, Read}, fs::{OpenOptions, File}, collections::VecDeque};

use anyhow::Context;
use chrono::{DateTime, Utc, Duration};
use memmap::{MmapMut, MmapOptions};
use polars::export::arrow::types::NativeType;
use rmp::Marker;
use serde::{Deserialize, Deserializer};

use crate::{symbol::Symbol, client::types::TradeRecord};

use super::time::{datetime_utc_from_timestamp, KLinesTimeUnit, now_floor_time, floor_time};

#[derive(Debug, Clone)]
pub enum KLineRow {
    Empty,
    Data(KLineRowData),
}

#[derive(Debug, Clone)]
pub struct KLineRowData {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl KLineRow {
    /// 固定長のバイナリ形式で書き込む
    /// ```txt
    /// array_len: 1byte, f64: 8bytes
    /// total: 41bytes
    /// ```
    pub fn write_bytes(&self) -> anyhow::Result<[u8; Self::MSGPACK_LEN]> {
        let mut buf = [0u8; Self::MSGPACK_LEN];
        match self {
            KLineRow::Empty => {
                buf[0..1].copy_from_slice(&[Marker::Null.to_u8()]);
            },
            KLineRow::Data(data) => {
                buf[0..1].copy_from_slice(&[Marker::FixArray(5).to_u8()]);
                let mut ohlcv = [0u8; 40];
                ohlcv[0..8].copy_from_slice(&data.open.to_be_bytes());
                ohlcv[8..16].copy_from_slice(&data.high.to_be_bytes());
                ohlcv[16..24].copy_from_slice(&data.low.to_be_bytes());
                ohlcv[24..32].copy_from_slice(&data.close.to_be_bytes());
                ohlcv[32..40].copy_from_slice(&data.volume.to_be_bytes());
                buf[1..41].copy_from_slice(&ohlcv);
            }
        }
        Ok(buf)
    }

    pub fn read_bytes<R: Read>(rd: &mut R) -> anyhow::Result<Self> {
        match rmp::decode::read_marker(rd) {
            Ok(Marker::Null) => {
                rd.read_exact(&mut [0; 40])?;
                return Ok(KLineRow::Empty);
            },
            Ok(Marker::FixArray(5)) => {
            },
            Ok(others) => anyhow::bail!("invalid marker: {:?}", others),
            Err(e) => anyhow::bail!("invalid marker: {:?}", e),
        };
        let mut buf = [0u8; 40];
        rd.read_exact(buf.as_mut())?;
        let open = f64::from_be_bytes(buf[0..8].try_into()?);
        let high = f64::from_be_bytes(buf[8..16].try_into()?);
        let low = f64::from_be_bytes(buf[16..24].try_into()?);
        let close = f64::from_be_bytes(buf[24..32].try_into()?);
        let volume = f64::from_be_bytes(buf[32..40].try_into()?);
        Ok(KLineRow::Data(KLineRowData {
            open,
            high,
            low,
            close,
            volume,
        }))
    }

    pub const MSGPACK_LEN: usize = 41;

    pub fn to_vec(&self) -> Vec<Option<f64>> {
        match self {
            KLineRow::Empty => vec![None; 5],
            KLineRow::Data(data) => vec![
                Some(data.open),
                Some(data.high),
                Some(data.low),
                Some(data.close),
                Some(data.volume),
            ]
        }
    }
}

#[derive(Debug)]
pub struct KLineMMap {
    symbol: Symbol,
    timeframe: Duration,
    len: usize,
    mmap: MmapMut,
    /// opentimeの降順
    state: VecDeque<KLineRow>,
    head_opentime: DateTime<Utc>,
}

impl KLineMMap {
    pub fn new(symbol: Symbol, timeframe: Duration, len: usize) -> anyhow::Result<Self> {
        // iff file is not found
        if !std::path::Path::new(&Self::mmap_path(symbol, timeframe)).exists() {
            let f = OpenOptions::new().read(true).write(true).create_new(true).open(Self::mmap_path(symbol, timeframe))?;
            f.set_len(Self::mmap_size(len) as u64)?;
            let mmap = unsafe { MmapOptions::new().map_mut(&f)? };
            let mut ret = Self {
                symbol,
                timeframe,
                len,
                mmap,
                state: (0..len).map(|_| KLineRow::Empty).collect(),
                head_opentime: now_floor_time(timeframe, 0),
            };
            ret.update_mmap()?;
            Ok(ret)
        } else {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(Self::mmap_path(symbol, timeframe))?;
            f.set_len(Self::mmap_size(len) as u64)?;
            let mmap = unsafe { MmapOptions::new().map_mut(&f)? };
            let mut ret = Self {
                symbol,
                timeframe,
                len,
                mmap,
                state: VecDeque::with_capacity(len),
                head_opentime: now_floor_time(timeframe, 0),
            };
            ret.head_opentime = ret.mmap_read_header();
            for i in 0..len {
                ret.state.push_back(ret.mmap_read_row(i)?);
            }
            Ok(ret)
        }
    }

    /// stateをmmapに書き込む
    pub fn update_mmap(&mut self) -> anyhow::Result<()> {
        self.mmap_write_header(self.head_opentime)?;
        for i in 0..self.len {
            self.mmap_write_row(i, &self.state[i].clone())?;
        }
        Ok(())
    }

    /// `[[opentime_ms, open, high, low, close, volume], ...]]`
    pub fn mmap_read_all(&self) -> anyhow::Result<Vec<Vec<Option<f64>>>> {
        let head_opentime = self.mmap_read_header().timestamp_millis();
        let mut ret = Vec::with_capacity(self.len);
        // opentime昇順に並べ替える
        for i in (0..self.len).rev() {
            let mut row = vec![Some((head_opentime - (i as i64)*self.timeframe.num_milliseconds()) as f64)];
            row.extend(self.mmap_read_row(i)?.to_vec());
            ret.push(row);
        }
        Ok(ret)
    }

    fn mmap_write_header(&mut self, head_opentime: DateTime<Utc>) -> anyhow::Result<()> {
        (&mut self.mmap[0..8]).write_all(&head_opentime.timestamp_millis().to_be_bytes())?;
        Ok(())
    }

    fn mmap_write_row(&mut self, i: usize, row: &KLineRow) -> anyhow::Result<()> {
        (&mut self.mmap[8 + i*KLineRow::MSGPACK_LEN.. 8 + (i+1)*KLineRow::MSGPACK_LEN]).write_all(&row.write_bytes()?)?;
        Ok(())
    }

    fn mmap_read_header(&self) -> DateTime<Utc> {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.mmap[0..8]);
        let millis = i64::from_be_bytes(buf);
        datetime_utc_from_timestamp(millis, KLinesTimeUnit::MilliSecond)
    }

    fn mmap_read_row(&self, i: usize) -> anyhow::Result<KLineRow> {
        let mut buf = [0u8; KLineRow::MSGPACK_LEN];
        buf.copy_from_slice(&self.mmap[8 + i*KLineRow::MSGPACK_LEN.. 8 + (i+1)*KLineRow::MSGPACK_LEN]);
        KLineRow::read_bytes(&mut &buf[..])
    }

    const fn mmap_size(len: usize) -> usize {
        8 + len * KLineRow::MSGPACK_LEN
    }

    fn mmap_path(symbol: Symbol, timeframe: Duration) -> String {
        format!("/var/tmp/kline_{}_{}s", symbol.to_file_form(), timeframe.num_seconds())
    }

    pub fn get_mmap_path(&self) -> String {
        Self::mmap_path(self.symbol, self.timeframe)
    }

    /// next_head_opentimeがheadに来るようにstateをシフトし、head_opentimeを更新する
    fn shift_state(&mut self, next_head_opentime: DateTime<Utc>) {
        let shift = (next_head_opentime - self.head_opentime).num_seconds() / self.timeframe.num_seconds();
        if shift <= 0 {
            return;
        }
        self.head_opentime = next_head_opentime;
        if shift > self.len as i64 {
            self.state = (0..self.len).map(|_| KLineRow::Empty).collect();
            return;
        }
        for _ in 0..shift {
            self.state.push_front(KLineRow::Empty);
            self.state.pop_back();
        }
    }

    fn index_of(&self, opentime: DateTime<Utc>) -> anyhow::Result<usize> {
        let i = (self.head_opentime - opentime).num_seconds() / self.timeframe.num_seconds();
        if i < 0 || i >= self.len as i64 {
            anyhow::bail!("invalid opentime: {}", opentime);
        }
        Ok(i as usize)
    }

    pub fn update_ohlcvs(&mut self, records: &Vec<TradeRecord>) -> anyhow::Result<()> {
        for record in records {
            self.update_ohlcv(record)?;
        }
        Ok(())
    }

    pub fn update_ohlcv(&mut self, record: &TradeRecord) -> anyhow::Result<()> {
        let opentime = floor_time(datetime_utc_from_timestamp(record.timestamp, KLinesTimeUnit::MilliSecond), self.timeframe, 0);
        // recordのopentimeがstateになければシフト
        self.shift_state(opentime);
        let i: usize = self.index_of(opentime)?;
        match &self.state[i] {
            KLineRow::Empty => {
                let data = KLineRowData {
                    open: record.price,
                    high: record.price,
                    low: record.price,
                    close: record.price,
                    volume: record.amount,
                };
                let row = KLineRow::Data(data);
                self.state[i] = row;
            },
            KLineRow::Data(ref data) => {
                let mut data = data.clone();
                data.high = data.high.max(record.price);
                data.low = data.low.min(record.price);
                data.close = record.price;
                data.volume += record.amount;
                let row = KLineRow::Data(data);
                self.state[i] = row;
            },
        }
        Ok(())
    }
}

#[test]
fn test_kline_row() {
    let b = KLineRow::Empty.write_bytes().unwrap();
    assert_eq!(b.len(), 41);
    assert_eq!(b[0], 0xc0);
}