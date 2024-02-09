use std::io::{stdout, Write};

use crossterm::{execute, cursor::MoveTo, terminal::{Clear, ClearType}, queue, style::Print, ExecutableCommand};
use anyhow;

use crate::{order_types::Side, symbol::Symbol, data_structure::float_exp::FloatExp};

const N: usize = 10;

/// orderbookをターミナルに描画する
#[derive(Debug)]
pub struct OrderbookDrawer {
    sx: u16,
    sy: u16,
    prev: [[(f64,f64);N];2],
    col1_width: u16,
    col2_width: u16,
    table_width: u16,
    symbols: Vec<Symbol>,
}

impl OrderbookDrawer {
    pub const fn new(sx: u16, sy: u16, symbols: Vec<Symbol>) -> Self {
        Self {
            sx,
            sy,
            prev: [[(0.0,0.0);10];2],
            col1_width: 12,
            col2_width: 18,
            table_width: 30,
            symbols,
        }
    }

    fn cursor(&self, symbol: Symbol, side: Side, nth_order: u16) -> (u16, u16) {
        let center = self.sy + N as u16 + 2;
        match side {
            Side::Buy => (self.table_x(symbol), center + 1 + nth_order),
            Side::Sell => (self.table_x(symbol), center - 1 - nth_order),
        }
    }

    fn table_x(&self, symbol: Symbol) -> u16 {
        let mut offset = 0;
        for s in &self.symbols {
            if s == &symbol {
                break;
            }
            offset += self.table_width;
        }
        self.sx + offset
    }

    fn print_header(&self, symbol: Symbol) -> anyhow::Result<()> {
        let mut stdout = stdout();
        let col1name = "Price";
        let col2name = "Amount";
        execute!(
            stdout,
            MoveTo(self.table_x(symbol), self.sy), Print(symbol.to_file_form()),
            MoveTo(self.table_x(symbol) + self.col1_width - col1name.len() as u16, self.sy + 1), Print(col1name),
            MoveTo(self.table_x(symbol) + self.col1_width + self.col2_width - col2name.len() as u16, self.sy + 1), Print(col2name),
        )?;
        Ok(())
    }

    pub fn print_orderbook(&mut self, next: [[(f64, f64); N];2], symbol: Symbol) -> anyhow::Result<()> {
        self.print_header(symbol)?;
        let mut stdout = stdout();
        for &side in &[Side::Buy, Side::Sell] {
            for i in 0..N {
                let (price, amount) = next[side as usize][i];
                let (prev_price, prev_amount) = self.prev[side as usize][i];
                let price = format!("{}", FloatExp::from_f64(price, symbol.price_precision()));
                let amount = format!("{}", FloatExp::from_f64(amount, symbol.amount_precision()));
                let (x, y) = self.cursor(symbol, side, i as u16);
                if price != format!("{}", FloatExp::from_f64(prev_price, symbol.price_precision())) {
                    execute!(
                        stdout,
                        MoveTo(x + self.col1_width - price.len() as u16, y), Print(price),
                    )?;
                }
                if amount != format!("{}", FloatExp::from_f64(prev_amount, symbol.amount_precision())) {
                    execute!(
                        stdout,
                        MoveTo(x + self.col1_width + 1 + self.col2_width - amount.len() as u16, y), Print(amount),
                    )?;
                }
            }
        }
        let (_x, y) = self.cursor(symbol, Side::Buy, N as u16);
        stdout.execute(MoveTo(0, y))?;
        self.prev = next;
        Ok(())
    }
}

#[test]
fn test_orderbook_drawer() {
    use crate::symbol::*;
    use std::io::Read;
    let symbol = Symbol::new(Currency::BTC, Currency::JPY, SymbolType::Perp, Exchange::Bitflyer);
    let mut d = OrderbookDrawer::new(0, 0, vec![symbol]);
    let orderbook = [
        [(4459318.0, 0.01), (4458000.0, 0.02), (4457079.0, 0.05), (4456950.0, 0.01), (4456940.0, 0.05), (4456000.0, 0.02), (4455010.0, 0.01), (4453780.0, 0.01), (4453752.0, 0.012), (4452700.0, 0.1)],
        [(4462671.0, 0.04), (4462672.0, 0.012), (4464026.0, 0.02), (4464027.0, 0.1), (4464411.0, 0.03), (4464749.0, 0.03), (4464750.0, 0.04), (4464751.0, 0.2), (4465225.0, 0.01), (4465350.0, 0.01)],
    ];
    let mut stdout = stdout();
    stdout.execute(crossterm::terminal::EnterAlternateScreen).unwrap();
    d.print_orderbook(orderbook, symbol).unwrap();
    // press enter key to end
    stdout.execute(Print("Press enter key to end")).unwrap();
    let mut buf = [0; 1];
    std::io::stdin().read_exact(&mut buf).unwrap();
    stdout.execute(crossterm::terminal::LeaveAlternateScreen).unwrap();   
}