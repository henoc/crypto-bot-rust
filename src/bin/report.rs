use labo::{export::{polars::{export::regex::Regex, prelude::{DataFrame, NamedFrom}, series::Series}, anyhow::Context}, export::anyhow};
use duct::cmd;

use bot::client::mail::send_mail;


fn main() -> anyhow::Result<()> {
    // immortalctl status &> output
    // stderr_to_stdoutをしないとimmortalctlの改行が入らない
    // 標準ライブラリではできない https://stackoverflow.com/questions/41019780/merge-child-process-stdout-and-stderr
    let output = cmd!("immortalctl", "status").stderr_to_stdout().read()?;
    let (status, is_up) = parse_immotalctl_status(&output)?;

    // https://pola-rs.github.io/polars/polars/index.html#config-with-env-vars
    std::env::set_var("POLARS_FMT_TABLE_FORMATTING", "NOTHING");
    std::env::set_var("POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION", "1");
    std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES", "1");
    let body = format!("{}", status.select(["Time", "Name"])?);
    let hostname = cmd!("hostname").read()?;
    let ox = is_up.iter().map(|b| if *b {'o'} else {'x'}).collect::<String>();
    send_mail(format!("Bot Report {hostname} {ox}"), body)?;
    Ok(())
}

fn parse_immotalctl_status(output: &str) -> anyhow::Result<(DataFrame, Vec<bool>)> {
    let ansi_color = Regex::new(r"\x1B\[([0-9]{1,2}(;[0-9]{1,2})*)?m")?;
    let ansi_red = Regex::new(r"\x1B\[0;31m")?;
    let is_up = output.lines().skip(1).map(|line| !ansi_red.is_match(line)).collect::<Vec<_>>();
    let output = ansi_color.replace_all(output, "");
    let immortalctl_line_re = Regex::new(r"^\s*(\d+)\s+([\ddhms\.]+)\s+(\w+)\s+(.+)$").unwrap();
    let mut ret = vec![vec![];4];
    for (line, &up) in output.lines().skip(1).zip(is_up.iter()) {
        let caps = immortalctl_line_re.captures(line).context(format!("failed to parse immortalctl status line: {}", line))?;
        for i in 0..4 {
            ret[i].push(caps.get(i+1).unwrap().as_str());
        }
        if !up {*ret[1].last_mut().unwrap() = "---"}
    }
    let d = DataFrame::new(
        vec![
            Series::new("PID", &ret[0]),
            Series::new("Time", &ret[1]),
            Series::new("Name", &ret[2]),
            Series::new("CMD", &ret[3]),
        ]
    )?;
    Ok((d, is_up))
}

#[test]
fn test_parse_immortalctl_status() {
    // sudo immortalctl status &> sample.txt
    let s = std::fs::read_to_string("test/immortalctl_status_output.txt").unwrap();
    let parsed = parse_immotalctl_status(&s).unwrap();
    std::env::set_var("POLARS_FMT_TABLE_FORMATTING", "NOTHING");
    std::env::set_var("POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION", "1");
    std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES", "1");
    println!("{}", parsed.0);
}

#[test]
fn test_parse_immortalctl_line() {
    let re = Regex::new(r"^\s*(\d+)\s+([\ddhms\.]+)\s+(\w+)\s+(.+)$").unwrap();
    let line = "3590810   1d12h46m57.4s          crawler_binance         /bin/bash /home/ec2-user/immortal/wrapped_cmd.sh";
    let caps = re.captures(line).unwrap();
    assert_eq!(caps.get(1).unwrap().as_str(), "3590810");
    assert_eq!(caps.get(2).unwrap().as_str(), "1d12h46m57.4s");
    assert_eq!(caps.get(3).unwrap().as_str(), "crawler_binance");
    assert_eq!(caps.get(4).unwrap().as_str(), "/bin/bash /home/ec2-user/immortal/wrapped_cmd.sh");

    let line = " 1860   21h47m52.4s          crawler_binance         /bin/bash /home/ec2-user/immortal/wrapped_cmd.sh";
    let caps = re.captures(line).unwrap();
    assert_eq!(caps.get(4).unwrap().as_str(), "/bin/bash /home/ec2-user/immortal/wrapped_cmd.sh");
}