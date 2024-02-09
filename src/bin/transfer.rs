use std::fs;

use bot::{utils::time::today_jst, logger};
use labo::export::{chrono::Duration, serde_json};
use anyhow;
use duct::cmd;
use log::{info, LevelFilter};

static LOGGER: logger::BotLogger = logger::BotLogger;
static LOCAL_DIR: &str = "market";
static BUCKET_NAME: &str = "henoc-market";

/// s3 transfer
/// rustのaws sdkはプレビュー版らしいのでコマンドでやる
/// 
/// ローカル実行:
/// ```shell
/// env AWS_PROFILE=s3user ./target/x86_64-unknown-linux-gnu/release/transfer
/// ```
fn main() -> anyhow::Result<()> {
    
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))?;

    // aws.exe s3api list-objects-v2 --profile s3user --bucket 'henoc-market' --query 'Contents[*].Key' --no-cli-pager
    let last_update_limit = today_jst() + Duration::hours(1);
    let delete_limit = today_jst() - Duration::days(60);
    let remote_files = cmd!("aws", "s3api", "list-objects-v2", "--bucket", BUCKET_NAME, "--query", "Contents[*].Key", "--no-cli-pager").read()?;
    let remote_files = serde_json::from_str::<Vec<String>>(&remote_files)?;

    // for all files in local dir market/
    for entry in fs::read_dir(LOCAL_DIR)? {
        let entry = entry?;
        // continue if entry is directory
        if !(entry.file_type()?.is_file() && entry.metadata()?.modified()? < last_update_limit.into()) {
            continue;
        }

        let path = entry.path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        if !remote_files.contains(&filename.to_string()) {
            let output = cmd!("aws", "s3", "cp", &path, format!("s3://{}/", BUCKET_NAME)).stderr_to_stdout().read()?;
            info!("aws-cli: {}", output);
        }

        if entry.metadata()?.modified()? < delete_limit.into() {
            fs::remove_file(&path)?;
            info!("{} is deleted", filename);
        }
    }
    Ok(())
}
