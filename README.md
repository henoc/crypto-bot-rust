## EC2設定

- amazon linux 2023

```shell
sudo yum install openssl-devel

curl -s https://packagecloud.io/install/repositories/immortal/immortal/script.rpm.sh | sudo bash
sudo yum install immortal
sudo vim /etc/systemd/system/immortaldir.service    # immortalの対象ディレクトリを /home/ec2-user/immortal に変更
sudo systemctl start immortaldir
# hostname変更
sudo hostnamectl set-hostname s3

# cron設定
sudo yum install cronie
sudo systemctl start crond.service
sudo crontab -e
# 58 * * * * cd /home/ec2-user/; ./report >> /tmp/report.log 2>&1
# 1 22 * * * cd /home/ec2-user/; ./transfer >> /tmp/transfer.log 2>&1
```

### EC2メモ

- cronのログ確認: `journalctl -u crond`

## クロスコンパイル

- glibcの要求バージョン2.28を満たすamazon linux 2023でないと実行できない
- Dockerfileを参照してcrossでコンパイルする

```shell
# verboseをつけると失敗箇所がわかりやすい
cross build --target x86_64-unknown-linux-gnu --verbose --release
```

## bot

```bash
# ローカルでも起動できる
sudo ./target/x86_64-unknown-linux-gnu/release/bot --name crawler_bitflyer
# ステータスファイルの確認など
sudo ./bot --name crawler_bitflyer --debug
```

## 実装メモ

- static変数ではArcは不要
    - Arcは（スレッド共有用）参照カウンタなので
- OptionはcontextでResultに変換、なるべくtryで回すことで正しいbacktraceを得る

### git submodule

外側のリポジトリのブランチを変えてもsubmodule内のファイルは手動で参照先の内容に変わらないので手動で変える:
- git submodule update

- git submodule status
  - 参照先のcommitを表示