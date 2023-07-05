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
```

## クロスコンパイル

- glibcの要求バージョン2.28を満たすamazon linux 2023でないと実行できない
- Dockerfileを参照してcrossでコンパイルする

```
# verboseをつけると失敗箇所がわかりやすい
cross build --target x86_64-unknown-linux-gnu --verbose --release
```

## bot

```bash
# ステータスファイルの確認など
sudo ./bot --name crawler_bitflyer --check
```
