#!/bin/bash

POSITIONAL_ARGS=()
SERVER="aws-ec2-4"

while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--server)
      SERVER="$2"
      shift # past argument
      shift # past value
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters

rsync -uvz target/x86_64-unknown-linux-gnu/release/bot "${SERVER}":~/
rsync -uvz config.bot.yaml "${SERVER}":~/
rsync -uvz config.yaml "${SERVER}":~/
rsync -uvz cron-settings.crontab "${SERVER}":~/cron-settings.crontab
# rsync -uvzr --delete cron/ "${SERVER}":~/.cron    # Rename dirname to avoid moving the cron files to /usr/local/bot/

ssh "${SERVER}" -t << EOL
  sudo su -
  mkdir -p /usr/local/bot
  mv /home/ec2-user/* /usr/local/bot/
  crontab /usr/local/bot/cron-settings.crontab
EOL
