#!/bin/bash

POSITIONAL_ARGS=()
SERVER="aws-ec2-3"

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
rsync -uvzr --delete immortal/ "${SERVER}":~/immortal/
