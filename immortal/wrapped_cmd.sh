#!/bin/bash

# trap EXIT and kill child process
trap 'kill $(jobs -p)' EXIT

PID=$$
# get the immortal process name from the PID, and remove the ANSI escape codes
NAME=$(immortalctl status | grep "$PID" | awk '{print $3}' | sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})*)?m//g")
if [ -z "$NAME" ]; then
    echo "Could not find immortal process name for PID $$"
    exit 0
fi

eval "$CMD"
# if the exit code is 0, then `immortalctl stop` will be called
if [ $? -eq 0 ]; then
    immortalctl stop "$NAME"
fi
