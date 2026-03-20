#!/bin/bash
# init/wait-for-it.sh

TIMEOUT=30
QUIET=0

echo "Waiting for $HOST:$PORT..."

for i in $(seq $TIMEOUT); do
    nc -z "$HOST" "$PORT" > /dev/null 2>&1
    result=$?
    if [ $result -eq 0 ]; then
        echo "$HOST:$PORT is available!"
        exit 0
    fi
    sleep 1
done

echo "Timeout occurred after waiting $TIMEOUT seconds for $HOST:$PORT"
exit 1