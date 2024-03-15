#!/bin/sh

cat $RIO_CONTEXT_FILE | jq -r '.[0].artifacts[] | select (.name | endswith(".jar")) | .url'
