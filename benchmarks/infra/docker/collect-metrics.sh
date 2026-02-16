#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Container-level memory metrics collector.
#
# Polls cgroup memory stats at a fixed interval and writes a CSV with
# columns: timestamp, memory_usage_bytes, memory_limit_bytes, rss_bytes,
# cache_bytes, swap_bytes.
#
# Works with both cgroup v1 and v2.
#
# Usage:
#   collect-metrics.sh [INTERVAL_SECS] [OUTPUT_CSV]
#
# Defaults: interval=1, output=/results/container-metrics.csv

set -e

INTERVAL="${1:-1}"
OUTPUT="${2:-/results/container-metrics.csv}"

# Detect cgroup version
if [ -f /sys/fs/cgroup/memory/memory.usage_in_bytes ]; then
    CGROUP_VERSION=1
elif [ -f /sys/fs/cgroup/memory.current ]; then
    CGROUP_VERSION=2
else
    echo "Warning: cannot detect cgroup memory files; polling disabled" >&2
    # Still write a header so downstream tools don't break on a missing file.
    echo "timestamp_ms,memory_usage_bytes,memory_limit_bytes,rss_bytes,cache_bytes,swap_bytes" > "$OUTPUT"
    # Sleep forever so the container stays up (compose expects it to keep running).
    exec sleep infinity
fi

# ---- helpers ----

read_file() {
    # Return the contents of a file, or "0" if it doesn't exist.
    if [ -f "$1" ]; then cat "$1"; else echo "0"; fi
}

read_stat() {
    # Extract a named field from memory.stat (cgroup v1 format: "key value").
    grep "^$1 " "$2" 2>/dev/null | awk '{print $2}' || echo "0"
}

poll_v1() {
    local usage limit rss cache swap
    usage=$(read_file /sys/fs/cgroup/memory/memory.usage_in_bytes)
    limit=$(read_file /sys/fs/cgroup/memory/memory.limit_in_bytes)
    local stat=/sys/fs/cgroup/memory/memory.stat
    rss=$(read_stat total_rss "$stat")
    cache=$(read_stat total_cache "$stat")
    swap=$(read_file /sys/fs/cgroup/memory/memory.memsw.usage_in_bytes)
    # swap file reports memory+swap; subtract memory to get swap only
    if [ "$swap" != "0" ]; then
        swap=$((swap - usage))
        [ "$swap" -lt 0 ] && swap=0
    fi
    echo "$usage,$limit,$rss,$cache,$swap"
}

poll_v2() {
    local usage limit rss cache swap
    usage=$(read_file /sys/fs/cgroup/memory.current)
    limit=$(read_file /sys/fs/cgroup/memory.max)
    [ "$limit" = "max" ] && limit=0
    local stat=/sys/fs/cgroup/memory.stat
    rss=$(read_stat anon "$stat")
    cache=$(read_stat file "$stat")
    swap=$(read_file /sys/fs/cgroup/memory.swap.current)
    echo "$usage,$limit,$rss,$cache,$swap"
}

# ---- main loop ----

echo "timestamp_ms,memory_usage_bytes,memory_limit_bytes,rss_bytes,cache_bytes,swap_bytes" > "$OUTPUT"
echo "Collecting container memory metrics every ${INTERVAL}s -> ${OUTPUT} (cgroup v${CGROUP_VERSION})" >&2

while true; do
    ts=$(date +%s%3N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))')
    if [ "$CGROUP_VERSION" = "1" ]; then
        vals=$(poll_v1)
    else
        vals=$(poll_v2)
    fi
    echo "${ts},${vals}" >> "$OUTPUT"
    sleep "$INTERVAL"
done
