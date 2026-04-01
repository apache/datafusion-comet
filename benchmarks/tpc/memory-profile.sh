#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Memory profiling script for TPC-H queries.
# Runs each query under different configurations and records peak RSS.
#
# Usage:
#   ./memory-profile.sh [--queries "1 5 9"] [--offheap-sizes "4g 8g 16g"]
#                        [--cores 4] [--data /path/to/tpch]
#
# Requires: SPARK_HOME, COMET_JAR (or builds from source)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Defaults
QUERIES="${QUERIES:-$(seq 1 22)}"
OFFHEAP_SIZES="${OFFHEAP_SIZES:-4g 8g 16g}"
LOCAL_CORES="${LOCAL_CORES:-4}"
TPCH_DATA="${TPCH_DATA:-/opt/tpch/sf100}"
SPARK_HOME="${SPARK_HOME:-/opt/spark-3.5.8-bin-hadoop3}"
DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR/memory-profile-results}"

# Find Comet JAR
if [ -z "${COMET_JAR:-}" ]; then
    COMET_JAR=$(ls "$REPO_ROOT"/spark/target/comet-spark-spark3.5_2.12-*-SNAPSHOT.jar 2>/dev/null \
        | grep -v sources | grep -v test | head -1)
    if [ -z "$COMET_JAR" ]; then
        echo "Error: No Comet JAR found. Set COMET_JAR or run 'make'."
        exit 1
    fi
fi

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --queries) QUERIES="$2"; shift 2 ;;
        --offheap-sizes) OFFHEAP_SIZES="$2"; shift 2 ;;
        --cores) LOCAL_CORES="$2"; shift 2 ;;
        --data) TPCH_DATA="$2"; shift 2 ;;
        --output) OUTPUT_DIR="$2"; shift 2 ;;
        --driver-memory) DRIVER_MEMORY="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

mkdir -p "$OUTPUT_DIR"
RESULTS_FILE="$OUTPUT_DIR/results.csv"
LOG_DIR="$OUTPUT_DIR/logs"
mkdir -p "$LOG_DIR"

# Ensure spark-events dir exists
mkdir -p /tmp/spark-events

echo "Configuration:"
echo "  SPARK_HOME:     $SPARK_HOME"
echo "  COMET_JAR:      $COMET_JAR"
echo "  TPCH_DATA:      $TPCH_DATA"
echo "  LOCAL_CORES:    $LOCAL_CORES"
echo "  DRIVER_MEMORY:  $DRIVER_MEMORY"
echo "  OFFHEAP_SIZES:  $OFFHEAP_SIZES"
echo "  QUERIES:        $QUERIES"
echo "  OUTPUT_DIR:     $OUTPUT_DIR"
echo ""

# CSV header
echo "engine,offheap_size,query,peak_rss_mb,wall_time_sec,exit_code" > "$RESULTS_FILE"

# Run a single query and capture peak RSS
run_query() {
    local engine="$1"
    local offheap="$2"
    local query_num="$3"
    local label="${engine}-offheap${offheap}-q${query_num}"
    local log_file="$LOG_DIR/${label}.log"
    local time_file="$LOG_DIR/${label}.time"

    # Common Spark conf
    local conf=(
        --master "local[${LOCAL_CORES}]"
        --driver-memory "$DRIVER_MEMORY"
        --conf "spark.memory.offHeap.enabled=true"
        --conf "spark.memory.offHeap.size=${offheap}"
        --conf "spark.eventLog.enabled=false"
        --conf "spark.ui.enabled=false"
    )

    if [ "$engine" = "comet" ]; then
        conf+=(
            --jars "$COMET_JAR"
            --driver-class-path "$COMET_JAR"
            --conf "spark.driver.extraClassPath=$COMET_JAR"
            --conf "spark.plugins=org.apache.spark.CometPlugin"
            --conf "spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
            --conf "spark.comet.scan.impl=native_datafusion"
            --conf "spark.comet.expression.Cast.allowIncompatible=true"
        )
    fi

    conf+=(
        "$SCRIPT_DIR/tpcbench.py"
        --name "$label"
        --benchmark tpch
        --data "$TPCH_DATA"
        --format parquet
        --output "$OUTPUT_DIR"
        --iterations 1
        --query "$query_num"
    )

    echo -n "  $label ... "

    # Run with /usr/bin/time -l, capture stderr (where time writes) separately
    local exit_code=0
    /usr/bin/time -l "$SPARK_HOME/bin/spark-submit" "${conf[@]}" \
        > "$log_file" 2> "$time_file" || exit_code=$?

    # Parse peak RSS from /usr/bin/time -l output (macOS format: bytes)
    local peak_rss_bytes
    peak_rss_bytes=$(grep "maximum resident set size" "$time_file" | awk '{print $1}') || true
    local peak_rss_mb="N/A"
    if [ -n "$peak_rss_bytes" ]; then
        peak_rss_mb=$((peak_rss_bytes / 1048576))
    fi

    # Parse wall clock time (macOS format: "  89.94 real  363.21 user  10.80 sys")
    local wall_secs="N/A"
    local wall_time
    wall_time=$(grep "real" "$time_file" | tail -1 | awk '{print $1}') || true
    if [ -n "$wall_time" ]; then
        wall_secs="$wall_time"
    fi

    if [ "$exit_code" -eq 0 ]; then
        echo "RSS=${peak_rss_mb}MB, time=${wall_secs}s"
    else
        echo "FAILED (exit=$exit_code), RSS=${peak_rss_mb}MB"
    fi

    echo "${engine},${offheap},${query_num},${peak_rss_mb},${wall_secs},${exit_code}" >> "$RESULTS_FILE"
}

# Main loop
echo "=== Running Spark baseline (no Comet) ==="
for q in $QUERIES; do
    run_query "spark" "4g" "$q"
done

echo ""
echo "=== Running Comet with varying offHeap sizes ==="
for offheap in $OFFHEAP_SIZES; do
    echo "--- offHeap = $offheap ---"
    for q in $QUERIES; do
        run_query "comet" "$offheap" "$q"
    done
    echo ""
done

echo "=== Results ==="
column -t -s, "$RESULTS_FILE"
echo ""
echo "Full results: $RESULTS_FILE"
echo "Logs: $LOG_DIR/"
