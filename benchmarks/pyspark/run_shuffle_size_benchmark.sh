#!/bin/bash
#
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
#

# Compare shuffle write sizes between Spark and Comet shuffle.
#
# This benchmark measures actual shuffle write bytes reported by Spark
# to quantify the overhead of Comet's Arrow IPC shuffle format.
# See https://github.com/apache/datafusion-comet/issues/3882
#
# Prerequisites:
#   - SPARK_HOME set to a Spark 3.5 installation
#   - Comet JAR built (make)
#   - Input parquet data generated (see generate_data.py)
#
# Usage:
#   ./run_shuffle_size_benchmark.sh /path/to/parquet/data
#
# Environment variables:
#   COMET_JAR        Path to Comet JAR (default: auto-detected from repo)
#   SPARK_MASTER     Spark master URL (default: local[*])
#   EXECUTOR_MEMORY  Executor memory (default: 16g)
#   OFFHEAP_SIZE     Off-heap memory for Comet (default: 16g)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_PATH="${1:?Usage: $0 /path/to/parquet/data}"
COMET_JAR="${COMET_JAR:-$SCRIPT_DIR/../../spark/target/comet-spark-spark3.5_2.12-0.15.0-SNAPSHOT.jar}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16g}"
OFFHEAP_SIZE="${OFFHEAP_SIZE:-16g}"

if [ -z "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME is not set"
  exit 1
fi

if [ ! -f "$COMET_JAR" ]; then
  echo "Error: Comet JAR not found at $COMET_JAR"
  echo "Build with 'make' or set COMET_JAR to the correct path."
  exit 1
fi

echo "========================================"
echo "Shuffle Size Comparison Benchmark"
echo "========================================"
echo "Data path:       $DATA_PATH"
echo "Comet JAR:       $COMET_JAR"
echo "Spark master:    $SPARK_MASTER"
echo "Executor memory: $EXECUTOR_MEMORY"
echo "Off-heap size:   $OFFHEAP_SIZE"
echo "========================================"

# Use dedicated local dirs so we can measure actual shuffle file sizes on disk
SPARK_LOCAL_DIR=$(mktemp -d /tmp/spark-shuffle-bench-spark-XXXXXX)
COMET_LOCAL_DIR=$(mktemp -d /tmp/spark-shuffle-bench-comet-XXXXXX)

cleanup() {
  rm -rf "$SPARK_LOCAL_DIR" "$COMET_LOCAL_DIR"
}
trap cleanup EXIT

# Run Spark baseline (no Comet)
echo ""
echo ">>> Running SPARK (no Comet) shuffle size benchmark..."
$SPARK_HOME/bin/spark-submit \
  --master "$SPARK_MASTER" \
  --driver-memory "$EXECUTOR_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --conf spark.local.dir="$SPARK_LOCAL_DIR" \
  --conf spark.comet.enabled=false \
  "$SCRIPT_DIR/run_benchmark.py" \
  --data "$DATA_PATH" \
  --mode spark \
  --benchmark shuffle-size

# Run Comet Native shuffle
echo ""
echo ">>> Running COMET NATIVE shuffle size benchmark..."
$SPARK_HOME/bin/spark-submit \
  --master "$SPARK_MASTER" \
  --driver-memory "$EXECUTOR_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --jars "$COMET_JAR" \
  --driver-class-path "$COMET_JAR" \
  --conf spark.executor.extraClassPath="$COMET_JAR" \
  --conf spark.local.dir="$COMET_LOCAL_DIR" \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size="$OFFHEAP_SIZE" \
  --conf spark.comet.enabled=true \
  --conf spark.comet.exec.shuffle.enabled=true \
  --conf spark.comet.exec.shuffle.mode=native \
  --conf spark.comet.explainFallback.enabled=true \
  "$SCRIPT_DIR/run_benchmark.py" \
  --data "$DATA_PATH" \
  --mode native \
  --benchmark shuffle-size

echo ""
echo "========================================"
echo "BENCHMARK COMPLETE"
echo "========================================"
echo "Compare 'Shuffle disk' bytes/record between the two runs above."
