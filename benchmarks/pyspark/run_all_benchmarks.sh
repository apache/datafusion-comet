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

# Run all shuffle benchmarks (Spark, Comet JVM, Comet Native)
# Check the Spark UI during each run to compare shuffle sizes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_PATH="${1:-/tmp/shuffle-benchmark-data}"
COMET_JAR="${COMET_JAR:-$SCRIPT_DIR/../../spark/target/comet-spark-spark3.5_2.12-0.13.0-SNAPSHOT.jar}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16g}"
EVENT_LOG_DIR="${EVENT_LOG_DIR:-/tmp/spark-events}"

# Create event log directory
mkdir -p "$EVENT_LOG_DIR"

echo "========================================"
echo "Shuffle Size Comparison Benchmark"
echo "========================================"
echo "Data path:       $DATA_PATH"
echo "Comet JAR:       $COMET_JAR"
echo "Spark master:    $SPARK_MASTER"
echo "Executor memory: $EXECUTOR_MEMORY"
echo "Event log dir:   $EVENT_LOG_DIR"
echo "========================================"

# Run Spark baseline (no Comet)
echo ""
echo ">>> Running SPARK shuffle benchmark (baseline)..."
$SPARK_HOME/bin/spark-submit \
  --master "$SPARK_MASTER" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="$EVENT_LOG_DIR" \
  --conf spark.comet.enabled=false \
  --conf spark.comet.exec.shuffle.enabled=false \
  "$SCRIPT_DIR/run_benchmark.py" \
  --data "$DATA_PATH" \
  --shuffle-mode spark \
  --benchmark shuffle-hash

# Helper function to run a Comet benchmark
# Usage: run_comet_benchmark <mode> <benchmark_name>
run_comet_benchmark() {
  local shuffle_mode=$1
  local benchmark=$2

  echo ""
  echo ">>> Running COMET $shuffle_mode-shuffle benchmark: $benchmark..."

  $SPARK_HOME/bin/spark-submit \
    --master "$SPARK_MASTER" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --jars "$COMET_JAR" \
    --driver-class-path "$COMET_JAR" \
    --conf spark.executor.extraClassPath="$COMET_JAR" \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir="$EVENT_LOG_DIR" \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16g \
    --conf spark.comet.exec.shuffle.mode="$shuffle_mode" \
    --conf spark.comet.exec.replaceSortMergeJoin=true \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.cast.allowIncompatible=true \
    "$SCRIPT_DIR/run_benchmark.py" \
    --data "$DATA_PATH" \
    --shuffle-mode "$shuffle_mode" \
    --benchmark "$benchmark"
}

# Run Comet JVM shuffle with native writes
run_comet_benchmark jvm shuffle-hash-native-write
run_comet_benchmark jvm shuffle-roundrobin-native-write

# Run Comet JVM shuffle with Spark writes
run_comet_benchmark jvm shuffle-hash-spark-write
run_comet_benchmark jvm shuffle-roundrobin-spark-write

# Run Comet Native shuffle with native writes
run_comet_benchmark native shuffle-hash-native-write
run_comet_benchmark native shuffle-roundrobin-native-write

# Run Comet Native shuffle with Spark writes
run_comet_benchmark native shuffle-hash-spark-write
run_comet_benchmark native shuffle-roundrobin-spark-write

echo ""
echo "========================================"
echo "BENCHMARK COMPLETE"
echo "========================================"
echo "Event logs written to: $EVENT_LOG_DIR"
echo ""
