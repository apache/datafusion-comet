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

# Wrapper script for running TPC benchmarks in a Docker Compose Spark cluster.
#
# Orchestrates the full lifecycle: start cluster, run benchmark, tear down.
# All arguments (except --laptop) are passed through to run.py.
#
# Usage:
#   ./benchmarks/tpc/docker-bench.sh --engine comet --benchmark tpch
#   ./benchmarks/tpc/docker-bench.sh --laptop --engine comet --benchmark tpch
#   ./benchmarks/tpc/docker-bench.sh --engine comet --benchmark tpch --profile
#
# Required environment variables:
#   DATA_DIR   - Host path to TPC data
#
# Engine-specific environment variables (set the one matching --engine):
#   COMET_JAR   - Host path to Comet JAR
#   GLUTEN_JAR  - Host path to Gluten JAR
#   ICEBERG_JAR - Host path to Iceberg Spark runtime JAR
#
# Optional environment variables:
#   RESULTS_DIR        - Host path for results (default: /tmp/bench-results)
#   BENCH_IMAGE        - Docker image name (default: comet-bench)
#   BENCH_JAVA_HOME    - Java home inside container
#   WORKER_MEM_LIMIT   - Memory limit per worker container
#   BENCH_MEM_LIMIT    - Memory limit for bench container
#   WORKER_MEMORY      - Spark executor memory
#   WORKER_CORES       - Spark executor cores
#   METRICS_INTERVAL   - Metrics collection interval in seconds

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/infra/docker"

# --- Parse our own flags (--laptop), pass the rest to run.py ---
LAPTOP=false
RUN_ARGS=()

for arg in "$@"; do
  if [ "$arg" = "--laptop" ]; then
    LAPTOP=true
  else
    RUN_ARGS+=("$arg")
  fi
done

if [ "$LAPTOP" = true ]; then
  COMPOSE_FILE="$COMPOSE_DIR/docker-compose-laptop.yml"
else
  COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"
fi

# --- Validate environment ---
if [ -z "${DATA_DIR:-}" ]; then
  echo "Error: DATA_DIR is not set. Set it to the host path of your TPC data." >&2
  exit 1
fi

RESULTS_DIR="${RESULTS_DIR:-/tmp/bench-results}"

# --- Ensure results directories exist ---
mkdir -p "$RESULTS_DIR/spark-events"

# --- Cleanup on exit (Ctrl-C, errors, normal exit) ---
cleanup() {
  echo ""
  echo "Stopping Docker Compose cluster..."
  docker compose -f "$COMPOSE_FILE" down
}
trap cleanup EXIT

# --- Start the cluster ---
echo "Starting Spark cluster with: $COMPOSE_FILE"
docker compose -f "$COMPOSE_FILE" up -d

# --- Run the benchmark ---
echo "Running benchmark: run.py ${RUN_ARGS[*]:-}"
docker compose -f "$COMPOSE_FILE" \
  run --rm -p 4040:4040 bench \
  python3 /opt/benchmarks/run.py \
  --output /results --no-restart \
  "${RUN_ARGS[@]}"
