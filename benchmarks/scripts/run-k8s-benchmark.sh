#!/bin/bash
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

set -euo pipefail

MODE="${1:-spark}"
QUERY="${2:-q1}"

CLUSTER_NAME="${COMET_BENCH_CLUSTER:-comet-bench}"
NAMESPACE="${COMET_BENCH_NAMESPACE:-comet-bench}"
DOCKER_IMAGE="${COMET_DOCKER_IMAGE:-comet-bench:local}"
DATA_PATH="${TPCH_DATA_PATH:-/data/tpch}"
RESULTS_DIR="${RESULTS_DIR:-/tmp/comet-bench-results}"

DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
EXECUTOR_INSTANCES="${EXECUTOR_INSTANCES:-2}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"

log_info() { echo "[INFO] $1"; }
log_error() { echo "[ERROR] $1" >&2; }

get_k8s_api_server() {
    kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'
}

build_spark_submit_cmd() {
    local mode="$1"
    local query="$2"
    local api_server
    api_server=$(get_k8s_api_server)

    local cmd="$SPARK_HOME/bin/spark-submit"
    cmd+=" --master k8s://${api_server}"
    cmd+=" --deploy-mode cluster"
    cmd+=" --name comet-bench-${mode}-${query}"
    cmd+=" --conf spark.kubernetes.namespace=${NAMESPACE}"
    cmd+=" --conf spark.kubernetes.container.image=${DOCKER_IMAGE}"
    cmd+=" --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark"
    cmd+=" --conf spark.kubernetes.driver.pod.name=comet-bench-driver"
    cmd+=" --conf spark.driver.memory=${DRIVER_MEMORY}"
    cmd+=" --conf spark.executor.instances=${EXECUTOR_INSTANCES}"
    cmd+=" --conf spark.executor.memory=${EXECUTOR_MEMORY}"
    cmd+=" --conf spark.executor.cores=${EXECUTOR_CORES}"
    cmd+=" --conf spark.kubernetes.driver.volumes.hostPath.data.mount.path=/data"
    cmd+=" --conf spark.kubernetes.driver.volumes.hostPath.data.options.path=/tmp/comet-bench-data"
    cmd+=" --conf spark.kubernetes.executor.volumes.hostPath.data.mount.path=/data"
    cmd+=" --conf spark.kubernetes.executor.volumes.hostPath.data.options.path=/tmp/comet-bench-data"

    if [[ "$mode" == "comet" ]]; then
        local comet_jar
        comet_jar=$(find "$SPARK_HOME/jars" -name "comet-spark-*.jar" 2>/dev/null | head -1)
        if [[ -n "$comet_jar" ]]; then
            cmd+=" --jars local://${comet_jar}"
            cmd+=" --conf spark.executor.extraClassPath=${comet_jar}"
            cmd+=" --conf spark.driver.extraClassPath=${comet_jar}"
        fi
        cmd+=" --conf spark.plugins=org.apache.spark.CometPlugin"
        cmd+=" --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions"
        cmd+=" --conf spark.comet.enabled=true"
        cmd+=" --conf spark.comet.exec.enabled=true"
        cmd+=" --conf spark.comet.exec.all.enabled=true"
        cmd+=" --conf spark.comet.cast.allowIncompatible=true"
        cmd+=" --conf spark.comet.exec.shuffle.enabled=true"
        cmd+=" --conf spark.comet.exec.shuffle.mode=auto"
        cmd+=" --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
    fi

    cmd+=" local:///opt/comet-bench/scripts/tpch-benchmark.py"
    cmd+=" --query ${query}"
    cmd+=" --data ${DATA_PATH}"
    cmd+=" --mode ${mode}"

    echo "$cmd"
}

run_benchmark() {
    local mode="$1"
    local query="$2"
    local result_file="${RESULTS_DIR}/${mode}_${query}_result.json"

    mkdir -p "$RESULTS_DIR"
    log_info "Running benchmark: mode=$mode, query=$query"

    local start_time
    start_time=$(date +%s.%N)

    local cmd
    cmd=$(build_spark_submit_cmd "$mode" "$query")
    log_info "Command: $cmd"

    if eval "$cmd"; then
        local end_time duration
        end_time=$(date +%s.%N)
        duration=$(echo "$end_time - $start_time" | bc)

        cat > "$result_file" << EOF
{
    "mode": "$mode",
    "query": "$query",
    "duration_seconds": $duration,
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
        log_info "Results: $result_file"
        echo "result_file=$result_file" >> "${GITHUB_OUTPUT:-/dev/null}"
    else
        log_error "Benchmark failed"
        exit 1
    fi
}

cleanup() {
    kubectl delete pod comet-bench-driver -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true
}

main() {
    if [[ "$MODE" == "-h" ]] || [[ "$MODE" == "--help" ]]; then
        echo "Usage: $0 [spark|comet] [query]"
        exit 0
    fi

    if [[ "$MODE" != "spark" ]] && [[ "$MODE" != "comet" ]]; then
        log_error "Invalid mode: $MODE"
        exit 1
    fi

    cleanup
    run_benchmark "$MODE" "$QUERY"
}

main "$@"
