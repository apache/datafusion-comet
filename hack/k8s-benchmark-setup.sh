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

CLUSTER_NAME="${COMET_BENCH_CLUSTER:-comet-bench}"
NAMESPACE="${COMET_BENCH_NAMESPACE:-comet-bench}"
K8S_VERSION="${K8S_VERSION:-1.32.0}"
KIND_BIN="${KIND:-kind}"
SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-2.1.0}"
KIND_NODE_IMAGE="kindest/node:v${K8S_VERSION}"

log_info() { echo "[INFO] $1"; }
log_error() { echo "[ERROR] $1" >&2; }

check_prerequisites() {
    local missing=()
    command -v "$KIND_BIN" &>/dev/null || missing+=("kind")
    command -v kubectl &>/dev/null || missing+=("kubectl")
    command -v helm &>/dev/null || missing+=("helm")
    command -v docker &>/dev/null || missing+=("docker")

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing tools: ${missing[*]}"
        exit 1
    fi
}

delete_cluster() {
    log_info "Deleting cluster: $CLUSTER_NAME"
    "$KIND_BIN" delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
}

create_cluster() {
    if "$KIND_BIN" get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log_info "Cluster '$CLUSTER_NAME' already exists"
        return 0
    fi

    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    mkdir -p /tmp/comet-bench-data

    log_info "Creating cluster: $CLUSTER_NAME"
    "$KIND_BIN" create cluster \
        --name "$CLUSTER_NAME" \
        --image "$KIND_NODE_IMAGE" \
        --config "${script_dir}/kind-benchmark-config.yaml" \
        --wait 120s
}

setup_namespace() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    log_info "Setting up namespace: $NAMESPACE"
    kubectl apply -f "${script_dir}/k8s-benchmark-rbac.yaml"
}

install_spark_operator() {
    log_info "Installing Spark Operator v${SPARK_OPERATOR_VERSION}"
    kubectl config use-context "kind-${CLUSTER_NAME}" 2>/dev/null || true

    helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
    helm repo update

    if helm list -n spark-operator 2>/dev/null | grep -q spark-operator; then
        helm upgrade spark-operator spark-operator/spark-operator \
            --namespace spark-operator \
            --version "$SPARK_OPERATOR_VERSION" \
            --set webhook.enable=true \
            --set "spark.jobNamespaces[0]=$NAMESPACE" \
            --timeout 10m --wait || true
    else
        helm install spark-operator spark-operator/spark-operator \
            --namespace spark-operator --create-namespace \
            --version "$SPARK_OPERATOR_VERSION" \
            --set webhook.enable=true \
            --set "spark.jobNamespaces[0]=$NAMESPACE" \
            --timeout 10m --wait || true
    fi

    sleep 5
    kubectl get deployment -n spark-operator 2>/dev/null || true
}

print_status() {
    echo ""
    log_info "Cluster: $CLUSTER_NAME"
    log_info "API Server: $(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
    echo ""
    kubectl get deployment -n spark-operator 2>/dev/null || true
    echo ""
    log_info "Run benchmarks: ./benchmarks/scripts/run-k8s-benchmark.sh [spark|comet]"
    log_info "Delete cluster: ./hack/k8s-benchmark-setup.sh --delete"
}

main() {
    if [[ "${1:-}" == "--delete" ]]; then
        delete_cluster
        exit 0
    fi

    check_prerequisites
    create_cluster
    setup_namespace
    install_spark_operator
    print_status
}

main "$@"
