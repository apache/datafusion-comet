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

SCALE_FACTOR="${1:-1}"
OUTPUT_DIR="${2:-/tmp/comet-bench-data/tpch}"
TPCH_DBGEN_DIR="${TPCH_DBGEN_DIR:-/tmp/tpch-dbgen}"

log_info() { echo "[INFO] $1"; }
log_error() { echo "[ERROR] $1" >&2; }

check_dbgen() {
    if [[ ! -d "$TPCH_DBGEN_DIR" ]]; then
        log_info "Cloning tpch-dbgen"
        git clone https://github.com/databricks/tpch-dbgen.git "$TPCH_DBGEN_DIR"
        cd "$TPCH_DBGEN_DIR" && make
    fi
}

generate_data() {
    log_info "Generating TPC-H SF=$SCALE_FACTOR"
    mkdir -p "$OUTPUT_DIR"
    cd "$TPCH_DBGEN_DIR"
    ./dbgen -s "$SCALE_FACTOR" -f
    mv *.tbl "$OUTPUT_DIR/" 2>/dev/null || true
    log_info "Data generated: $OUTPUT_DIR"
    ls -lh "$OUTPUT_DIR"
}

main() {
    log_info "TPC-H Data Generation (SF=$SCALE_FACTOR)"
    check_dbgen
    generate_data
}

main "$@"
