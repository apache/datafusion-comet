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

# Script to regenerate golden files for plan stability testing.
# This script must be run from the root of the Comet repository.
#
# Usage: ./dev/regenerate-golden-files.sh [--spark-version <version>]
#
# Options:
#   --spark-version <version>  Only regenerate for specified Spark version (3.4, 3.5, or 4.0)
#                              If not specified, regenerates for all versions.
#
# Examples:
#   ./dev/regenerate-golden-files.sh              # Regenerate for all Spark versions
#   ./dev/regenerate-golden-files.sh --spark-version 3.5  # Regenerate only for Spark 3.5

set -e
set -o pipefail

# Check for JDK 17 or later (required for Spark 4.0)
check_jdk_version() {
    if [ -z "$JAVA_HOME" ]; then
        echo "[ERROR] JAVA_HOME is not set"
        exit 1
    fi

    java_version=$("$JAVA_HOME/bin/java" -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)

    # Handle both "17" and "17.0.x" formats
    if [[ "$java_version" =~ ^1\. ]]; then
        # Old format like 1.8.0 -> extract 8
        java_version=$(echo "$java_version" | cut -d'.' -f2)
    fi

    if [ "$java_version" -lt 17 ]; then
        echo "[ERROR] JDK 17 or later is required for Spark 4.0 compatibility"
        echo "[ERROR] Current JDK version: $java_version"
        echo "[ERROR] Please set JAVA_HOME to point to JDK 17 or later"
        exit 1
    fi

    echo "[INFO] JDK version check passed: version $java_version"
}

# Check if running from repo root
check_repo_root() {
    if [ ! -f "pom.xml" ] || [ ! -d "spark" ] || [ ! -d "native" ]; then
        echo "[ERROR] This script must be run from the root of the Comet repository"
        exit 1
    fi
}

# Build native code
build_native() {
    echo ""
    echo "=============================================="
    echo "[INFO] Building native code"
    echo "=============================================="
    cd native && cargo build && cd ..
}

# Install Comet for a specific Spark version
install_for_spark_version() {
    local spark_version=$1
    echo ""
    echo "=============================================="
    echo "[INFO] Installing Comet for Spark $spark_version"
    echo "=============================================="
    ./mvnw install -DskipTests -Pspark-$spark_version
}

# Regenerate golden files for a specific Spark version
regenerate_golden_files() {
    local spark_version=$1

    echo ""
    echo "=============================================="
    echo "[INFO] Regenerating golden files for Spark $spark_version"
    echo "=============================================="

    echo "[INFO] Running CometTPCDSV1_4_PlanStabilitySuite..."
    SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark \
        -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" \
        -Pspark-$spark_version -nsu test

    echo "[INFO] Running CometTPCDSV2_7_PlanStabilitySuite..."
    SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark \
        -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" \
        -Pspark-$spark_version -nsu test
}

# Main script
main() {
    local target_version=""

    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --spark-version)
                target_version="$2"
                shift 2
                ;;
            -h|--help)
                echo "Usage: $0 [--spark-version <version>]"
                echo ""
                echo "Options:"
                echo "  --spark-version <version>  Only regenerate for specified Spark version (3.4, 3.5, or 4.0)"
                echo "                             If not specified, regenerates for all versions."
                exit 0
                ;;
            *)
                echo "[ERROR] Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done

    # Validate target version if specified
    if [ -n "$target_version" ]; then
        if [[ ! "$target_version" =~ ^(3\.4|3\.5|4\.0)$ ]]; then
            echo "[ERROR] Invalid Spark version: $target_version"
            echo "[ERROR] Supported versions: 3.4, 3.5, 4.0"
            exit 1
        fi
    fi

    check_repo_root
    check_jdk_version

    # Set SPARK_HOME to current directory (required for golden file output)
    export SPARK_HOME=$(pwd)
    echo "[INFO] SPARK_HOME set to: $SPARK_HOME"

    # Build native code first
    build_native

    # Determine which versions to process
    local versions
    if [ -n "$target_version" ]; then
        versions=("$target_version")
    else
        versions=("3.4" "3.5" "4.0")
    fi

    # Install and regenerate for each version
    for version in "${versions[@]}"; do
        install_for_spark_version "$version"
        regenerate_golden_files "$version"
    done

    echo ""
    echo "=============================================="
    echo "[INFO] Golden file regeneration complete!"
    echo "=============================================="
    echo ""
    echo "The golden files have been updated in:"
    echo "  spark/src/test/resources/tpcds-plan-stability/"
    echo ""
    echo "Please review the changes with 'git diff' before committing."
}

main "$@"
