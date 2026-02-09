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

# TPC-H benchmark using Iceberg tables with Comet's native iceberg-rust integration.
#
# Required environment variables:
#   SPARK_HOME      - Path to Spark installation
#   SPARK_MASTER    - Spark master URL (e.g., spark://localhost:7077)
#   COMET_JAR       - Path to Comet JAR
#   ICEBERG_JAR     - Path to Iceberg Spark runtime JAR
#   ICEBERG_WAREHOUSE - Path to Iceberg warehouse directory
#   TPCH_QUERIES    - Path to TPC-H query files
#
# Optional:
#   ICEBERG_CATALOG - Catalog name (default: local)
#   ICEBERG_DATABASE - Database name (default: tpch)
#
# Setup (run once to create Iceberg tables from Parquet):
#   $SPARK_HOME/bin/spark-submit \
#       --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1 \
#       --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
#       --conf spark.sql.catalog.local.type=hadoop \
#       --conf spark.sql.catalog.local.warehouse=$ICEBERG_WAREHOUSE \
#       create-iceberg-tpch.py \
#       --parquet-path $TPCH_DATA \
#       --catalog local \
#       --database tpch

set -e

# Defaults
ICEBERG_CATALOG=${ICEBERG_CATALOG:-local}
ICEBERG_DATABASE=${ICEBERG_DATABASE:-tpch}

# Validate required variables
if [ -z "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME is not set"
    exit 1
fi
if [ -z "$COMET_JAR" ]; then
    echo "Error: COMET_JAR is not set"
    exit 1
fi
if [ -z "$ICEBERG_JAR" ]; then
    echo "Error: ICEBERG_JAR is not set"
    echo "Download from: https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.8.1/"
    exit 1
fi
if [ -z "$ICEBERG_WAREHOUSE" ]; then
    echo "Error: ICEBERG_WAREHOUSE is not set"
    exit 1
fi
if [ -z "$TPCH_QUERIES" ]; then
    echo "Error: TPCH_QUERIES is not set"
    exit 1
fi

$SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
$SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER

$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --jars $COMET_JAR,$ICEBERG_JAR \
    --driver-class-path $COMET_JAR:$ICEBERG_JAR \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.executor.memory=16g \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16g \
    --conf spark.eventLog.enabled=true \
    --conf spark.driver.extraClassPath=$COMET_JAR:$ICEBERG_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR:$ICEBERG_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.exec.replaceSortMergeJoin=true \
    --conf spark.comet.expression.Cast.allowIncompatible=true \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.scan.icebergNative.enabled=true \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop \
    --conf spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=$ICEBERG_WAREHOUSE \
    --conf spark.sql.defaultCatalog=${ICEBERG_CATALOG} \
    tpcbench.py \
    --name comet-iceberg \
    --benchmark tpch \
    --catalog $ICEBERG_CATALOG \
    --database $ICEBERG_DATABASE \
    --queries $TPCH_QUERIES \
    --output . \
    --iterations 1
