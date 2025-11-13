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
# Usage: ./run.sh
# Builds necessary JARs, generates data and queries, and runs fuzz tests for Comet Spark.
# Environment variables:
#   SPARK_HOME - path to Spark installation
#   SPARK_MASTER - Spark master URL (default: local[*])
#   SCALA_MAJOR_VERSION - Scala major version to use (default: 2.12)
#   SPARK_MAJOR_VERSION - Spark major version to use (default: 3.5)
#   NUM_FILES - number of data files to generate (default: 2)
#   NUM_ROWS - number of rows per file (default: 200)
#   NUM_QUERIES - number of queries to generate (default: 500)

set -eux

DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="${DIR}/.."
MVN_CMD="${PARENT_DIR}/mvnw"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SCALA_MAJOR_VERSION="${SCALA_MAJOR_VERSION:-2.12}"
SPARK_MAJOR_VERSION="${SPARK_MAJOR_VERSION:-3.5}"
PROFILES="-Pscala-${SCALA_MAJOR_VERSION},spark-${SPARK_MAJOR_VERSION}"
PROJECT_VERSION=$("${MVN_CMD}" -f "${DIR}/pom.xml" -q help:evaluate -Dexpression=project.version -DforceStdout)
COMET_SPARK_JAR="${PARENT_DIR}/spark/target/comet-spark${SPARK_MAJOR_VERSION}_${SCALA_MAJOR_VERSION}-${PROJECT_VERSION}.jar"
COMET_FUZZ_JAR="${DIR}/target/comet-fuzz-spark${SPARK_MAJOR_VERSION}_${SCALA_MAJOR_VERSION}-${PROJECT_VERSION}-jar-with-dependencies.jar"
NUM_FILES="${NUM_FILES:-2}"
NUM_ROWS="${NUM_ROWS:-200}"
NUM_QUERIES="${NUM_QUERIES:-500}"

if [ ! -f "${COMET_SPARK_JAR}" ]; then
  echo "Building Comet Spark jar..."
  pushd "${PARENT_DIR}"
  PROFILES="${PROFILES}" make
  popd
else
  echo "Building Fuzz testing jar..."
  "${MVN_CMD}" -f "${DIR}/pom.xml" package -DskipTests "${PROFILES}"
fi

echo "Generating data..."
"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --class org.apache.comet.fuzz.Main \
  "${COMET_FUZZ_JAR}" \
  data --num-files="${NUM_FILES}" --num-rows="${NUM_ROWS}" \
  --exclude-negative-zero \
  --generate-arrays --generate-structs --generate-maps

echo "Generating queries..."
"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --class org.apache.comet.fuzz.Main \
  "${COMET_FUZZ_JAR}" \
  queries --num-files="${NUM_FILES}" --num-queries="${NUM_QUERIES}"

echo "Running fuzz tests..."
"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=16G \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.comet.enabled=true \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.comet.exec.shuffle.enabled=true \
  --jars "${COMET_SPARK_JAR}" \
  --conf spark.driver.extraClassPath="${COMET_SPARK_JAR}" \
  --conf spark.executor.extraClassPath="${COMET_SPARK_JAR}" \
  --class org.apache.comet.fuzz.Main \
  "${COMET_FUZZ_JAR}" \
  run --num-files="${NUM_FILES}" --filename="queries.sql"
