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

set -e

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}')
if [ "$JAVA_VERSION" -lt 17 ]; then
  echo "Error: Java version must be at least 17. Current version: $(java -version 2>&1 | head -n 1)"
  exit 1
fi
echo "Java version check passed: $JAVA_VERSION"

export SPARK_HOME=`pwd`

./mvnw install -DskipTests -Pspark-3.4
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.4 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.4 -nsu test

./mvnw install -DskipTests -Pspark-3.5
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.5 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.5 -nsu test

./mvnw install -DskipTests -Pspark-4.0
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-4.0 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-4.0 -nsu test
