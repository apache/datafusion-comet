<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Reference checks for #7573

Run commands from `dev/repros/delta`.

Standalone, test-only probes related to [apache/datafusion-comet#7573](https://github.com/apache/datafusion-comet/issues/7573). They do not implement the requested Comet expression or change admission rules. The issue remains open.

Run with Spark 4.2.0, Scala 2.13.18 and Java 17. Supply matching Delta Spark and storage JARs, version 4.4.0 or 4.4.1. Use a fresh output directory for every invocation.

The setup requires `curl`. Set `SPARK_HOME` and `JAVA_HOME` to your local installations before running it. The Comet artifact is `org.apache.datafusion:comet-spark-spark4.2_2.13:1.2.0`; match the Spark 4.2 and Scala 2.13 lines. This assembly contains the platform library used by the validated Linux x86_64 run; no separate Comet companion JAR was used. Other platforms remain unverified.

```bash
set -eu
: "${SPARK_HOME:?Set SPARK_HOME to a Spark 4.2.0 installation}"
: "${JAVA_HOME:?Set JAVA_HOME to a Java 17 installation}"
export PATH="$JAVA_HOME/bin:$PATH"
DELTA_VERSION=4.4.0 # Use 4.4.1 to repeat the other validated pairing.
ARTIFACT_DIR=$(mktemp -d)
export DELTA_SPARK_JAR="$ARTIFACT_DIR/delta-spark_2.13-$DELTA_VERSION.jar"
export DELTA_STORAGE_JAR="$ARTIFACT_DIR/delta-storage-$DELTA_VERSION.jar"
export COMET_JAR="$ARTIFACT_DIR/comet-spark-spark4.2_2.13-1.2.0.jar"
curl --fail --location --retry 3 -o "$DELTA_SPARK_JAR" \
  "https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.13/$DELTA_VERSION/delta-spark_2.13-$DELTA_VERSION.jar"
curl --fail --location --retry 3 -o "$DELTA_STORAGE_JAR" \
  "https://repo.maven.apache.org/maven2/io/delta/delta-storage/$DELTA_VERSION/delta-storage-$DELTA_VERSION.jar"
curl --fail --location --retry 3 -o "$COMET_JAR" \
  https://repo.maven.apache.org/maven2/org/apache/datafusion/comet-spark-spark4.2_2.13/1.2.0/comet-spark-spark4.2_2.13-1.2.0.jar
mkdir classes
java -Xmx1g -cp "$SPARK_HOME/jars/*" scala.tools.nsc.Main \
  -classpath "$SPARK_HOME/jars/*:$DELTA_SPARK_JAR:$DELTA_STORAGE_JAR" \
  -d classes DeltaInterleaveBitsChecks.scala
jar cf checks.jar -C classes .
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaInterleaveBitsChecks \
  checks.jar stock /tmp/DeltaInterleaveBitsChecks-stock
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR,$COMET_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaInterleaveBitsChecks \
  checks.jar comet /tmp/DeltaInterleaveBitsChecks-comet
```

Expected: 15 named cases, matching row/schema JSON across stock and Comet runs, and `native_acceptance: false` in `summary.json`. The Comet lane requires an accelerated control query; passing Delta probes can still execute in Spark. Execution plans are saved beside rows. These are manually run probes, not tests wired into the Comet CI reactor.

Grounding: selected equivalents of the following versioned upstream tests, not a replacement for their full suites:

- [InterleaveBitsSuite (v4.4.0)](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/test/scala/org/apache/spark/sql/delta/expressions/InterleaveBitsSuite.scala)
- [InterleaveBitsSuite (v4.4.1)](https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/expressions/InterleaveBitsSuite.scala)

Validation uses Comet 1.2.0, not a build of this branch. Current-main expression support and the production image remain unverified.
