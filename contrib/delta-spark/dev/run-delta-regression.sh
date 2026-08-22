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
# Run Delta Lake's own Spark test suites against a Comet build with the
# native Delta scan enabled. Clones delta at $DELTA_VERSION into $WORKDIR,
# injects Comet into the test SparkSession (DeltaSQLCommandTest) and the
# test classpath (unmanagedJars), then runs the given testOnly selectors.
#
# Usage:
#   COMET_JARS=/path/comet-spark.jar,/path/comet-contrib-delta.jar,/path/flatbuffers.jar \
#   ./run-delta-regression.sh <workdir> 'org.apache.spark.sql.delta.DeletionVectorsSuite' [...]
#
# Env:
#   DELTA_VERSION  delta tag to test against (default 3.3.2)
#   COMET_JARS     comma-separated jars added to the test classpath (required)
#   JAVA_HOME      JDK for sbt (17 recommended)
set -euo pipefail

DELTA_VERSION="${DELTA_VERSION:-3.3.2}"
WORKDIR="${1:?usage: run-delta-regression.sh <workdir> <suite> [...suites]}"
shift
[ $# -ge 1 ] || { echo "no suites given" >&2; exit 2; }
: "${COMET_JARS:?COMET_JARS must list the comet jars}"

IFS=',' read -ra _jars <<< "$COMET_JARS"
for j in "${_jars[@]}"; do
  [ -f "$j" ] || { echo "COMET_JARS entry not found: $j" >&2; exit 2; }
done

DELTA_DIR="$WORKDIR/delta-$DELTA_VERSION"
if [ ! -d "$DELTA_DIR" ]; then
  git clone --depth 1 --branch "v$DELTA_VERSION" https://github.com/delta-io/delta.git "$DELTA_DIR"
elif [ ! -d "$DELTA_DIR/.git" ]; then
  echo "stale/partial checkout at $DELTA_DIR; remove it (rm -rf) and rerun" >&2
  exit 2
fi
cd "$DELTA_DIR"

# Add COMET_EXTRA_JARS to every project's test classpath, plus the JDK-17
# module-access flags Spark needs (both for forked test JVMs and sbt's own JVM).
if ! grep -q "COMET_EXTRA_JARS" build.sbt; then
  python3 - <<'EOF'
s = open('build.sbt').read()
marker = 'lazy val commonSettings = Seq('
opens = [
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
]
opts = ", ".join('"%s"' % o for o in opens)
inject = (
    'lazy val commonSettings = Seq(\n'
    '  Test / unmanagedJars ++= sys.env.get("COMET_EXTRA_JARS").toSeq\n'
    '    .flatMap(_.split(",")).map(p => Attributed.blank(file(p))),\n'
    '  Test / fork := true,\n'
    '  Test / javaOptions ++= Seq(%s),\n' % opts
)
assert marker in s, 'commonSettings marker not found'
open('build.sbt', 'w').write(s.replace(marker, inject, 1))
EOF
fi

# Inject Comet into the shared test SparkSession when COMET_EXTRA_JARS is set.
TEST_BASE=spark/src/test/scala/org/apache/spark/sql/delta/test/DeltaSQLCommandTest.scala
if ! grep -q "CometSparkSessionExtensions" "$TEST_BASE"; then
  python3 - "$TEST_BASE" <<'EOF'
import sys
p = sys.argv[1]
s = open(p).read()
old = '''  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)
  }'''
new = '''  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)
    if (sys.env.contains("COMET_EXTRA_JARS")) {
      conf
        .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
          classOf[DeltaSparkSessionExtension].getName +
            ",org.apache.comet.CometSparkSessionExtensions")
        .set("spark.comet.enabled", "true")
        .set("spark.comet.exec.enabled", "true")
        .set("spark.comet.exec.shuffle.enabled", "true")
        .set("spark.shuffle.manager",
          "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "2g")
        .set("spark.comet.scan.delta.enabled", "true")
    } else conf
  }'''
assert old in s, 'sparkConf block not found'
open(p, 'w').write(s.replace(old, new))
EOF
fi

# ScanReportHelper is a test-only trait that counts scans by pattern-matching
# FileSourceScanExec in the executed plan. The Comet Delta scan replaces those
# nodes, so claimed scans would go uncounted ("0 did not equal 2" in
# MergeIntoSuiteBase's insert-only data-skipping test). Map the Comet node back
# to the FileSourceScanExec it was built from: originalPlan carries the same
# PreparedDeltaFileIndex, so the reported paths and skipping stats are identical.
SCAN_HELPER=spark/src/test/scala/org/apache/spark/sql/delta/test/ScanReportHelper.scala
if [ -f "$SCAN_HELPER" ] && ! grep -q "CometDeltaNativeScanExec" "$SCAN_HELPER"; then
  python3 - "$SCAN_HELPER" <<'EOF'
import sys
p = sys.argv[1]
s = open(p).read()
old = "      case fs: FileSourceScanExec => Seq(fs)\n"
new = ("      case fs: FileSourceScanExec => Seq(fs)\n"
       "      case c: org.apache.spark.sql.comet.CometDeltaNativeScanExec =>\n"
       "        Seq(c.originalPlan)\n")
assert s.count(old) == 1, s.count(old)
open(p, 'w').write(s.replace(old, new))
EOF
fi

export COMET_EXTRA_JARS="$COMET_JARS"
export SPARK_LOCAL_IP=127.0.0.1
export RUST_BACKTRACE=1

cmds=()
for sel in "$@"; do
  cmds+=("spark/testOnly $sel")
done

LOG="$WORKDIR/delta-regression-$(date +%Y%m%d-%H%M%S).log"
echo "==> logging to $LOG"
build/sbt "${cmds[@]}" 2>&1 | tee "$LOG" | grep -E "^\[info\] (Tests:|Suites:|All tests|.*\*\*\* FAILED| - )" | tail -80
