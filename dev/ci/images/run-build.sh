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

# Run from a fresh Comet checkout containing the patched apache-spark/ tree.
# Both warmup and offline verification execute the same build and smoke tests.
set -euo pipefail
source /opt/comet-ci/bin/versions.env
mode=${1:?Usage: run-build.sh warm|offline}
maven_args=(-B -Prelease -DskipTests "-Pspark-$SPARK_SHORT_VERSION" -Dmaven.gitcommitid.skip=true)
sbt_args=(-batch -Dsbt.log.noformat=true -mem 3072)
case "$mode" in
  warm) ;;
  offline)
    export CARGO_NET_OFFLINE=true
    maven_args+=(-o)
    sbt_args+=(-Dsbt.offline=true)
    python3 /opt/comet-ci/bin/cache.py audit /opt/comet-ci
    python3 /opt/comet-ci/bin/cache.py inputs . --spark-version "$SPARK_VERSION" > /tmp/comet-ci-inputs.sha256
    diff -u /opt/comet-ci/inputs.sha256 /tmp/comet-ci-inputs.sha256
    ;;
  *) echo "Expected warm or offline" >&2; exit 2 ;;
esac
bash /opt/comet-ci/bin/check-tools.sh
test ! -d native/target
test ! -d spark/target
test ! -d apache-spark/sql/core/target
# The first command must work without fetching a Maven distribution offline.
./mvnw -B --version | tee /tmp/comet-ci-maven-version.log
grep -F "Apache Maven $MAVEN_VERSION" /tmp/comet-ci-maven-version.log
grep -Fx "sbt.version=$SBT_VERSION" apache-spark/project/build.properties

if [ "$mode" = warm ]; then
  # proto/build.rs writes generated sources into the checkout, outside target/.
  # A reused native target cache otherwise skips that generator in a fresh tree.
  (cd native && cargo clean --profile ci -p datafusion-comet-proto)
fi
(cd native && cargo build --locked --profile ci)
mkdir -p native/target/release
native_target=${CARGO_TARGET_DIR:-$PWD/native/target}
cp "$native_target/ci/libcomet.so" native/target/release/libcomet.so
./mvnw "${maven_args[@]}" install

if [ "$mode" = warm ]; then
  # Capture Maven's dependencies BEFORE the Parquet/POM workaround deletes any.
  python3 /opt/comet-ci/bin/cache.py maven \
    "$MAVEN_USER_HOME/repository" /export/maven/repository
  curl --fail --location --retry 3 --retry-delay 5 \
    --output /opt/comet-ci/sbt/launcher.jar \
    "https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/$SBT_VERSION/sbt-launch-$SBT_VERSION.jar"
fi
printf '%s  %s\n' "$SBT_LAUNCH_SHA256" /opt/comet-ci/sbt/launcher.jar | sha256sum --check
python3 /opt/comet-ci/bin/cache.py purge "$MAVEN_USER_HOME/repository"
# Spark's build/sbt checks this path before processing its command-line options.
cp /opt/comet-ci/sbt/launcher.jar "apache-spark/build/sbt-launch-$SBT_VERSION.jar"
# Spark's BOM plugin constructs its own Ivy resolver and does not honor the
# launcher repository override. A scoped setting also directs it to Central.
cp /opt/comet-ci/bin/CometCiRepositories.scala apache-spark/project/

cd apache-spark
export NOLINT_ON_COMPILE=true
export ENABLE_COMET=true
export ENABLE_COMET_ONHEAP=true
export SERIAL_SBT_TESTS=1
export SPARK_LOCAL_IP=127.0.0.1
export HEAP_SIZE=3g
export METASPACE_SIZE=1g
build/sbt "${sbt_args[@]}" \
  'catalyst/Test/compile' 'sql/Test/compile' 'hive/Test/compile'
# A successful sbt invocation with zero matched tests is not a passing smoke test.
for suite in \
  'catalyst/testOnly org.apache.spark.sql.catalyst.expressions.LiteralExpressionSuite' \
  'sql/testOnly org.apache.spark.sql.MathFunctionsSuite'; do
  build/sbt "${sbt_args[@]}" "$suite" | tee /tmp/comet-ci-smoke.log
  grep -E 'Total number of tests run: [1-9][0-9]*' /tmp/comet-ci-smoke.log
  grep -F 'All tests passed.' /tmp/comet-ci-smoke.log
done
