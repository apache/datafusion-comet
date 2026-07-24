#!/usr/bin/env bash
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
# Verify the `contrib-delta` build gate keeps Delta surface out of default builds.
#
# Three independent layers are checked:
#   1. Cargo: default `cargo build` doesn't compile `comet-contrib-delta` and
#      doesn't pull `delta_kernel` into the dependency tree.
#   2. Maven: default `mvn ... package` doesn't compile any
#      `org/apache/comet/contrib/` classes and doesn't pull `io.delta:*` deps.
#   3. Symbol/size: the resulting `libcomet` (`.so` on Linux, `.dylib` on macOS) from the default build is
#      meaningfully smaller than the contrib-enabled build, and carries no
#      `comet_contrib_delta`/`delta_kernel`/etc. external symbols.
#
# Exit non-zero on the first failure. Designed to be wired into CI so a future
# change that leaks Delta into core gets caught immediately.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NATIVE_DIR="$ROOT/native"
SPARK_DIR="$ROOT/spark"

# CI containers check out the repo as a different user than the job runs as, so git refuses to
# operate on it ("detected dubious ownership") -- which can make Maven plugins that read git
# metadata fail fast with no useful output. Mark the tree safe up front (best-effort).
git config --global --add safe.directory "$ROOT" 2>/dev/null || true
git config --global --add safe.directory '*' 2>/dev/null || true

# Use the repo's Maven wrapper, not a bare `mvn`. The CI gate job runs in a bare
# `amd64/rust` container that has no system Maven on PATH, so `mvn` would fail to
# launch, produce no dependency output, and trip the anti-vacuous guard below.
# `./mvnw` self-provisions the pinned Maven version regardless of the image.
MVNW="$ROOT/mvnw"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
hdr()   { printf '\n\033[36m==> %s\033[0m\n' "$*"; }

# ---- Cargo gate -----------------------------------------------------------

hdr "Cargo: default build does not depend on comet-contrib-delta / delta_kernel"
cd "$NATIVE_DIR"
TREE_DEFAULT="$(cargo tree -p datafusion-comet --no-default-features 2>/dev/null)"
# Anti-vacuous (mirrors the Maven gate below): a failing `cargo tree` yields empty output, and the
# command-substitution failure doesn't trip `set -e` in an assignment -- so assert the root crate we
# KNOW is always present before concluding "no Delta deps", otherwise a broken cargo-tree run would
# pass the leak check vacuously. (`datafusion-comet ` with a trailing space matches only the root
# crate line, not `datafusion-comet-proto`/`-common`.)
if ! grep -q 'datafusion-comet ' <<<"$TREE_DEFAULT"; then
  red "FAIL: default cargo tree produced no datafusion-comet entry (cargo tree likely failed;"
  red "      refusing to conclude 'no Delta deps' vacuously)"
  exit 1
fi
if grep -qE 'comet-contrib-delta|delta_kernel|delta-kernel' <<<"$TREE_DEFAULT"; then
  red "FAIL: default cargo tree contains Delta-related deps:"
  grep -E 'comet-contrib-delta|delta_kernel|delta-kernel' <<<"$TREE_DEFAULT"
  exit 1
fi
green "OK: cargo tree default is clean of contrib + kernel"

TREE_CONTRIB="$(cargo tree -p datafusion-comet --features contrib-delta 2>/dev/null)"
# The build-gate unit ships a STUB contrib crate, so the gated tree pulls in
# `comet-contrib-delta` but not yet the heavy `delta_kernel` (that arrives with the
# native read-path unit). Require the contrib crate to be present; the symbol check
# below guards against grep-pattern drift either way.
CONTRIB_HITS="$(printf '%s\n' "$TREE_CONTRIB" | grep -cE 'comet-contrib-delta' || true)"
if [[ "$CONTRIB_HITS" -lt 1 ]]; then
  red "FAIL: --features contrib-delta tree missing the comet-contrib-delta crate (hits=$CONTRIB_HITS)"
  exit 1
fi
green "OK: cargo tree with contrib-delta correctly pulls comet-contrib-delta"

# ---- Maven gate -----------------------------------------------------------

hdr "Maven: default profile excludes io.delta:* dependencies"
cd "$ROOT"
# `dependency:list` can't run in a fresh CI checkout: it needs the sibling reactor JARs
# (comet-common, the shims) which aren't built, so it fails with a resolution error and no
# output. `help:effective-pom` only merges POM models (no artifact resolution), so it works
# without a build. We extract the ACTIVE top-level <dependencies> -- after </dependencyManagement>,
# before the <profiles> listing -- which is exactly what dependency:list would have shown for the
# active profiles (and excludes the inactive contrib-delta profile's own io.delta declaration).
# Last effective-pom invocation's combined output, kept so the anti-vacuous guard can SHOW why
# mvn failed instead of swallowing it.
EPOM_LOG="$(mktemp)"
delta_active_deps() { # args: -P / -D flags
  local epom
  epom="$(mktemp)"
  "$MVNW" help:effective-pom -Djava.version=17 -Dmaven.gitcommitid.skip -pl spark \
    -Doutput="$epom" "$@" >"$EPOM_LOG" 2>&1 || true
  awk '/<\/dependencyManagement>/{f=1} /<profiles>/{f=0} f' "$epom"
  rm -f "$epom"
}
# Resolved delta-spark version from the active deps (empty if absent).
delta_spark_version() { # args: -P flags
  delta_active_deps "$@" |
    grep -A2 'artifactId>delta-spark' |
    grep -oE '<version>[^<]+' | sed -n '1s/<version>//p'
}

DEPS_DEFAULT="$(delta_active_deps -Pspark-4.1)"
# Anti-vacuous: a broken mvn run yields empty output; assert a dep we KNOW is always present so a
# broken run fails loudly instead of "passing" the io.delta check by finding nothing.
if ! grep -q '<groupId>org.apache.spark</groupId>' <<<"$DEPS_DEFAULT"; then
  red "FAIL: default effective-pom produced no org.apache.spark deps (mvn likely failed;"
  red "      refusing to conclude 'zero io.delta' vacuously)"
  red "      --- mvnw: $MVNW (java=${JAVA_HOME:-unset}) ---"
  red "      --- effective-pom output (last 60 lines) ---"
  tail -60 "$EPOM_LOG" >&2 || true
  exit 1
fi
if grep -q '<groupId>io.delta</groupId>' <<<"$DEPS_DEFAULT"; then
  red "FAIL: default Maven build pulls io.delta dependencies:"
  echo "$DEPS_DEFAULT" | grep -A2 '<groupId>io.delta</groupId>'
  exit 1
fi
green "OK: default Maven build has zero io.delta dependencies"

# Per-Spark Delta version pinning: spark-4.1 -> delta-spark 4.1.x, spark-3.5 -> 3.x, spark-4.0 ->
# 4.0.x (Delta 4.1 needs Spark 4.1 internals; 4.0 must stay on 4.0.x to avoid a runtime
# NoSuchMethodError on ParserInterface.$init$).
V41="$(delta_spark_version -Pspark-4.1,contrib-delta)"
case "$V41" in
  4.1.*) green "OK: -Pcontrib-delta + spark-4.1 correctly pulls delta-spark $V41" ;;
  *) red "FAIL: -Pcontrib-delta + spark-4.1 expected delta-spark 4.1.x, got '${V41:-<none>}'"; exit 1 ;;
esac
V35="$(delta_spark_version -Pspark-3.5,contrib-delta)"
case "$V35" in
  3.*) green "OK: -Pcontrib-delta + spark-3.5 correctly pulls delta-spark $V35" ;;
  *) red "FAIL: -Pcontrib-delta + spark-3.5 expected delta-spark 3.x, got '${V35:-<none>}'"; exit 1 ;;
esac
V40="$(delta_spark_version -Pspark-4.0,contrib-delta)"
case "$V40" in
  4.0.*) green "OK: -Pcontrib-delta + spark-4.0 correctly pulls delta-spark $V40" ;;
  *) red "FAIL: -Pcontrib-delta + spark-4.0 expected delta-spark 4.0.x, got '${V40:-<none>}'"; exit 1 ;;
esac

# ---- Compiled-class gate --------------------------------------------------

hdr "Compiled classes: no contrib/delta classes in default build"
cd "$ROOT"
"$MVNW" -Pspark-4.1 -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 -Dmaven.gitcommitid.skip -pl spark -am test-compile -q -DskipTests=true >/dev/null 2>&1
LEAK_CLASSES="$(find spark/target/classes -path '*comet/contrib*' -name '*.class' 2>/dev/null)"
if [[ -n "$LEAK_CLASSES" ]]; then
  red "FAIL: default Maven build compiled contrib classes:"
  echo "$LEAK_CLASSES"
  exit 1
fi
DELTA_IMPL_LEAKS="$(find spark/target/classes \
  \( -name 'CometDeltaNativeScan*' \
     -o -name 'CometDeltaNativeScanExec*' \
     -o -name 'DeltaScanRule*' \
     -o -name 'DeltaReflection*' \) \
  2>/dev/null)"
if [[ -n "$DELTA_IMPL_LEAKS" ]]; then
  red "FAIL: default Maven build compiled Delta-implementation classes:"
  echo "$DELTA_IMPL_LEAKS"
  exit 1
fi
green "OK: only the always-present DeltaIntegration reflection bridge in default classes"

# ---- libcomet symbol/size gate -------------------------------------------

hdr "libcomet: default build is smaller and has no Delta symbols"
cd "$NATIVE_DIR"
# The cdylib extension is platform-specific: `libcomet.so` on Linux (CI), `libcomet.dylib` on
# macOS. Find whichever the build produced; `stat`/`nm` flags also differ across the two.
# Echo whichever lib exists (empty if neither). MUST return 0: `ls a b` exits non-zero when one of
# the two paths is absent (the normal case -- only one extension exists per platform), and under
# `set -euo pipefail` that would kill the script at `LIB=$(comet_lib)` even though it found the lib.
comet_lib() {
  local f
  for f in target/debug/libcomet.so target/debug/libcomet.dylib; do
    if [[ -f "$f" ]]; then
      echo "$f"
      return 0
    fi
  done
  return 0
}
lib_size() { stat -c%s "$1" 2>/dev/null || stat -f%z "$1"; }
# Plain `nm` on the (unstripped debug) lib lists the full symbol table on both platforms, so a
# name grep finds the Delta symbols whether they're exported or internal.
delta_syms() {
  nm "$1" 2>/dev/null | grep -ciE 'comet_contrib_delta|delta_kernel|deltadvfilter|deltasynthetic' || true
}

cargo clean -p comet-contrib-delta -p datafusion-comet >/dev/null 2>&1 || true
cargo build -j 4 -p datafusion-comet >/dev/null 2>&1
LIB_DEFAULT="$(comet_lib)"
if [[ -z "$LIB_DEFAULT" ]]; then
  red "FAIL: default build produced no libcomet.{so,dylib} under $NATIVE_DIR/target/debug"
  exit 1
fi
SIZE_DEFAULT="$(lib_size "$LIB_DEFAULT")"
if command -v nm >/dev/null 2>&1; then
  EXT_SYMS="$(delta_syms "$LIB_DEFAULT")"
else
  EXT_SYMS=0
fi
if [[ "$EXT_SYMS" -ne 0 ]]; then
  red "FAIL: default libcomet contains $EXT_SYMS Delta-related symbols"
  exit 1
fi
green "OK: default libcomet has 0 Delta symbols (size=$SIZE_DEFAULT bytes)"

cargo build -j 4 -p datafusion-comet --features contrib-delta >/dev/null 2>&1
LIB_CONTRIB="$(comet_lib)"
SIZE_CONTRIB="$(lib_size "$LIB_CONTRIB")"
if [[ "$SIZE_CONTRIB" -le "$SIZE_DEFAULT" ]]; then
  red "FAIL: contrib-enabled libcomet (size=$SIZE_CONTRIB) is not larger than default (size=$SIZE_DEFAULT)"
  red "       (would indicate contrib was being linked into default build too)"
  exit 1
fi
# Sanity check: the contrib-enabled libcomet MUST contain Delta-related symbols.
# Without this, a future Rust toolchain that mangles symbols differently (so our
# grep pattern stops matching) would silently make the default-build check a no-op
# while still passing -- the gate would lie about being enforced. Asserting both
# "default has 0" AND "contrib has >0" catches grep-pattern drift.
if command -v nm >/dev/null 2>&1; then
  CONTRIB_SYMS="$(delta_syms "$LIB_CONTRIB")"
  if [[ "$CONTRIB_SYMS" -lt 1 ]]; then
    red "FAIL: contrib-enabled libcomet has 0 Delta-related symbols matching our grep pattern."
    red "      This means the symbol-name pattern in this script has drifted from what"
    red "      Rust currently emits, and the default-build check above is now a no-op."
    red "      Inspect the dylib's exports and update the grep pattern."
    exit 1
  fi
fi
DIFF_MB=$(( (SIZE_CONTRIB - SIZE_DEFAULT) / 1024 / 1024 ))
green "OK: contrib-enabled libcomet is ${DIFF_MB} MB larger than default (size=$SIZE_CONTRIB bytes)"

# ---- Summary --------------------------------------------------------------

hdr "All gate checks passed"
echo "  default cargo:  no comet-contrib-delta, no delta_kernel"
echo "  default mvn:    no io.delta:*, no contrib/delta classes"
echo "  default dylib:  ${DIFF_MB} MB smaller than contrib build, 0 Delta symbols"
echo
echo "Run with: dev/verify-contrib-delta-gate.sh"
