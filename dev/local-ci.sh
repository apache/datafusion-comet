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
# Run the Spark SQL or Iceberg CI workflow locally. Mirrors
# .github/workflows/spark_sql_test_reusable.yml and
# .github/workflows/iceberg_spark_test_reusable.yml.
#
# Versions, matrix rows and the shard count are read from ci.yml and dev/ci/ at
# run time, so a version bump needs no change here. Written for bash 3.2.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHARDS_PY="$REPO/dev/ci/check-iceberg-shards.py"
# Rebuildable trees, so keep them out of $HOME. Override to survive a reboot.
SANDBOX="${COMET_LOCAL_CI_HOME:-/tmp/comet-local-ci}"
case "$(uname -s)" in
  # bsdtar reads Spark's dot-prefixed .crc fixtures as AppleDouble metadata and
  # exits nonzero after extracting them correctly.
  Darwin) LIB=libcomet.dylib TAR_FLAGS=--no-mac-metadata ;;
  *) LIB=libcomet.so TAR_FLAGS= ;;
esac

# Yellow starting, green finished, red failed.
say() { printf '\n\033[1;33m[local-ci] %s\033[0m\n' "$*" >&2; }
ok() { printf '\n\033[1;32m[local-ci] %s\033[0m\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[local-ci] %s\033[0m\n' "$*" >&2; }
die() {
  fail "$*"
  exit 1
}
FAILED=""

# Seconds as 1h 05m 12s.
hms() {
  if [ "$1" -ge 3600 ]; then
    printf '%dh %02dm %02ds' $(($1 / 3600)) $(($1 % 3600 / 60)) $(($1 % 60))
  elif [ "$1" -ge 60 ]; then
    printf '%dm %02ds' $(($1 / 60)) $(($1 % 60))
  else
    printf '%ds' "$1"
  fi
}

usage() {
  cat >&2 <<'EOF'
Usage: dev/local-ci.sh <spark|iceberg> [version] [target...]

  dev/local-ci.sh spark                every Spark SQL matrix row
  dev/local-ci.sh spark sql_core-1     one row, or all/core/hive
  dev/local-ci.sh iceberg              every Iceberg target
  dev/local-ci.sh iceberg shard-2      one shard, or extensions/runtime

The version defaults to the one the merge queue gates on. Name an older one to
reproduce a nightly failure: dev/local-ci.sh spark 3.5 sql_core-1

  --print-config        show what is read from ci.yml and dev/ci, then exit

  SKIP_PREPARE=1        run the tests only. Skips the Comet install too, so do
                        not use it after changing Comet
  COMET_LOCAL_CI_HOME   where the sources live (default /tmp/comet-local-ci)
EOF
  exit 2
}

# Read every value from dev/ci/local-ci-config.py, the single parser. It
# validates shapes and fails loudly, so nothing below has to re-guard a parse.
# shellcheck disable=SC2153  # VERSION/FULL/JAVA/ROWS/... all come from here
load_config() {
  # Assign first and check that, rather than `eval "$(...)"`: eval reports its
  # own status, so a parser that failed and printed nothing would look like
  # success and the run would exit 0 having tested nothing.
  conf="$(cd "$REPO" && python3 dev/ci/local-ci-config.py --shell "$@")" ||
    die "could not read the CI configuration"
  eval "$conf"
  if [ -n "$DEFAULTED" ]; then
    say "$1 $VERSION (the version the merge queue gates on)"
  fi
}

# --- sandbox --------------------------------------------------------------—--

setup_jdk() {
  if [ -x /usr/libexec/java_home ]; then
    JAVA_HOME="$(/usr/libexec/java_home -v "$1")" || die "JDK $1 not installed"
    export JAVA_HOME
  fi
  [ -n "${JAVA_HOME:-}" ] || die "export JAVA_HOME pointing at a JDK $1"
  say "JDK $1: $JAVA_HOME"
}

# The ci profile is release without LTO. Stage it where -Prelease looks.
build_native() {
  say "cargo build --profile ci"
  (cd "$REPO/native" && cargo build --profile ci)
  mkdir -p "$REPO/native/target/release"
  cp "$REPO/native/target/ci/$LIB" "$REPO/native/target/release/$LIB"
}

# Spark's build reaches for git only through build/spark-build-info, which has no
# `set -e`, so the tag archive works and skips the git objects a clone carries.
# Extract beside the target and move, so a failed download leaves nothing that
# the next run mistakes for a complete tree.
fetch_archive() {
  if [ -d "$2" ]; then return 0; fi
  say "downloading $1"
  rm -rf "$2.part"
  mkdir -p "$2.part"
  # Checked, not left to `set -e`, which bash disables inside a function called
  # from a conditional.
  # shellcheck disable=SC2086
  if ! curl -fsSL "$1" | tar -xz $TAR_FLAGS -C "$2.part" --strip-components=1; then
    rm -rf "$2.part"
    die "could not download $1"
  fi
  mv "$2.part" "$2"
}

# Iceberg does need a repository: its build takes the project version from the
# latest apache-iceberg-* tag via com.palantir.git-version.
clone_tag() {
  if [ -d "$3/.git" ]; then return 0; fi
  say "cloning $1 at $2"
  git clone --depth 1 --branch "$2" "$1" "$3"
}

# `git apply` works outside a repository, so this covers both trees. The applied
# diff is recorded so that editing dev/diffs/ and re-running reverts the old one
# first; otherwise the tree wedges and only deleting it helps.
apply_diff() {
  marker="$1/.local-ci-applied.diff"
  if [ -f "$marker" ] && cmp -s "$marker" "$2"; then return 0; fi
  # A tree patched before the record existed: adopt it rather than fail.
  if [ ! -f "$marker" ] && (cd "$1" && git apply --check --reverse "$2") 2>/dev/null; then
    cp "$2" "$marker"
    return 0
  fi
  if [ -f "$marker" ]; then
    say "reverting the previously applied diff"
    (cd "$1" && git apply -R "$marker") || die "cannot revert $marker. Delete $1 and re-run."
    rm -f "$marker"
  fi
  say "applying $(basename "$2")"
  (cd "$1" && git apply "$2") || die "$(basename "$2") does not apply. Delete $1 and re-run."
  cp "$2" "$marker"
}

install_comet() {
  say "mvnw install -Prelease -DskipTests $*"
  (cd "$REPO" && ./mvnw -B install -Prelease -DskipTests "$@")
}

# Ask Maven rather than assuming ~/.m2/repository: settings.xml and
# -Dmaven.repo.local can relocate it, and guessing wrong makes the purges below
# silent no-ops for the people who need them most.
MAVEN_REPO=""
maven_repo() {
  if [ -z "$MAVEN_REPO" ]; then
    MAVEN_REPO="$(cd "$REPO" && ./mvnw -q -N help:evaluate \
      -Dexpression=settings.localRepository -DforceStdout 2>/dev/null | tail -1)"
    case "$MAVEN_REPO" in /*) ;; *) die "could not resolve the local Maven repository" ;; esac
  fi
  printf '%s\n' "$MAVEN_REPO"
}

# Comet's install leaves POMs whose JARs it never fetched. Coursier then calls
# the artifact found-locally and refuses to fall back to Maven Central, so sbt
# dies on a JAR it can see a POM for. Both workflows drop the Parquet tree for
# that reason; the wider sweep is the same problem one level out.
purge_parquet() {
  dir="$(maven_repo)/org/apache/parquet"
  [ -d "$dir" ] || return 0
  say "removing $dir so sbt and gradle refetch it"
  rm -rf "$dir"
}

# setup-spark-builder greps for an explicit <packaging>jar|bundle</packaging> and
# so misses a POM declaring none, which Maven defaults to jar. org.antlr:antlr4
# is one of those. A <packaging>pom</packaging> parent has no JAR by design.
purge_partial_poms() {
  repo="$(maven_repo)"
  [ -d "$repo" ] || return 0
  say "dropping POM-only entries across all of $repo"
  find "$repo" -name '*.pom' | while read -r pom; do
    [ -f "${pom%.pom}.jar" ] && continue
    packaging="$(sed -n 's:.*<packaging>\(.*\)</packaging>.*:\1:p' "$pom" | head -1)"
    case "${packaging:-jar}" in jar | bundle) ;; *) continue ;; esac
    rm -f "$pom" "$pom.sha1" "${pom%.pom}.pom.lastUpdated" \
      "$(dirname "$pom")/_remote.repositories"
  done
}

# Purging invalidates what sbt already resolved: plugins such as sbt-antlr4 build
# their classpath from the cached update report, not the filesystem, so a
# refetched artifact stays invisible until the report is rebuilt.
drop_cached_resolution() {
  [ -d "$1" ] || return 0
  say "clearing cached sbt resolution so it re-reads the Maven repository"
  find "$1" -type d -name update -path '*/target/*' -prune -exec rm -rf {} +
}

# Copy a prepared tree so a shard can have one to itself, the way each CI
# matrix row gets its own runner and its own extracted apache-spark/. On APFS
# and btrfs this is copy-on-write, so a 4 GB tree costs kilobytes until the
# shards start writing their own reports.
clone_tree() {
  rm -rf "$2"
  cp -Rc "$1" "$2" 2>/dev/null ||
    cp -R --reflink=auto "$1" "$2" 2>/dev/null ||
    cp -R "$1" "$2"
}

# --- runners -----------------------------------------------------------------

row_label() { printf 'spark-sql-%s / spark-%s-jdk%s\n' "$1" "$FULL" "$JAVA"; }

# spark_row <name> <args1> <args2> <heap> <metaspace> <tree>
# One row, in the tree it is given. A subshell so HEAP_SIZE cannot leak.
spark_row() {
  (
    cd "$6"
    printf -- '-J-Xms1g\n-J-Xmx4g\n-J-XX:MaxMetaspaceSize=1g\n' > .sbtopts
    export LC_ALL=C.UTF-8 NOLINT_ON_COMPILE=true
    # shellcheck disable=SC2030
    export ENABLE_COMET=true ENABLE_COMET_ONHEAP=true
    export SBT_OPTS="-Xss4m -XX:+UseG1GC -XX:+UseStringDeduplication -XX:MaxMetaspaceSize=384m -XX:G1HeapRegionSize=2m -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled -XX:+ExitOnOutOfMemoryError"
    [ -n "$4" ] && export HEAP_SIZE="$4"
    [ -n "$5" ] && export METASPACE_SIZE="$5"
    # What the workflow exports. SERIAL_SBT_TESTS suppresses Spark's own test
    # grouping and parallelExecution; the cap keeps one forked test JVM. Rows run
    # beside each other in their own trees instead, the way CI does it.
    export SERIAL_SBT_TESTS=1
    set -- -Dsbt.log.noformat=true -mem 1024 \
      "set Global / concurrentRestrictions := Seq(Tags.limit(Tags.ForkedTestGroup, 1))" \
      ${2:+"$2"} ${3:+"$3"}
    build/sbt "$@"
  )
}

# Every selected row at once, each in its own copy of the tree. That is exactly
# what CI does: seven matrix rows, seven runners, seven extracted apache-spark/
# trees. Because each row gets a tree to itself, the per-row settings stay
# identical to CI's -- no shared sbt server, target/ or metastore tmpdir.
run_spark_rows() {
  logs="$SANDBOX/logs-spark-$FULL"
  mkdir -p "$logs"
  # Copy every tree before starting anything. Interleaving the copies with the
  # launches means a copy that fails -- a full disk is the likely way -- exits
  # on set -e with earlier rows still running and unawaited.
  while IFS=$'\037' read -r name args1 args2 heap metaspace; do
    [ -n "$name" ] || continue
    clone_tree "$dest" "$dest-$name"
  done <<< "$ROWS"

  running=""
  while IFS=$'\037' read -r name args1 args2 heap metaspace; do
    [ -n "$name" ] || continue
    # Seven concurrent sbt processes would interleave unreadably, so each row
    # gets its own log.
    spark_row "$name" "$args1" "$args2" "$heap" "$metaspace" "$dest-$name" \
      > "$logs/$name.log" 2>&1 &
    say "started spark-sql-$name, log $logs/$name.log"
    running="$running$name $SECONDS $! "
  done <<< "$ROWS"
  await_rows "$running"
  [ -z "$FAILED" ] || die "failed:$FAILED"
}

# await_rows "<name> <start> <pid> ..." - one line per row as it finishes.
await_rows() {
  # shellcheck disable=SC2086  # word splitting is the point: name start pid ...
  set -- $1
  while [ $# -ge 3 ]; do
    if wait "$3"; then
      ok "spark-sql-$1 passed in $(hms $((SECONDS - $2)))"
    else
      FAILED="$FAILED $1"
      fail "spark-sql-$1 failed in $(hms $((SECONDS - $2))), last 20 lines of $logs/$1.log:"
      tail -20 "$logs/$1.log" >&2
    fi
    shift 3
  done
}

run_spark() {
  load_config spark "$@"
  dest="$SANDBOX/apache-spark-$FULL"
  setup_jdk "$JAVA"

  if [ -z "${SKIP_PREPARE:-}" ]; then
    prep=$SECONDS
    build_native
    fetch_archive "https://github.com/apache/spark/archive/refs/tags/v$FULL.tar.gz" "$dest"
    apply_diff "$dest" "$REPO/dev/diffs/$FULL.diff"
    install_comet "-Pspark-$VERSION"
    purge_parquet
    purge_partial_poms
    drop_cached_resolution "$dest"
    # Only what the selected rows need. CI compiles all three because one
    # artifact feeds seven shards; there is no artifact here.
    compile=""
    for p in $PROJECTS; do compile="$compile $p/Test/compile"; done
    say "pre-compiling test classes:$compile"
    # shellcheck disable=SC2086
    (cd "$dest" && NOLINT_ON_COMPILE=true build/sbt -Dsbt.log.noformat=true -mem 3072 $compile)
    ok "prepare took $(hms $((SECONDS - prep)))"
  fi

  [ -n "$DEDICATED" ] && export DEDICATED_JVM_SBT_TESTS="$DEDICATED"

  # A single row runs in the prepared tree; there is nothing to run beside it.
  if [ "$(printf '%s\n' "$ROWS" | grep -c .)" -eq 1 ]; then
    IFS=$'\037' read -r name args1 args2 heap metaspace <<< "$ROWS"
    started=$SECONDS
    say "started $(row_label "$name")"
    spark_row "$name" "$args1" "$args2" "$heap" "$metaspace" "$dest"
    ok "spark-sql-$name took $(hms $((SECONDS - started)))"
  else
    run_spark_rows
  fi
}

run_iceberg() {
  load_config iceberg "$1"
  shift
  dest="$SANDBOX/apache-iceberg-$FULL"

  if [ $# -eq 0 ]; then
    i=1
    while [ "$i" -le "$SHARDS" ]; do
      set -- "$@" "shard-$i"
      i=$((i + 1))
    done
    set -- "$@" extensions runtime
  fi
  # Vet the whole list before anything is built, so a typo costs nothing.
  for target in "$@"; do
    case "$target" in
      extensions | runtime) ;;
      shard-[1-9] | shard-[1-9][0-9])
        [ "${target#shard-}" -le "$SHARDS" ] || die "no $target; there are $SHARDS shards"
        ;;
      *) die "unknown target '$target'; try shard-1..$SHARDS, extensions, runtime" ;;
    esac
  done

  setup_jdk "$JAVA"
  if [ -z "${SKIP_PREPARE:-}" ]; then
    prep=$SECONDS
    build_native
    clone_tag https://github.com/apache/iceberg.git "apache-iceberg-$FULL" "$dest"
    apply_diff "$dest" "$REPO/dev/diffs/iceberg/$FULL.diff"
    install_comet "-Pspark-$SPARK" "-Pscala-$SCALA"
    purge_parquet
    purge_partial_poms
    ok "prepare took $(hms $((SECONDS - prep)))"
  fi

  core=":iceberg-spark:iceberg-spark-${SPARK}_${SCALA}:test"
  for target in "$@"; do
    say "iceberg-$FULL / spark-$SPARK / $target"
    started=$SECONDS
    case "$target" in
      shard-*)
        gradlew "$core" --init-script "$REPO/dev/ci/iceberg-test-shards.gradle" \
          "-PcometShardTask=$core" "-PcometShardIndex=${target#shard-}" \
          "-PcometShardCount=$SHARDS"
        ;;
      extensions) gradlew ":iceberg-spark:iceberg-spark-extensions-${SPARK}_${SCALA}:test" ;;
      runtime)
        # The workflow runs the sharding fixture in this job before the test.
        python3 "$SHARDS_PY" --gradle "$dest/gradlew"
        gradlew ":iceberg-spark:iceberg-spark-runtime-${SPARK}_${SCALA}:integrationTest"
        ;;
    esac
    case "$target" in
      shard-* | extensions)
        python3 "$REPO/dev/ci/summarize-iceberg-writes.py" --title "$target" \
          "$dest/build/comet-iceberg-writes/$target"
        ;;
    esac
    ok "$target took $(hms $((SECONDS - started)))"
  done
}

# Reads $dest, $spark, $SCALA and $target from run_iceberg.
gradlew() {
  (
    cd "$dest"
    # shellcheck disable=SC2031
    export SPARK_LOCAL_IP=localhost ENABLE_COMET=true ENABLE_COMET_ONHEAP=true
    # One directory per target, emptied first, so a rerun reports only its own writes.
    export COMET_ICEBERG_WRITE_REPORT_DIR="$dest/build/comet-iceberg-writes/$target"
    rm -rf "$COMET_ICEBERG_WRITE_REPORT_DIR"
    ./gradlew "-DsparkVersions=$SPARK" "-DscalaVersion=$SCALA" \
      -DflinkVersions= -DkafkaVersions= "$@" -Pquick=true -x javadoc
  )
}

# --- dispatch ----------------------------------------------------------------

[ $# -ge 1 ] || usage
if [ "$1" = "--print-config" ]; then
  (cd "$REPO" && python3 dev/ci/local-ci-config.py --print)
  exit 0
fi
what="$1"
shift
case "$what" in spark | iceberg) ;; *) usage ;; esac

# A version is the only argument shaped like N.N; anything else is a target.
# load_config defaults it and reports back through VERSION and DEFAULTED.
case "${1:-}" in
  [0-9]*.[0-9]*) ;;
  *) set -- "" "$@" ;;
esac

# However this exits, say how long it took. Capture and re-raise the status
# first: the trap's own last command would otherwise become the exit status and
# turn every failure into a green run.
# shellcheck disable=SC2154  # assigned in the trap body, which shellcheck cannot see
trap 'status=$?; ok "total runtime $(hms $SECONDS)"; exit $status' EXIT

"run_$what" "$@"
