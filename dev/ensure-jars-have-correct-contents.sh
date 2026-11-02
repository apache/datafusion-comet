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

# Borrowed from Hadoop

# Usage: $0 'main.jar;other.jar' [sources.jar] [skip-sources=false]
#
# The first argument is a semicolon-separated list of JARs to check. An optional
# sources JAR is also checked unless the third argument is true (maven.source.skip).
# Checks all contained paths against the safe list below.

# We use +=, which is a bash 3.1+ feature
if [[ -z "${BASH_VERSINFO[0]}" ]] \
   || [[ "${BASH_VERSINFO[0]}" -lt 3 ]] \
   || [[ "${BASH_VERSINFO[0]}" -eq 3 && "${BASH_VERSINFO[1]}" -lt 1 ]]; then
  echo "bash v3.1+ is required. Sorry."
  exit 1
fi

set -e
set -o pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
binary_license="${script_dir}/licenses/LICENSE-binary.txt"

locked_version ()
{
  # Cargo generates these package/name/version records. Reject absent or ambiguous versions.
  awk -v wanted="$1" '
    /^\[\[package\]\]/ { name = "" }
    $1 == "name" && $2 == "=" { name = $3; gsub(/["\r]/, "", name) }
    $1 == "version" && $2 == "=" && name == wanted {
      version = $3; gsub(/["\r]/, "", version); count++
    }
    END { if (count != 1) exit 1; print version }
  ' "${script_dir}/../native/Cargo.lock"
}

check_native_license_versions ()
{
  local lz4_version zstd_sys_version zstd_version
  if ! lz4_version=$(locked_version lz4_flex) || ! zstd_sys_version=$(locked_version zstd-sys); then
    echo "[ERROR] Expected one lz4_flex and one zstd-sys version in native/Cargo.lock."
    return 1
  fi
  case "${zstd_sys_version}" in
    *+zstd.*) zstd_version=${zstd_sys_version##*+zstd.} ;;
    *) echo "[ERROR] Cannot read the vendored Zstandard version from zstd-sys ${zstd_sys_version}."; return 1 ;;
  esac
  if ! tr -d '\r' < "${binary_license}" |
      grep -Fx "The native Comet library bundles lz4_flex ${lz4_version} (MIT)." > /dev/null ||
      ! tr -d '\r' < "${binary_license}" |
      grep -Fx "The native Comet library bundles Zstandard ${zstd_version} (BSD-3-Clause)." > /dev/null; then
    echo "[ERROR] Update LICENSE-binary.txt for locked lz4_flex ${lz4_version} / Zstandard ${zstd_version}."
    return 1
  fi
}

allowed_expr="(^org/$|^org/apache/$"
# we have to allow the directories that lead to the org/apache/comet dir
# We allow all the classes under the following packages:
#   * org.apache.comet
#   * org.apache.spark.comet
#   * org.apache.spark.sql.comet
#   * org.apache.arrow.c
allowed_expr+="|^org/apache/comet/"
allowed_expr+="|^org/apache/spark/comet/"
allowed_expr+="|^org/apache/spark/sql/comet/"
allowed_expr+="|^org/apache/arrow/c/"
#   * whatever in the "META-INF" directory
allowed_expr+="|^META-INF/"
#   * whatever under the "conf" directory
allowed_expr+="|^conf/"
#   * whatever under the "lib" directory
allowed_expr+="|^lib/"
# Native dynamic library from Arrow
allowed_expr+="|^x86_64/"
allowed_expr+="|^aarch_64/"
allowed_expr+="|^x86_64/libarrow_cdata_jni.so$"
allowed_expr+="|^x86_64/libarrow_cdata_jni.dylib$"
allowed_expr+="|^x86_64/arrow_cdata_jni.dll$"
allowed_expr+="|^aarch_64/libarrow_cdata_jni.dylib$"

allowed_expr+="|^arrow_cdata_jni/"
allowed_expr+="|^arrow_cdata_jni/x86_64/"
allowed_expr+="|^arrow_cdata_jni/aarch_64/"
allowed_expr+="|^arrow_cdata_jni/x86_64/libarrow_cdata_jni.so$"
allowed_expr+="|^arrow_cdata_jni/x86_64/libarrow_cdata_jni.dylib$"
allowed_expr+="|^arrow_cdata_jni/x86_64/arrow_cdata_jni.dll$"
allowed_expr+="|^arrow_cdata_jni/aarch_64/libarrow_cdata_jni.dylib$"
# Two classes in Arrow C module: StructVectorLoader and StructVectorUnloader, are not
# under org/apache/arrow/c, so we'll need to treat them specially.
allowed_expr+="|^org/apache/arrow/$"
allowed_expr+="|^org/apache/arrow/vector/$"
allowed_expr+="|^org/apache/arrow/vector/StructVectorLoader.class$"
allowed_expr+="|^org/apache/arrow/vector/StructVectorUnloader.class$"
# Log4J stuff
allowed_expr+="|log4j2.properties"
# Git Info properties
allowed_expr+="|comet-git-info.properties"
# For some reason org/apache/spark/sql directory is also included, but with no content
allowed_expr+="|^org/apache/spark/$"
# Some shuffle related classes are spark-private, e.g. TempShuffleBlockId, ShuffleWriteMetricsReporter,
# so these classes which use shuffle classes have to be in org/apache/spark.
allowed_expr+="|^org/apache/spark/shuffle/$"
allowed_expr+="|^org/apache/spark/shuffle/sort/$"
allowed_expr+="|^org/apache/spark/shuffle/sort/CometShuffleExternalSorter.*$"
allowed_expr+="|^org/apache/spark/shuffle/sort/RowPartition.class$"
allowed_expr+="|^org/apache/spark/shuffle/sort/SpillSorter.*$"
allowed_expr+="|^org/apache/spark/shuffle/comet/.*$"
allowed_expr+="|^org/apache/spark/sql/$"
# allow ExplainPlanGenerator trait since it may not be available in older Spark versions
allowed_expr+="|^org/apache/spark/sql/ExtendedExplainGenerator.*$"
# PyArrow UDF acceleration runner classes are under org/apache/spark/sql/execution/python
# because PythonArrowInput and BasicPythonArrowOutput are private[python]; Comet's classes
# must be in that package to mix them in.
allowed_expr+="|^org/apache/spark/sql/execution/$"
allowed_expr+="|^org/apache/spark/sql/execution/python/$"
allowed_expr+="|^org/apache/spark/sql/execution/python/CometColumnarPythonInput.*$"
allowed_expr+="|^org/apache/spark/sql/execution/python/CometArrowPythonRunner.*$"
# Builds Spark's PythonException for native Python UDFs. PySpark 4.1 recognizes it only when
# one of its stack frames is in this package.
allowed_expr+="|^org/apache/spark/sql/execution/python/CometPythonException.*$"
allowed_expr+="|^org/apache/spark/CometPlugin.class$"
allowed_expr+="|^org/apache/spark/CometDriverPlugin.*$"
allowed_expr+="|^org/apache/spark/CometExecutorPlugin.*$"
allowed_expr+="|^org/apache/spark/CometSource.*$"
allowed_expr+="|^org/apache/spark/CometTaskMemoryManager.class$"
allowed_expr+="|^org/apache/spark/CometTaskMemoryManager.*$"
allowed_expr+="|^scala-collection-compat.properties$"
allowed_expr+="|^scala/$"
allowed_expr+="|^scala/annotation/"
allowed_expr+="|^scala/collection/"
allowed_expr+="|^scala/jdk/"
allowed_expr+="|^scala/util/"

# Source-only exceptions apply to the optional sources artifact, not the main list.
sources_allowed_expr="${allowed_expr}"
sources_allowed_expr+="|^org/apache/spark/Plugins\.scala$"
sources_allowed_expr+="|^org/apache/spark/shuffle/sort/RowPartition\.scala$"
sources_allowed_expr+="|^module-info\.java$"
sources_allowed_expr+=")"
allowed_expr+=")"
declare -i bad_artifacts=0
declare -a bad_contents
declare -a artifact_list
while IFS='' read -r -d ';' line; do artifact_list+=("$line"); done < <(printf '%s;' "$1")
if [ "${#artifact_list[@]}" -eq 0 ]; then
  echo "[ERROR] No artifacts passed in."
  exit 1
fi

sources_index=-1
if [ -n "${2:-}" ]; then
  skip_sources=$(printf '%s' "${3:-false}" | tr '[:upper:]' '[:lower:]')
  if [ "${skip_sources}" = "true" ]; then
    echo "[INFO] Skipping sources-JAR verification (maven.source.skip=true)."
  elif [ -f "$2" ]; then
    sources_index=${#artifact_list[@]}
    artifact_list+=("$2")
  else
    echo "[ERROR] Expected sources JAR does not exist: '$2'."
    echo "[ERROR] Override comet.spark.sources.jar for a custom path, or set maven.source.skip=true for an intentional skip."
    exit 1
  fi
fi

jar_list_failed ()
{
    echo "[ERROR] Listing jar contents for file '${artifact}' failed."
    exit 1
}
trap jar_list_failed SIGUSR1

for artifact_index in "${!artifact_list[@]}"; do
  artifact="${artifact_list[artifact_index]}"
  artifact_allowed_expr="${allowed_expr}"
  if [ "${artifact_index}" -eq "${sources_index}" ]; then
    artifact_allowed_expr="${sources_allowed_expr}"
  fi
  bad_contents=()
  # Note: On Windows the output from jar tf may contain \r\n's.  Normalize to \n.
  while IFS='' read -r line; do bad_contents+=("$line"); done < <( ( jar tf "${artifact}" | sed 's/\\r//' || kill -SIGUSR1 $$ ) | grep -v -E "${artifact_allowed_expr}" )
  if [ ${#bad_contents[@]} -gt 0 ]; then
    echo "[ERROR] Found artifact with unexpected contents: '${artifact}'"
    echo "    Please check the following and either correct the build or update"
    echo "    the allowed list with reasoning."
    echo ""
    for bad_line in "${bad_contents[@]}"; do
      echo "    ${bad_line}"
    done
    bad_artifacts=${bad_artifacts}+1
  else
    echo "[INFO] Artifact looks correct: '$(basename "${artifact}")'"
  fi

  # Check the complete license text in both main and sources jars containing libcomet.
  # Common/test-only jars without the native library keep their existing legal resources.
  if jar tf "${artifact}" | tr -d '\r' |
      grep -E '^org/apache/comet/[^/]+/[^/]+/(libcomet\.(so|dylib)|comet\.dll)$' > /dev/null; then
    if ! check_native_license_versions; then
      bad_artifacts=${bad_artifacts}+1
      continue
    fi
    license_count=$(jar tf "${artifact}" | tr -d '\r' | awk '$0 == "META-INF/LICENSE" { n++ } END { print n+0 }')
    if [ "${license_count}" -ne 1 ]; then
      echo "[ERROR] Expected exactly one META-INF/LICENSE in '${artifact}'."
      bad_artifacts=${bad_artifacts}+1
    elif ! unzip -p "${artifact}" META-INF/LICENSE | cmp -s "${binary_license}" -; then
      echo "[ERROR] Binary LICENSE differs or is missing native attribution in '${artifact}'."
      bad_artifacts=${bad_artifacts}+1
    else
      echo "[INFO] Binary LICENSE matches: '$(basename "${artifact}")'"
    fi
  fi
done

if [ "${bad_artifacts}" -gt 0 ]; then
  exit 1
fi
