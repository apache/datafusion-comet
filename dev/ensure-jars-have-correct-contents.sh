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

# Usage: $0 [/path/to/some/example.jar;/path/to/another/example/created.jar]
#
# accepts a single command line argument with a colon separated list of
# paths to jars to check. Iterates through each such passed jar and checks
# all the contained paths to make sure they follow the below constructed
# safe list.

# We use +=, which is a bash 3.1+ feature
if [[ -z "${BASH_VERSINFO[0]}" ]] \
   || [[ "${BASH_VERSINFO[0]}" -lt 3 ]] \
   || [[ "${BASH_VERSINFO[0]}" -eq 3 && "${BASH_VERSINFO[1]}" -lt 1 ]]; then
  echo "bash v3.1+ is required. Sorry."
  exit 1
fi

set -e
set -o pipefail

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
allowed_expr+="|^org/apache/spark/shuffle/comet/.*$"
allowed_expr+="|^org/apache/spark/sql/$"
# allow ExplainPlanGenerator trait since it may not be available in older Spark versions
allowed_expr+="|^org/apache/spark/sql/ExtendedExplainGenerator.*$"
allowed_expr+="|^org/apache/spark/CometPlugin.class$"
allowed_expr+="|^org/apache/spark/CometDriverPlugin.*$"
allowed_expr+="|^org/apache/spark/CometTaskMemoryManager.class$"
allowed_expr+="|^org/apache/spark/CometTaskMemoryManager.*$"

# TODO: add reason
allowed_expr+="|^org/apache/spark/sql/execution/$"
allowed_expr+="|^org/apache/spark/sql/execution/joins/$"
allowed_expr+="|^org/apache/spark/sql/execution/joins/CometHashedRelation.*$"
allowed_expr+="|^org/apache/spark/sql/execution/joins/CometLongHashedRelation.class$"
allowed_expr+="|^org/apache/spark/sql/execution/joins/CometUnsafeHashedRelation.class$"

allowed_expr+=")"
declare -i bad_artifacts=0
declare -a bad_contents
declare -a artifact_list
while IFS='' read -r -d ';' line; do artifact_list+=("$line"); done < <(printf '%s;' "$1")
if [ "${#artifact_list[@]}" -eq 0 ]; then
  echo "[ERROR] No artifacts passed in."
  exit 1
fi

jar_list_failed ()
{
    echo "[ERROR] Listing jar contents for file '${artifact}' failed."
    exit 1
}
trap jar_list_failed SIGUSR1

for artifact in "${artifact_list[@]}"; do
  bad_contents=()
  # Note: On Windows the output from jar tf may contain \r\n's.  Normalize to \n.
  while IFS='' read -r line; do bad_contents+=("$line"); done < <( ( jar tf "${artifact}" | sed 's/\\r//' || kill -SIGUSR1 $$ ) | grep -v -E "${allowed_expr}" )
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
done

if [ "${bad_artifacts}" -gt 0 ]; then
  exit 1
fi
