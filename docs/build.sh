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

set -e
rm -rf build 2> /dev/null
rm -rf temp 2> /dev/null
mkdir temp
cp -rf source/* temp/

# Add user guide from published releases
rm -rf comet-0.14
rm -rf comet-0.15
rm -rf comet-0.16
python3 generate-versions.py

# Generate dynamic content (configs, compatibility matrices) for latest docs
# This runs GenerateDocs against the temp copy, not source files. Each supported Spark
# profile gets its own copy of the compatibility/expressions templates so that the
# generated content can differ per Spark version (some expressions are only registered
# through version-specific shims).
echo "Generating dynamic documentation content..."
cd ..

LATEST_USER_GUIDE="$(pwd)/docs/temp/user-guide/latest"
COMPAT_EXPR_DIR="${LATEST_USER_GUIDE}/compatibility/expressions"
TEMPLATE_DIR="${COMPAT_EXPR_DIR}/_category_template"

# Spark profiles for which we publish per-version compatibility pages. Add or remove
# entries here when adding/dropping support for a Spark version. Each entry must match
# both a Maven profile id (-P<profile>) and a source-tree subdirectory under
# docs/source/user-guide/latest/compatibility/expressions/.
SPARK_PROFILES=(spark-3.4 spark-3.5 spark-4.0 spark-4.1)

for profile in "${SPARK_PROFILES[@]}"; do
  echo "Generating compatibility pages for ${profile}..."
  mkdir -p "${COMPAT_EXPR_DIR}/${profile}"
  cp "${TEMPLATE_DIR}"/*.md "${COMPAT_EXPR_DIR}/${profile}/"
  ./mvnw -q package -P${profile} -Pgenerate-docs -DskipTests -Dmaven.test.skip=true \
    -Dexec.arguments="${LATEST_USER_GUIDE},${profile}"
done

# Templates aren't a real doc page; remove from the build input so Sphinx doesn't see them.
rm -rf "${TEMPLATE_DIR}"
cd docs

make SOURCEDIR=`pwd`/temp html
