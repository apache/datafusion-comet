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

# This script freezes generated documentation content onto a release branch.
# It should be run once when creating a new release branch.
#
# On main, docs/source/user-guide/latest/ holds only template markers; CI fills them
# at publish time via docs/build.sh. A release branch instead commits the generated
# content so that the archived docs for the release render real tables (archived
# versions are served from the frozen branch, not regenerated at publish).
#
# This mirrors the generation in docs/build.sh, except it runs against the source tree
# (so the result can be committed) rather than a throwaway temp copy. Two kinds of
# content are generated:
#   - configs.md: the config reference (not per-Spark-version)
#   - compatibility/expressions/spark-<ver>/*.md: per-Spark-version compatibility pages,
#     generated from the _category_template/ markers. Support can differ per Spark
#     version, so GenerateDocs runs once per profile against that profile's build.
#
# Usage: ./dev/generate-release-docs.sh
#
# Example workflow when cutting release 0.13.0 (release-branch changes go via PR):
#   git checkout -b release-0.13-docs branch-0.13
#   ./dev/generate-release-docs.sh
#   git add docs/source/user-guide/latest/
#   git commit -m "Generate docs for 0.13.0 release"
#   # open a PR targeting branch-0.13

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

LATEST_USER_GUIDE="${PROJECT_ROOT}/docs/source/user-guide/latest"
COMPAT_EXPR_DIR="${LATEST_USER_GUIDE}/compatibility/expressions"
TEMPLATE_DIR="${COMPAT_EXPR_DIR}/_category_template"

# Spark profiles for which we publish per-version compatibility pages. Keep this list
# in sync with docs/build.sh. Each entry must match both a Maven profile id (-P<profile>)
# and a source-tree subdirectory under compatibility/expressions/.
SPARK_PROFILES=(spark-3.4 spark-3.5 spark-4.0 spark-4.1)

echo "Compiling and generating documentation content..."
for profile in "${SPARK_PROFILES[@]}"; do
  echo "Generating configs and compatibility pages for ${profile}..."
  mkdir -p "${COMPAT_EXPR_DIR}/${profile}"
  cp "${TEMPLATE_DIR}"/*.md "${COMPAT_EXPR_DIR}/${profile}/"
  ./mvnw package -P"${profile}" -Pgenerate-docs -DskipTests -Dmaven.test.skip=true \
    -Dexec.arguments="${LATEST_USER_GUIDE},${profile}"
done

# Format the generated Markdown. CI runs `prettier --check "**/*.md"`, and unlike
# docs/build.sh (whose output is a throwaway temp build) this output is committed, so it
# must pass the check. GenerateDocs wraps the config/cast tables in prettier-ignore, but
# the surrounding generated Markdown is not guaranteed prettier-clean. prettier honors
# .prettierignore, so excluded pages (e.g. expressions.md) are left untouched.
if ! command -v npx >/dev/null 2>&1; then
  echo "ERROR: npx not found. Install Node/prettier (see docs/source/contributor-guide/development.md)" >&2
  exit 1
fi
echo "Formatting generated Markdown with prettier..."
npx prettier "${LATEST_USER_GUIDE}/**/*.md" --write

echo ""
echo "Done! Generated documentation content in docs/source/user-guide/latest/"
echo "  - configs.md (config reference)"
echo "  - compatibility/expressions/spark-*/ (per-Spark-version compatibility pages)"
echo ""
echo "Next steps (release-branch changes go via PR):"
echo "  git add docs/source/user-guide/latest/"
echo "  git commit -m 'Generate docs for release'"
echo "  # push to your fork and open a PR targeting the release branch"
