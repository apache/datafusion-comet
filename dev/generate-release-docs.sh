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
# On main, the user guide source directory holds only template markers; CI fills them
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
#   git add docs/source/user-guide/<guide>/
#   git commit -m "Generate docs for 0.13.0 release"
#   # open a PR targeting branch-0.13

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

# The user guide lives under `dev` on main and on release branches cut after it was
# renamed from `latest`, and under `latest` on branch-1.2 and older. Detect it rather
# than hardcoding one name, so the same script works wherever it runs.
USER_GUIDE_DIR="${PROJECT_ROOT}/docs/source/user-guide/dev"
if [ ! -d "${USER_GUIDE_DIR}" ]; then
  USER_GUIDE_DIR="${PROJECT_ROOT}/docs/source/user-guide/latest"
fi
COMPAT_EXPR_DIR="${USER_GUIDE_DIR}/compatibility/expressions"
TEMPLATE_DIR="${COMPAT_EXPR_DIR}/_category_template"

# Spark profiles for which we publish per-version compatibility pages. Keep this list
# in sync with docs/build.sh. Each entry must match both a Maven profile id (-P<profile>)
# and a source-tree subdirectory under compatibility/expressions/.
# Every run also rewrites the shared configs.md and expressions.md pages, so the last
# entry decides their content. Keep the default build profile (spark-4.1) last.
SPARK_PROFILES=(spark-3.5 spark-4.0 spark-4.2 spark-4.1)

echo "Compiling and generating documentation content..."
for profile in "${SPARK_PROFILES[@]}"; do
  echo "Generating configs and compatibility pages for ${profile}..."
  mkdir -p "${COMPAT_EXPR_DIR}/${profile}"
  cp "${TEMPLATE_DIR}"/*.md "${COMPAT_EXPR_DIR}/${profile}/"
  ./mvnw package -P"${profile}" -Pgenerate-docs -DskipTests -Dmaven.test.skip=true \
    -Dexec.arguments="${USER_GUIDE_DIR},${profile}"
done

# Point the generated guide links at this release instead of the development guide.
# GenerateDocs derives the links from the version it builds, and this script runs before the
# release branch's version is bumped, so they come out as user-guide/dev/. Those pages are
# frozen onto the branch and served as the archived user-guide/<major>.<minor>/ guide, where a
# link to dev sends readers to the moving snapshot. Take the version from the branch name so a
# run on main, where dev links are correct, is left alone.
RELEASE_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
case "${RELEASE_BRANCH}" in
  branch-[0-9]*.[0-9]*)
    MAJOR_MINOR="${RELEASE_BRANCH#branch-}"
    echo "Rewriting guide links to user-guide/${MAJOR_MINOR}/ for ${RELEASE_BRANCH}..."
    python3 - "${USER_GUIDE_DIR}" "${MAJOR_MINOR}" <<'PY'
import pathlib
import sys

root, version = pathlib.Path(sys.argv[1]), sys.argv[2]
changed = 0
for page in root.rglob("*.md"):
    text = page.read_text(encoding="utf-8")
    if "user-guide/dev/" in text:
        page.write_text(
            text.replace("user-guide/dev/", f"user-guide/{version}/"), encoding="utf-8")
        changed += 1
print(f"rewrote guide links in {changed} file(s)")
PY
    ;;
  *)
    echo "Not on a release branch (${RELEASE_BRANCH}), leaving user-guide/dev/ links as they are."
    ;;
esac

# Format the generated Markdown. CI runs `prettier --check "**/*.md"`, and unlike
# docs/build.sh (whose output is a throwaway temp build) this output is committed, so it
# must pass the check. GenerateDocs wraps the config/cast tables in prettier-ignore, but
# the surrounding generated Markdown is not guaranteed prettier-clean. prettier honors
# .prettierignore, so excluded pages (e.g. expressions.md) are left untouched.
# CI installs the latest prettier (`npm install -g prettier`), and releases of prettier differ
# in what they accept, so format with the latest too rather than with whatever version is
# installed locally or cached by npx. Generating the 1.2.0 docs with prettier 3.2.5 left 40
# pages that the latest prettier rejects.
if ! command -v npx >/dev/null 2>&1; then
  echo "ERROR: npx not found. Install Node/prettier (see docs/source/contributor-guide/development.md)" >&2
  exit 1
fi
echo "Formatting generated Markdown with prettier..."
npx --yes prettier@latest "${USER_GUIDE_DIR}/**/*.md" --write

echo ""
echo "Done! Generated documentation content in ${USER_GUIDE_DIR#"${PROJECT_ROOT}/"}/"
echo "  - configs.md (config reference)"
echo "  - compatibility/expressions/spark-*/ (per-Spark-version compatibility pages)"
echo ""
echo "Next steps (release-branch changes go via PR):"
echo "  git add ${USER_GUIDE_DIR#"${PROJECT_ROOT}/"}/"
echo "  git commit -m 'Generate docs for release'"
echo "  # push to your fork and open a PR targeting the release branch"
