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

# This script generates documentation content for a release branch.
# It should be run once when creating a new release branch to "freeze"
# the generated docs (configs, compatibility matrices) into the branch.
#
# Usage: ./dev/generate-release-docs.sh
#
# This script:
# 1. Compiles the spark module to access CometConf and CometCast
# 2. Runs GenerateDocs to populate the template markers in the docs
# 3. The resulting changes should be committed to the release branch
#
# Example workflow when cutting release 0.13.0:
#   git checkout -b branch-0.13 main
#   ./dev/generate-release-docs.sh
#   git add docs/source/user-guide/latest/
#   git commit -m "Generate docs for 0.13.0 release"
#   git push origin branch-0.13

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

echo "Compiling common and spark modules..."
./mvnw package -Pgenerate-docs -DskipTests -Dmaven.test.skip=true

echo ""
echo "Done! Generated documentation content in docs/source/user-guide/latest/"
echo ""
echo "Next steps:"
echo "  git add docs/source/user-guide/latest/"
echo "  git commit -m 'Generate docs for release'"
echo "  git push"
