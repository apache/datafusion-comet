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

# Prints the files a push to a branch of a contributor's fork changed, as the
# pull request from that branch would show them: relative to upstream main.
#
# There is no pull request to diff against on a push, and the fork's own default
# branch is the wrong base. A fork that has not synced is behind upstream, so a
# branch based on newer upstream code that reverts one of its files matches the
# fork's old copy, and a diff against it omits the file. Fetching upstream main
# gives the diff the pull request will have. The fetch drops the credentials
# actions/checkout leaves in the git config: they belong to the fork, and the
# upstream repository is public.
#
# Prints every tracked file when there is nothing to go on: the fetch fails, there
# is no merge base, or the diff is empty. An empty diff means the branch adds
# nothing to upstream main, such as `git push fork main:my-fix` before upstream
# has the commits, and selecting no suites would report a green run for a change
# nobody built.
#
# Usage: fork-push-changed-files.sh <head sha>
# UPSTREAM_URL overrides the repository to diff against, for the tests.

set -euo pipefail

head="${1:?usage: fork-push-changed-files.sh <head sha>}"
upstream="${UPSTREAM_URL:-https://github.com/apache/datafusion-comet.git}"
ref="refs/remotes/upstream/main"

changed=""
if git -c "http.https://github.com/.extraheader=" fetch --quiet --no-tags "$upstream" "+main:$ref" \
    && git merge-base "$ref" "$head" >/dev/null 2>&1; then
  changed=$(git diff --name-only "$ref"..."$head")
fi

if [[ -n "$changed" ]]; then
  printf '%s\n' "$changed"
else
  git ls-tree -r --name-only "$head"
fi
