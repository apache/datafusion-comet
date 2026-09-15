# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Print the head sha of the most recent successful scheduled run of ci.yml,
# or nothing.
#
# The nightly diffs main against that commit, so every commit that landed
# since the last green nightly is covered exactly once, and a red nightly
# keeps the regressing commits in scope until a green one supersedes it. The
# `changes` job in ci.yml falls back to a time-based base when this prints
# nothing: the first scheduled run, or an API error, which is reported as a
# warning rather than failing the run.
#
# Needs GITHUB_TOKEN with `actions: read`, plus the GITHUB_REPOSITORY and
# GITHUB_API_URL variables every job has.

import json
import os
import sys
import urllib.error
import urllib.request


def main():
    repo = os.environ["GITHUB_REPOSITORY"]
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    url = f"{api}/repos/{repo}/actions/workflows/ci.yml/runs?event=schedule&status=success&per_page=1"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            runs = json.load(response)["workflow_runs"]
    except (urllib.error.URLError, OSError, KeyError, ValueError) as e:
        # stderr, so the caller's command substitution stays empty.
        print(
            f"::warning::could not list previous scheduled runs ({e}); using the time-based base",
            file=sys.stderr,
        )
        return 0
    if runs:
        print(runs[0]["head_sha"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
