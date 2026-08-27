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

# Only wrap dependency acquisition or tool bootstrap, never a build or test.
# A missing artifact, a bad build definition, and other permanent failures must
# still fail immediately. Keep the output visible and preserve the exit status.
set -uo pipefail

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 command [argument ...]" >&2
  exit 2
fi

download_log=$(mktemp "${TMPDIR:-/tmp}/comet-download.XXXXXX") || exit 1
trap 'rm -f "$download_log"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for attempt in 1 2 3 4; do
  "$@" 2>&1 | tee "$download_log"
  command_status=("${PIPESTATUS[@]}")
  status=${command_status[0]}
  if [ "${command_status[1]}" -ne 0 ]; then
    echo "Could not capture dependency download output." >&2
    exit "${command_status[1]}"
  fi
  if [ "$status" -eq 0 ]; then
    exit 0
  fi

  # An earlier connection warning may have recovered before a different download
  # failed permanently. Be conservative when both appear: a known permanent error
  # must not acquire retries just because the same attempt also logged a timeout.
  # Do not inspect only a fixed tail: Maven/SBT can print long failure summaries.
  if grep -Eiq \
    '(status code:|response code:|HTTP/[0-9.]+|HTTP error|returned error:)[[:space:]]*(400|401|403|404|405|410|422)([^0-9]|$)|Could not find artifact|not found: value|COMPILATION ERROR|There are test failures' \
    "$download_log"; then
    exit "$status"
  fi

  # Signals (including cancellation and OOM kills) are not download failures.
  if [ "$status" -ge 128 ] || ! grep -Eiq \
    '(status code:|response code:|HTTP/[0-9.]+|HTTP error|returned error:)[[:space:]]*(429|500|502|503|504)([^0-9]|$)|Connection reset|Connection timed out|ConnectTimeoutException|SocketTimeoutException|Read timed out|Temporary failure in name resolution|Network is unreachable|Remote host terminated the handshake' \
    "$download_log"; then
    exit "$status"
  fi
  if [ "$attempt" -eq 4 ]; then
    echo "::error::Dependency download failed after $attempt attempts."
    exit "$status"
  fi

  delay=$((10 * (1 << (attempt - 1)) + RANDOM % 5))
  echo "::warning::Transient download failure; retrying in ${delay}s (attempt $attempt of 4)."
  sleep "$delay" || exit "$?"
done
