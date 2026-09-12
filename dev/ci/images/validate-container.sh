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

set -euo pipefail
source /opt/comet-ci/bin/versions.env
# The host supplies source archives only, not previous target/ or ~/.m2 output.
test ! -e native
tar -xzf /inputs/comet.tar.gz
mkdir apache-spark
tar -xzf /inputs/spark.tar.gz -C apache-spark --strip-components=1
git -C apache-spark apply "$PWD/dev/diffs/$SPARK_VERSION.diff"
bash /opt/comet-ci/bin/run-build.sh offline
