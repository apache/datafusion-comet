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

set -euo pipefail

# Match mvnw's JVM and option order. In job containers, shell HOME is
# /github/home while the JVM's user.home (and the wrapper cache) is /root.
java_command="${JAVACMD:-${JAVA_HOME:+$JAVA_HOME/bin/}java}"
java_options="${MAVEN_OPTS:-}"
if [ -f .mvn/jvm.config ]; then
  java_options="$(tr '\n' ' ' < .mvn/jvm.config) $java_options"
fi
java_options="${java_options//$'\r'/ }"
java_options="${java_options//$'\n'/ }"
read -r -a java_arguments <<< "$java_options"
properties=$("$java_command" "${java_arguments[@]}" -XshowSettings:properties -version 2>&1) || {
  status=$?
  printf '%s\n' "$properties" >&2
  exit "$status"
}

# Wrapper 3.2.0 prefers the JVM property, then the environment override,
# then user.home/.m2. Both its distribution and ZIP paths use wrapper/dists.
maven_user_home=$(sed -n 's/^[[:space:]]*maven\.user\.home = //p' <<< "$properties")
maven_user_home="${maven_user_home:-${MAVEN_USER_HOME:-}}"
if [ -z "$maven_user_home" ]; then
  jvm_user_home=$(sed -n 's/^[[:space:]]*user\.home = //p' <<< "$properties")
  if [ -z "$jvm_user_home" ]; then
    echo "Could not determine the Maven wrapper cache directory from JVM properties." >&2
    exit 1
  fi
  maven_user_home="$jvm_user_home/.m2"
fi
printf '%s/wrapper/dists\n' "${maven_user_home%/}"
