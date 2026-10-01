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

# Pin the platform-specific manifests; this pilot intentionally targets amd64.
FROM docker.io/azul/zulu-openjdk:17@sha256:e5ca962bc4dde62036176b7af408c320269164428aa2462f925288d7600a979b AS jdk
FROM docker.io/library/rust:1.98.0-trixie@sha256:8aea3a845c48caac4091a8d0e6494d213061f8f014dc2f5a5f1a88a1a8c9f301 AS toolchain

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
COPY --from=jdk /usr/lib/jvm/zulu17-ca-amd64/ /opt/comet-ci/jdk/
COPY dev/ci/images/versions.env dev/ci/images/install-tools.sh dev/ci/images/check-tools.sh /opt/comet-ci/bin/
COPY dev/ci/images/maven-settings.xml /opt/comet-ci/maven/settings.xml
COPY dev/ci/images/maven-settings.xml dev/ci/images/repositories /opt/comet-ci/bin/

# GitHub mounts HOME and the workspace. Keep toolchains and caches outside both.
# Set Java's user.home explicitly too: it need not agree with the shell's HOME.
ENV JAVA_HOME=/opt/comet-ci/jdk \
    CARGO_HOME=/opt/comet-ci/cargo \
    MAVEN_USER_HOME=/opt/comet-ci/maven \
    COURSIER_CACHE=/opt/comet-ci/coursier \
    LC_ALL=C.UTF-8 \
    RUST_BACKTRACE=1 \
    CARGO_NET_RETRY=3 \
    RUSTFLAGS="-Ctarget-cpu=x86-64-v3 -Clink-arg=-fuse-ld=bfd"
ENV PATH="${JAVA_HOME}/bin:${PATH}" \
    LD_LIBRARY_PATH="${JAVA_HOME}/lib/server" \
    JAVA_TOOL_OPTIONS="-Duser.home=/opt/comet-ci/home -Dmaven.repo.local=/opt/comet-ci/maven/repository -Dsbt.boot.directory=/opt/comet-ci/sbt/boot -Dsbt.global.base=/opt/comet-ci/sbt/global -Dsbt.ivy.home=/opt/comet-ci/ivy -Dsbt.coursier.home=/opt/comet-ci/coursier -Dsbt.override.build.repos=true -Dsbt.repository.config=/opt/comet-ci/bin/repositories" \
    MAVEN_OPTS="-Xmx2g -Daether.connector.http.retryHandler.count=3"

RUN bash /opt/comet-ci/bin/install-tools.sh

# Override the checkout's floating "stable" selector even in a fresh workspace.
ENV RUSTUP_TOOLCHAIN=1.98.0

# Never use this intermediate stage as the published image or export its layers
# as a public build cache: it contains locally built Comet and Spark artifacts.
FROM toolchain AS dependencies
ARG BUILD_JOBS=4
WORKDIR /warmup/comet
COPY . .
COPY dev/ci/images/ /opt/comet-ci/bin/
RUN --mount=type=cache,id=apache-comet-ci-image-native,target=/tmp/comet-ci-native-target,sharing=locked \
    source /opt/comet-ci/bin/versions.env \
    && bash /opt/comet-ci/bin/fetch-spark.sh /tmp/spark.tar.gz \
    && mkdir apache-spark \
    && tar -xzf /tmp/spark.tar.gz -C apache-spark --strip-components=1 \
    && git -C apache-spark apply "/warmup/comet/dev/diffs/$SPARK_VERSION.diff" \
    && CARGO_TARGET_DIR=/tmp/comet-ci-native-target CARGO_BUILD_JOBS="$BUILD_JOBS" bash /opt/comet-ci/bin/run-build.sh warm \
    && python3 /opt/comet-ci/bin/cache.py export /opt/comet-ci /export \
    && python3 /opt/comet-ci/bin/cache.py inputs . --spark-version "$SPARK_VERSION" > /export/inputs.sha256
ARG SOURCE_REVISION=unknown
RUN printf '%s\n' "$SOURCE_REVISION" > /export/source-revision

# Start from the toolchain, not dependencies. Deleted build outputs must not
# remain recoverable from earlier layers of the final image.
FROM toolchain AS ci
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/apache/datafusion-comet" \
      org.opencontainers.image.description="Comet CI toolchains and third-party dependencies; not a Comet release" \
      org.opencontainers.image.revision="${SOURCE_REVISION}"
COPY dev/ci/images/ /opt/comet-ci/bin/
COPY --from=dependencies /export/ /opt/comet-ci/
RUN python3 /opt/comet-ci/bin/cache.py audit /opt/comet-ci
WORKDIR /work
