# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Benchmark image for running TPC-H and TPC-DS benchmarks across engines
# (Spark, Comet, Gluten).
#
# Build (from repository root):
#   docker build -t comet-bench -f benchmarks/tpc/infra/docker/Dockerfile .

ARG SPARK_IMAGE=apache/spark:3.5.2-python3
FROM ${SPARK_IMAGE}

USER root

# Install Java 8 (Gluten) and Java 17 (Comet) plus Python 3.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-8-jdk-headless \
       openjdk-17-jdk-headless \
       python3 python3-pip procps wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install async-profiler for profiling Java + native (Rust/C++) code.
ARG ASYNC_PROFILER_VERSION=3.0
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then AP_ARCH="linux-x64"; \
    elif [ "$ARCH" = "aarch64" ]; then AP_ARCH="linux-aarch64"; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    wget -q "https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/async-profiler-${ASYNC_PROFILER_VERSION}-${AP_ARCH}.tar.gz" \
         -O /tmp/async-profiler.tar.gz && \
    mkdir -p /opt/async-profiler && \
    tar xzf /tmp/async-profiler.tar.gz -C /opt/async-profiler --strip-components=1 && \
    rm /tmp/async-profiler.tar.gz
ENV ASYNC_PROFILER_HOME=/opt/async-profiler

# Default to Java 17 (override with JAVA_HOME at runtime for Gluten).
# Detect architecture (amd64 or arm64) so the image works on both Linux and macOS.
ARG TARGETARCH
RUN ln -s /usr/lib/jvm/java-17-openjdk-${TARGETARCH} /usr/lib/jvm/java-17-openjdk && \
    ln -s /usr/lib/jvm/java-8-openjdk-${TARGETARCH} /usr/lib/jvm/java-8-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Copy the benchmark scripts into the image.
COPY benchmarks/tpc/run.py              /opt/benchmarks/run.py
COPY benchmarks/tpc/tpcbench.py         /opt/benchmarks/tpcbench.py
COPY benchmarks/tpc/engines             /opt/benchmarks/engines
COPY benchmarks/tpc/queries             /opt/benchmarks/queries
COPY benchmarks/tpc/create-iceberg-tables.py /opt/benchmarks/create-iceberg-tables.py
COPY benchmarks/tpc/generate-comparison.py   /opt/benchmarks/generate-comparison.py

# Engine JARs are bind-mounted or copied in at runtime via --jars.
# Data and query paths are also bind-mounted.

WORKDIR /opt/benchmarks

# Defined in the base apache/spark image.
ARG spark_uid
USER ${spark_uid}
