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

# Unified benchmark image for running TPC and shuffle benchmarks across
# engines (Spark, Comet, Gluten, Blaze).
#
# Build:
#   docker build -t comet-bench -f benchmarks/infra/docker/Dockerfile .
#
# The build context should be the repository root so that benchmarks/ is
# available.

ARG SPARK_IMAGE=apache/spark:3.5.5-python3
FROM ${SPARK_IMAGE}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the unified benchmark runner into the image.
COPY benchmarks/conf    /opt/benchmarks/conf
COPY benchmarks/runner  /opt/benchmarks/runner
COPY benchmarks/suites  /opt/benchmarks/suites
COPY benchmarks/run.py  /opt/benchmarks/run.py

# Copy the metrics collector script.
COPY benchmarks/infra/docker/collect-metrics.sh /opt/benchmarks/collect-metrics.sh
RUN chmod +x /opt/benchmarks/collect-metrics.sh

# Engine JARs are bind-mounted or copied in at runtime via --jars.
# Data and query paths are also bind-mounted.

ENV PYTHONPATH="/opt:${PYTHONPATH}"

WORKDIR /opt/benchmarks

USER ${spark_uid}
