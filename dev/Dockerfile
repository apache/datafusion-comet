#
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
#

FROM ubuntu:24.10

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    git \
    make \
    build-essential \
    ca-certificates \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install protoc
ENV PROTOC_VERSION=24.4

RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    && unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local \
    && rm protoc-${PROTOC_VERSION}-linux-x86_64.zip

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustc --version && cargo --version

# Install Java
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
RUN java -version

# Add Comet sources
RUN mkdir /comet
WORKDIR /comet
COPY ./.mvn /comet/.mvn
COPY ./common /comet/common
COPY ./dev /comet/dev
COPY ./docs /comet/docs
COPY ./fuzz-testing /comet/fuzz-testing
COPY ./spark /comet/spark
COPY ./spark-integration /comet/spark-integration
COPY ./native /comet/native
COPY ./pom.xml /comet
COPY ./Makefile /comet
COPY ./rust-toolchain.toml /comet
COPY ./mvnw /comet
COPY ./scalafmt.conf /comet
COPY ./.scalafix.conf /comet

# run make to download and cache dependencies and build Rust code
RUN make

# Shallow clone of Apache Spark and apply diffs
#RUN git clone -b 'v3.5.5' --single-branch --depth 1 https://github.com/apache/spark.git apache-spark \
#    && cd apache-spark \
#    && git apply ../dev/diffs/3.5.5.diff

