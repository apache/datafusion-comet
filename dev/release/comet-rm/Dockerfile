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
FROM ubuntu:20.04 AS base

USER root

# For apt to be noninteractive
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true

ENV LC_ALL=C
# Install pr-requisites for rust
RUN export LC_ALL=C \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates \
        build-essential \
        curl \
        wget \
        git \
        llvm \
        clang \
        libssl-dev \
        lzma-dev \
        liblzma-dev \
        openssh-client \
        cmake \
        cpio \
        libxml2-dev \
        patch \
        bzip2 \
        libbz2-dev \
        zlib1g-dev \
        default-jdk

RUN apt install -y gcc-10 g++-10 cpp-10 unzip
ENV CC="gcc-10"
ENV CXX="g++-10"

RUN PB_REL="https://github.com/protocolbuffers/protobuf/releases" \
    && curl -LO $PB_REL/download/v30.2/protoc-30.2-linux-x86_64.zip \
    && unzip protoc-30.2-linux-x86_64.zip -d /root/.local
ENV PATH="$PATH:/root/.local/bin"

# Install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install cargo2junit

FROM base AS final

COPY build-comet-native-libs.sh /opt/comet-rm/build-comet-native-libs.sh
WORKDIR /opt/comet-rm

ENTRYPOINT [ "/opt/comet-rm/build-comet-native-libs.sh"]
