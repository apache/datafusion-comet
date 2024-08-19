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

FROM apache/spark:3.4.2 AS builder

USER root

# Installing JDK11 as the image comes with JRE
RUN apt update \
    && apt install -y curl \
    && apt install -y openjdk-11-jdk \
    && apt clean

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
ENV RUSTFLAGS="-C debuginfo=line-tables-only -C incremental=false"
ENV SPARK_VERSION=3.4
ENV SCALA_VERSION=2.12

# copy source files to Docker image
RUN mkdir /comet
WORKDIR /comet

COPY .mvn /comet/.mvn
COPY common /comet/common
COPY fuzz-testing /comet/fuzz-testing
COPY native /comet/native
COPY spark /comet/spark
COPY spark-integration /comet/spark-integration
COPY .scalafix.conf /comet/.scalafix.conf
COPY Makefile /comet/Makefile
COPY pom.xml /comet/pom.xml

# Pick the JDK instead of JRE to compile Comet
RUN cd /comet \
    && JAVA_HOME=$(readlink -f $(which javac) | sed "s/\/bin\/javac//") make release-nogit PROFILES="-Pspark-$SPARK_VERSION -Pscala-$SCALA_VERSION"

FROM apache/spark:3.4.2
ENV SPARK_VERSION=3.4
ENV SCALA_VERSION=2.12
USER root
COPY --from=builder  /opt/datafusion-comet/spark/target/comet-spark-spark${SPARK_VERSION}_$SCALA_VERSION-0.2.0-SNAPSHOT.jar $SPARK_HOME/jars