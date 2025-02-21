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

FROM apache/spark:3.4.3 AS builder

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

RUN du -h --max-depth=1

# build native code first so that this layer can be re-used
# if only Scala code gets modified
COPY rust-toolchain.toml /comet/rust-toolchain.toml
COPY native /comet/native
RUN cd native && RUSTFLAGS="-Ctarget-cpu=native" cargo build --release

# copy the rest of the project
COPY .mvn /comet/.mvn
COPY mvnw /comet/mvnw
COPY common /comet/common
COPY dev /comet/dev
COPY docs /comet/docs
COPY fuzz-testing /comet/fuzz-testing
COPY spark /comet/spark
COPY spark-integration /comet/spark-integration
COPY scalafmt.conf /comet/scalafmt.conf
COPY .scalafix.conf /comet/.scalafix.conf
COPY Makefile /comet/Makefile
COPY pom.xml /comet/pom.xml

RUN mkdir -p /root/.m2 && \
    echo '<settings><mirrors><mirror><id>central</id><mirrorOf>central</mirrorOf><url>https://repo1.maven.org/maven2</url></mirror></mirrors></settings>' > /root/.m2/settings.xml

RUN rm -rf /root/.rustup

RUN du -h --max-depth=1

# Pick the JDK instead of JRE to compile Comet
RUN cd /comet \
    && JAVA_HOME=$(readlink -f $(which javac) | sed "s/\/bin\/javac//") make release-nogit PROFILES="-Pspark-$SPARK_VERSION -Pscala-$SCALA_VERSION"

FROM apache/spark:3.4.3
ENV SPARK_VERSION=3.4
ENV SCALA_VERSION=2.12
USER root

# note the use of a wildcard in the file name so that this works with both snapshot and final release versions
COPY --from=builder  /comet/spark/target/comet-spark-spark${SPARK_VERSION}_$SCALA_VERSION-*.jar $SPARK_HOME/jars
