#!/bin/bash
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
#

# Install Rust
sudo yum groupinstall -y "Development Tools"
curl https://sh.rustup.rs -sSf | sh -s -- -y
. "$HOME/.cargo/env"

# Install Java
sudo yum install -y java-17-amazon-corretto-headless java-17-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto

# Build Comet
make release PROFILES="-Pspark-3.5"

# Clone datafusion-benchmarks repo, which has the queries
git clone https://github.com/apache/datafusion-benchmarks.git

# Install Spark
mkdir /home/ec2-user/tmp
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar xzf spark-3.5.5-bin-hadoop3.tgz
cp spark-env.sh spark-3.5.5-bin-hadoop3/conf
export SPARK_HOME=/home/ec2-user/spark-3.5.5-bin-hadoop3/
export SPARK_MASTER=spark://localhost:7077
$SPARK_HOME/sbin/start-master.sh --host localhost
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
