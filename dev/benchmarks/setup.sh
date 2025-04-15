#!/bin/bash

# Install Rust
sudo yum groupinstall -y "Development Tools"
curl https://sh.rustup.rs -sSf | sh -s -- -y
. "$HOME/.cargo/env"

# Install Java
sudo yum install -y java-17-amazon-corretto-headless java-17-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto

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
