# Benchmarks

[WIP] the purpose of this document / folder is to automate running the 100 GB benchmark in AWS so that anyone 
can run this as part of the release process and update the charts that we use in the repo.

This is separate from the general benchmarking advice that we have in the contributor guide but does duplicate much of it.

The documentation assumes that we are using instance type `m5.2xlarge` (subject to change). 

For now I am using 1024 GB EBS but this can probably be much smaller.


Connect to the instance and clone this repo (or the fork/branch to be used for benchmarking).

```shell
git clone https://github.com/apache/datafusion-comet
```

Install prerequisites (this will all get scripted once verified).

```shell
git clone https://github.com/apache/datafusion-benchmarks.git
```


Java

```shell
sudo yum install -y java-17-amazon-corretto-headless java-17-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
```

Rust

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"

sudo yum groupinstall "Development Tools"
```



Generate data locally

```shell
cargo install tpchgen-cli
tpchgen-cli -s 100 --format parquet
```

Spark

```shell
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar xzf spark-3.5.5-bin-hadoop3.tgz
export SPARK_HOME=/home/ec2-user/spark-3.5.5-bin-hadoop3/

$SPARK_HOME/sbin/start-master.sh --host localhost

export SPARK_MASTER=spark://localhost:7077
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER





```


