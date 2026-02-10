<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Comet Benchmarking in EC2

This guide is for setting up benchmarks on AWS EC2 with a single node with Parquet files either located on an
attached EBS volume, or stored in S3.

## Create EC2 Instance

- Create an EC2 instance with an EBS volume sized for approximately 2x the size of
  the dataset to be generated (200 GB for scale factor 100, 2 TB for scale factor 1000, and so on)
- Optionally, create an S3 bucket to store the Parquet files

Recommendation: Use `c7i.4xlarge` instance type with `provisioned iops 2` EBS volume (8000 IOPS).

## Install Prerequisites

```shell
sudo yum update -y
sudo yum install -y git protobuf-compiler java-17-amazon-corretto-headless java-17-amazon-corretto-devel
sudo yum groupinstall -y "Development Tools"
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
```

## Generate Benchmark Data

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
RUSTFLAGS='-C target-cpu=native' cargo install tpchgen-cli
tpchgen-cli -s 100 --format parquet --parts 32 --output-dir data
```

Rename the generated directories so that they have a `.parquet` suffix. For example, rename `customer` to
`customer.parquet`.

Set the `TPCH_DATA` environment variable. This will be referenced by the benchmarking scripts.

```shell
export TPCH_DATA=/home/ec2-user/data
```

## Install Apache Spark

```shell
export SPARK_VERSION=3.5.6
wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz
tar xzf spark-$SPARK_VERSION-bin-hadoop3.tgz
sudo mv spark-$SPARK_VERSION-bin-hadoop3 /opt
export SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop3/
mkdir /tmp/spark-events
```

Set `SPARK_MASTER` env var (IP address will need to be edited):

```shell
export SPARK_MASTER=spark://172.31.34.87:7077
```

Set `SPARK_LOCAL_DIRS` to point to EBS volume

```shell
sudo mkdir /mnt/tmp
sudo chmod 777 /mnt/tmp
mv $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
```

Add the following entry to `spark-env.sh`:

```shell
SPARK_LOCAL_DIRS=/mnt/tmp
```

## Git Clone DataFusion Repositories

```shell
git clone https://github.com/apache/datafusion-benchmarks.git
git clone https://github.com/apache/datafusion-comet.git
```

Build Comet

```shell
cd datafusion-comet
make release
```

Set `COMET_JAR` environment variable.

```shell
export COMET_JAR=/home/ec2-user/datafusion-comet/spark/target/comet-spark-spark3.5_2.12-$COMET_VERSION.jar
```

## Run Benchmarks

Use the scripts in `dev/benchmarks` in the Comet repository.

```shell
cd dev/benchmarks
export TPCH_QUERIES=/home/ec2-user/datafusion-benchmarks/tpch/queries/
```

Run Spark benchmark:

```shell
./spark-tpch.sh
```

Run Comet benchmark:

```shell
./comet-tpch.sh
```

## Running Benchmarks with S3

Copy the Parquet data to an S3 bucket.

```shell
aws s3 cp /home/ec2-user/data s3://your-bucket-name/--recursive
```

Update `TPCH_DATA` environment variable.

```shell
export TPCH_DATA=s3a://your-bucket-name
```

Install Hadoop jar files:

```shell
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -P $SPARK_HOME/jars
```

Add credentials to `~/.aws/credentials`:

```shell
[default]
aws_access_key_id=your-access-key
aws_secret_access_key=your-secret-key
```

Modify the scripts to add the following configurations.

```shell
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
```

Now run the `spark-tpch.sh` and `comet-tpch.sh` scripts.
