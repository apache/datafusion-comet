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

# Comet Benchmarking in AWS

This guide is for setting up benchmarks on AWS EC2 with a single node with Parquet files located in S3.

## Data Generation

- Create an EC2 instance with an EBS volume sized for approximately 2x the size of
  the dataset to be generated (200 GB for scale factor 100, 2 TB for scale factor 1000, and so on)
- Create an S3 bucket to store the Parquet files

Install prerequisites:

```shell
sudo yum install -y docker git python3-pip

sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
newgrp docker

docker pull ghcr.io/scalytics/tpch-docker:main

pip3 install datafusion
```

Run the data generation script:

```shell
git clone https://github.com/apache/datafusion-benchmarks.git
cd datafusion-benchmarks/tpch
nohup python3 tpchgen.py generate --scale-factor 100 --partitions 16 &
```

Check on progress with the following commands:

```shell
docker ps
du -h -d 1 data
```

Fix ownership in the generated files:

```shell
sudo chown -R ec2-user:docker data
```

Convert to Parquet:

```shell
nohup python3 tpchgen.py convert --scale-factor 100 --partitions 16 &
```

Delete the CSV files:

```shell
cd data
rm *.tbl.*
```

Copy the Parquet files to S3:

```shell
aws s3 cp . s3://your-bucket-name/top-level-folder/ --recursive
```

## Install Spark

Install Java

```shell
sudo yum install -y java-17-amazon-corretto-headless java-17-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
```

Install Spark

```shell
wget https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
tar xzf spark-3.5.4-bin-hadoop3.tgz
sudo mv spark-3.5.4-bin-hadoop3 /opt
export SPARK_HOME=/opt/spark-3.5.4-bin-hadoop3/
export SPARK_MASTER=spark://172.31.34.87:7077
mkdir /tmp/spark-events
```

Set `SPARK_LOCAL_DIRS` to point to EBS volume

```shell
sudo mkdir /mnt/tmp
sudo chown 777 /mnt/tmp
mv $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
```

Add the following entry to `spark-env.sh`:

```shell
SPARK_LOCAL_DIRS=/mnt/tmp
```

Start Spark in standalone mode:

```shell
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
```

Install Hadoop jar files:

```shell
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -P $SPARK_HOME/jars
```

Add credentials to `~/.aws/credentials`:

```shell
`[default]
aws_access_key_id=your-access-key
aws_secret_access_key=your-secret-key`
```

## Run Spark Benchmarks

Run the following command (the `--data` parameter will need to be updated to point to your S3 bucket):

```shell
$SPARK_HOME/bin/spark-submit \
  --master $SPARK_MASTER \
  --conf spark.driver.memory=4G \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=8 \
  --conf spark.cores.max=8 \
  --conf spark.executor.memory=16g \
  --conf spark.eventLog.enabled=false \
  --conf spark.local.dir=/mnt/tmp \
  --conf spark.driver.extraJavaOptions="-Djava.io.tmpdir=/mnt/tmp" \
  --conf spark.executor.extraJavaOptions="-Djava.io.tmpdir=/mnt/tmp" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  tpcbench.py \
  --benchmark tpch \
  --data s3a://your-bucket-name/top-level-folder \
  --queries /home/ec2-user/datafusion-benchmarks/tpch/queries \
  --output . \
  --iterations 1
```

## Run Comet Benchmarks

Install Comet JAR from Maven:

```shell
wget https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.12/0.7.0/comet-spark-spark3.5_2.12-0.7.0.jar -P $SPARK_HOME/jars
export COMET_JAR=$SPARK_HOME/jars/comet-spark-spark3.5_2.12-0.7.0.jar
```

Run the following command (the `--data` parameter will need to be updated to point to your S3 bucket):

```shell
$SPARK_HOME/bin/spark-submit \
  --master $SPARK_MASTER \
  --conf spark.driver.memory=4G \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=8 \
  --conf spark.cores.max=8 \
  --conf spark.executor.memory=16g \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=16g \
  --conf spark.eventLog.enabled=false \
  --conf spark.local.dir=/mnt/tmp \
  --conf spark.driver.extraJavaOptions="-Djava.io.tmpdir=/mnt/tmp" \
  --conf spark.executor.extraJavaOptions="-Djava.io.tmpdir=/mnt/tmp" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --jars $COMET_JAR \
  --driver-class-path $COMET_JAR \
  --conf spark.driver.extraClassPath=$COMET_JAR \
  --conf spark.executor.extraClassPath=$COMET_JAR \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.comet.enabled=true \
  --conf spark.comet.cast.allowIncompatible=true \
  --conf spark.comet.exec.replaceSortMergeJoin=true \
  --conf spark.comet.exec.shuffle.enabled=true \
  --conf spark.comet.exec.shuffle.fallbackToColumnar=true \
  --conf spark.comet.exec.shuffle.compression.codec=lz4 \
  --conf spark.comet.exec.shuffle.compression.level=1 \
  tpcbench.py \
  --benchmark tpch \
  --data s3a://your-bucket-name/top-level-folder \
  --queries /home/ec2-user/datafusion-benchmarks/tpch/queries \
  --output . \
  --iterations 1
```
