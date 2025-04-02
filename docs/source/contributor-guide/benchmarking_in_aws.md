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

This guide is for setting up benchmarks on AWS EC2 single node with Parquet files located in S3.

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

Fix ownership n the generated files:

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

```shell
wget https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
tar xzf spark-3.5.4-bin-hadoop3.tgz
sudo mv spark-3.5.4-bin-hadoop3 /opt
export SPARK_HOME=/opt/spark-3.5.4-bin-hadoop3/

sudo yum install -y java-17-amazon-corretto-headless
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64

export SPARK_MASTER=spark://172.31.34.87:7077
mkdir /tmp/spark-events

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER

```

Run benchmarks against local data:

```shell
cd ~/datafusion-benchmarks/runners/datafusion-comet

$SPARK_HOME/bin/spark-submit \
  --master $SPARK_MASTER \
  --conf spark.driver.memory=4G \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=8 \
  --conf spark.cores.max=8 \
  --conf spark.executor.memory=8g \
  --conf spark.eventLog.enabled=true \
  tpcbench.py \
  --benchmark tpch \
  --data /home/ec2-user/datafusion-benchmarks/tpch/data \
  --queries /home/ec2-user/datafusion-benchmarks/tpch/queries \
  --output . \
  --iterations 1
```

Run benchmarks against S3:

TBD

## Comet

TBD
