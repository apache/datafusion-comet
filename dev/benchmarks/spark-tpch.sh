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

export SPARK_MASTER=spark://localhost:7077

$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.executor.memory=8g \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=8g \
    --conf spark.local.dir=/home/ec2-user/tmp \
    --conf spark.driver.extraJavaOptions="-Djava.io.tmpdir=/home/ec2-user/tmp" \
    --conf spark.executor.extraJavaOptions="-Djava.io.tmpdir=/home/ec2-user/tmp" \
    tpcbench.py \
    --name spark \
    --benchmark tpch \
    --data /home/ec2-user/ \
    --queries /home/ec2-user/datafusion-benchmarks/tpch/queries \
    --output . \
    --iterations 1
