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

$SPARK_HOME/sbin/stop-master.sh
$SPARK_HOME/sbin/stop-worker.sh

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER

$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --jars $COMET_JAR \
    --driver-class-path $COMET_JAR \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.executor.memory=16g \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16g \
    --conf spark.eventLog.enabled=true \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.exec.replaceSortMergeJoin=true \
    --conf spark.comet.expression.allowIncompatible=true \
    --conf spark.comet.scan.impl=native_datafusion \
    tpcbench.py \
    --name comet \
    --benchmark tpch \
    --data $TPCH_DATA \
    --queries $TPCH_QUERIES \
    --output . \
    --iterations 1
