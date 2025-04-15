#!/bin/bash

export COMET_JAR=`pwd`/../../spark/target/comet-spark-spark3.5_2.12-0.7.0-SNAPSHOT.jar

export SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3/
export SPARK_MASTER=spark://woody:7077


$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.executor.memory=8g \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=8g \
    --conf spark.comet.explain.verbose.enabled=false \
    --conf spark.comet.explainFallback.enabled=false \
    --conf spark.comet.exec.sortMergeJoinWithJoinFilter.enabled=false \
    --conf spark.comet.exec.replaceSortMergeJoin=true \
    --conf spark.comet.scan.enabled=true \
    --conf spark.comet.explain.native.enabled=false \
    --conf spark.comet.scan.impl=native_comet \
    --conf spark.eventLog.enabled=true \
    --jars $COMET_JAR \
    --driver-class-path $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.batchSize=8192 \
    --conf spark.comet.columnar.shuffle.batch.size=8192 \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.comet.exec.shuffle.mode=auto \
    --conf spark.comet.exec.shuffle.enableFastEncoding=true \
    --conf spark.comet.exec.shuffle.fallbackToColumnar=true \
    --conf spark.comet.cast.allowIncompatible=true \
    --conf spark.comet.exec.shuffle.compression.codec=lz4 \
    --conf spark.comet.exec.shuffle.compression.level=1 \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    tpcbench.py \
    --name comet \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf100/ \
    --queries ../../tpch \
    --output . \
    --iterations 1
