#!/bin/bash

export COMET_JAR=`pwd`/../../spark/target/comet-spark-spark3.5_2.12-0.8.0-SNAPSHOT.jar

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
    --conf spark.comet.exec.replaceSortMergeJoin=true \
    --jars $COMET_JAR \
    --driver-class-path $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.shuffle.enableFastEncoding=true \
    --conf spark.comet.exec.shuffle.fallbackToColumnar=true \
    --conf spark.comet.cast.allowIncompatible=true \
    tpcbench.py \
    --name comet \
    --benchmark tpch \
    --data /home/ec2-user/ \
    --queries /home/ec2-user/datafusion-benchmarks/tpch/queries \
    --output . \
    --iterations 1
