#!/bin/bash

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
    tpcbench.py \
    --name spark \
    --benchmark tpch \
    --data /home/ec2-user/ \
    --queries ../../tpch \
    --output . \
    --iterations 1
