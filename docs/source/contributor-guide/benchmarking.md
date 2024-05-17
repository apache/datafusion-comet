# Comet Benchmarking Guide

To track progress on performance, we regularly run benchmarks derived from TPC-H and TPC-DS. Benchmarking scripts are
available in the [DataFusion Benchmarks](https://github.com/apache/datafusion-benchmarks) GitHub repository.

Here is an example command for running the benchmarks. This command will need to be adapted based on the Spark 
environment and location of data files.

This command assumes that `datafusion-benchmarks` is checked out in a parallel directory to `datafusion-comet`.

```shell
$SPARK_HOME/bin/spark-submit \ 
    --master "local[*]" \ 
    --conf spark.driver.memory=8G \ 
    --conf spark.executor.memory=64G \ 
    --conf spark.executor.cores=16 \ 
    --conf spark.cores.max=16 \ 
    --conf spark.eventLog.enabled=true \ 
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \ 
    --jars $COMET_JAR \ 
    --conf spark.driver.extraClassPath=$COMET_JAR \ 
    --conf spark.executor.extraClassPath=$COMET_JAR \ 
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \ 
    --conf spark.comet.enabled=true \ 
    --conf spark.comet.exec.enabled=true \ 
    --conf spark.comet.exec.all.enabled=true \ 
    --conf spark.comet.cast.allowIncompatible=true \ 
    --conf spark.comet.explainFallback.enabled=true \ 
    --conf spark.comet.parquet.io.enabled=false \ 
    --conf spark.comet.batchSize=8192 \ 
    --conf spark.comet.columnar.shuffle.enabled=false \ 
    --conf spark.comet.exec.shuffle.enabled=true \ 
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \ 
    --conf spark.sql.adaptive.coalescePartitions.enabled=false \ 
    --conf spark.comet.shuffle.enforceMode.enabled=true \
    ../datafusion-benchmarks/runners/datafusion-comet/tpcbench.py \
    --benchmark tpch \ 
    --data /mnt/bigdata/tpch/sf100-parquet/ \ 
    --queries ../datafusion-benchmarks/tpch/queries 
```

Comet performance can be compared to regular Spark performance by running the benchmark twice, once with 
`spark.comet.enabled` set to `true` and once with it set to `false`. 