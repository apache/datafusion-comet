<!---
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

# Tuning Guide

Comet provides some tuning options to help you get the best performance from your queries.

## Metrics

Comet metrics are not directly comparable to Spark metrics in some cases.

`CometScanExec` uses nanoseconds for total scan time. Spark also measures scan time in nanoseconds but converts to
milliseconds *per batch* which can result in a large loss of precision. In one case we saw total scan time
of 41 seconds reported as 23 seconds for example.

## Memory Tuning

Comet currently doesn't share the memory allocation from Spark but owns its own memory allocation.
That's said, Comet requires additional memory to be allocated. Comet provides some memory related configurations to help you tune the memory usage.

By default, the amount of memory is `spark.comet.memory.overhead.factor` * `spark.executor.memory`.
The default value for `spark.comet.memory.overhead.factor` is 0.2. You can increase the factor to require more
memory for Comet to use, if you see OOM error. Generally, increasing memory overhead will improve the performance of your queries.
For example, some operators like `SortMergeJoin` and `HashAggregate` may require more memory to run.
Once the memory is not enough, the operator will spill to disk, which will slow down the query.

Besides, you can also set the memory explicitly by setting `spark.comet.memoryOverhead` to the desired value.
Comet will allocate at least `spark.comet.memory.overhead.min` memory.

If both `spark.comet.memoryOverhead` and `spark.comet.memory.overhead.factor` are set, the former will be used.

## Memory Tuning using CometPlugin
Configuring memory for Spark and Comet might be a tedious task as it requires to tune Spark executor overhead memory and Comet memory overhead configs. Comet provides a Spark plugin `CometPlugin` which can be set up to your Spark application to help memory settings.

For users running the Comet in clusters like Kubernetes or YARN, `CometPlugin` can also make the resource manager respect correctly Comet memory parameters `spark.comet.memory*`. 
it is needed to pass to the starting command line additional Spark configuration parameter `--conf spark.plugins=org.apache.spark.CometPlugin`

The resource managers respects Apache Spark memory configuration before starting the containers.

The `CometPlugin` plugin overrides `spark.executor.memoryOverhead` adding up the Comet memory configuration.


## Shuffle

Comet provides Comet shuffle features that can be used to improve the performance of your queries.
The following sections describe the different shuffle options available in Comet.

To enable Comet shuffle, set the following configuration in your Spark configuration:

```
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
spark.comet.exec.shuffle.enabled=true
```

`spark.shuffle.manager` is a Spark static configuration which cannot be changed at runtime.
It must be set before the Spark context is created. You can enable or disable Comet shuffle
at runtime by setting `spark.comet.exec.shuffle.enabled` to `true` or `false`.
Once it is disabled, Comet will fallback to the default Spark shuffle manager.

> **_NOTE:_** At the moment Comet Shuffle is not compatible with Spark AQE partition coalesce. To disable set `spark.sql.adaptive.coalescePartitions.enabled` to `false`.

### Shuffle Mode

Comet provides three shuffle modes: Columnar Shuffle, Native Shuffle and Auto Mode.

#### Columnar Shuffle

By default, once `spark.comet.exec.shuffle.enabled` is enabled, Comet uses JVM-based columnar shuffle
to improve the performance of shuffle operations. Columnar shuffle supports HashPartitioning,
RoundRobinPartitioning, RangePartitioning and SinglePartitioning. This mode has the highest
query coverage.

Columnar shuffle can be enabled by setting `spark.comet.exec.shuffle.mode` to `jvm`.

#### Native Shuffle

Comet also provides a fully native shuffle implementation that can be used to improve the performance.
To enable native shuffle, just set `spark.comet.exec.shuffle.mode` to `native`

Native shuffle only supports HashPartitioning and SinglePartitioning.

### Auto Mode

`spark.comet.exec.shuffle.mode` to `auto` will let Comet choose the best shuffle mode based on the query plan.
