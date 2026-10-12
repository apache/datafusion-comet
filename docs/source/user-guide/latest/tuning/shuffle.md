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

# Shuffle Tuning

Comet provides accelerated shuffle implementations that can be used to improve the performance of your queries.

To enable Comet shuffle, set the following configuration in your Spark configuration:

```
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
spark.comet.shuffle.enabled=true
```

`spark.shuffle.manager` is a Spark static configuration which cannot be changed at runtime.
It must be set before the Spark context is created. You can enable or disable Comet shuffle
at runtime by setting `spark.comet.shuffle.enabled` to `true` or `false`.
Once it is disabled, the configured shuffle manager handles ordinary Spark shuffle dependencies
without Comet's shuffle implementation.

Applications that use Apache Celeborn set a different shuffle manager. See
[Celeborn guide](../celeborn.md).

## Shuffle Implementations

Comet provides two shuffle implementations: Native Shuffle and Columnar Shuffle. Comet will first try to use Native
Shuffle and if that is not possible it will try to use Columnar Shuffle. If neither can be applied, it will fall
back to Spark for shuffle operations.

### Native Shuffle

Comet provides a fully native shuffle implementation, which generally provides the best performance. Native shuffle
supports `HashPartitioning`, `RangePartitioning`, and `SinglePartition`, plus `RoundRobinPartitioning` when enabled
(see [Round-Robin Partitioning](../compatibility/operators.md#round-robin-partitioning)). Range partitioning keys must be
scalar types. Hash partitioning keys must be scalar types unless
`spark.comet.shuffle.native.partitioning.hash.nested.enabled=true`, which also admits struct, array, and (Spark 4.0
and later) map keys. That setting is disabled by default until the performance of the nested hashing paths has been
measured. Columns that are not partitioning keys may contain complex types like maps, structs, and arrays.

### Columnar (JVM) Shuffle

Comet Columnar shuffle is JVM-based and supports `HashPartitioning`, `RoundRobinPartitioning`, `RangePartitioning`, and
`SinglePartition`. This shuffle implementation supports complex data types as partitioning keys.

By default, Comet will convert a Spark `ShuffleExchangeExec` to columnar shuffle even when the shuffle's child is a
non-Comet (Spark) plan. The benefit is that the next query stage can start as native Comet execution, since the
shuffle output is already in Arrow format. The cost is a row to columnar conversion at the shuffle boundary on the
write side. To restrict columnar shuffle to cases where the child is already a Comet plan, set
`spark.comet.shuffle.convertFromSparkPlan.enabled=false`. Shuffles whose child is a Spark plan will then be left
as native Spark shuffles, which avoids the row to columnar conversion but means the downstream stage will also start
on Spark.

Setting `spark.comet.convert.shuffleInput.enabled=true` makes such a shuffle use native shuffle instead. Comet
converts the child's rows to Arrow, as the `spark.comet.convert.*` settings do for Spark scans, and shuffles the
batches natively. With hash partitioning that is usually faster than columnar shuffle, while with range partitioning
the two come out about even. This applies only where native shuffle supports the partitioning and Comet can convert
all of the child's columns, which rules out calendar intervals, and arrays and maps other than `array<string>` and
`map<string,string>`. A shuffle that hashes a decimal wider than 18 digits, or a string or a value computed from a
string, stays on columnar shuffle, so that it partitions rows as Spark does. The converted batches are allocated on
the JVM outside Comet's memory pools. The setting is disabled by default.

### Automatic Revert to Spark Shuffle

When a Comet columnar shuffle, or a native shuffle over rows that `spark.comet.convert.shuffleInput.enabled` converted,
ends up between a partial and a final aggregate that Comet could not convert (both remain Spark `HashAggregateExec`,
`ObjectHashAggregateExec` or `SortAggregateExec` operators), Comet reverts it to Spark's built-in shuffle.
Keeping columnar shuffle between the two row-based aggregates would add `row -> Arrow -> shuffle -> Arrow -> row`
conversions with no Comet consumer on either side to benefit from columnar output. The final aggregate has to read the
shuffle directly, which a `SortAggregateExec` with grouping keys does not: it reads the shuffle through a sort, so that
shuffle is not reverted. Other shuffles between non-Comet operators are not reverted either.

This shifts the affected shuffles from Comet's off-heap memory pool back to the JVM execution memory pool. Clusters
tuned for a small JVM heap may see `ExternalSorter` spills on queries where this revert fires. Shuffle I/O may also
grow marginally because Spark's row-based serializer generally compresses less well than Comet's Arrow IPC format.

Each revert is logged at `INFO` level on the driver as `Reverting Comet shuffle to Spark shuffle between <parent>
and <child>`, which lets you correlate any unexpected behavior with this optimization.

This optimization is enabled by default and can be disabled by setting
`spark.comet.shuffle.revertRedundantColumnar.enabled=false`, in which case Comet will keep its shuffle
even when both of those aggregates run on Spark.

## Shuffle Compression

`spark.comet.shuffle.compression.codec` controls the codec used to compress shuffle data written by
both Comet's native shuffle and the JVM columnar shuffle writer. Supported values are `lz4` (default),
`zstd`, and `snappy`. LZ4 favors CPU efficiency; ZSTD produces smaller shuffle files at higher CPU cost —
useful when shuffle I/O or network bandwidth is the bottleneck. When ZSTD is selected, the level is
controlled by `spark.comet.shuffle.compression.zstd.level` (default `1`).

`spark.shuffle.compress=false` disables compression for Comet's native shuffle only. It has no effect on
the JVM columnar shuffle writer, which always compresses spill files with the codec above. Disabling
compression on the native path may result in faster shuffle times in certain environments, such as
single-node setups with fast NVMe drives, at the expense of increased disk space usage.
