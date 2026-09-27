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

# Scan Tuning

## Parquet Reader Tuning

### Filter Pushdown / Late Materialization

Setting `spark.comet.parquet.rowFilterPushdown.enabled=true` pushes filter evaluation into the Parquet
decode step and lazily materializes projected columns for surviving rows. This can significantly reduce
CPU and memory when the filter is highly selective on a small subset of columns. It is disabled by default
because it can hurt when the filter is not selective or when most columns must be read anyway. Row-group,
page-index, and bloom-filter pruning happen regardless of this flag whenever Spark's
`spark.sql.parquet.filterPushdown` is on.

### Parquet Native Scans

Spark and DataFusion's native Parquet scans use different rules to decide which row groups belong to a
given scan range (split). Spark assigns a row group to a split if the row group's start offset falls
within `[split.start, split.start + split.length)`, guaranteeing that every task Spark plans reads at
least one row group when the file layout permits. DataFusion's `prune_by_range` also checks whether a
row group's start offset falls within the split's byte range, but because row group sizes are not aligned
with Spark's split boundaries, the two systems can disagree on which split "owns" a given row group.

When a file contains row groups whose sizes are close to `spark.sql.files.maxPartitionBytes`, this
mismatch can leave some Comet scan tasks with no row groups to read. Those tasks still load Parquet
metadata but return zero rows, while neighboring tasks end up reading more row groups than Spark
intended. The overall effect is that Comet uses only a fraction of the parallelism that Spark planned
for the scan stage, and end-to-end scan latency increases even though the total amount of data read
is unchanged.

Symptoms to look for:

- A subset of scan tasks completes almost immediately and reports 0 input rows, while the remaining
  tasks read noticeably more rows than the equivalent Spark tasks would.
- The Comet scan stage has the same number of planned tasks as Spark but a much lower count of tasks
  that actually do work.

Workaround: lower `spark.sql.files.maxPartitionBytes` so that each split is smaller than a single row
group. For example, if the file's row groups are around 120 MB and `spark.sql.files.maxPartitionBytes`
is left at the 128 MB default, most splits will contain at most one row group boundary and the
mismatch is amplified; setting `spark.sql.files.maxPartitionBytes` below 120 MB (for example, 64 MB)
distributes row groups across more splits and reduces the number of idle tasks. Smaller values produce
more splits overall, so some idle tasks may remain — tune the value against your file layout.

See [#3817](https://github.com/apache/datafusion-comet/issues/3817#issuecomment-4193279630) for a
worked example and further discussion.

## Iceberg Scan Tuning

Comet's native Iceberg scan (`spark.comet.scan.icebergNative.enabled`, enabled by default) reads each
task's data files one at a time by default. For tables with many small files or high-latency storage,
increase `spark.comet.scan.icebergNative.dataFileConcurrencyLimit` (default `1`; values of 2–8 are
suggested) to overlap I/O across files at the cost of extra memory.
