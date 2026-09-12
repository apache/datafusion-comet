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

# Delta Lake (experimental)

Comet can execute DSv1 Delta Lake table scans natively. Reads planned by
delta-spark run through Comet's native Parquet scan, inheriting row-group
pruning, page-index pruning, and filter pushdown, with deletion vectors
applied inside the scan.

Support is experimental and explicitly opt-in. Two things are required:

1. The `comet-contrib-delta-spark` contrib jar on the classpath, alongside
   `delta-spark`. It is never bundled into `comet-spark`.
2. `spark.comet.scan.delta.enabled=true`. The default is `false`, so
   the jar alone does nothing.

Unsupported tables and features fall back to Spark's reader. See the
[contrib module README](https://github.com/apache/datafusion-comet/blob/main/contrib/delta-spark/README.md)
for the supported Spark/Delta version matrix and build instructions.

Unlike the core native scan, the Delta scan resolves each data file's datetime
calendar-rebase policy from the file's own writer metadata
(`org.apache.spark.legacyDateTime` and friends), the same way Spark's reader
does, selecting the `datetimeRebaseModeInRead` spec for dates and INT64
timestamps and the `int96RebaseModeInRead` spec for INT96 timestamps, at any
nesting depth: dates written with the legacy hybrid Julian/Gregorian calendar
are rebased exactly, timestamps are rebased exactly when the file records a
fixed UTC writer time zone, and ancient values whose calendar cannot be
applied natively (non-UTC legacy writer zones, or files that do not declare a
policy under the `EXCEPTION` read mode) raise an error rather than silently
returning shifted values. Modern values are unaffected: dates from 1582-10-15
onward, and timestamps from 1900-01-01T00:00:00Z onward (Spark's own
rebase cutoff). Disable `spark.comet.scan.delta.enabled` for such tables to
read them through Spark.

## Configuration

<!--BEGIN:CONFIG_TABLE[delta]-->
<!-- prettier-ignore-start -->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.scan.delta.dv.maxDeletedRowsPerFile` | Upper bound on a single file's deletion-vector cardinality (deleted row count) the native Delta scan will claim. Applying a deletion vector expands it into per-row selectors that are retained in memory for the file's scan; this bound is a deliberately pessimistic planning-time proxy for that retained memory (deletion vector cardinality, not the exact selector count), so a large but contiguous deletion is declined the same as a large alternating one. Scans whose deletion vectors exceed this bound for any file fall back to Spark's reader. | 1000000 |
| `spark.comet.scan.delta.enabled` | Whether to enable native Delta table scans. When enabled, DSv1 Delta table reads planned by delta-spark are executed through Comet's native Parquet scan, inheriting row-group pruning, page-index pruning, and filter pushdown, with deletion vectors applied inside the scan. Experimental: defaults to false, so adding the contrib jar does not by itself change how any query is read. | false |
<!-- prettier-ignore-end -->
<!--END:CONFIG_TABLE-->
