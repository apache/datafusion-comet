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

# Spark Version Compatibility

Comet supports Apache Spark 3.4, 3.5, and 4.0. This page documents differences
between supported Spark versions. For per-feature compatibility notes that
apply across versions, see the other pages in this guide.

## Spark 3.4 and 3.5

Spark 3.4 and 3.5 are the most thoroughly exercised versions in CI and are
suitable for production use. There are no version-specific gaps to call out
beyond the per-feature compatibility notes in the rest of this guide.

## Spark 4.0

Comet works with Spark 4.0 and runs the full Comet test suite plus a subset of
the Spark SQL test suite in CI. Most workloads that run on Spark 3.5 will run
on Spark 4.0 with the same level of acceleration. The known gaps below are
specific to Spark 4.0 features that Comet has not yet implemented.

### ANSI mode

Spark 4.0 enables ANSI mode by default. Comet has good coverage for ANSI
behavior across arithmetic, aggregates, and most cast expressions. A few
`Cast` source/target combinations still fall back to Spark in ANSI mode;
see the [Cast compatibility](expressions/cast.md) page for details.

### Non-default collations

Spark 4.0 adds collation support for `StringType` columns. Comet's native
hashing, equality, and ordering compare raw UTF-8 bytes, which would put
rows that compare equal under a non-default collation into different
groups, partitions, or sort positions. Operations on columns with a
non-default collation (group-by, distinct, sort, join keys, hash and
range partitioning in shuffle) fall back to Spark. Columns using the
session-default collation are unaffected.

### V2 bucketing with partial cluster distribution

Spark 4.0's DataSource V2 bucketing supports partially clustered
distribution for storage-partitioned joins
(`spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled`).
Comet does not yet support this distribution and falls back to Spark for
the affected exchange.

### Variant type

The `VariantType` introduced in Spark 4.0 is not yet supported. Queries that
read or produce variant columns fall back to Spark.

### Parquet type widening

Spark 4.0 added broader support for reading Parquet files with widened types
(for example, reading an `INT32` column as `BIGINT`). Comet covers the common
integer and floating-point widenings but does not yet implement the full set
that Spark 4.0 supports, and unsupported widenings are not detected as a
fallback condition.

The underlying scan behavior, including silent acceptance of schema mismatches
that Spark would reject, is documented under
[`native_datafusion` Limitations](scans.md#native_datafusion-limitations) and
applies on all supported Spark versions. Spark 4.0 exercises more of these
paths because of its expanded widening rules.
