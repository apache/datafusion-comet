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

### Variant type

The `VariantType` introduced in Spark 4.0 is not yet supported. Queries that
read or produce variant columns fall back to Spark.

### Parquet type widening

Spark 4.0 added broader support for reading Parquet files with widened types
(for example, reading an `INT32` column as `BIGINT`). Comet supports the
common integer widenings but does not yet cover the full set that Spark
supports. Unsupported widenings are not detected as a fallback condition,
so queries that rely on them may fail or return incorrect results rather
than transparently falling back to Spark.
