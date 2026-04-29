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

# Operator Compatibility

## Window Functions

Comet's support for window functions is incomplete and known to be incorrect. It is disabled by default and
should not be used in production. The feature will be enabled in a future release. Tracking issue: [#2721](https://github.com/apache/datafusion-comet/issues/2721).

## Round-Robin Partitioning

Comet's native shuffle implementation of round-robin partitioning (`df.repartition(n)`) is not compatible with
Spark's implementation and is disabled by default. It can be enabled by setting
`spark.comet.native.shuffle.partitioning.roundrobin.enabled=true`.

**Why the incompatibility exists:**

Spark's round-robin partitioning sorts rows by their binary `UnsafeRow` representation before assigning them to
partitions. This ensures deterministic output for fault tolerance (task retries produce identical results).
Comet uses Arrow format internally, which has a completely different binary layout than `UnsafeRow`, making it
impossible to match Spark's exact partition assignments.

**Comet's approach:**

Instead of true round-robin assignment, Comet implements round-robin as hash partitioning on ALL columns. This
achieves the same semantic goals:

- **Even distribution**: Rows are distributed evenly across partitions (as long as the hash varies sufficiently -
  in some cases there could be skew)
- **Deterministic**: Same input always produces the same partition assignments (important for fault tolerance)
- **No semantic grouping**: Unlike hash partitioning on specific columns, this doesn't group related rows together

The only difference is that Comet's partition assignments will differ from Spark's. When results are sorted,
they will be identical to Spark. Unsorted results may have different row ordering.
