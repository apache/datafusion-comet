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

# In-Memory Cache

Comet can store Spark's in-memory cache (`CACHE TABLE`, `df.cache()`, `df.persist()`) in an Arrow
format that Comet operators read directly. Without it, a cached table is stored in Spark's own
format and every scan of it has to convert each batch before Comet can continue, which shows up in
the plan as a `CometSparkColumnarToColumnar` above the cache scan.

This feature is **experimental and disabled by default**.

```scala
spark.conf.set("spark.comet.exec.inMemoryCache.enabled", "true")
```

## What changes when it is enabled

`spark.comet.exec.inMemoryCache.enabled` is read at startup, and its value then decides whether
Comet installs its cache serializer as `spark.sql.cache.serializer`. When it is installed:

- Cached data is stored as `CometCachedBatch` rather than Spark's `DefaultCachedBatch`.
- Cached tables are scanned by `CometInMemoryTableScan`, which feeds Comet operators directly.
- Per-batch column statistics are recorded in the layout Spark's `SimpleMetricsCachedBatchSerializer`
  expects, so Spark can prune whole cached batches on a predicate before any of them is decoded.

Relations whose schema Comet's Arrow writer cannot store — interval types, most notably — are
delegated in full to Spark's default cache format, per relation. Nothing about the format depends
on a runtime config, because `spark.sql.cache.serializer` is a static setting and a relation whose
format could change mid-session could not be read back reliably. Turning
`spark.comet.exec.inMemoryCache.enabled` off at runtime only sends cached scans back to Spark's
execution path; the cached data stays readable either way.

## Storage format

Each cached batch is stored as a single Arrow IPC record batch message and its body.

The message carries **no Arrow schema**. The reader already has one: `InMemoryRelation` knows the
cached relation's attributes, and Comet maps them to exactly the Arrow fields the writer produced.
Storing a schema in every batch would repeat the same bytes once per cached batch — for a wide
relation cached in many batches, a large share of a payload that is not data.

Compression is applied by Arrow to **each buffer separately**, rather than by wrapping the whole
payload in a Spark compression codec. That is what makes a projected read cheap: the message
metadata records every buffer's offset and length within the body, so a scan copies out only the
byte ranges belonging to the columns it selected, and only those are decompressed. A read of one
column out of six does roughly a sixth of the decompression work, and a `SELECT count(*)`, which
selects no columns at all, answers from the row count stored beside the payload without touching
it.

Compression defaults to `zstd`, which is faster than storing cached batches uncompressed: the
bytes it saves cost more to copy and store than compressing them costs. Measured over a 200k-row,
six-column relation:

| Codec  | Materialize | Footprint | Read 1 of 6 | Read 6 of 6 |
| ------ | ----------: | --------: | ----------: | ----------: |
| `zstd` |      363 ms |     2 MiB |       56 ms |       62 ms |
| `none` |     1776 ms |    13 MiB |       78 ms |       81 ms |

Arrow's other IPC codec, LZ4, is deliberately not offered. It is commons-compress's pure-Java
implementation and is unrelated to the JNI-accelerated lz4-java behind `spark.io.compression.codec`;
it measured three orders of magnitude slower to write than `zstd` while also producing larger
output, so no workload prefers it.

Dictionary-encoded columns are decoded before they are stored. A payload with no schema message has
nowhere to record either that a column is dictionary encoded or the dictionary itself.

## Configuration

| Config                                                  | Default | Description                                                                                                                                    |
| ------------------------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.comet.exec.inMemoryCache.enabled`                | `false` | Whether to store and scan Spark's in-memory cache in Comet's format. Read at startup.                                                          |
| `spark.comet.exec.inMemoryCache.compression.codec`      | `zstd`  | Arrow IPC compression codec for cached data: `zstd` or `none`. Affects newly cached data only — a batch records the codec it was written with. |
| `spark.comet.exec.inMemoryCache.compression.zstd.level` | `1`     | Compression level when the codec is `zstd`. Ignored otherwise.                                                                                 |

## Performance

Measured with `CometInMemoryCacheBenchmark` (Apple M3 Max, JDK 17, Spark 4.1, release build).
Regenerate with:

```sh
SPARK_GENERATE_BENCHMARK_FILES=1 \
  make benchmark-org.apache.spark.sql.benchmark.CometInMemoryCacheBenchmark
```

On a 5M-row relation of six flat columns:

| Query shape                    | Spark cache scan + convert | `CometInMemoryTableScan` | Relative |
| ------------------------------ | -------------------------: | -----------------------: | -------: |
| Repeated scan (3 of 6 columns) |                     201 ms |                   167 ms |     1.2x |
| Selective filter               |                      69 ms |                    61 ms |     1.1x |
| Row count only (0 of 6)        |                      45 ms |                    47 ms |     1.0x |
| Narrow projection (1 of 6)     |                      70 ms |                    57 ms |     1.2x |
| Full projection (6 of 6)       |                     556 ms |                   290 ms |     1.9x |

And on a 1M-row relation of six columns whose middle three are structs, one of them nested two
levels deep:

| Query shape                | Spark cache scan + convert | `CometInMemoryTableScan` | Relative |
| -------------------------- | -------------------------: | -----------------------: | -------: |
| Row count only (0 of 6)    |                      39 ms |                    35 ms |     1.1x |
| Narrow projection (1 of 6) |                     109 ms |                    61 ms |     1.8x |
| Full projection (6 of 6)   |                     282 ms |                   126 ms |     2.2x |

The two relations are not comparable to each other — different row counts, and a struct column
carries several values per row. Within the struct relation the gap is wider than the flat one at
every width, because the conversion the left column pays scales with the values per row rather than
with the columns.

Array and map columns are deliberately absent from the benchmark, not from the format — the cache
stores and projects them, and `CometInMemoryCacheSuite` covers them. They cannot be measured _here_
because the left column would not exist: it needs Spark's cache scan to bridge into Comet operators,
and `CometSparkToColumnarExec` declines `ArrayType` and `MapType`, so a query projecting one falls
back to Spark row execution above the scan and the two columns stop measuring the same boundary.

Read what this compares carefully. Comet execution is on in both columns, so the aggregation runs
on Comet either way and only the cache-scan boundary moves: on the left, Spark's
`InMemoryTableScanExec` feeds those same Comet operators through a `CometSparkColumnarToColumnar`
bridge; on the right, `CometInMemoryTableScan` feeds them directly. Both columns read the same
Comet-written `CometCachedBatch` — `spark.sql.cache.serializer` is static, so one session cannot
also materialize Spark's format to compare against. These numbers are therefore "keep the cached
scan native" against "fall back to a Spark cache scan and convert", not Comet against Spark
execution, and not a comparison with Spark's own cache format.

## Kryo

Spark serializes a cached batch with `spark.serializer` whenever the block leaves the heap: the
`_SER` storage levels, replication, cross-executor fetches, and the disk half of the default
`MEMORY_AND_DISK`. So an ordinary `df.cache()` that spills is enough to reach it.

If you run with `spark.kryo.registrationRequired=true`, register Comet's classes:

```
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryo.registrationRequired=true
spark.kryo.registrator=org.apache.comet.CometKryoRegistrator
```

Comet cannot set `spark.kryo.registrator` for you the way it sets `spark.sql.cache.serializer`:
`KryoSerializer` reads it when `SparkEnv` builds the serializer, which happens before any plugin
runs. Without it, caching fails with a "Class is not registered" error that does not name this
feature. Comet's driver plugin warns at startup when it sees Kryo, `registrationRequired`, and no
registrator.

## Limitations

Reads that feed **Spark** operators rather than Comet ones are still slower than Spark's own cache
format, by roughly 1.7x to 2.5x depending on how wide the projection is. Those reads pay a row
conversion that Spark's format avoids with generated code over its own layout. This is why the
feature is off by default.

Comet's serializer exists because Spark's own Arrow cache format
([SPARK-57268](https://issues.apache.org/jira/browse/SPARK-57268)) is only available from Spark
4.3, which Comet does not yet support.
