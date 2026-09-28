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

This feature is **experimental and disabled by default**. Turn it on at startup, alongside the rest
of Comet's configuration:

```shell
$SPARK_HOME/bin/spark-shell \
    ... \
    --conf spark.comet.exec.inMemoryCache.enabled=true
```

It has to be set before the `SparkContext` starts. Comet's driver plugin chooses
`spark.sql.cache.serializer` once, while the context is initializing, so a session that started
with the default goes on using Spark's cache format however the config is set afterwards.

## What changes when it is enabled

With Comet's serializer installed as `spark.sql.cache.serializer`:

- Cached data is stored as `CometCachedBatch` rather than Spark's `DefaultCachedBatch`.
- Cached tables are scanned by `CometInMemoryTableScan`, which feeds Comet operators directly.
- Per-batch column statistics are recorded in the layout Spark's `SimpleMetricsCachedBatchSerializer`
  expects, so Spark can prune whole cached batches on a predicate before any of them is decoded.

Relations whose schema Comet's Arrow writer cannot store — interval types, most notably — are
delegated in full to Spark's default cache format, per relation. Which format a relation uses does
not depend on a runtime config, because `spark.sql.cache.serializer` is a static setting and a
relation whose format could change mid-session could not be read back reliably. The compression
codec is a runtime config, but each batch records the codec it was written with, so data cached
under one setting stays readable after the setting changes. Turning
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

Compression defaults to `zstd`, for footprint rather than for speed. Over the same 5M-row,
six-column relation the tables under [Performance](#performance) use — and measured by the same
benchmark — it holds the data in a sixth of the memory and pays for that on both sides: about 40%
longer to materialize, and, on a read wide enough to inflate everything, close to five times
longer. A narrow projection pays far less, because it only inflates the columns it asked for.

| Codec  | Materialize | Footprint | Read 1 of 6 | Read 6 of 6 |
| ------ | ----------: | --------: | ----------: | ----------: |
| `zstd` |     1507 ms |    55 MiB |       45 ms |      295 ms |
| `none` |     1081 ms |   315 MiB |       35 ms |       64 ms |

`none` is the better setting for a relation that fits in memory uncompressed and is read at close
to full width. The default is the other way round because a cache that does not fit costs more than
one that is slower to read, and Spark's own cache format compresses by default too.

Arrow's other IPC codec, LZ4, is deliberately not offered and the config rejects it. It is
commons-compress's pure-Java implementation, unrelated to the JNI-accelerated lz4-java behind
`spark.io.compression.codec`, and is orders of magnitude slower to write than `zstd` while also
producing larger output.

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
| Repeated scan (3 of 6 columns) |                     209 ms |                   172 ms |     1.2x |
| Selective filter               |                      72 ms |                    61 ms |     1.2x |
| Row count only (0 of 6)        |                      46 ms |                    38 ms |     1.2x |
| Narrow projection (1 of 6)     |                      70 ms |                    58 ms |     1.2x |
| Full projection (6 of 6)       |                     566 ms |                   324 ms |     1.7x |

And on a 1M-row relation of six columns whose middle three are structs, one of them nested two
levels deep:

| Query shape                | Spark cache scan + convert | `CometInMemoryTableScan` | Relative |
| -------------------------- | -------------------------: | -----------------------: | -------: |
| Row count only (0 of 6)    |                      38 ms |                    32 ms |     1.2x |
| Narrow projection (1 of 6) |                     109 ms |                    58 ms |     1.9x |
| Full projection (6 of 6)   |                     275 ms |                   130 ms |     2.1x |

Both columns read the cache at the default codec, `zstd`. The codec table above shows what `none`
changes, and it is the full projection that moves most: nothing has to be inflated, so it runs
several times faster, at six times the memory.

The two relations are not comparable to each other — different row counts, and a struct column
carries several values per row.

Array and map columns are deliberately absent from the benchmark, not from the format — the cache
stores and projects them, and `CometInMemoryCacheSuite` covers them. They cannot be measured _here_
because the left column would not exist: it needs Spark's cache scan to bridge into Comet operators,
and `CometSparkToColumnarExec` declines `ArrayType` and `MapType`, so a query projecting one falls
back to Spark row execution above the scan and the two columns stop measuring the same boundary.

Read what this compares carefully. Comet execution is on in both columns, so the aggregation runs
on Comet either way and only the cache-scan boundary moves: on the left, Spark's
`InMemoryTableScanExec` feeds those same Comet operators through a `CometSparkColumnarToColumnar`
bridge; on the right, `CometInMemoryTableScan` feeds them directly. Both columns read the same
Comet-written `CometCachedBatch`. These numbers are therefore "keep the cached scan native" against
"fall back to a Spark cache scan and convert", not Comet against Spark execution, and not a
comparison with Spark's own cache format. That comparison is under [Limitations](#limitations).

## Kryo

Spark serializes a cached batch with `spark.serializer` whenever the block leaves the heap: the
`_SER` storage levels, replication, cross-executor fetches, and the disk half of the default
`MEMORY_AND_DISK`. So an ordinary `df.cache()` that spills is enough to reach it.

If you run Kryo with `spark.kryo.registrationRequired=true`, register Comet's classes:

```
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryo.registrationRequired=true
spark.kryo.registrator=org.apache.comet.CometKryoRegistrator
```

Comet cannot set `spark.kryo.registrator` for you the way it sets `spark.sql.cache.serializer`:
`KryoSerializer` reads it when `SparkEnv` builds the serializer, which happens before any plugin
runs. Without it, caching fails with a "Class is not registered" error that does not name this
feature. Comet's driver plugin warns at startup when it sees Kryo, `registrationRequired`, and no
registrator. Native broadcast needs the same registrator even when the cache is disabled; see
[Kryo serialization](installation.md#kryo-serialization).

## Limitations

Reads that feed **Spark** operators rather than Comet ones are slower than Spark's own cache
format, and the narrower the read, the wider the gap. Measured by the same benchmark over the same
5M-row relation, with Comet off so that Spark operators consume the cached data:

| Read shape              | Spark's cache format | Comet's cache format | Slowdown |
| ----------------------- | -------------------: | -------------------: | -------: |
| Row count only (0 of 6) |                35 ms |               183 ms |     5.2x |
| 1 of 6 columns          |                54 ms |               257 ms |     4.8x |
| 3 of 6 columns          |                98 ms |               331 ms |     3.4x |
| 6 of 6 columns          |               410 ms |               623 ms |     1.5x |

This is why the feature is off by default. The cause is not yet established;
[#5485](https://github.com/apache/datafusion-comet/issues/5485) tracks it.

Comet's serializer exists because Spark's own Arrow cache format
([SPARK-57268](https://issues.apache.org/jira/browse/SPARK-57268)) is only available from Spark
4.3, which Comet does not yet support.
