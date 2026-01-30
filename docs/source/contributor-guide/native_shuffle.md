<!--
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

# Native Shuffle

This document describes Comet's native shuffle implementation (`CometNativeShuffle`), which performs
shuffle operations entirely in Rust code for maximum performance. For the JVM-based alternative,
see [JVM Shuffle](jvm_shuffle.md).

## Overview

Native shuffle takes columnar input directly from Comet native operators and performs partitioning,
encoding, and writing in native Rust code. This avoids the columnar-to-row-to-columnar conversion
overhead that JVM shuffle incurs.

```
Comet Native (columnar) → Native Shuffle → Arrow IPC → columnar
```

Compare this to JVM shuffle's data path:

```
Comet Native (columnar) → ColumnarToRowExec → rows → JVM Shuffle → Arrow IPC → columnar
```

## When Native Shuffle is Used

Native shuffle (`CometExchange`) is selected when all of the following conditions are met:

1. **Shuffle mode allows native**: `spark.comet.exec.shuffle.mode` is `native` or `auto`.

2. **Child plan is a Comet native operator**: The child must be a `CometPlan` that produces
   columnar output. Row-based Spark operators require JVM shuffle.

3. **Supported partitioning type**: Native shuffle supports:
   - `HashPartitioning`
   - `RangePartitioning`
   - `SinglePartition`
   - `RoundRobinPartitioning`

4. **Supported partition key types**: For `HashPartitioning` and `RangePartitioning`, partition
   keys must be primitive types. Complex types (struct, array, map) as partition keys require
   JVM shuffle. Note that complex types are fully supported as data columns in native shuffle.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CometShuffleManager                                │
│  - Routes to CometNativeShuffleWriter for CometNativeShuffleHandle           │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CometNativeShuffleWriter                             │
│  - Constructs protobuf operator plan                                         │
│  - Invokes native execution via CometExec.getCometIterator()                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ (JNI)
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ShuffleWriterExec (Rust)                             │
│  - DataFusion ExecutionPlan                                                  │
│  - Orchestrates partitioning and writing                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                    │                                     │
                    ▼                                     ▼
┌───────────────────────────────────┐   ┌───────────────────────────────────┐
│ MultiPartitionShuffleRepartitioner │   │ SinglePartitionShufflePartitioner │
│ (hash/range partitioning)          │   │ (single partition case)           │
└───────────────────────────────────┘   └───────────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────────┐
│ ShuffleBlockWriter                 │
│ (Arrow IPC + compression)          │
└───────────────────────────────────┘
                    │
                    ▼
         ┌─────────────────┐
         │  Data + Index   │
         │     Files       │
         └─────────────────┘
```

## Key Classes

### Scala Side

| Class                          | Location                                         | Description                                                                                   |
| ------------------------------ | ------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `CometShuffleExchangeExec`     | `.../shuffle/CometShuffleExchangeExec.scala`     | Physical plan node. Validates types and partitioning, creates `CometShuffleDependency`.       |
| `CometNativeShuffleWriter`     | `.../shuffle/CometNativeShuffleWriter.scala`     | Implements `ShuffleWriter`. Builds protobuf plan and invokes native execution.                |
| `CometShuffleDependency`       | `.../shuffle/CometShuffleDependency.scala`       | Extends `ShuffleDependency`. Holds shuffle type, schema, and range partition bounds.          |
| `CometBlockStoreShuffleReader` | `.../shuffle/CometBlockStoreShuffleReader.scala` | Reads shuffle blocks via `ShuffleBlockFetcherIterator`. Decodes Arrow IPC to `ColumnarBatch`. |
| `NativeBatchDecoderIterator`   | `.../shuffle/NativeBatchDecoderIterator.scala`   | Reads compressed Arrow IPC from input stream. Calls native decode via JNI.                    |

### Rust Side

| File                    | Location                             | Description                                                                          |
| ----------------------- | ------------------------------------ | ------------------------------------------------------------------------------------ |
| `shuffle_writer.rs`     | `native/core/src/execution/shuffle/` | `ShuffleWriterExec` plan and partitioners. Main shuffle logic.                       |
| `codec.rs`              | `native/core/src/execution/shuffle/` | `ShuffleBlockWriter` for Arrow IPC encoding with compression. Also handles decoding. |
| `comet_partitioning.rs` | `native/core/src/execution/shuffle/` | `CometPartitioning` enum defining partition schemes (Hash, Range, Single).           |

## Data Flow

### Write Path

1. **Plan construction**: `CometNativeShuffleWriter` builds a protobuf operator plan containing:
   - A scan operator reading from the input iterator
   - A `ShuffleWriter` operator with partitioning config and compression codec

2. **Native execution**: `CometExec.getCometIterator()` executes the plan in Rust.

3. **Partitioning**: `ShuffleWriterExec` receives batches and routes to the appropriate partitioner:
   - `MultiPartitionShuffleRepartitioner`: For hash/range/round-robin partitioning
   - `SinglePartitionShufflePartitioner`: For single partition (simpler path)

4. **Buffering and spilling**: The partitioner buffers rows per partition. When memory pressure
   exceeds the threshold, partitions spill to temporary files.

5. **Encoding**: `ShuffleBlockWriter` encodes each partition's data as compressed Arrow IPC:
   - Writes compression type header
   - Writes field count header
   - Writes compressed IPC stream

6. **Output files**: Two files are produced:
   - **Data file**: Concatenated partition data
   - **Index file**: Array of 8-byte little-endian offsets marking partition boundaries

7. **Commit**: Back in JVM, `CometNativeShuffleWriter` reads the index file to get partition
   lengths and commits via Spark's `IndexShuffleBlockResolver`.

### Read Path

1. `CometBlockStoreShuffleReader` fetches shuffle blocks via `ShuffleBlockFetcherIterator`.

2. For each block, `NativeBatchDecoderIterator`:
   - Reads the 8-byte compressed length header
   - Reads the 8-byte field count header
   - Reads the compressed IPC data
   - Calls `Native.decodeShuffleBlock()` via JNI

3. Native code decompresses and deserializes the Arrow IPC stream.

4. Arrow FFI transfers the `RecordBatch` to JVM as a `ColumnarBatch`.

## Partitioning

### Hash Partitioning

Native shuffle implements Spark-compatible hash partitioning:

- Uses Murmur3 hash function with seed 42 (matching Spark)
- Computes hash of partition key columns
- Applies modulo by partition count: `partition_id = hash % num_partitions`

### Range Partitioning

For range partitioning:

1. Spark's `RangePartitioner` samples data and computes partition boundaries on the driver.
2. Boundaries are serialized to the native plan.
3. Native code converts sort key columns to comparable row format.
4. Binary search (`partition_point`) determines which partition each row belongs to.

### Single Partition

The simplest case: all rows go to partition 0. Uses `SinglePartitionShufflePartitioner` which
simply concatenates batches to reach the configured batch size.

### Round Robin Partitioning

Comet implements round robin partitioning using hash-based assignment for determinism:

1. Computes a Murmur3 hash of columns (using seed 42)
2. Assigns partitions directly using the hash: `partition_id = hash % num_partitions`

This approach guarantees determinism across retries, which is critical for fault tolerance.
However, unlike true round robin which cycles through partitions row-by-row, hash-based
assignment only provides even distribution when the data has sufficient variation in the
hashed columns. Data with low cardinality or identical values may result in skewed partition
sizes.

## Memory Management

Native shuffle uses DataFusion's memory management with spilling support:

- **Memory pool**: Tracks memory usage across the shuffle operation.
- **Spill threshold**: When buffered data exceeds the threshold, partitions spill to disk.
- **Per-partition spilling**: Each partition has its own spill file. Multiple spills for a
  partition are concatenated when writing the final output.
- **Scratch space**: Reusable buffers for partition ID computation to reduce allocations.

The `MultiPartitionShuffleRepartitioner` manages:

- `PartitionBuffer`: In-memory buffer for each partition
- `SpillFile`: Temporary file for spilled data
- Memory tracking via `MemoryConsumer` trait

## Compression

Native shuffle supports multiple compression codecs configured via
`spark.comet.exec.shuffle.compression.codec`:

| Codec    | Description                                            |
| -------- | ------------------------------------------------------ |
| `zstd`   | Zstandard compression. Best ratio, configurable level. |
| `lz4`    | LZ4 compression. Fast with good ratio.                 |
| `snappy` | Snappy compression. Fastest, lower ratio.              |
| `none`   | No compression.                                        |

The compression codec is applied uniformly to all partitions. Each partition's data is
independently compressed, allowing parallel decompression during reads.

## Configuration

| Config                                            | Default | Description                              |
| ------------------------------------------------- | ------- | ---------------------------------------- |
| `spark.comet.exec.shuffle.enabled`                | `true`  | Enable Comet shuffle                     |
| `spark.comet.exec.shuffle.mode`                   | `auto`  | Shuffle mode: `native`, `jvm`, or `auto` |
| `spark.comet.exec.shuffle.compression.codec`      | `zstd`  | Compression codec                        |
| `spark.comet.exec.shuffle.compression.zstd.level` | `1`     | Zstd compression level                   |
| `spark.comet.shuffle.write.buffer.size`           | `1MB`   | Write buffer size                        |
| `spark.comet.columnar.shuffle.batch.size`         | `8192`  | Target rows per batch                    |

## Comparison with JVM Shuffle

| Aspect              | Native Shuffle                         | JVM Shuffle                       |
| ------------------- | -------------------------------------- | --------------------------------- |
| Input format        | Columnar (direct from Comet operators) | Row-based (via ColumnarToRowExec) |
| Partitioning logic  | Rust implementation                    | Spark's partitioner               |
| Supported schemes   | Hash, Range, Single, RoundRobin        | Hash, Range, Single, RoundRobin   |
| Partition key types | Primitives only (Hash, Range)          | Any type                          |
| Performance         | Higher (no format conversion)          | Lower (columnar→row→columnar)     |
| Writer variants     | Single path                            | Bypass (hash) and sort-based      |

See [JVM Shuffle](jvm_shuffle.md) for details on the JVM-based implementation.
