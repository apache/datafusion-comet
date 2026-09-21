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

1. **Shuffle mode allows native**: `spark.comet.shuffle.mode` is `native` or `auto`.

2. **Child plan is a Comet native operator**: The child must be a `CometPlan` that produces
   columnar output. Row-based Spark operators require JVM shuffle.

3. **Supported partitioning type**: Native shuffle supports:

   - `HashPartitioning`
   - `RangePartitioning`
   - `SinglePartition`
   - `RoundRobinPartitioning`, disabled by default via
     `spark.comet.shuffle.native.partitioning.roundrobin.enabled`, because Comet's hash-based
     assignment puts unsorted rows in different partitions than Spark does

4. **Supported partition key types**: The rule differs by partitioning, and neither restricts data
   columns. Complex types are fully supported as data columns in native shuffle.
   - `RangePartitioning` keys must be primitive. `supportedRangePartitioningDataType` rejects every
     nested type, because native cannot sort them, and rejects collated strings, because Comet
     compares raw bytes. Scalar float and double are supported, including when
     `spark.comet.exec.strictFloatingPoint` is enabled, because the native range partitioner
     normalizes its comparison keys and its sampled boundary rows the same way the native sort
     does. Strict floating point only affects floating-point values nested in arrays, structs, or
     maps, which are rejected as range keys for being nested anyway.
   - `HashPartitioning` keys must be primitive **by default**. Setting
     `spark.comet.shuffle.native.partitioning.hash.nested.enabled` to `true` admits structs and
     arrays as keys, checked recursively to their leaves, and maps on Spark 4.0 and later, where
     Spark's `mapsort` normalization makes physical entry order irrelevant. A collated string at any
     depth still disqualifies the key. The config defaults to `false` pending measurement of the
     nested hashing paths, so by default a complex hash key falls back to JVM shuffle.

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
│  - Builds protobuf operator plan: ShuffleWriter(child = childNativeOp)       │
│  - Reads per-partition leaf iterators from CometNativeShuffleInputIterator   │
│  - Drives one CometExecIterator per partition                                │
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

| Class                          | Location                                         | Description                                                                                                                                         |
| ------------------------------ | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CometShuffleExchangeExec`     | `.../shuffle/CometShuffleExchangeExec.scala`     | Physical plan node. Validates types and partitioning, creates `CometShuffleDependency`.                                                             |
| `CometNativeShuffleWriter`     | `.../shuffle/CometNativeShuffleWriter.scala`     | Implements `ShuffleWriter`. Builds the unified `ShuffleWriter(child = childNativeOp)` plan and runs it in one `CometExecIterator` per partition.    |
| `CometShuffleDependency`       | `.../shuffle/CometShuffleDependency.scala`       | Extends `ShuffleDependency`. Holds shuffle type, schema, range partition bounds, and (native shuffle only) a `NativeShuffleSpec`.                   |
| `CometNativeShuffleInputRDD`   | `.../shuffle/CometNativeShuffleInputRDD.scala`   | Thin scheduling-anchor RDD on the native-shuffle path. `compute` returns a `CometNativeShuffleInputIterator` carrying per-partition leaf iterators. |
| `CometBlockStoreShuffleReader` | `.../shuffle/CometBlockStoreShuffleReader.scala` | Reads shuffle blocks via `ShuffleBlockFetcherIterator`. Decodes Arrow IPC to `ColumnarBatch`.                                                       |
| `NativeBatchDecoderIterator`   | `.../shuffle/NativeBatchDecoderIterator.scala`   | Reads compressed Arrow IPC from input stream. Calls native decode via JNI.                                                                          |

### Rust Side

The native shuffle implementation is its own workspace crate, `datafusion-comet-shuffle`, rooted at
`native/shuffle/`. Paths below are relative to `native/shuffle/src/`.

| File                               | Description                                                                                                                      |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `shuffle_writer.rs`                | `ShuffleWriterExec`, the DataFusion `ExecutionPlan` that drives partitioning and writing.                                        |
| `comet_partitioning.rs`            | `CometPartitioning` enum defining the partition schemes (Hash, Range, Single, RoundRobin).                                       |
| `partitioners/multi_partition.rs`  | `MultiPartitionShuffleRepartitioner` for hash, range, and round robin partitioning.                                              |
| `partitioners/single_partition.rs` | `SinglePartitionShufflePartitioner` for the single partition case.                                                               |
| `writers/shuffle_block_writer.rs`  | `ShuffleBlockWriter` and the `CompressionCodec` enum. Arrow IPC encoding with compression.                                       |
| `writers/partition_writer.rs`      | The `PartitionWriter` trait. Implemented by `LocalPartitionWriter` (`writers/local/`) and `RssPartitionWriter` (`writers/rss/`). |
| `writers/buf_batch_writer.rs`      | `BufBatchWriter`, which coalesces sub-`batch_size` batches through Arrow's `BatchCoalescer` before serializing.                  |
| `writers/local/spill.rs`           | `PartitionedSpill`, the one spill file shared by every output partition, and its per-partition byte ranges.                      |
| `ipc.rs`                           | `read_ipc_compressed` and `read_ipc_compressed_validated`, the decode side used by the shuffle reader.                           |

## Data Flow

### Write Path

1. **Plan construction**: `CometNativeShuffleWriter` builds a protobuf operator tree with a
   `ShuffleWriter` operator at the root and `childNativeOp` as its child. `childNativeOp` takes
   one of two shapes:

   - The child plan's `nativeOp` directly, when `CometShuffleExchangeExec`'s child is a
     `CometNativeExec` subtree. The upstream operators run inside the same `CometExecIterator`
     as the writer, with no JVM-to-native batch boundary between them.
   - A synthetic `Scan("ShuffleWriterInput")` placeholder, when the dep was built via the
     convenience `prepareShuffleDependency(rdd, ...)` overload (used by
     `CometCollectLimitExec` and `CometTakeOrderedAndProjectExec`, or when the
     exchange's child is a non-native `CometPlan` such as `CometSparkToColumnarExec`). Native
     code reads `ColumnarBatch`es from the JVM input iterator via Arrow C Stream Interface.

2. **Native execution**: A single `CometExecIterator` per partition runs the unified plan.

3. **Partitioning**: `ShuffleWriterExec` receives batches and routes to the appropriate partitioner:

   - `MultiPartitionShuffleRepartitioner`: For hash/range/round-robin partitioning
   - `SinglePartitionShufflePartitioner`: For single partition (simpler path)

4. **Buffering and spilling**: The partitioner buffers rows per partition. When memory pressure
   exceeds the threshold, partitions spill to temporary files.

5. **Encoding**: `ShuffleBlockWriter` encodes each partition's data as compressed Arrow IPC:

   - Writes compression type header
   - Writes field count header
   - Writes compressed IPC stream

6. **Output**: One data file holds the concatenated partition data. The writer records the byte
   offset where each partition begins, plus the total length, and keeps them in memory.

7. **Commit**: Back in JVM, `CometNativeShuffleWriter` fetches the offsets with
   `Native.getShufflePartitionOffsets`, converts them to partition lengths, and commits via
   Spark's `IndexShuffleBlockResolver.writeMetadataFileAndCommit`, which writes Spark's index file.

### Read Path

There are two read paths, and which one runs is decided at plan serialization time rather than at
read time. See [Direct Read](#direct-read-shufflescan) below for how the choice is made.

**Direct read**, the default, decodes inside the native plan:

1. `CometBlockStoreShuffleReader.readAsRawStream()` concatenates the fetched block streams and
   skips decoding entirely.
2. `CometShuffleBlockIterator` hands native one compressed block at a time.
3. The native `ShuffleScanExec` decodes each block with `read_ipc_compressed` and feeds the
   resulting `RecordBatch` straight into the plan. No Arrow FFI export or import happens.

**JVM decode**, used when direct read does not apply:

1. `CometBlockStoreShuffleReader` fetches shuffle blocks via `ShuffleBlockFetcherIterator`.

2. For each block, `NativeBatchDecoderIterator`:

   - Reads the 8-byte compressed length header
   - Reads the 8-byte field count header
   - Reads the compressed IPC data
   - Calls `Native.decodeShuffleBlock()` via JNI

3. Native code decompresses and deserializes the Arrow IPC stream.

4. Arrow FFI transfers the `RecordBatch` to JVM as a `ColumnarBatch`.

## Direct Read (ShuffleScan)

Direct read lets a native operator consume shuffle output without the batch ever being decoded in
the JVM or crossing Arrow FFI. It is controlled by `spark.comet.shuffle.directRead.enabled`, which
defaults to `true` and requires `spark.comet.shuffle.enabled`. It applies to both native shuffle and
JVM columnar shuffle, because both write the same Arrow IPC block format.

### How the path is selected

`CometExchangeSink.shouldUseShuffleScan` (`spark/src/main/scala/org/apache/comet/serde/operator/CometSink.scala`)
decides during plan serialization. When direct read is enabled and the sink's input is a Comet shuffle
exchange, `convertToShuffleScan` emits a `ShuffleScan` operator. When either is false the sink falls
through to the base `CometSink.convert`, which emits the usual `Scan`.

The two are not alternatives on failure. If any output type fails `supportedSinkDataType`,
`convertToShuffleScan` records the fallback reason `Unsupported data type for shuffle direct read`
and returns `None`. It does not retry as a regular `Scan`, and retrying would not help, because
`CometSink.convert` gates on the same `supportedSinkDataType`. `CometExecRule` calls this as
`convertToComet(s, CometExchangeSink).getOrElse(s)`, so `None` leaves the original Spark shuffle stage
in the plan. An unsupported output type therefore means the stage is not converted natively at all,
not that it is served over the FFI read path instead.

**The protobuf is the source of truth for which slots are direct read, not the config.**
`findShuffleScanIndices` (`operators.scala`) walks the serialized plan, counting scan slots in order
and collecting the indices that carry a `ShuffleScan`. JVM-side input dispatch reads that set rather
than re-checking the config, so the two cannot disagree. Anything changing the serde condition has to
leave that walk consistent with it.

### How blocks reach native

`CometExecRDD.resolveInputObjects` fills one input slot per scan input, in scan-input order:

- A slot in `shuffleScanIndices` gets a `CometShuffleBlockIterator`, obtained from
  `CometShuffledBatchRDD.computeAsShuffleBlockIterator`. A slot marked as a shuffle scan whose RDD is
  not a `CometShuffledBatchRDD` throws `CometRuntimeException`.
- Every other slot gets the `ArrowArrayStream` exported by the ordinary FFI path.

`CometShuffleBlockIterator` reads a 16-byte header per block: an 8-byte compressed length, which
includes the field count but not itself, followed by an 8-byte field count that is discarded because
the schema comes from the `ShuffleScan` protobuf fields. The compressed body is read into a reused
`DirectByteBuffer`, **valid only until the next `hasNext()` call**. Native must fully consume it
before pulling the next block, which `read_ipc_compressed` satisfies because it allocates fresh
native memory for the decoded data.

### Native side

`ShuffleScanExec` (`native/core/src/execution/operators/shuffle_scan.rs`) pulls blocks through the
iterator's `hasNext()` and `getBuffer()` JNI methods and decodes them with `read_ipc_compressed`.
Two details matter when changing it:

- `get_next_batch` is called from outside `poll_next`, because JNI calls cannot be made from tokio
  worker threads. A change that moves the JNI call into the stream's `poll_next` breaks this.
- Dictionary-encoded columns are unpacked to their value type by `unpack_dictionary`, so the schema
  the plan sees matches the declared `ShuffleScan` fields.

Blocks are validated before decoding when `CometShuffleBlockIterator.requiresValidation()` is true,
which happens when the underlying stream implements `CometShuffleReadFailureHandler` so a corrupt
block can be reported back as a shuffle read failure rather than a native decode panic.

Celeborn reuses the same expected-schema contract from the JVM side. `CometCelebornShuffleReader`
builds a `ShuffleScan` message from the dependency's output attributes and passes it to
`NativeBatchDecoderIterator`, so remote logical types are validated before Arrow import and an
unsupported type fails before any shuffle resource is acquired.

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

The simplest case: all rows go to partition 0. Uses `SinglePartitionShufflePartitioner`, which
streams each batch straight to the writer, whose `BatchCoalescer` combines small batches up to the
configured batch size. Batches already at least that size pass through unchanged, so a large input
batch is written as a single block that may exceed the batch size.

### Round Robin Partitioning

`CometPartitioning::RoundRobin` carries a `RoundRobinStrategy` that decides how rows reach output
partitions. The default is `HashAll`; `WholeBatch` is opt-in through
`spark.comet.shuffle.native.partitioning.roundrobin.batchGranular`.

#### `HashAll`: hash-based assignment (default)

1. Computes a Murmur3 hash of columns (using seed 42)
2. Assigns partitions directly using the hash: `partition_id = hash % num_partitions`

This approach guarantees determinism across retries, which is critical for fault tolerance.
However, unlike true round robin which cycles through partitions row-by-row, hash-based
assignment only provides even distribution when the data has sufficient variation in the
hashed columns. Data with low cardinality or identical values may result in skewed partition
sizes.

Because Spark assigns round robin partitions by sorting rows on their binary `UnsafeRow` form,
which Arrow's layout does not reproduce, unsorted output can land in different partitions than
Spark's. Sorted output is identical. That difference is why
`spark.comet.shuffle.native.partitioning.roundrobin.enabled` defaults to `false`.

`spark.comet.shuffle.native.partitioning.roundrobin.maxHashColumns` caps how many leading columns
are hashed. `0`, the default, hashes all of them.

#### `WholeBatch`: positional assignment

Hashing every column of every row dominates the shuffle write on wide nested schemas, because
`create_murmur3_hashes` recurses into every struct child per row and the resulting row-level
scatter forces `interleave_record_batch` to walk every column and child again on flush.
`WholeBatch` assigns each incoming `RecordBatch` whole to `counter % num_partitions`, where the
counter is seeded with the input partition id so concurrent mappers do not all target partition 0
first. `partition_row_indices` is left unmaterialized and the flush clones the source batch instead
of interleaving it, so the per-batch cost is one modulo, two `resize` calls, and some `Arc` bumps.

Distribution is even at batch granularity rather than row granularity: fewer batches than
partitions leaves partitions empty, and unequal batch sizes give unequal partitions.

#### Retry safety under `WholeBatch`

Positional assignment is not a function of the rows, so it is only reproducible when the upstream
operator replays the same batches in the same order and with the same framing. Re-executing one map
task against differently framed input writes a different partitioning of the same rows, and once
any consumer has fetched the output that attempt replaces, the reduce side silently gets some rows
twice and others not at all. Spark faces the same problem with its own round robin and answers it
in two ways, both of which Comet's native path mirrors:

- **Declaring the risk.** Spark wraps a round-robin repartition in a `MapPartitionsRDD` with
  `isOrderSensitive = true`, which reports `INDETERMINATE` whenever its parent is `UNORDERED`. The
  `DAGScheduler` then rolls the whole stage back rather than re-running a single task, and aborts
  the job outright when a result stage has already consumed output. The native path has no
  `MapPartitionsRDD` to carry the flag, so `CometNativeShuffleInputRDD.getOutputDeterministicLevel`
  applies the same rule directly. A determinate parent such as a plain scan stays determinate and
  keeps cheap per-task retry; anything below another exchange is unordered, because reduce tasks
  see shuffle blocks in arrival order, and goes indeterminate.

- **Refusing the retry.** Rollback is only as sound as the parent's determinism level being an
  accurate description of what the upstream operator replays. While the strategy is opt-in and
  unproven, `spark.comet.shuffle.native.partitioning.roundrobin.batchGranular.failOnRetry`
  (default `true`) makes `CometNativeShuffleWriter` refuse to run at all when `TaskContext` reports
  a task attempt after the first or a re-submitted stage attempt. Both counters are needed:
  `TaskSetManager.executorLost` re-enqueues a dead executor's map tasks inside the current task set
  with a fresh task attempt, while a fetch failure against that executor's output resubmits the
  stage instead. Spark has no API for failing an application from inside a task, so this throws an
  ordinary exception; because the condition only gets more true on each attempt, the task set
  aborts after `spark.task.maxFailures` and takes the job with it. Setting the config to `false`
  leaves the indeterminate declaration above as the only defence.

Neither applies to `HashAll`, whose output is a pure function of the rows it sees.

## Memory Management

Native shuffle uses DataFusion's memory management with spilling support:

- **Memory pool**: Tracks memory usage across the shuffle operation.
- **Spill triggers**: Partitions spill to disk when the memory pool denies an allocation, or
  when the buffered bytes reach `spark.comet.shuffle.native.maxBufferBytes`. That config defaults to
  0, which disables the fixed limit and leaves memory pressure as the only trigger.
- **One spill file per task**: Every output partition spills into the same file, not one file per
  partition. `PartitionedSpill` records the byte ranges each partition's blocks occupy, in write
  order, and the final output copies a partition's ranges back in that order.
- **Scratch space**: Reusable buffers for partition ID computation to reduce allocations.

The `MultiPartitionShuffleRepartitioner` holds:

- `buffered_batches`, a `Vec<RecordBatch>` of incoming batches, alongside `partition_indices`
  recording which rows of those batches belong to each partition. Rows are not copied into
  per-partition buffers as they arrive.
- `reservation`, a `MemoryReservation` charged for the bytes each buffered batch newly pins.
  `pinned_buffers` tracks backing buffer start addresses so one allocation shared by many sliced
  batches is charged once rather than once per slice. Charging a per-batch size instead would
  overstate memory by the slice count and spill spuriously.
- `max_buffer_bytes`, the optional fixed spill threshold described above. `None` leaves pool
  pressure as the only trigger.
- `scratch`, reusable buffers for partition ID computation.

The spill file is owned by `PartitionedSpill` in `writers/local/spill.rs`. It holds one DataFusion
`SpillFile`, created lazily on the first spill and shared by every output partition, and tracks per
partition the byte ranges holding that partition's blocks in write order. A partial write sets a
`failed` flag, because a write that stops midway leaves the recorded ranges unable to describe the
file. The single-partition writer writes straight to the output file and never spills.

## Compression

Native shuffle supports multiple compression codecs configured via
`spark.comet.shuffle.compression.codec`:

| Codec    | Description                                            |
| -------- | ------------------------------------------------------ |
| `zstd`   | Zstandard compression. Best ratio, configurable level. |
| `lz4`    | LZ4 compression. Fast with good ratio.                 |
| `snappy` | Snappy compression. Fastest, lower ratio.              |
| `none`   | No compression.                                        |

The compression codec is applied uniformly to all partitions. Each partition's data is
independently compressed, allowing parallel decompression during reads.

## Configuration

| Config                                                              | Default | Description                                                   |
| ------------------------------------------------------------------- | ------- | ------------------------------------------------------------- |
| `spark.comet.shuffle.enabled`                                       | `true`  | Enable Comet shuffle                                          |
| `spark.comet.shuffle.mode`                                          | `auto`  | Shuffle mode: `native`, `jvm`, or `auto`                      |
| `spark.comet.shuffle.directRead.enabled`                            | `true`  | Decode shuffle blocks in native code, bypassing Arrow FFI     |
| `spark.comet.shuffle.compression.codec`                             | `lz4`   | Compression codec                                             |
| `spark.comet.shuffle.compression.zstd.level`                        | `1`     | Zstd compression level                                        |
| `spark.comet.shuffle.native.writeBufferSize`                        | `1MB`   | Write buffer size                                             |
| `spark.comet.shuffle.native.maxBufferBytes`                         | `0`     | Fixed spill threshold. `0` disables it, leaving pool pressure |
| `spark.comet.shuffle.native.partitioning.hash.enabled`              | `true`  | Allow `HashPartitioning` on the native path                   |
| `spark.comet.shuffle.native.partitioning.hash.nested.enabled`       | `false` | Allow struct and array hash keys, and map keys on Spark 4.0+  |
| `spark.comet.shuffle.native.partitioning.range.enabled`             | `true`  | Allow `RangePartitioning` on the native path                  |
| `spark.comet.shuffle.native.partitioning.roundrobin.enabled`        | `false` | Allow `RoundRobinPartitioning` on the native path             |
| `spark.comet.shuffle.native.partitioning.roundrobin.maxHashColumns` | `0`     | Columns to hash for round robin. `0` hashes all of them       |
| `spark.comet.shuffle.jvm.batchSize`                                 | `8192`  | Target rows per batch                                         |

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
