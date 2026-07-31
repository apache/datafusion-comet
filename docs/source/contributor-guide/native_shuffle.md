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

| File                              | Location              | Description                                                                                                 |
| --------------------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------- |
| `shuffle_writer.rs`               | `native/shuffle/src/` | `ShuffleWriterExec` plan and the partitioner selection in `external_shuffle`.                               |
| `partitioners/`                   | `native/shuffle/src/` | `MultiPartitionShuffleRepartitioner`, `SinglePartitionShufflePartitioner`, `EmptySchemaShufflePartitioner`. |
| `writers/shuffle_block_writer.rs` | `native/shuffle/src/` | `ShuffleBlockWriter`: Arrow IPC encoding plus the block header and whole-stream compression.                |
| `ipc.rs`                          | `native/shuffle/src/` | `read_ipc_compressed`, the decode half of the block format.                                                 |
| `comet_partitioning.rs`           | `native/shuffle/src/` | `CometPartitioning` enum defining partition schemes (Hash, Range, RoundRobin, Single), and `pmod`.          |
| `schema_align.rs`                 | `native/shuffle/src/` | `SchemaAlignExec`, which reshapes the writer's input to the schema Spark catalyst declared.                 |

These files live in the `datafusion-comet-shuffle` crate. `native/core` re-exports it as
`execution::shuffle` (`native/core/src/execution/mod.rs`), so paths such as
`crate::execution::shuffle::ShuffleBlockWriter` in `native/core` resolve into `native/shuffle/src/`.

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

3. **Partitioning**: `ShuffleWriterExec` receives batches and routes to the appropriate partitioner,
   checked in this order:
   - `EmptySchemaShufflePartitioner`: For zero-column input, whatever partitioning was requested
   - `SinglePartitionShufflePartitioner`: For single partition (simpler path)
   - `MultiPartitionShuffleRepartitioner`: For hash/range/round-robin partitioning

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
- Applies Spark's positive modulo by partition count: `partition_id = pmod(hash, num_partitions)`,
  where `pmod` reinterprets the hash as a signed 32-bit integer and folds a negative remainder back
  into range

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

1. Computes a Murmur3 hash of columns (using seed 42), over the first `max_hash_columns` columns,
   where `0` means all of them
2. Assigns partitions directly using the hash, through the same `pmod` as hash partitioning

This approach guarantees determinism across retries, which is critical for fault tolerance.
However, unlike true round robin which cycles through partitions row-by-row, hash-based
assignment only provides even distribution when the data has sufficient variation in the
hashed columns. Data with low cardinality or identical values may result in skewed partition
sizes.

## Memory Management

`MultiPartitionShuffleRepartitioner` is the only operator in Comet's own native code that registers
a DataFusion `MemoryConsumer`, so it is also the reference for how a buffering native operator
reserves memory. See [Native Memory Management](memory_management.md) for how pools are chosen and
sized, what `try_grow` returning an error means, and how the reservation reaches Spark's
`TaskMemoryManager`.

The shuffle-specific parts:

- **Spill triggers**: Partitions spill to disk when the memory pool denies an allocation, or
  when the buffered bytes reach `spark.comet.shuffle.native.maxBufferBytes`. That config defaults to
  0, which disables the fixed limit and leaves memory pressure as the only trigger.
- **Per-partition spilling**: Each partition has its own spill file (`writers/local/spill.rs`).
  Multiple spills for a partition are concatenated when writing the final output.
- **Scratch space**: Reusable buffers for partition ID computation to reduce allocations
  (`ScratchSpace` in `partitioners/multi_partition.rs`).
- **What is charged**: the reservation covers the batches held in `buffered_batches` plus the
  per-partition row index vectors. `count_new_buffers` sums each distinct backing allocation once,
  keyed by buffer start address, rather than using `RecordBatch::get_array_memory_size` or a sum of
  slice sizes; its doc comment records why both of those measures are wrong here.

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

## Rules and Common Mistakes

Shuffle carries more cross-boundary invariants than most of the native code, because its output has
to satisfy Spark's planner, its bytes have to satisfy two separate readers, and neither side
re-checks the other. Every rule below is grounded in a specific file. Where a rule cites a file,
read that file before relying on the summary here.

### Reproduce Spark's hash exactly, or rows land in the wrong reducer

`MultiPartitionShuffleRepartitioner::partitioning_batch`
(`native/shuffle/src/partitioners/multi_partition.rs`) derives a partition id in three steps, and
all three have to match Spark:

1. The hash buffer is filled with `42` before hashing (`hashes_buf.fill(42_u32)`), which is the seed
   Spark's `Murmur3Hash` uses.
2. `create_murmur3_hashes` (`native/spark-expr/src/hash_funcs/murmur3.rs`) chains one hash per
   column, feeding each value's little-endian bytes at a width fixed by the arrow data type.
   `hash_array_primitive!` (`native/spark-expr/src/hash_funcs/utils.rs`) widens `Int8`, `Int16`, and
   `Int32` to `i32` and hashes four bytes, and `Int64` to `i64` and hashes eight. The finalizer mixes
   the byte length in, so the same numeric value hashed as `Int32` versus `Int64` yields a different
   hash. Null slots are skipped and leave the running hash unchanged.
3. `pmod` (`native/shuffle/src/comet_partitioning.rs`) reinterprets the `u32` hash as `i32` before
   taking the remainder, then folds a negative result back into range. The signed reinterpretation is
   load bearing: Spark's `Murmur3Hash` returns a signed `Int`, so dropping the `as i32` moves hashes
   at or above 2^31 to a different partition for any partition count that is not a power of two,
   including 200, Spark's default. A power of two divides 2^32 exactly, which is why testing this
   with `repartition(1024)` shows no difference at all. `test_pmod` in that file pins five hashes,
   all of them above 2^31, against the partition ids Spark produces for `n = 200`.

Round-robin partitioning runs the same three steps over the first `max_hash_columns` columns, so it
inherits all of them.

Nothing downstream re-checks the assignment. `CometShuffleExchangeExec` extends `ShuffleExchangeLike`
and overrides `outputPartitioning` with the `Partitioning` object Spark's planner handed it, so
`EnsureRequirements` treats the exchange as already satisfying that distribution, and the read path
only decodes blocks. A partition id that disagrees with Spark's therefore surfaces as a wrong answer,
typically rows missing from a join or a group split across reducers, not as an error. Verify changes
here by comparing full query results against Spark, not with a unit test of the Rust function alone.

### Align the writer's input schema before partitioning

`SchemaAlignExec` (`native/shuffle/src/schema_align.rs`) casts each writer input column to the type
Spark catalyst declared. `align_shuffle_writer_input` in
`native/core/src/execution/planner.rs` inserts it around the writer's child, and `create_partitioning`
is then called with `writer_input.schema()`, so both the hash expressions and the range
`SortField`s are built from the aligned types. That ordering is the point of the operator.

Everywhere else in the native runtime a return-type drift from DataFusion or `datafusion-spark` is
self-healing, because the `ScanExec` that imports the batch on the other side of the JVM boundary
casts every column to the catalyst-declared type. Shuffle is the exception on two counts:

- The partition id depends on the arrow type, per the hashing rule above, so a drifted type routes
  rows to the wrong partition, and a read-side cast cannot undo a partition assignment.
- The read side does not cast. `ShuffleScanExec::get_next`
  (`native/core/src/execution/operators/shuffle_scan.rs`) decodes the block, unpacks any
  dictionary-encoded column to its value type, and then stamps the protobuf-declared schema on with
  `RecordBatch::try_new_with_options`, which fails on any data type that does not match. Dictionary
  unpacking is the only conversion on that path.

[Issue #4515](https://github.com/apache/datafusion-comet/issues/4515) is the running list of
functions whose return types drift. The tests in `schema_align.rs` are written so that when a drift
pair becomes identical, `try_new_or_passthrough` returns the child unwrapped and the test flips to
the passthrough assertion. That is the signal the workaround for that function can be deleted.

### The block format is deliberately not stock Arrow IPC

`ShuffleBlockWriter::write_batch` (`native/shuffle/src/writers/shuffle_block_writer.rs`) emits each
block as an 8-byte little-endian length, an 8-byte field count, a 4-byte ASCII codec tag (`ZSTD`,
`LZ4_`, `SNAP`, or `NONE`), and then one complete Arrow IPC stream compressed as a single frame.
The length covers everything after itself. The field count is written because the JVM reader has to
know how many array addresses to allocate, and `NativeBatchDecoderIterator` reads both headers
before handing the remainder, starting at the codec tag, to native code.

Compression wraps the whole IPC stream rather than individual buffers. `read_ipc_compressed`
(`native/shuffle/src/ipc.rs`) dispatches on the 4-byte tag, wraps the rest in the matching frame
decoder, and feeds it to a single arrow `StreamReader`. Arrow's own IPC compression is a different
byte layout, applied per buffer and recorded in the flatbuffer message, and it offers no Snappy
codec. `IpcWriteOptions::try_with_compression` is called nowhere in the native tree. Refactoring the
writer onto arrow's IPC compression would change the bytes on disk and break both readers, so this
divergence is settled; it is on the verified-intentional list in
[epic #5104](https://github.com/apache/datafusion-comet/issues/5104).

The decoder also runs with `with_skip_validation(true)`, which is why the `StreamReader` construction
sits in an `unsafe` block: it trusts the writer to have produced a well-formed stream. Anything that
changes what the writer emits has to keep that true.

### The pre-encoded schema path must stay byte-identical

For a schema with no dictionary types anywhere in its flattened field tree,
`ShuffleBlockWriter::try_new` encodes the IPC schema message once and `encode_ipc_stream` writes
those bytes verbatim at the start of every block, followed by the record batch message and an 8-byte
end-of-stream marker. This is a fast path that avoids re-serializing the schema per block, not a
format change: it uses the same `IpcDataGenerator` and the same `IpcWriteOptions` that
`StreamWriter` would, and spells the end-of-stream marker out as the `IPC_EOS` constant. That
constant is only correct for metadata version V5 with non-legacy framing, which is why `try_new`
pins `IpcWriteOptions::try_new(64, false, MetadataVersion::V5)` instead of taking the arrow default.
Schemas that do contain dictionary types fall back to `StreamWriter`, because the schema and the
batch have to share a dictionary tracker.

Two properties to preserve if you touch this. The output has to remain what `StreamWriter` would
have produced, since both readers parse it as an ordinary IPC stream. And both arms of
`SchemaEncoding` hold their payload behind an `Arc`, because the writer is cloned once per output
partition and a shuffle can request millions of partitions; `clone_shares_buffers` in the same file
asserts that a clone shares those buffers rather than deep-copying them.

### Range bounds come out of a private Spark field by reflection

`prepareNativeShuffleDependency` in
`.../shuffle/CometShuffleExchangeExec.scala` constructs a real Spark `RangePartitioner` on the driver
over a dedicated sampling RDD, then reads its private `rangeBounds` array with
`getDeclaredField("rangeBounds")` and `setAccessible(true)`. The bounds are serialized into the
native plan, and the native side reproduces `RangePartitioner.getPartition` as
`bounds.partition_point(|bound| bound.row() <= row)` over arrow `Row`s. The boundary rows and the
incoming batches are encoded by the same `RowConverter`, whose `SortField`s carry each column's sort
options (`create_partitioning` in `native/core/src/execution/planner.rs`), which is what makes the
byte comparison reproduce the Spark ordering.

There is no fallback around the reflection, so a Spark version that renames or restructures that
field fails at run time rather than mispartitioning silently. Check it when adding support for a new
Spark version.

### Zero-column batches still carry a row count

`external_shuffle` (`native/shuffle/src/shuffle_writer.rs`) selects the partitioner with a guarded
`match`, and the empty-schema guard is checked first, ahead of the single-partition guard.
A zero-column input therefore gets `EmptySchemaShufflePartitioner` whatever partitioning was
requested. That partitioner accumulates row counts and writes one zero-column IPC batch carrying the
total to partition 0, leaving every other partition empty in the index file. This is what lets
`count(*)`-shaped queries survive a shuffle after column pruning has removed every column, so
preserve the guard order when adding a partitioner.

### Kernel-first still applies, but here the answer is often no

[Working with Arrow in Native Code](working_with_arrow.md) says to look for an existing arrow-rs or
DataFusion kernel before writing array code by hand. Shuffle is one of the places where that search
legitimately comes back empty, and the #5104 audit confirmed the remaining hand-rolled pieces are
intentional:

- The block format above is fixed by the JVM reader, so arrow's IPC compression is not usable.
- Grouping rows by partition is a counting sort.
  `ScratchSpace::map_partition_ids_to_starts_and_indices` in `partitioners/multi_partition.rs` counts
  rows per partition, prefix-sums the counts into partition ends, then places every row index in one
  reverse pass. That is linear in the number of rows, where sorting a batch by partition id with a
  kernel is `n log n`.

Where a kernel does fit, one is already used: the per-partition row gather is
`interleave_record_batch` (`partitioners/partitioned_batch_iterator.rs`), and `SchemaAlignExec` casts
with `cast_with_options`. Add a new hand-rolled loop only with a reason of the same kind.

The radix sort over packed record pointers, which #5104 lists under shuffle, is not part of this
path: it is `Java_org_apache_comet_Native_sortRowPartitionsNative`
(`native/core/src/execution/jni_api.rs`), which sorts the in-memory partition id array of
`CometShuffleExternalSorter`. Separately, the CRC32, CRC32C, and Adler32 checksums in
`native/shuffle/src/writers/checksum.rs` are reached only from `process_sorted_row_partition`, called
by `Java_org_apache_comet_Native_writeSortedFileNative`. Both belong to
[JVM Shuffle](jvm_shuffle.md). The native shuffle writer emits no checksums at all: it commits with
an empty checksum array in `CometNativeShuffleWriter.scala`.

## Configuration

| Config                                       | Default | Description                              |
| -------------------------------------------- | ------- | ---------------------------------------- |
| `spark.comet.shuffle.enabled`                | `true`  | Enable Comet shuffle                     |
| `spark.comet.shuffle.mode`                   | `auto`  | Shuffle mode: `native`, `jvm`, or `auto` |
| `spark.comet.shuffle.compression.codec`      | `zstd`  | Compression codec                        |
| `spark.comet.shuffle.compression.zstd.level` | `1`     | Zstd compression level                   |
| `spark.comet.shuffle.native.writeBufferSize` | `1MB`   | Write buffer size                        |
| `spark.comet.shuffle.jvm.batchSize`          | `8192`  | Target rows per batch                    |

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
