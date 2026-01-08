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

# JVM Shuffle

This document describes Comet's JVM-based columnar shuffle implementation (`CometColumnarShuffle`), which
writes shuffle data in Arrow IPC format using JVM code with native encoding.

## Overview

Comet provides two shuffle implementations:

- **CometNativeShuffle**: Fully native shuffle using Rust
- **CometColumnarShuffle**: JVM-based shuffle that buffers `UnsafeRow`s in memory pages and uses native
  code (via JNI) to encode them to Arrow IPC format

The JVM shuffle is selected via `CometShuffleDependency.shuffleType`.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CometShuffleManager                             │
│  - Extends Spark's ShuffleManager                                       │
│  - Routes to appropriate writer/reader based on ShuffleHandle type      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
           ┌────────────────────────┼────────────────────────┐
           ▼                        ▼                        ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│ CometBypassMerge-   │  │ CometUnsafe-        │  │ CometNative-        │
│ SortShuffleWriter   │  │ ShuffleWriter       │  │ ShuffleWriter       │
│ (hash-based)        │  │ (sort-based)        │  │ (fully native)      │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
           │                        │
           ▼                        ▼
┌─────────────────────┐  ┌─────────────────────┐
│ CometDiskBlock-     │  │ CometShuffleExternal│
│ Writer              │  │ Sorter              │
└─────────────────────┘  └─────────────────────┘
           │                        │
           └────────────┬───────────┘
                        ▼
              ┌─────────────────────┐
              │ SpillWriter         │
              │ (native encoding    │
              │  via JNI)           │
              └─────────────────────┘
```

## Key Classes

### Shuffle Manager

| Class | Location | Description |
|-------|----------|-------------|
| `CometShuffleManager` | `.../shuffle/CometShuffleManager.scala` | Entry point. Extends Spark's `ShuffleManager`. Selects writer/reader based on handle type. Delegates non-Comet shuffles to `SortShuffleManager`. |
| `CometShuffleDependency` | `.../shuffle/CometShuffleDependency.scala` | Extends `ShuffleDependency`. Contains `shuffleType` (`CometColumnarShuffle` or `CometNativeShuffle`) and schema info. |

### Shuffle Handles

| Handle | Writer Strategy |
|--------|-----------------|
| `CometBypassMergeSortShuffleHandle` | Hash-based: one file per partition, merged at end |
| `CometSerializedShuffleHandle` | Sort-based: records sorted by partition ID, single output |
| `CometNativeShuffleHandle` | Fully native shuffle |

Selection logic in `CometShuffleManager.shouldBypassMergeSort()`:
- Uses bypass if partitions < threshold AND partitions × cores ≤ max threads
- Otherwise uses sort-based to avoid OOM from many concurrent writers

### Writers

| Class | Location | Description |
|-------|----------|-------------|
| `CometBypassMergeSortShuffleWriter` | `.../shuffle/CometBypassMergeSortShuffleWriter.java` | Hash-based writer. Creates one `CometDiskBlockWriter` per partition. Supports async writes. |
| `CometUnsafeShuffleWriter` | `.../shuffle/CometUnsafeShuffleWriter.java` | Sort-based writer. Uses `CometShuffleExternalSorter` to buffer and sort records, then merges spill files. |
| `CometDiskBlockWriter` | `.../shuffle/CometDiskBlockWriter.java` | Buffers rows in memory pages for a single partition. Spills to disk via native encoding. Used by bypass writer. |
| `CometShuffleExternalSorter` | `.../shuffle/sort/CometShuffleExternalSorter.java` | Buffers records across all partitions, sorts by partition ID, spills sorted data. Used by unsafe writer. |
| `SpillWriter` | `.../shuffle/SpillWriter.java` | Base class for spill logic. Manages memory pages and calls `Native.writeSortedFileNative()` for Arrow IPC encoding. |

### Reader

| Class | Location | Description |
|-------|----------|-------------|
| `CometBlockStoreShuffleReader` | `.../shuffle/CometBlockStoreShuffleReader.scala` | Fetches shuffle blocks via `ShuffleBlockFetcherIterator`. Decodes Arrow IPC to `ColumnarBatch`. |
| `NativeBatchDecoderIterator` | `.../shuffle/NativeBatchDecoderIterator.scala` | Reads compressed Arrow IPC batches from input stream. Calls `Native.decodeShuffleBlock()` via JNI. |

## Data Flow

### Write Path

1. `ShuffleWriteProcessor` calls `CometShuffleManager.getWriter()`
2. Writer receives `Iterator[Product2[K, V]]` where V is `UnsafeRow`
3. Rows are serialized and buffered in off-heap memory pages
4. When memory threshold or batch size is reached, `SpillWriter.doSpilling()` is called
5. Native code (`Native.writeSortedFileNative()`) converts rows to Arrow arrays and writes IPC format
6. For bypass writer: partition files are concatenated into final output
7. For sort writer: spill files are merged

### Read Path

1. `CometBlockStoreShuffleReader.read()` creates `ShuffleBlockFetcherIterator`
2. For each block, `NativeBatchDecoderIterator` reads the IPC stream
3. Native code (`Native.decodeShuffleBlock()`) decompresses and decodes to Arrow arrays
4. Arrow FFI imports arrays as `ColumnarBatch`

## Memory Management

- `CometShuffleMemoryAllocator`: Custom allocator for off-heap memory pages
- Memory is allocated in pages; when allocation fails, writers spill to disk
- `CometDiskBlockWriter` coordinates spilling across all partition writers (largest first)
- Async spilling is supported via `ShuffleThreadPool`

## Configuration

| Config | Description |
|--------|-------------|
| `spark.comet.columnar.shuffle.async.enabled` | Enable async spill writes |
| `spark.comet.columnar.shuffle.async.thread.num` | Threads per writer for async |
| `spark.comet.columnar.shuffle.batch.size` | Rows per Arrow batch |
| `spark.comet.columnar.shuffle.spill.threshold` | Row count threshold for spill |
| `spark.comet.exec.shuffle.compression.codec` | Compression codec (zstd, lz4, etc.) |
