---
name: review-comet-shuffle-pr
description: Use when reviewing a DataFusion Comet pull request that touches native or JVM columnar shuffle, the shuffle writers and readers, partitioning, the Arrow IPC block format, shuffle compression, or the Celeborn integration. Load alongside review-comet-pr.
argument-hint: <pr-number>
---

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

Shuffle-specific review for Comet PR #$ARGUMENTS.

**REQUIRED BACKGROUND:** Use `review-comet-pr` for PR metadata, existing comments, CI, the review
bar, and the output format. This skill only covers shuffle.

## Read the Contributor Guide First

| Doc                                                  | What you need from it                                                  |
| ---------------------------------------------------- | ---------------------------------------------------------------------- |
| `docs/source/contributor-guide/native_shuffle.md`    | Selection rules, architecture, partitioning, block format, spilling    |
| `docs/source/contributor-guide/jvm_shuffle.md`       | Writer variants, handle selection, the row-based path, spill mechanics |
| `docs/source/contributor-guide/memory_management.md` | Where shuffle memory comes from, which differs between the two paths   |

**Read both shuffle docs even if the PR only touches one path.** The two implementations share the
manager, the dependency, the reader, and the on-disk format, and a change to one side of a shared
piece is the most common way to break the other.

## 1. Which Implementation

| Implementation                        | Selected when                                                                                                  |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Native, `CometExchange`               | `shuffle.mode` is `native` or `auto`, child is a `CometPlan`, supported partitioning, primitive partition keys |
| JVM columnar, `CometColumnarExchange` | `shuffle.mode` is `jvm`, or the child is row-based, or partition keys are complex types                        |

Complex types are fully supported as **data** columns in both. The primitive-only restriction
applies to **partition keys** for `HashPartitioning` and `RangePartitioning` only.

- [ ] A PR that widens what native shuffle supports updates the fallback conditions in
      `CometShuffleExchangeExec` **and** both docs' "When X is Used" lists
- [ ] A PR that narrows support does not silently move workloads onto the slower path. The JVM path
      costs a columnar to row to columnar round trip through `ColumnarToRowExec`.
- [ ] Fallback decisions stay consistent across a stage. `CometShuffleFallbackStickinessSuite`
      exists because they did not once.

## 2. Spark Compatibility of Partitioning

Partitioning is where shuffle silently produces wrong answers rather than failing.

- [ ] **Hash partitioning uses Murmur3 with seed 42** and `partition_id = hash % num_partitions`,
      matching Spark. Any change to the hash, the seed, or the modulo changes which rows land in
      which partition, which breaks a join between a Comet-shuffled side and a Spark-shuffled side.
- [ ] **Round robin is hash-based on purpose.** Comet assigns partitions from a Murmur3 hash rather
      than cycling row by row, because determinism across task retries is required for correctness
      under fault tolerance. A PR that implements "true" round robin to fix skew breaks that. The
      known cost is that low-cardinality data distributes unevenly, and that is the accepted
      trade-off.
- [ ] **Range partitioning bounds come from the driver.** Spark's `RangePartitioner` samples and
      computes boundaries, they are serialized into the native plan, and native does a binary
      search over comparable-row-format keys. A change to the comparison or the row encoding must
      match Spark's ordering exactly, including nulls and signed zero.
- [ ] The JVM path uses Spark's own partitioner via `partitioner.getPartition(key)`, so it inherits
      Spark's semantics for free. A PR that reimplements partitioning on that path is solving a
      problem that does not exist.

## 3. On-Disk and On-Wire Format

Writer and reader must change together, and they are in different languages.

The block layout is an 8-byte compressed length header, an 8-byte field count header, then the
compressed Arrow IPC stream. It is written by the native `ShuffleBlockWriter` and read by
`NativeBatchDecoderIterator` calling `Native.decodeShuffleBlock()`.

- [ ] A format change updates the writer, the reader, and the Celeborn reader path
- [ ] A format change is not silently incompatible with shuffle files written by a previous version
      in the same cluster during a rolling deployment. If it is, the PR needs to say so.
- [ ] Compression codec changes apply uniformly to all partitions, and each partition stays
      independently decompressible so reads can parallelize
- [ ] The commit path still works. Native records the byte offset where each partition begins plus
      the total length, `CometNativeShuffleWriter` fetches them with
      `Native.getShufflePartitionOffsets`, converts them to partition lengths, and commits through
      Spark's `IndexShuffleBlockResolver.writeMetadataFileAndCommit`. Offsets and lengths are easy
      to confuse and the failure is a corrupt index file rather than an exception.
- [ ] Checksums via `CometShuffleChecksumSupport` still cover what Spark expects

## 4. Memory and Spilling

Shuffle is the largest memory consumer in most queries, and the two paths draw from different
budgets.

**Native shuffle** uses the DataFusion memory pool. Partitions spill when the pool denies an
allocation, or when buffered bytes reach `spark.comet.shuffle.native.maxBufferBytes`, which
defaults to `0`, meaning the fixed limit is disabled and memory pressure is the only trigger. Each
partition has its own spill file and multiple spills for a partition are concatenated when the
final output is written.

**JVM shuffle** allocates off-heap pages through `CometShuffleMemoryAllocator`, which is an
ordinary Spark `MemoryConsumer` drawing from `spark.memory.offHeap.size`. `CometDiskBlockWriter`
coordinates spilling across partition writers, largest first. `TooLargePageException` is the signal
that a single record does not fit in a page.

- [ ] A PR that adds buffering on either path says where the reservation is
- [ ] A PR that changes spill thresholds has benchmark evidence, because spilling too late is an
      OOM and spilling too early is a throughput loss
- [ ] Scratch buffers reused for partition-id computation are correctly reset between batches
- [ ] Use `review-comet-memory-pr` as well for anything touching reservations or the allocator

## 5. Writer Selection on the JVM Path

`CometShuffleManager.shouldBypassMergeSort()` picks between the two JVM writers. It uses bypass if
the partition count is below the threshold **and** partitions times cores is within the max thread
count, otherwise sort-based to avoid OOM from too many concurrent writers.

| Handle                              | Writer                                                                        |
| ----------------------------------- | ----------------------------------------------------------------------------- |
| `CometBypassMergeSortShuffleHandle` | `CometBypassMergeSortShuffleWriter`, one `CometDiskBlockWriter` per partition |
| `CometSerializedShuffleHandle`      | `CometUnsafeShuffleWriter`, via `CometShuffleExternalSorter`                  |
| `CometNativeShuffleHandle`          | `CometNativeShuffleWriter`                                                    |

A change to the selection heuristic changes the memory profile of every job with a large partition
count. Ask for the reasoning and the numbers.

## 6. Source Layout

Native shuffle lives in its own crate, `datafusion-comet-shuffle`, under `native/shuffle/`, with
`shuffle_writer.rs`, `comet_partitioning.rs`, `ipc.rs`, the `partitioners/` and `writers/` modules,
and benchmarks in `native/shuffle/benches/`. The JVM side is split between
`spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/`, the Java writers under
`spark/src/main/java/org/apache/spark/sql/comet/execution/shuffle/`, and the allocators under
`spark/src/main/java/org/apache/spark/shuffle/comet/`.

**Verify the paths cited in the docs still exist.** The native shuffle code has moved between
crates before, and the "Key Classes" tables in the docs are exactly what goes stale when it does.

## 7. Tests

| Suite                                                               | Covers                                           |
| ------------------------------------------------------------------- | ------------------------------------------------ |
| `org.apache.comet.exec.CometNativeShuffleSuite`                     | Native shuffle end to end                        |
| `org.apache.comet.exec.CometColumnarShuffleSuite`                   | JVM columnar shuffle end to end                  |
| `CometShuffle4_0Suite`                                              | Spark 4.x specific behavior                      |
| `CometDiskBlockWriterSuite`                                         | JVM spill and page handling                      |
| `NativeBatchDecoderIteratorLifecycleChecks`, `...ConcurrencyChecks` | Reader lifetime and concurrency                  |
| `CometNativeShuffleInputRDDSuite`                                   | The scheduling-anchor RDD                        |
| `CometCeleborn*Suite`                                               | The Celeborn path, which is easy to forget       |
| `CometShuffleBenchmark`                                             | Throughput, needs `-Dspark.comet.memoryOverhead` |

Ask specifically:

- [ ] Is the **other** shuffle path tested, if the change touched anything shared?
- [ ] Is the Celeborn path tested, if the change touched the manager, dependency, writer, or reader?
- [ ] Are multiple partitions and an actual spill exercised, rather than one small batch?
- [ ] Are complex types covered as data columns, and is the primitive-key restriction tested at its
      boundary?
- [ ] For a partitioning change, is there a test that the same input produces the same partition
      assignment as Spark, not just that the total row count matches?

## 8. Do the PR Changes Make the Shuffle Docs Stale?

Both docs are structured as tables of current fact, which is exactly what a refactor invalidates.
Check:

- **`native_shuffle.md`**: the "When Native Shuffle is Used" conditions, the architecture diagram,
  the Scala Side and Rust Side "Key Classes" tables including their file paths, the write-path and
  read-path numbered steps, the Partitioning sections and the guarantees they state, the
  Compression table, the Configuration table **with its defaults**, and the comparison table at the
  end
- **`jvm_shuffle.md`**: the "When JVM Shuffle is Used" list, the writer architecture diagram, the
  Shuffle Manager, Handles, Writers and Reader tables, the bypass selection description, the Memory
  Management section, and the Configuration table
- A config default changed in `CometConf.scala` but left at its old value in either doc's config
  table is a specific thing to look for, because those tables are hand-maintained while the
  user-facing `configs.md` is generated
- If the PR adds a partitioning scheme, both the native doc's Partitioning section and the
  comparison table's "Supported schemes" row need it
