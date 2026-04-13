# Sort-Based Shuffle Repartitioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a sort-based shuffle repartitioner that partitions each batch on-the-fly using counting sort + Arrow `take`/`slice`, avoiding per-partition builder memory overhead.

**Architecture:** A new `SortBasedPartitioner` implements `ShufflePartitioner`. On each `insert_batch`, it computes partition IDs, counting-sorts the batch by partition, slices it into per-partition sub-batches, and writes compressed IPC blocks to per-partition `PartitionWriter`s. Memory usage is O(batch_size) not O(batch_size x num_partitions). On `shuffle_write`, it concatenates per-partition spill files into the final data+index files.

**Tech Stack:** Rust, Arrow (`arrow::compute::take`), existing shuffle infrastructure (`ShuffleBlockWriter`, `PartitionWriter`, `BufBatchWriter`)

---

### Task 1: Create `SortBasedPartitioner` struct and constructor

**Files:**
- Create: `native/shuffle/src/partitioners/sort_based.rs`
- Modify: `native/shuffle/src/partitioners/mod.rs`

- [ ] **Step 1: Create the module file with struct definition**

Create `native/shuffle/src/partitioners/sort_based.rs`:

```rust
use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::ShufflePartitioner;
use crate::writers::PartitionWriter;
use crate::{comet_partitioning, CometPartitioning, CompressionCodec, ShuffleBlockWriter};
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::sync::Arc;
use tokio::time::Instant;

/// A shuffle repartitioner that sorts each batch by partition ID using counting sort,
/// then slices and writes per-partition sub-batches immediately. This avoids
/// per-partition Arrow builders, so memory usage is O(batch_size) regardless of
/// partition count.
pub(crate) struct SortBasedPartitioner {
    output_data_file: String,
    output_index_file: String,
    schema: SchemaRef,
    /// One writer per output partition (manages spill files)
    partition_writers: Vec<PartitionWriter>,
    shuffle_block_writer: ShuffleBlockWriter,
    partitioning: CometPartitioning,
    runtime: Arc<RuntimeEnv>,
    metrics: ShufflePartitionerMetrics,
    /// The configured batch size
    batch_size: usize,
    /// Memory reservation for tracking
    reservation: MemoryReservation,
    /// Size of the write buffer in bytes
    write_buffer_size: usize,
    /// Reusable scratch buffers
    hashes_buf: Vec<u32>,
    partition_ids: Vec<u32>,
    sorted_indices: Vec<u32>,
    /// Partition boundary offsets: partition_starts[i]..partition_starts[i+1]
    /// gives the range in sorted_indices for partition i
    partition_starts: Vec<usize>,
}
```

- [ ] **Step 2: Implement constructor**

Add to the same file:

```rust
impl SortBasedPartitioner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        partition: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: CometPartitioning,
        metrics: ShufflePartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        codec: CompressionCodec,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let num_output_partitions = partitioning.partition_count();

        let shuffle_block_writer =
            ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;

        let partition_writers = (0..num_output_partitions)
            .map(|_| PartitionWriter::try_new(shuffle_block_writer.clone()))
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let reservation =
            MemoryConsumer::new(format!("SortBasedPartitioner[{partition}]"))
                .register(&runtime.memory_pool);

        let hashes_buf = match partitioning {
            CometPartitioning::Hash(_, _) | CometPartitioning::RoundRobin(_, _) => {
                vec![0u32; batch_size]
            }
            _ => vec![],
        };

        Ok(Self {
            output_data_file,
            output_index_file,
            schema,
            partition_writers,
            shuffle_block_writer,
            partitioning,
            runtime,
            metrics,
            batch_size,
            reservation,
            write_buffer_size,
            hashes_buf,
            partition_ids: vec![0u32; batch_size],
            sorted_indices: vec![0u32; batch_size],
            partition_starts: vec![0usize; num_output_partitions + 1],
        })
    }
}
```

- [ ] **Step 3: Register module in mod.rs**

Modify `native/shuffle/src/partitioners/mod.rs` — add after `mod single_partition;`:

```rust
mod sort_based;
```

And add to the pub use section:

```rust
pub(crate) use sort_based::SortBasedPartitioner;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build --manifest-path native/Cargo.toml 2>&1 | tail -5`
Expected: Compiles (with dead code warnings, which is fine)

- [ ] **Step 5: Commit**

```bash
git add native/shuffle/src/partitioners/sort_based.rs native/shuffle/src/partitioners/mod.rs
git commit -m "feat: add SortBasedPartitioner struct and constructor"
```

---

### Task 2: Implement partition ID computation and counting sort

**Files:**
- Modify: `native/shuffle/src/partitioners/sort_based.rs`

- [ ] **Step 1: Add the counting sort method**

This method computes partition IDs for each row, then uses counting sort to produce sorted indices and partition boundary offsets. Add to the `impl SortBasedPartitioner` block:

```rust
    /// Compute partition IDs for each row and counting-sort the indices by partition.
    /// After this call:
    /// - `self.sorted_indices[self.partition_starts[p]..self.partition_starts[p+1]]`
    ///   contains the row indices belonging to partition `p`.
    fn compute_partition_ids_and_sort(
        &mut self,
        input: &RecordBatch,
    ) -> datafusion::common::Result<()> {
        let num_rows = input.num_rows();
        let num_partitions = self.partitioning.partition_count();

        match &self.partitioning {
            CometPartitioning::Hash(exprs, num_output_partitions) => {
                let arrays = exprs
                    .iter()
                    .map(|expr| expr.evaluate(input)?.into_array(num_rows))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;

                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&arrays, hashes_buf)?;

                let partition_ids = &mut self.partition_ids[..num_rows];
                for (idx, hash) in hashes_buf.iter().enumerate() {
                    partition_ids[idx] =
                        comet_partitioning::pmod(*hash, *num_output_partitions) as u32;
                }
            }
            CometPartitioning::RangePartitioning(
                lex_ordering,
                _num_output_partitions,
                row_converter,
                bounds,
            ) => {
                let arrays = lex_ordering
                    .iter()
                    .map(|expr| expr.expr.evaluate(input)?.into_array(num_rows))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;

                let row_batch = row_converter.convert_columns(arrays.as_slice())?;
                let partition_ids = &mut self.partition_ids[..num_rows];
                row_batch.iter().enumerate().for_each(|(row_idx, row)| {
                    partition_ids[row_idx] = bounds
                        .as_slice()
                        .partition_point(|bound| bound.row() <= row)
                        as u32;
                });
            }
            CometPartitioning::RoundRobin(num_output_partitions, max_hash_columns) => {
                let num_columns_to_hash = if *max_hash_columns == 0 {
                    input.num_columns()
                } else {
                    (*max_hash_columns).min(input.num_columns())
                };
                let columns_to_hash: Vec<ArrayRef> = (0..num_columns_to_hash)
                    .map(|i| Arc::clone(input.column(i)))
                    .collect();

                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&columns_to_hash, hashes_buf)?;

                let partition_ids = &mut self.partition_ids[..num_rows];
                for (idx, hash) in hashes_buf.iter().enumerate() {
                    partition_ids[idx] =
                        comet_partitioning::pmod(*hash, *num_output_partitions) as u32;
                }
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported shuffle partitioning scheme {other:?}"
                )));
            }
        }

        // Counting sort: count rows per partition
        let partition_starts = &mut self.partition_starts[..num_partitions + 1];
        partition_starts.fill(0);
        let partition_ids = &self.partition_ids[..num_rows];
        for &pid in partition_ids.iter() {
            partition_starts[pid as usize + 1] += 1;
        }

        // Prefix sum to get start offsets
        for i in 1..=num_partitions {
            partition_starts[i] += partition_starts[i - 1];
        }

        // Place indices into sorted order
        let sorted_indices = &mut self.sorted_indices[..num_rows];
        // We need a mutable copy of the starts to use as cursors
        // Use partition_starts as-is, advancing each partition's cursor
        let mut cursors = partition_starts.to_vec();
        for (row_idx, &pid) in partition_ids.iter().enumerate() {
            let pos = cursors[pid as usize];
            sorted_indices[pos] = row_idx as u32;
            cursors[pid as usize] += 1;
        }

        Ok(())
    }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --manifest-path native/Cargo.toml 2>&1 | tail -5`
Expected: Compiles

- [ ] **Step 3: Commit**

```bash
git add native/shuffle/src/partitioners/sort_based.rs
git commit -m "feat: add partition ID computation and counting sort"
```

---

### Task 3: Implement `insert_batch` — sort, slice, and write per-partition

**Files:**
- Modify: `native/shuffle/src/partitioners/sort_based.rs`

- [ ] **Step 1: Add the process_batch method**

This is the core method that sorts a single batch by partition and writes each partition's slice. Add to the `impl SortBasedPartitioner` block:

```rust
    /// Process a single batch: compute partition IDs, sort, slice by partition,
    /// and write each slice to the corresponding partition writer.
    fn process_batch(&mut self, input: RecordBatch) -> datafusion::common::Result<()> {
        if input.num_rows() == 0 {
            return Ok(());
        }

        let num_rows = input.num_rows();
        let num_partitions = self.partitioning.partition_count();

        // Update metrics
        self.metrics.data_size.add(input.get_array_memory_size());
        self.metrics.baseline.record_output(num_rows);

        // Phase 1: Compute partition IDs and counting-sort indices
        {
            let mut timer = self.metrics.repart_time.timer();
            self.compute_partition_ids_and_sort(&input)?;
            timer.stop();
        }

        // Phase 2: Reorder the batch using take, then slice by partition boundaries
        let sorted_indices = &self.sorted_indices[..num_rows];
        let partition_starts = &self.partition_starts[..num_partitions + 1];

        let indices_array = UInt32Array::from_iter_values(sorted_indices.iter().copied());
        let sorted_batch = RecordBatch::try_new(
            input.schema(),
            input
                .columns()
                .iter()
                .map(|col| take(col, &indices_array, None))
                .collect::<Result<Vec<_>, _>>()?,
        )?;

        // Phase 3: Slice the sorted batch at partition boundaries and write each slice
        for partition_id in 0..num_partitions {
            let start = partition_starts[partition_id];
            let end = partition_starts[partition_id + 1];
            let len = end - start;
            if len == 0 {
                continue;
            }

            let partition_batch = sorted_batch.slice(start, len);
            self.partition_writers[partition_id].spill_batch(
                &partition_batch,
                &self.runtime,
                &self.metrics,
                self.write_buffer_size,
                self.batch_size,
            )?;
        }

        Ok(())
    }
```

- [ ] **Step 2: Add `spill_batch` method to `PartitionWriter`**

Modify `native/shuffle/src/writers/spill.rs` to add a method that writes a single batch (not an iterator). Add after the existing `spill` method:

```rust
    /// Write a single batch to this partition's spill file.
    pub(crate) fn spill_batch(
        &mut self,
        batch: &RecordBatch,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
        write_buffer_size: usize,
        batch_size: usize,
    ) -> datafusion::common::Result<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        self.ensure_spill_file_created(runtime)?;

        let mut buf_batch_writer = BufBatchWriter::new(
            &mut self.shuffle_block_writer,
            &mut self.spill_file.as_mut().unwrap().file,
            write_buffer_size,
            batch_size,
        );
        let bytes_written =
            buf_batch_writer.write(batch, &metrics.encode_time, &metrics.write_time)?;
        buf_batch_writer.flush(&metrics.encode_time, &metrics.write_time)?;
        Ok(bytes_written)
    }
```

- [ ] **Step 3: Implement the `ShufflePartitioner` trait**

Add to `sort_based.rs`:

```rust
#[async_trait::async_trait]
impl ShufflePartitioner for SortBasedPartitioner {
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        let start_time = Instant::now();
        let mut start = 0;
        while start < batch.num_rows() {
            let end = (start + self.batch_size).min(batch.num_rows());
            let slice = batch.slice(start, end - start);
            self.process_batch(slice)?;
            start = end;
        }
        self.metrics.input_batches.add(1);
        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }

    fn shuffle_write(&mut self) -> datafusion::common::Result<()> {
        let start_time = Instant::now();
        let num_output_partitions = self.partition_writers.len();
        let mut offsets = vec![0i64; num_output_partitions + 1];

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        let output_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_file)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

        let mut output_data = BufWriter::new(output_data);

        for i in 0..num_output_partitions {
            offsets[i] = output_data.stream_position()? as i64;

            // Copy spill file contents to final data file
            if let Some(spill_path) = self.partition_writers[i].path() {
                let mut spill_file = File::open(spill_path)?;
                let mut write_timer = self.metrics.write_time.timer();
                std::io::copy(&mut spill_file, &mut output_data)?;
                write_timer.stop();
            }
        }

        let mut write_timer = self.metrics.write_time.timer();
        output_data.flush()?;
        write_timer.stop();

        offsets[num_output_partitions] = output_data.stream_position()? as i64;

        // Write index file
        let mut write_timer = self.metrics.write_time.timer();
        let mut output_index =
            BufWriter::new(File::create(index_file).map_err(|e| {
                DataFusionError::Execution(format!("shuffle write error: {e:?}"))
            })?);
        for offset in offsets {
            output_index.write_all(&offset.to_le_bytes()[..])?;
        }
        output_index.flush()?;
        write_timer.stop();

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());

        Ok(())
    }
}

impl Debug for SortBasedPartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SortBasedPartitioner")
            .field("partitions", &self.partition_writers.len())
            .finish()
    }
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build --manifest-path native/Cargo.toml 2>&1 | tail -5`
Expected: Compiles

- [ ] **Step 5: Commit**

```bash
git add native/shuffle/src/partitioners/sort_based.rs native/shuffle/src/writers/spill.rs
git commit -m "feat: implement insert_batch and shuffle_write for SortBasedPartitioner"
```

---

### Task 4: Wire up `SortBasedPartitioner` in the shuffle writer

**Files:**
- Modify: `native/shuffle/src/shuffle_writer.rs`

- [ ] **Step 1: Add sort-based mode to the shuffle_write function**

In `native/shuffle/src/shuffle_writer.rs`, the `shuffle_write` function (line ~196) currently constructs a `MultiPartitionShuffleRepartitioner` as the default for multi-partition cases. We need to add a `sort_based` boolean parameter and use `SortBasedPartitioner` when true.

First, update the `ShuffleWriterExec` struct to include the new flag. Add field after `write_buffer_size`:

```rust
    /// Whether to use sort-based repartitioning
    sort_based: bool,
```

Update `try_new` to accept the new parameter (add `sort_based: bool` as the last parameter) and store it in the struct.

Update `with_children` to pass `self.sort_based` through.

Then modify the `shuffle_write` function signature to add `sort_based: bool` parameter, and update the match arm for multi-partition:

```rust
        _ if sort_based => Box::new(SortBasedPartitioner::try_new(
            partition,
            output_data_file,
            output_index_file,
            Arc::clone(&schema),
            partitioning,
            metrics,
            context.runtime_env(),
            context.session_config().batch_size(),
            codec,
            write_buffer_size,
        )?),
        _ => Box::new(MultiPartitionShuffleRepartitioner::try_new(
```

Add the import at the top of the file:

```rust
use crate::partitioners::SortBasedPartitioner;
```

- [ ] **Step 2: Update existing tests to pass `sort_based: false`**

All existing `ShuffleWriterExec::try_new` calls in tests need the new `false` parameter added.

- [ ] **Step 3: Verify it compiles and existing tests pass**

Run: `cargo test --manifest-path native/Cargo.toml -p datafusion-comet-shuffle 2>&1 | tail -20`
Expected: All existing tests pass

- [ ] **Step 4: Commit**

```bash
git add native/shuffle/src/shuffle_writer.rs
git commit -m "feat: wire SortBasedPartitioner into shuffle writer"
```

---

### Task 5: Add tests for sort-based partitioner

**Files:**
- Modify: `native/shuffle/src/shuffle_writer.rs` (test module)

- [ ] **Step 1: Create a parameterized test helper**

Add a new test helper that runs the existing `shuffle_write_test` logic but with `sort_based: true`. The simplest approach is to parameterize the existing `shuffle_write_test` function:

Rename the existing `shuffle_write_test` to accept a `sort_based` parameter:

```rust
    fn shuffle_write_test(
        batch_size: usize,
        num_batches: usize,
        num_partitions: usize,
        memory_limit: Option<usize>,
        sort_based: bool,
    ) {
```

And pass it through to `ShuffleWriterExec::try_new`.

Update all existing callers to pass `false`.

- [ ] **Step 2: Add sort-based test cases**

Add new tests that mirror the existing ones but with `sort_based: true`:

```rust
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_sort_based_basic() {
        shuffle_write_test(1000, 100, 1, None, true);
        shuffle_write_test(10000, 10, 1, None, true);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_sort_based_insert_larger_batch() {
        shuffle_write_test(10000, 1, 16, true);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_sort_based_insert_smaller_batch() {
        shuffle_write_test(1000, 1, 16, None, true);
        shuffle_write_test(1000, 10, 16, None, true);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_sort_based_large_number_of_partitions() {
        shuffle_write_test(10000, 10, 200, Some(10 * 1024 * 1024), true);
        shuffle_write_test(10000, 10, 2000, Some(10 * 1024 * 1024), true);
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test --manifest-path native/Cargo.toml -p datafusion-comet-shuffle 2>&1 | tail -20`
Expected: All tests pass (both existing buffered and new sort-based)

- [ ] **Step 4: Commit**

```bash
git add native/shuffle/src/shuffle_writer.rs
git commit -m "test: add sort-based partitioner tests"
```

---

### Task 6: Add sort-based mode to benchmark binary

**Files:**
- Modify: `native/shuffle/src/bin/shuffle_bench.rs`

- [ ] **Step 1: Update benchmark to support sort-based mode**

The existing benchmark already has a `--mode` CLI arg. Add `"sort"` as a new mode option. Find where mode is parsed and `ShuffleWriterExec` is created, and pass `sort_based: true` when mode is `"sort"`.

Search for where `ShuffleWriterExec::try_new` is called in the benchmark file and add the `sort_based` parameter:

```rust
let sort_based = args.mode == "sort";
```

Pass `sort_based` to `ShuffleWriterExec::try_new`.

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --release --features shuffle-bench --bin shuffle_bench --manifest-path native/Cargo.toml 2>&1 | tail -5`
Expected: Compiles

- [ ] **Step 3: Commit**

```bash
git add native/shuffle/src/bin/shuffle_bench.rs
git commit -m "feat: add sort mode to shuffle benchmark"
```

---

### Task 7: Wire up JVM side — add config and pass through to native

**Files:**
- Modify: `common/src/main/java/org/apache/comet/CometConf.scala` (add config)
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala` (pass config)
- Modify: `native/core/src/execution/planner.rs` (receive and forward)

- [ ] **Step 1: Add config option in CometConf.scala**

Find the existing shuffle-related configs (search for `SHUFFLE` or `shuffle`). Add a new config:

```scala
  val COMET_SHUFFLE_SORT_BASED: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.shuffle.sort_based")
      .doc(
        "When enabled, uses sort-based repartitioning for native shuffle. " +
        "This avoids per-partition memory overhead from builders, making it more " +
        "memory-efficient for large partition counts. Default is false (uses buffered mode).")
      .booleanConf
      .createWithDefault(false)
```

- [ ] **Step 2: Pass config through CometNativeShuffleWriter**

In `CometNativeShuffleWriter.scala`, find where `ShuffleWriterExec` parameters are passed to native via protobuf or JNI. Add `sort_based` to the serialized plan.

- [ ] **Step 3: Receive in planner.rs**

In `native/core/src/execution/planner.rs`, find where `ShuffleWriterExec` is constructed. Extract the `sort_based` field from the protobuf and pass it to `ShuffleWriterExec::try_new`.

- [ ] **Step 4: Update protobuf if needed**

If the shuffle writer parameters are passed via protobuf (`native/proto/src/`), add a `sort_based` field to the relevant message.

- [ ] **Step 5: Build and verify**

Run: `make core`
Expected: Compiles

- [ ] **Step 6: Commit**

```bash
git add common/src/main/java/org/apache/comet/CometConf.scala \
        spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala \
        native/core/src/execution/planner.rs
git commit -m "feat: add sort_based shuffle config and JVM-native plumbing"
```

---

### Task 8: Run clippy and format

**Files:**
- Potentially any Rust files modified above

- [ ] **Step 1: Run cargo fmt**

Run: `cd native && cargo fmt`

- [ ] **Step 2: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings 2>&1 | tail -20`
Expected: No warnings

- [ ] **Step 3: Fix any issues and commit**

```bash
git add -A
git commit -m "chore: fix formatting and clippy warnings"
```

---

### Task 9: Run full test suite and benchmarks

**Files:** None (verification only)

- [ ] **Step 1: Run Rust tests**

Run: `cargo test --manifest-path native/Cargo.toml -p datafusion-comet-shuffle`
Expected: All tests pass

- [ ] **Step 2: Run benchmark comparison**

Run the shuffle benchmark with all three modes and compare:

```bash
./native/target/release/shuffle_bench \
  --input /opt/tpch/sf100/lineitem \
  --limit 100000000 \
  --partitions 200 \
  --codec lz4 \
  --hash-columns 0,3 \
  --memory-limit 4294000000 \
  --mode sort

./native/target/release/shuffle_bench \
  --input /opt/tpch/sf100/lineitem \
  --limit 100000000 \
  --partitions 800 \
  --codec lz4 \
  --hash-columns 0,3 \
  --memory-limit 4294000000 \
  --mode sort
```

Compare throughput and peak memory against `buffered` mode.

- [ ] **Step 3: Run JVM tests**

Run: `make && ./mvnw test -DwildcardSuites="CometNativeShuffle"`
Expected: All shuffle tests pass
