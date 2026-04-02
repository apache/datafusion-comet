# Shuffle IPC Stream Per Partition — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce shuffle format overhead by writing one Arrow IPC stream per partition (schema once, N batches) instead of one IPC stream per batch, and move IPC stream reading to Rust via JNI InputStream callbacks.

**Architecture:** Replace the custom header format + per-batch IPC stream with standard Arrow IPC streams using built-in body compression (LZ4_FRAME/ZSTD). On the read side, Rust pulls bytes from JVM's InputStream via a `JniInputStream` adapter and uses Arrow's `StreamReader` to decode batches. A native handle pattern (`open`/`next`/`close`) manages the reader lifecycle across JNI calls.

**Tech Stack:** Arrow IPC (`StreamWriter`/`StreamReader` with `IpcWriteOptions`), JNI (`jni` crate), Arrow FFI

**Spec:** `docs/superpowers/specs/2026-04-02-shuffle-ipc-stream-per-partition-design.md`

---

## File Structure

### Rust — Modified

| File | Responsibility |
|---|---|
| `native/Cargo.toml` | Add `ipc_compression` feature to arrow dependency |
| `native/shuffle/Cargo.toml` | Remove `snap` dependency |
| `native/shuffle/src/lib.rs` | Update exports (remove `read_ipc_compressed`, `ShuffleBlockWriter`) |
| `native/shuffle/src/writers/mod.rs` | Remove `shuffle_block_writer` module, remove `ShuffleBlockWriter` export |
| `native/shuffle/src/writers/shuffle_block_writer.rs` | **Delete** — replaced by persistent `StreamWriter` |
| `native/shuffle/src/writers/buf_batch_writer.rs` | Remove `ShuffleBlockWriter` dependency, write directly via `StreamWriter` |
| `native/shuffle/src/writers/spill.rs` | Remove `ShuffleBlockWriter` dependency, write via `StreamWriter` |
| `native/shuffle/src/ipc.rs` | Replace `read_ipc_compressed` with `JniInputStream` and stream reader handle |
| `native/shuffle/src/partitioners/immediate_mode.rs` | `PartitionOutputStream`: persistent `StreamWriter`, Arrow-native compression |
| `native/shuffle/src/partitioners/single_partition.rs` | Use persistent `StreamWriter` directly |
| `native/shuffle/src/partitioners/multi_partition.rs` | Use persistent `StreamWriter` per partition |
| `native/shuffle/src/shuffle_writer.rs` | Update `CompressionCodec` → Arrow IPC `CompressionType`, update tests |
| `native/core/src/execution/jni_api.rs` | Replace `decodeShuffleBlock` with `openShuffleStream`/`nextShuffleStreamBatch`/`closeShuffleStream` |

### Scala — Modified

| File | Responsibility |
|---|---|
| `spark/src/main/scala/org/apache/comet/Native.scala` | Replace `decodeShuffleBlock` with new JNI method declarations |
| `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/NativeBatchDecoderIterator.scala` | Use native handle pattern, remove header parsing |
| `common/src/main/scala/org/apache/comet/CometConf.scala` | Remove `snappy` from codec options |

---

## Task 1: Enable Arrow IPC Compression Feature

**Files:**
- Modify: `native/Cargo.toml:37`

- [ ] **Step 1: Add `ipc_compression` feature to arrow dependency**

In `native/Cargo.toml`, change line 37 from:
```toml
arrow = { version = "57.3.0", features = ["prettyprint", "ffi", "chrono-tz"] }
```
to:
```toml
arrow = { version = "57.3.0", features = ["prettyprint", "ffi", "chrono-tz", "ipc_compression"] }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check --manifest-path native/Cargo.toml`
Expected: Compilation succeeds with no errors.

- [ ] **Step 3: Commit**

```bash
git add native/Cargo.toml
git commit -m "feat: enable Arrow IPC compression feature for shuffle format"
```

---

## Task 2: Replace `CompressionCodec` with Arrow IPC Compression

The existing `CompressionCodec` enum in `shuffle_block_writer.rs` manages Snappy/LZ4/Zstd/None with external compression wrappers. Replace it with a thin wrapper around Arrow IPC's `CompressionType` so the `StreamWriter` handles compression internally.

**Files:**
- Modify: `native/shuffle/src/writers/shuffle_block_writer.rs` (will be replaced with just the codec enum)
- Modify: `native/shuffle/src/writers/mod.rs`
- Modify: `native/shuffle/src/lib.rs`

- [ ] **Step 1: Replace `CompressionCodec` in `shuffle_block_writer.rs`**

Replace the entire contents of `native/shuffle/src/writers/shuffle_block_writer.rs` with:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::CompressionType;

/// Compression codec for shuffle IPC streams.
///
/// Maps to Arrow IPC's built-in body compression. Each record batch's data
/// buffers are individually compressed by the `StreamWriter`.
#[derive(Debug, Clone)]
pub enum CompressionCodec {
    None,
    Lz4Frame,
    Zstd(i32),
}

impl CompressionCodec {
    /// Create `IpcWriteOptions` with the appropriate compression setting.
    pub fn ipc_write_options(&self) -> datafusion::error::Result<IpcWriteOptions> {
        let compression = match self {
            CompressionCodec::None => None,
            CompressionCodec::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            CompressionCodec::Zstd(_) => Some(CompressionType::ZSTD),
        };
        IpcWriteOptions::try_with_compression(8, false, arrow::ipc::MetadataVersion::V5, compression)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::from(e), None))
    }
}
```

- [ ] **Step 2: Update `writers/mod.rs` exports**

Replace the contents of `native/shuffle/src/writers/mod.rs`:

```rust
// (keep license header)

mod buf_batch_writer;
mod checksum;
mod shuffle_block_writer;
mod spill;

pub(crate) use buf_batch_writer::BufBatchWriter;
pub(crate) use checksum::Checksum;
pub use shuffle_block_writer::CompressionCodec;
pub(crate) use spill::PartitionWriter;
```

Note: `ShuffleBlockWriter` is no longer exported — only `CompressionCodec` remains.

- [ ] **Step 3: Update `lib.rs` exports**

In `native/shuffle/src/lib.rs`, change:
```rust
pub use writers::{CompressionCodec, ShuffleBlockWriter};
```
to:
```rust
pub use writers::CompressionCodec;
```

Also remove the `read_ipc_compressed` export:
```rust
pub use ipc::read_ipc_compressed;
```
becomes:
```rust
// (remove this line entirely — will be replaced in Task 6)
```

- [ ] **Step 4: Verify it compiles (expect errors in dependent code)**

Run: `cargo check --manifest-path native/Cargo.toml 2>&1 | head -50`
Expected: Errors in files that still reference `ShuffleBlockWriter` and `read_ipc_compressed`. This is expected — we'll fix them in subsequent tasks.

- [ ] **Step 5: Commit**

```bash
git add native/shuffle/src/writers/shuffle_block_writer.rs native/shuffle/src/writers/mod.rs native/shuffle/src/lib.rs
git commit -m "refactor: replace ShuffleBlockWriter with CompressionCodec wrapper for Arrow IPC"
```

---

## Task 3: Update `PartitionOutputStream` (Immediate Mode) to Persistent StreamWriter

Currently `PartitionOutputStream` creates a new `StreamWriter` per `write_ipc_block()` call. Change it to hold a persistent `StreamWriter` that writes the schema once and appends record batch messages for subsequent writes.

**Files:**
- Modify: `native/shuffle/src/partitioners/immediate_mode.rs:284-369` (the `PartitionOutputStream` struct and impl)

- [ ] **Step 1: Replace `PartitionOutputStream`**

Replace the `PartitionOutputStream` struct and its `impl` block (lines 284-369) with:

```rust
pub(crate) struct PartitionOutputStream {
    /// Buffer that the StreamWriter writes into.
    buffer: Vec<u8>,
    /// Persistent Arrow IPC stream writer. Created lazily on first write.
    writer: Option<StreamWriter<&'static mut Vec<u8>>>,
    /// IPC write options with compression settings.
    write_options: IpcWriteOptions,
    /// Schema for creating the writer on first use.
    schema: SchemaRef,
}

impl PartitionOutputStream {
    pub(crate) fn try_new(schema: SchemaRef, codec: CompressionCodec) -> Result<Self> {
        let write_options = codec.ipc_write_options()?;
        Ok(Self {
            buffer: Vec::new(),
            writer: None,
            write_options,
            schema,
        })
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<usize> {
        let start_pos = self.buffer.len();

        if self.writer.is_none() {
            // SAFETY: We ensure the buffer outlives the writer by owning both
            // in this struct and only dropping the writer before the buffer.
            let buf_ptr: *mut Vec<u8> = &mut self.buffer;
            let buf_ref: &'static mut Vec<u8> = unsafe { &mut *buf_ptr };
            let w = StreamWriter::try_new_with_options(buf_ref, &self.schema, self.write_options.clone())?;
            self.writer = Some(w);
        }

        self.writer.as_mut().unwrap().write(batch)?;

        Ok(self.buffer.len() - start_pos)
    }

    /// Finish the IPC stream (writes EOS marker) and return the buffer.
    fn finish(&mut self) -> Result<Vec<u8>> {
        if let Some(mut w) = self.writer.take() {
            w.finish()?;
        }
        Ok(std::mem::take(&mut self.buffer))
    }

    fn drain_buffer(&mut self) -> Vec<u8> {
        // When draining mid-stream (for spilling), we take the buffer contents
        // but keep the writer alive. The writer's internal reference still points
        // to self.buffer, which is now empty but still valid.
        std::mem::take(&mut self.buffer)
    }

    fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}
```

**Important:** The unsafe lifetime trick with the buffer pointer is needed because `StreamWriter` borrows the `Write` target. We need the writer to persist across calls while the buffer is drained. An alternative is to use `Cursor<Vec<u8>>` or a shared buffer wrapper — the implementer should evaluate which is cleanest. If the unsafe approach is concerning, consider using `StreamWriter<Vec<u8>>` and calling `into_inner()` when draining.

**Simpler alternative using `StreamWriter<Vec<u8>>`:**

Actually, a cleaner approach: since `Vec<u8>` implements `Write`, we can have the `StreamWriter` own the `Vec<u8>`. When we need to drain, we `finish()` the writer, take the buffer, and set `writer = None` so the next write creates a fresh stream. This means each spill creates a separate IPC stream. On read, the reader sees multiple concatenated IPC streams per partition (from spills), which Arrow `StreamReader` handles — it reads until EOS, then the JVM-side iterator moves to the next stream.

Replace the above with this cleaner version:

```rust
pub(crate) struct PartitionOutputStream {
    /// Persistent Arrow IPC stream writer. Owns the buffer.
    /// None before first write or after a drain (spill).
    writer: Option<StreamWriter<Vec<u8>>>,
    /// IPC write options with compression settings.
    write_options: IpcWriteOptions,
    /// Schema for creating the writer.
    schema: SchemaRef,
    /// Accumulated bytes from finished (drained) streams.
    spilled_bytes: Vec<u8>,
}

impl PartitionOutputStream {
    pub(crate) fn try_new(schema: SchemaRef, codec: CompressionCodec) -> Result<Self> {
        let write_options = codec.ipc_write_options()?;
        Ok(Self {
            writer: None,
            write_options,
            schema,
            spilled_bytes: Vec::new(),
        })
    }

    fn ensure_writer(&mut self) -> Result<()> {
        if self.writer.is_none() {
            let w = StreamWriter::try_new_with_options(
                Vec::new(),
                &self.schema,
                self.write_options.clone(),
            )?;
            self.writer = Some(w);
        }
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<usize> {
        self.ensure_writer()?;
        let w = self.writer.as_mut().unwrap();
        // Get position before write to calculate bytes written
        // StreamWriter writes to Vec<u8> which grows, so track via get_ref().len()
        let before = w.get_ref().len();
        w.write(batch)?;
        let after = w.get_ref().len();
        Ok(after - before)
    }

    /// Finish the current IPC stream and drain all accumulated bytes.
    /// After this call, the next write_batch creates a new IPC stream.
    fn drain_buffer(&mut self) -> Result<Vec<u8>> {
        let mut result = std::mem::take(&mut self.spilled_bytes);
        if let Some(mut w) = self.writer.take() {
            w.finish()?;
            result.extend(w.into_inner()?);
        }
        Ok(result)
    }

    /// Finish the IPC stream and return all bytes (spilled + in-memory).
    fn finish(mut self) -> Result<Vec<u8>> {
        self.drain_buffer()
    }

    fn buffer_len(&self) -> usize {
        self.spilled_bytes.len()
            + self.writer.as_ref().map_or(0, |w| w.get_ref().len())
    }
}
```

- [ ] **Step 2: Update `flush_partition` to use new API**

In `ImmediateModePartitioner::flush_partition` (around line 600), change:
```rust
let ipc_bytes = self.streams[pid].write_ipc_block(&output_batch)?;
```
to:
```rust
let ipc_bytes = self.streams[pid].write_batch(&output_batch)?;
```

- [ ] **Step 3: Update `spill_all` to use `drain_buffer`**

In `ImmediateModePartitioner::spill_all` (around line 622), the drain call changes from:
```rust
let buf = self.streams[pid].drain_buffer();
```
to:
```rust
let buf = self.streams[pid].drain_buffer()?;
```
(It now returns `Result<Vec<u8>>` because it finishes the IPC stream.)

- [ ] **Step 4: Update `shuffle_write` to use `drain_buffer`**

In `ImmediateModePartitioner::shuffle_write` (around line 741), change:
```rust
let buf = self.streams[pid].drain_buffer();
```
to:
```rust
let buf = self.streams[pid].drain_buffer()?;
```

- [ ] **Step 5: Remove old imports**

In `immediate_mode.rs`, remove the `CompressionCodec` import from `crate::` (if it was used for the codec tag matching) and remove `use arrow::ipc::writer::StreamWriter;` at the top — it's now used inside `PartitionOutputStream` via the `StreamWriter` from `arrow::ipc::writer`. Actually, keep the `StreamWriter` import since `PartitionOutputStream` uses it. Remove imports that are no longer needed:
- Remove the old `CompressionCodec` matching arms for `Snappy` codec tags (`b"SNAP"` etc.)

Add new import:
```rust
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
```

- [ ] **Step 6: Update test `test_partition_output_stream_write_and_read`**

Replace the test (around line 856) with:

```rust
#[test]
#[cfg_attr(miri, ignore)]
fn test_partition_output_stream_write_and_read() {
    use arrow::ipc::reader::StreamReader;

    let batch = make_test_batch(&[1, 2, 3, 4, 5]);
    let schema = batch.schema();

    for codec in [
        CompressionCodec::None,
        CompressionCodec::Lz4Frame,
        CompressionCodec::Zstd(1),
    ] {
        let mut stream =
            PartitionOutputStream::try_new(Arc::clone(&schema), codec).unwrap();
        stream.write_batch(&batch).unwrap();
        stream.write_batch(&batch).unwrap(); // write 2 batches

        let buf = stream.finish().unwrap();
        assert!(!buf.is_empty());

        // Read back using standard Arrow IPC StreamReader
        let cursor = std::io::Cursor::new(&buf);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[1].num_rows(), 5);
    }
}
```

- [ ] **Step 7: Run tests**

Run: `cd native && cargo test -p datafusion-comet-shuffle test_partition_output_stream`
Expected: `test_partition_output_stream_write_and_read` passes.

- [ ] **Step 8: Commit**

```bash
git add native/shuffle/src/partitioners/immediate_mode.rs
git commit -m "feat: persistent StreamWriter per partition in immediate mode shuffle"
```

---

## Task 4: Update Buffered Mode (`MultiPartitionShuffleRepartitioner`) Writers

The buffered mode uses `ShuffleBlockWriter` via `PartitionWriter` and `BufBatchWriter`. Replace these with persistent `StreamWriter` per partition.

**Files:**
- Modify: `native/shuffle/src/writers/spill.rs`
- Modify: `native/shuffle/src/writers/buf_batch_writer.rs`
- Modify: `native/shuffle/src/partitioners/multi_partition.rs`

- [ ] **Step 1: Rewrite `PartitionWriter` in `spill.rs`**

Replace the entire contents of `native/shuffle/src/writers/spill.rs` with:

```rust
// (keep license header)

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::Time;
use std::fs::{File, OpenOptions};
use std::io::Write;

use crate::partitioners::PartitionedBatchIterator;
use crate::metrics::ShufflePartitionerMetrics;

/// A temporary disk file for spilling a partition's intermediate shuffle data.
struct SpillFile {
    temp_file: RefCountedTempFile,
    file: File,
}

/// Manages encoding and optional disk spilling for a single shuffle partition.
/// Each partition gets one IPC stream (schema written once), with spills creating
/// additional IPC streams that are concatenated in the final output.
pub(crate) struct PartitionWriter {
    spill_file: Option<SpillFile>,
    schema: SchemaRef,
    write_options: IpcWriteOptions,
}

impl PartitionWriter {
    pub(crate) fn try_new(
        schema: SchemaRef,
        write_options: IpcWriteOptions,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            spill_file: None,
            schema,
            write_options,
        })
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            let spill_file = runtime
                .disk_manager
                .create_tmp_file("shuffle writer spill")?;
            let spill_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(spill_file.path())
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error occurred while spilling {e}"))
                })?;
            self.spill_file = Some(SpillFile {
                temp_file: spill_file,
                file: spill_data,
            });
        }
        Ok(())
    }

    pub(crate) fn spill(
        &mut self,
        iter: &mut PartitionedBatchIterator,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<usize> {
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;

            let file = &mut self.spill_file.as_mut().unwrap().file;
            let mut writer = StreamWriter::try_new_with_options(
                file,
                &self.schema,
                self.write_options.clone(),
            )?;

            let batch = batch?;
            let mut encode_timer = metrics.encode_time.timer();
            writer.write(&batch)?;
            encode_timer.stop();

            for batch in iter {
                let batch = batch?;
                let mut encode_timer = metrics.encode_time.timer();
                writer.write(&batch)?;
                encode_timer.stop();
            }

            let mut encode_timer = metrics.encode_time.timer();
            writer.finish()?;
            let file = writer.into_inner()?;
            file.flush()?;
            encode_timer.stop();

            let bytes_written = file.metadata()
                .map(|m| m.len() as usize)
                .unwrap_or(0);

            Ok(bytes_written)
        } else {
            Ok(0)
        }
    }

    pub(crate) fn path(&self) -> Option<&std::path::Path> {
        self.spill_file
            .as_ref()
            .map(|spill_file| spill_file.temp_file.path())
    }

    #[cfg(test)]
    pub(crate) fn has_spill_file(&self) -> bool {
        self.spill_file.is_some()
    }
}
```

- [ ] **Step 2: Rewrite `BufBatchWriter` in `buf_batch_writer.rs`**

The `BufBatchWriter` now wraps a `StreamWriter` directly instead of a `ShuffleBlockWriter`. Replace the entire contents of `native/shuffle/src/writers/buf_batch_writer.rs` with:

```rust
// (keep license header)

use arrow::array::RecordBatch;
use arrow::compute::kernels::coalesce::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::physical_plan::metrics::Time;
use std::io::Write;

/// Write batches to an Arrow IPC stream with coalescing.
///
/// Small batches are coalesced using Arrow's [`BatchCoalescer`] before serialization,
/// producing exactly `batch_size`-row output batches to reduce per-batch overhead.
/// Uses a persistent `StreamWriter` — schema is written once.
pub(crate) struct BufBatchWriter<W: Write> {
    writer: StreamWriter<W>,
    /// Coalesces small batches into target_batch_size before serialization.
    coalescer: Option<BatchCoalescer>,
    schema: SchemaRef,
    batch_size: usize,
}

impl<W: Write> BufBatchWriter<W> {
    pub(crate) fn new(
        inner: W,
        schema: &SchemaRef,
        write_options: &IpcWriteOptions,
        batch_size: usize,
    ) -> datafusion::common::Result<Self> {
        let writer = StreamWriter::try_new_with_options(
            inner,
            schema,
            write_options.clone(),
        )?;
        Ok(Self {
            writer,
            coalescer: None,
            schema: schema.clone(),
            batch_size,
        })
    }

    pub(crate) fn write(
        &mut self,
        batch: &RecordBatch,
        encode_time: &Time,
        _write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        let coalescer = self
            .coalescer
            .get_or_insert_with(|| BatchCoalescer::new(self.schema.clone(), self.batch_size));
        coalescer.push_batch(batch.clone())?;

        let mut completed = Vec::new();
        while let Some(batch) = coalescer.next_completed_batch() {
            completed.push(batch);
        }

        let mut bytes_written = 0;
        for batch in &completed {
            let mut timer = encode_time.timer();
            let before = self.writer.get_ref().len();
            self.writer.write(batch)?;
            let after = self.writer.get_ref().len();
            bytes_written += after - before;
            timer.stop();
        }
        Ok(bytes_written)
    }

    pub(crate) fn flush(
        &mut self,
        encode_time: &Time,
        _write_time: &Time,
    ) -> datafusion::common::Result<()> {
        let mut remaining = Vec::new();
        if let Some(coalescer) = &mut self.coalescer {
            coalescer.finish_buffered_batch()?;
            while let Some(batch) = coalescer.next_completed_batch() {
                remaining.push(batch);
            }
        }
        for batch in &remaining {
            let mut timer = encode_time.timer();
            self.writer.write(batch)?;
            timer.stop();
        }

        let mut timer = encode_time.timer();
        self.writer.finish()?;
        timer.stop();
        Ok(())
    }
}
```

**Note:** The `write()` return value (bytes written) is only used for metrics. Since the `StreamWriter` writes directly to the underlying `W`, and for file-backed writers we track offsets via `stream_position()`, the return value from `write()` can be dropped. Change the signature to return `Result<()>` and remove the byte counting logic. Callers that previously used the return value for offset tracking should use `stream_position()` on the underlying file instead.

- [ ] **Step 3: Update `writers/mod.rs`**

```rust
// (keep license header)

mod buf_batch_writer;
mod checksum;
mod shuffle_block_writer;
mod spill;

pub(crate) use buf_batch_writer::BufBatchWriter;
pub(crate) use checksum::Checksum;
pub use shuffle_block_writer::CompressionCodec;
pub(crate) use spill::PartitionWriter;
```

(No change needed — `BufBatchWriter` and `PartitionWriter` are still exported with the same names.)

- [ ] **Step 4: Update `MultiPartitionShuffleRepartitioner`**

In `native/shuffle/src/partitioners/multi_partition.rs`:

Change the struct fields — remove `shuffle_block_writer: ShuffleBlockWriter` and add `write_options: IpcWriteOptions`:

At approximately line 113, replace:
```rust
    shuffle_block_writer: ShuffleBlockWriter,
```
with:
```rust
    write_options: IpcWriteOptions,
```

In `try_new`, change the construction of `partition_writers` from:
```rust
let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;
// ...
.map(|_| PartitionWriter::try_new(shuffle_block_writer.clone()))
```
to:
```rust
let write_options = codec.ipc_write_options()?;
// ...
.map(|_| PartitionWriter::try_new(schema.clone(), write_options.clone()))
```

Update `shuffle_write_partition` to create a `BufBatchWriter` with the new API:
```rust
fn shuffle_write_partition(
    partition_iter: &mut PartitionedBatchIterator,
    output_data: &mut BufWriter<File>,
    schema: &SchemaRef,
    write_options: &IpcWriteOptions,
    encode_time: &Time,
    write_time: &Time,
    batch_size: usize,
) -> datafusion::common::Result<()> {
    let mut buf_batch_writer = BufBatchWriter::new(
        output_data,
        schema,
        write_options,
        batch_size,
    )?;
    for batch in partition_iter {
        let batch = batch?;
        buf_batch_writer.write(&batch, encode_time, write_time)?;
    }
    buf_batch_writer.flush(encode_time, write_time)?;
    Ok(())
}
```

Update the call site in `shuffle_write` (around line 595):
```rust
Self::shuffle_write_partition(
    &mut partition_iter,
    &mut output_data,
    &self.schema,
    &self.write_options,
    &self.metrics.encode_time,
    &self.metrics.write_time,
    self.batch_size,
)?;
```

Update `spill` to pass the new args:
```rust
spilled_bytes += partition_writer.spill(
    &mut iter,
    &self.runtime,
    &self.metrics,
)?;
```
(Remove `write_buffer_size` and `batch_size` args from `spill` — the spill writer now creates its own `StreamWriter`.)

- [ ] **Step 5: Update `SinglePartitionShufflePartitioner`**

In `native/shuffle/src/partitioners/single_partition.rs`, change the struct to hold a `BufBatchWriter` created with the new API:

```rust
pub(crate) struct SinglePartitionShufflePartitioner {
    output_data_writer: BufBatchWriter<File>,
    output_index_path: String,
    metrics: ShufflePartitionerMetrics,
    batch_size: usize,
}
```

Update `try_new`:
```rust
pub(crate) fn try_new(
    output_data_path: String,
    output_index_path: String,
    schema: SchemaRef,
    metrics: ShufflePartitionerMetrics,
    batch_size: usize,
    codec: CompressionCodec,
    _write_buffer_size: usize,
) -> datafusion::common::Result<Self> {
    let write_options = codec.ipc_write_options()?;
    let output_data_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_data_path)?;

    let output_data_writer = BufBatchWriter::new(
        output_data_file,
        &schema,
        &write_options,
        batch_size,
    )?;

    Ok(Self {
        output_data_writer,
        output_index_path,
        metrics,
        batch_size,
    })
}
```

Remove `buffered_batches`, `num_buffered_rows`, `add_buffered_batch`, and `concat_buffered_batches` — the `BufBatchWriter` handles coalescing internally. Simplify `insert_batch` to just call `self.output_data_writer.write(...)`.

- [ ] **Step 6: Verify Rust compilation**

Run: `cargo check --manifest-path native/Cargo.toml`
Expected: Compiles with only remaining errors in `jni_api.rs` (the read side, Task 6).

- [ ] **Step 7: Run existing shuffle writer tests**

Run: `cd native && cargo test -p datafusion-comet-shuffle`
Expected: All write-side tests pass. Read-side tests (`read_ipc_compressed`) will fail — expected.

- [ ] **Step 8: Commit**

```bash
git add native/shuffle/src/writers/ native/shuffle/src/partitioners/
git commit -m "feat: persistent StreamWriter per partition in buffered mode shuffle"
```

---

## Task 5: Remove Snappy Support from Config and Dependencies

**Files:**
- Modify: `native/shuffle/Cargo.toml`
- Modify: `common/src/main/scala/org/apache/comet/CometConf.scala:433-442`

- [ ] **Step 1: Remove `snap` dependency from Cargo.toml**

In `native/shuffle/Cargo.toml`, remove line:
```toml
snap = "1.1"
```

- [ ] **Step 2: Update CometConf to remove snappy**

In `common/src/main/scala/org/apache/comet/CometConf.scala`, change line 441:
```scala
      .checkValues(Set("zstd", "lz4", "snappy"))
```
to:
```scala
      .checkValues(Set("zstd", "lz4"))
```

And update the doc string (line 437-438):
```scala
        "The codec of Comet native shuffle used to compress shuffle data. lz4, zstd, and " +
          "snappy are supported. Compression can be disabled by setting " +
```
to:
```scala
        "The codec of Comet native shuffle used to compress shuffle data. " +
          "lz4 and zstd are supported. Compression can be disabled by setting " +
```

- [ ] **Step 3: Remove any remaining Snappy references in Rust code**

Search for and remove any `snap::` or `Snappy` references in the shuffle crate. After Tasks 2-4, there should be none left in the writer code. Check with:

Run: `grep -rn "snap\|Snappy\|SNAP" native/shuffle/src/`

Remove any remaining references.

- [ ] **Step 4: Commit**

```bash
git add native/shuffle/Cargo.toml common/src/main/scala/org/apache/comet/CometConf.scala
git commit -m "chore: remove Snappy codec support from shuffle (Arrow IPC supports LZ4/ZSTD)"
```

---

## Task 6: Implement `JniInputStream` and Stream Reader Handle (Rust Read Side)

Create the Rust-side JNI infrastructure for reading Arrow IPC streams from JVM InputStreams.

**Files:**
- Modify: `native/shuffle/src/ipc.rs` — replace `read_ipc_compressed` with `JniInputStream` and `ShuffleStreamReader`
- Modify: `native/core/src/execution/jni_api.rs` — replace `decodeShuffleBlock` with new JNI functions

- [ ] **Step 1: Rewrite `ipc.rs` with `JniInputStream` and `ShuffleStreamReader`**

Replace the entire contents of `native/shuffle/src/ipc.rs` with:

```rust
// (keep license header)

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use jni::objects::{GlobalRef, JByteArray, JObject, JValue};
use jni::sys::jint;
use jni::{JNIEnv, JavaVM};
use std::io::{self, Read};

/// Buffer size for JNI read-ahead. Minimizes JNI boundary crossings.
const JNI_READ_BUFFER_SIZE: usize = 64 * 1024; // 64KB

/// A `Read` adapter that pulls bytes from a JVM `InputStream` via JNI callbacks.
///
/// Uses an internal read-ahead buffer to minimize JNI call overhead.
pub struct JniInputStream {
    jvm: JavaVM,
    stream: GlobalRef,
    jbuf: JByteArray,
    buf: Vec<u8>,
    pos: usize,
    len: usize,
}

impl JniInputStream {
    /// Create a new `JniInputStream` wrapping a JVM `InputStream`.
    ///
    /// The `input_stream` object must be a valid `java.io.InputStream`.
    pub fn new(env: &mut JNIEnv, input_stream: &JObject) -> jni::errors::Result<Self> {
        let jvm = env.get_java_vm()?;
        let stream = env.new_global_ref(input_stream)?;
        let jbuf = env.new_byte_array(JNI_READ_BUFFER_SIZE as jint)?;
        let jbuf = JByteArray::from(env.new_global_ref(jbuf)?.as_ref().as_raw());
        Ok(Self {
            jvm,
            stream,
            jbuf,
            buf: vec![0u8; JNI_READ_BUFFER_SIZE],
            pos: 0,
            len: 0,
        })
    }

    fn refill(&mut self) -> io::Result<()> {
        let mut env = self.jvm.attach_current_thread().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("JNI attach failed: {e}"))
        })?;

        let n = env
            .call_method(
                &self.stream,
                "read",
                "([BII)I",
                &[
                    JValue::Object(self.jbuf.as_ref().into()),
                    JValue::Int(0),
                    JValue::Int(JNI_READ_BUFFER_SIZE as jint),
                ],
            )
            .and_then(|v| v.i())
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("JNI read failed: {e}"))
            })?;

        if n <= 0 {
            self.pos = 0;
            self.len = 0;
            return Ok(());
        }

        let n = n as usize;
        env.get_byte_array_region(&self.jbuf, 0, &mut self.buf[..n].iter().map(|b| *b as i8).collect::<Vec<i8>>())
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("JNI get_byte_array_region failed: {e}"))
            })?;

        // Actually, get_byte_array_region writes into an &mut [i8], not returns.
        // Let's use a simpler approach: get_byte_array_elements or copy into our buffer.
        // The cleanest approach:
        let mut i8_buf = vec![0i8; n];
        env.get_byte_array_region(&self.jbuf, 0, &mut i8_buf)
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("JNI copy failed: {e}"))
            })?;
        // Reinterpret i8 as u8
        self.buf[..n].copy_from_slice(unsafe {
            std::slice::from_raw_parts(i8_buf.as_ptr() as *const u8, n)
        });

        self.pos = 0;
        self.len = n;
        Ok(())
    }
}

impl Read for JniInputStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.len {
            self.refill()?;
            if self.len == 0 {
                return Ok(0); // EOF
            }
        }

        let available = self.len - self.pos;
        let to_copy = available.min(buf.len());
        buf[..to_copy].copy_from_slice(&self.buf[self.pos..self.pos + to_copy]);
        self.pos += to_copy;
        Ok(to_copy)
    }
}

/// Holds a persistent Arrow IPC `StreamReader` across JNI calls.
///
/// The reader is created once when the shuffle stream is opened, and
/// `next_batch()` is called repeatedly to read individual batches.
pub struct ShuffleStreamReader {
    reader: StreamReader<JniInputStream>,
    num_fields: usize,
}

impl ShuffleStreamReader {
    /// Create a new reader wrapping a JVM InputStream.
    pub fn new(env: &mut JNIEnv, input_stream: &JObject) -> Result<Self, String> {
        let jni_stream = JniInputStream::new(env, input_stream)
            .map_err(|e| format!("Failed to create JniInputStream: {e}"))?;

        let reader = unsafe {
            StreamReader::try_new(jni_stream, None)
                .map_err(|e| format!("Failed to create StreamReader: {e}"))?
                .with_skip_validation(true)
        };

        let num_fields = reader.schema().fields().len();

        Ok(Self { reader, num_fields })
    }

    /// Read the next batch from the stream.
    /// Returns `None` when the stream is exhausted.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        match self.reader.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(format!("Error reading batch: {e}")),
            None => Ok(None),
        }
    }

    /// Number of fields (columns) in the schema.
    pub fn num_fields(&self) -> usize {
        self.num_fields
    }
}
```

**Note to implementer:** The `JByteArray` handling in `refill()` above is pseudocode — the exact JNI API for byte array handling with the `jni` crate needs careful implementation. The `jni` crate's `get_byte_array_region` takes `&mut [jbyte]` (which is `&mut [i8]`). The key pattern is:
1. Call `inputStream.read(byte[], 0, len)` via `call_method`
2. Copy the Java byte[] contents to the Rust buffer via `get_byte_array_region`
3. Reinterpret `i8` as `u8`

Also, the `JByteArray` from a `GlobalRef` conversion needs care — the implementer should verify this compiles. A `GlobalRef` to the byte array must be stored, and a local ref obtained for each JNI call.

- [ ] **Step 2: Update `lib.rs` exports**

In `native/shuffle/src/lib.rs`, replace:
```rust
pub use ipc::read_ipc_compressed;
```
with:
```rust
pub use ipc::{JniInputStream, ShuffleStreamReader};
```

- [ ] **Step 3: Add JNI functions in `jni_api.rs`**

In `native/core/src/execution/jni_api.rs`, replace the `decodeShuffleBlock` function (lines 878-900) with three new functions:

```rust
#[no_mangle]
/// Open a shuffle stream reader over a JVM InputStream.
/// Returns an opaque handle (pointer) to a ShuffleStreamReader.
/// # Safety
/// This function is inherently unsafe since it deals with JNI objects.
pub unsafe extern "system" fn Java_org_apache_comet_Native_openShuffleStream(
    e: JNIEnv,
    _class: JClass,
    input_stream: JObject,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| {
        let reader = datafusion_comet_shuffle::ShuffleStreamReader::new(&mut env, &input_stream)
            .map_err(|e| CometError::Internal(e))?;
        let boxed = Box::new(reader);
        Ok(Box::into_raw(boxed) as jlong)
    })
}

#[no_mangle]
/// Read the next batch from a shuffle stream.
/// Returns the number of rows, or -1 if the stream is exhausted.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_nextShuffleStreamBatch(
    e: JNIEnv,
    _class: JClass,
    handle: jlong,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| {
        let reader = unsafe { &mut *(handle as *mut datafusion_comet_shuffle::ShuffleStreamReader) };
        match reader.next_batch() {
            Ok(Some(batch)) => {
                prepare_output(&mut env, array_addrs, schema_addrs, batch, false)
            }
            Ok(None) => Ok(-1),
            Err(e) => Err(CometError::Internal(e)),
        }
    })
}

#[no_mangle]
/// Close and drop a shuffle stream reader.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_closeShuffleStream(
    e: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&e, |_env| {
        if handle != 0 {
            unsafe { drop(Box::from_raw(handle as *mut datafusion_comet_shuffle::ShuffleStreamReader)) };
        }
        Ok(())
    });
}
```

Remove the old `decodeShuffleBlock` function.

- [ ] **Step 4: Verify Rust compilation**

Run: `cargo check --manifest-path native/Cargo.toml`
Expected: Compiles (possibly with warnings about unused code). JNI type details may need adjustment.

- [ ] **Step 5: Commit**

```bash
git add native/shuffle/src/ipc.rs native/shuffle/src/lib.rs native/core/src/execution/jni_api.rs
git commit -m "feat: JniInputStream and stream reader handle for shuffle read path"
```

---

## Task 7: Update JVM Read Side (Native.scala + NativeBatchDecoderIterator)

**Files:**
- Modify: `spark/src/main/scala/org/apache/comet/Native.scala`
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/NativeBatchDecoderIterator.scala`

- [ ] **Step 1: Update `Native.scala` JNI declarations**

Replace the `decodeShuffleBlock` declaration (lines 175-180) with:

```scala
  /**
   * Open a shuffle stream reader over a JVM InputStream.
   * @param inputStream the InputStream to read from
   * @return an opaque handle to the native stream reader
   */
  @native def openShuffleStream(inputStream: java.io.InputStream): Long

  /**
   * Read the next batch from a shuffle stream.
   * @param handle the native stream reader handle
   * @param arrayAddrs Arrow Array addresses for FFI export
   * @param schemaAddrs Arrow Schema addresses for FFI export
   * @return the number of rows in the batch, or -1 if stream is exhausted
   */
  @native def nextShuffleStreamBatch(
      handle: Long,
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long]): Long

  /**
   * Close and release a native shuffle stream reader.
   * @param handle the native stream reader handle
   */
  @native def closeShuffleStream(handle: Long): Unit
```

Also remove the `import java.nio.ByteBuffer` if it's no longer needed (check other usages first).

- [ ] **Step 2: Rewrite `NativeBatchDecoderIterator.scala`**

Replace the entire file with:

```scala
/*
 * (keep license header)
 */

package org.apache.spark.sql.comet.execution.shuffle

import java.io.InputStream

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.Native
import org.apache.comet.vector.NativeUtil

/**
 * Iterator that reads shuffle blocks from a JVM InputStream using native Arrow IPC
 * stream decoding. The native side pulls bytes from the InputStream via JNI callbacks.
 */
case class NativeBatchDecoderIterator(
    in: InputStream,
    decodeTime: SQLMetric,
    nativeLib: Native,
    nativeUtil: NativeUtil,
    tracingEnabled: Boolean)
    extends Iterator[ColumnarBatch] {

  private var isClosed = false
  private var currentBatch: ColumnarBatch = null

  // Open the native stream reader
  private val handle: Long = if (in != null) {
    nativeLib.openShuffleStream(in)
  } else {
    0L
  }

  private var batch = fetchNext()

  def hasNext(): Boolean = {
    if (handle == 0L || isClosed) {
      return false
    }
    if (batch.isDefined) {
      return true
    }

    if (currentBatch != null) {
      currentBatch.close()
      currentBatch = null
    }

    batch = fetchNext()
    if (batch.isEmpty) {
      close()
      return false
    }
    true
  }

  def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val nextBatch = batch.get
    currentBatch = nextBatch
    batch = None
    currentBatch
  }

  private def fetchNext(): Option[ColumnarBatch] = {
    if (handle == 0L || isClosed) {
      return None
    }

    val startTime = System.nanoTime()

    // Query the field count from the native reader (it parsed the schema on open).
    val batch = nativeUtil.getNextBatch(
      nativeLib.shuffleStreamNumFields(handle).toInt,
      (arrayAddrs, schemaAddrs) => {
        nativeLib.nextShuffleStreamBatch(handle, arrayAddrs, schemaAddrs)
      })

    decodeTime.add(System.nanoTime() - startTime)
    batch
  }

  def close(): Unit = {
    synchronized {
      if (!isClosed) {
        if (currentBatch != null) {
          currentBatch.close()
          currentBatch = null
        }
        if (handle != 0L) {
          nativeLib.closeShuffleStream(handle)
        }
        if (in != null) {
          in.close()
        }
        isClosed = true
      }
    }
  }
}
```

**Note:** This requires adding a `shuffleStreamNumFields` JNI method. Add to `Native.scala`:

```scala
  /**
   * Get the number of fields (columns) in a shuffle stream's schema.
   * @param handle the native stream reader handle
   * @return the number of fields
   */
  @native def shuffleStreamNumFields(handle: Long): Long
```

And add the corresponding Rust JNI function in `jni_api.rs`:

```rust
#[no_mangle]
/// Get the number of fields in the shuffle stream's schema.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_shuffleStreamNumFields(
    e: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    try_unwrap_or_throw(&e, |_env| {
        let reader = unsafe { &*(handle as *const datafusion_comet_shuffle::ShuffleStreamReader) };
        Ok(reader.num_fields() as jlong)
    })
}
```

- [ ] **Step 3: Remove the companion object with thread-local buffer**

The old `NativeBatchDecoderIterator` companion object (lines 185-198) with `threadLocalDataBuf` and `resetDataBuf` is no longer needed. Remove it entirely. The new version has no companion object.

- [ ] **Step 4: Commit**

```bash
git add spark/src/main/scala/org/apache/comet/Native.scala \
        spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/NativeBatchDecoderIterator.scala \
        native/core/src/execution/jni_api.rs
git commit -m "feat: JVM shuffle reader uses native IPC stream decoding via JNI"
```

---

## Task 8: Build and Run End-to-End Tests

Verify the full stack works together.

**Files:** No new files — testing existing test suites.

- [ ] **Step 1: Build native code**

Run: `make core`
Expected: Native Rust code compiles successfully.

- [ ] **Step 2: Run Rust shuffle tests**

Run: `cd native && cargo test -p datafusion-comet-shuffle`
Expected: All tests pass. Tests using `read_ipc_compressed` have been updated in previous tasks.

- [ ] **Step 3: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: No warnings or errors.

- [ ] **Step 4: Build JVM**

Run: `make`
Expected: Full build succeeds (native + JVM).

- [ ] **Step 5: Run JVM shuffle tests**

Run: `./mvnw test -Dsuites="org.apache.comet.exec.CometShuffleSuite"`
Expected: All shuffle tests pass with the new format.

- [ ] **Step 6: Run broader test suite to check for regressions**

Run: `./mvnw test -DwildcardSuites="CometShuffle"`
Expected: All shuffle-related tests pass.

- [ ] **Step 7: Commit any test fixes**

If any tests needed adjustments (e.g., tests that checked the old custom header format), commit them:

```bash
git add -A
git commit -m "test: update shuffle tests for IPC stream-per-partition format"
```

---

## Task 9: Clean Up Removed Code

Remove any dead code left over from the migration.

**Files:**
- Potentially: `native/shuffle/src/writers/shuffle_block_writer.rs` (if not already reduced to just `CompressionCodec`)
- Any remaining references to `read_ipc_compressed`, `decodeShuffleBlock`, Snappy

- [ ] **Step 1: Search for dead code**

Run:
```bash
grep -rn "read_ipc_compressed\|decodeShuffleBlock\|ShuffleBlockWriter\|snap::\|Snappy\|SNAP\|CompressionCodec::Snappy" native/shuffle/src/ spark/src/ common/src/
```

- [ ] **Step 2: Remove any remaining references**

Remove or update any files still referencing the old format.

- [ ] **Step 3: Rename `shuffle_block_writer.rs` to `codec.rs`**

Since the file now only contains the `CompressionCodec` enum, rename it:
- Rename `native/shuffle/src/writers/shuffle_block_writer.rs` to `native/shuffle/src/writers/codec.rs`
- Update `native/shuffle/src/writers/mod.rs`: change `mod shuffle_block_writer;` to `mod codec;` and update the use statement.

- [ ] **Step 4: Run full test suite**

Run: `cd native && cargo test -p datafusion-comet-shuffle`
Expected: All tests pass.

- [ ] **Step 5: Format code**

Run: `make format`
Expected: All code formatted.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: clean up dead code from shuffle format migration"
```
