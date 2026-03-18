# Shuffle Direct Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate double Arrow FFI crossing at shuffle boundaries by having native code consume compressed IPC blocks directly from JVM-provided byte buffers.

**Architecture:** A new `ShuffleScanExec` Rust operator pulls raw compressed bytes from a JVM `CometShuffleBlockIterator` via JNI, decompresses and decodes them in native code, and feeds `RecordBatch` directly into the execution plan. This bypasses the current path where data is decoded to JVM `ColumnarBatch` (FFI export), then re-exported back to native (FFI import).

**Tech Stack:** Scala, Java, Rust, Protobuf, JNI, Arrow IPC

**Spec:** `docs/superpowers/specs/2026-03-18-shuffle-direct-read-design.md`

---

### Task 1: Add config flag

**Files:**
- Modify: `common/src/main/scala/org/apache/comet/CometConf.scala`

- [ ] **Step 1: Add the config entry**

Find the existing shuffle config entries (search for `COMET_EXEC_SHUFFLE_ENABLED`) and add nearby:

```scala
val COMET_SHUFFLE_DIRECT_READ_ENABLED: ConfigEntry[Boolean] =
  conf("spark.comet.shuffle.directRead.enabled")
    .category(CATEGORY_EXEC)
    .doc(
      "When enabled, native operators that consume shuffle output will read " +
        "compressed shuffle blocks directly in native code, bypassing Arrow FFI. " +
        "Only applies to native shuffle (not JVM columnar shuffle). " +
        "Requires spark.comet.exec.shuffle.enabled to be true.")
    .booleanConf
    .createWithDefault(true)
```

- [ ] **Step 2: Verify it compiles**

Run: `./mvnw compile -DskipTests -pl common`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add common/src/main/scala/org/apache/comet/CometConf.scala
git commit -m "feat: add spark.comet.shuffle.directRead.enabled config"
```

---

### Task 2: Add ShuffleScan protobuf message

**Files:**
- Modify: `native/proto/src/proto/operator.proto`

- [ ] **Step 1: Add ShuffleScan message**

Add after the existing `Scan` message (after line 86):

```protobuf
message ShuffleScan {
  repeated spark.spark_expression.DataType fields = 1;
  // Informational label for debug output (e.g., "CometShuffleExchangeExec [id=5]")
  string source = 2;
}
```

- [ ] **Step 2: Add shuffle_scan to the Operator oneof**

In the `oneof op_struct` block (lines 38-55), add after `csv_scan = 115`:

```protobuf
    ShuffleScan shuffle_scan = 116;
```

- [ ] **Step 3: Rebuild protobuf and verify**

Run: `make core`
Expected: Successful build with generated protobuf code.

- [ ] **Step 4: Commit**

```bash
git add native/proto/src/proto/operator.proto
git commit -m "feat: add ShuffleScan protobuf message"
```

---

### Task 3: Create CometShuffleBlockIterator (Java)

**Files:**
- Create: `spark/src/main/java/org/apache/comet/CometShuffleBlockIterator.java`

- [ ] **Step 1: Create the class**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Provides raw compressed shuffle blocks to native code via JNI.
 *
 * <p>Reads block headers (compressed length + field count) from a shuffle InputStream and loads
 * the compressed body into a DirectByteBuffer. Native code pulls blocks by calling hasNext()
 * and getBuffer().
 *
 * <p>The DirectByteBuffer returned by getBuffer() is only valid until the next hasNext() call.
 * Native code must fully consume it (via read_ipc_compressed which allocates new memory for
 * the decompressed data) before pulling the next block.
 */
public class CometShuffleBlockIterator implements Closeable {

  private static final int INITIAL_BUFFER_SIZE = 128 * 1024;

  private final ReadableByteChannel channel;
  private final InputStream inputStream;
  private final ByteBuffer headerBuf =
      ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
  private ByteBuffer dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE);
  private boolean closed = false;
  private int currentBlockLength = 0;

  public CometShuffleBlockIterator(InputStream in) {
    this.inputStream = in;
    this.channel = Channels.newChannel(in);
  }

  /**
   * Reads the next block header and loads the compressed body into the internal buffer.
   * Called by native code via JNI.
   *
   * <p>Header format: 8-byte compressedLength (includes field count but not itself) +
   * 8-byte fieldCount (discarded, schema comes from protobuf).
   *
   * @return the compressed body length in bytes (codec prefix + compressed IPC), or -1 if EOF
   */
  public int hasNext() throws IOException {
    if (closed) {
      return -1;
    }

    // Read 16-byte header
    headerBuf.clear();
    while (headerBuf.hasRemaining()) {
      int bytesRead = channel.read(headerBuf);
      if (bytesRead < 0) {
        if (headerBuf.position() == 0) {
          return -1;
        }
        throw new EOFException(
            "Data corrupt: unexpected EOF while reading batch header");
      }
    }
    headerBuf.flip();
    long compressedLength = headerBuf.getLong();
    // Field count discarded - schema determined by ShuffleScan protobuf fields
    headerBuf.getLong();

    long bytesToRead = compressedLength - 8;
    if (bytesToRead > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Native shuffle block size of " + bytesToRead + " exceeds maximum of "
              + Integer.MAX_VALUE + ". Try reducing shuffle batch size.");
    }

    if (dataBuf.capacity() < bytesToRead) {
      int newCapacity = (int) Math.min(bytesToRead * 2L, Integer.MAX_VALUE);
      dataBuf = ByteBuffer.allocateDirect(newCapacity);
    }

    dataBuf.clear();
    dataBuf.limit((int) bytesToRead);
    while (dataBuf.hasRemaining()) {
      int bytesRead = channel.read(dataBuf);
      if (bytesRead < 0) {
        throw new EOFException(
            "Data corrupt: unexpected EOF while reading compressed batch");
      }
    }
    // Note: native side uses get_direct_buffer_address (base pointer) + currentBlockLength,
    // not the buffer's position/limit. No flip needed.

    currentBlockLength = (int) bytesToRead;
    return currentBlockLength;
  }

  /**
   * Returns the DirectByteBuffer containing the current block's compressed bytes
   * (4-byte codec prefix + compressed IPC data).
   * Called by native code via JNI.
   */
  public ByteBuffer getBuffer() {
    return dataBuf;
  }

  /**
   * Returns the length of the current block in bytes.
   * Called by native code via JNI.
   */
  public int getCurrentBlockLength() {
    return currentBlockLength;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      inputStream.close();
      if (dataBuf.capacity() > INITIAL_BUFFER_SIZE) {
        dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE);
      }
    }
  }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `./mvnw compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add spark/src/main/java/org/apache/comet/CometShuffleBlockIterator.java
git commit -m "feat: add CometShuffleBlockIterator for raw shuffle block access"
```

---

### Task 4: Add JNI bridge for CometShuffleBlockIterator (Rust)

**Files:**
- Create: `native/core/src/jvm_bridge/shuffle_block_iterator.rs`
- Modify: `native/core/src/jvm_bridge/mod.rs`

- [ ] **Step 1: Create the JNI bridge struct**

Create `native/core/src/jvm_bridge/shuffle_block_iterator.rs`:

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

use jni::signature::Primitive;
use jni::{
    errors::Result as JniResult,
    objects::{JClass, JMethodID},
    signature::ReturnType,
    JNIEnv,
};

/// JNI method IDs for `CometShuffleBlockIterator`.
#[allow(dead_code)]
pub struct CometShuffleBlockIterator<'a> {
    pub class: JClass<'a>,
    pub method_has_next: JMethodID,
    pub method_has_next_ret: ReturnType,
    pub method_get_buffer: JMethodID,
    pub method_get_buffer_ret: ReturnType,
    pub method_get_current_block_length: JMethodID,
    pub method_get_current_block_length_ret: ReturnType,
}

impl<'a> CometShuffleBlockIterator<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/CometShuffleBlockIterator";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometShuffleBlockIterator<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(CometShuffleBlockIterator {
            class,
            method_has_next: env.get_method_id(Self::JVM_CLASS, "hasNext", "()I")?,
            method_has_next_ret: ReturnType::Primitive(Primitive::Int),
            method_get_buffer: env.get_method_id(
                Self::JVM_CLASS,
                "getBuffer",
                "()Ljava/nio/ByteBuffer;",
            )?,
            method_get_buffer_ret: ReturnType::Object,
            method_get_current_block_length: env.get_method_id(
                Self::JVM_CLASS,
                "getCurrentBlockLength",
                "()I",
            )?,
            method_get_current_block_length_ret: ReturnType::Primitive(Primitive::Int),
        })
    }
}
```

- [ ] **Step 2: Register in mod.rs**

In `native/core/src/jvm_bridge/mod.rs`:

Add `mod shuffle_block_iterator;` alongside the existing `mod batch_iterator;` (line 174).

Add `use shuffle_block_iterator::CometShuffleBlockIterator as CometShuffleBlockIteratorBridge;` (to avoid name collision with the operator).

Add a field to the `JVMClasses` struct (around line 206):
```rust
pub comet_shuffle_block_iterator: CometShuffleBlockIteratorBridge<'a>,
```

Initialize it in `JVMClasses::init` alongside the existing `comet_batch_iterator` init (around line 259):
```rust
comet_shuffle_block_iterator: CometShuffleBlockIteratorBridge::new(env).unwrap(),
```

- [ ] **Step 3: Add a `jni_call!` compatible accessor**

Check how `comet_batch_iterator` is called in `scan.rs`. The `jni_call!` macro uses the field name from `JVMClasses`. Ensure `comet_shuffle_block_iterator` follows the same pattern. You may need to add a module in the `jni_bridge` macros — look at how `jni_call!(&mut env, comet_batch_iterator(iter).has_next() -> i32)` is defined and add equivalent patterns for `comet_shuffle_block_iterator`.

Check `native/core/src/jvm_bridge/` for macro definitions (likely in a separate file or in `mod.rs`) that define the `jni_call!` dispatch for each class.

- [ ] **Step 4: Verify it compiles**

Run: `cd native && cargo build`
Expected: Successful build.

- [ ] **Step 5: Commit**

```bash
git add native/core/src/jvm_bridge/shuffle_block_iterator.rs
git add native/core/src/jvm_bridge/mod.rs
git commit -m "feat: add JNI bridge for CometShuffleBlockIterator"
```

---

### Task 5: Create ShuffleScanExec (Rust)

**Files:**
- Create: `native/core/src/execution/operators/shuffle_scan.rs`
- Modify: `native/core/src/execution/operators/mod.rs`

**Design decision — pre-pull pattern:** `ShuffleScanExec` MUST use the pre-pull pattern (same as `ScanExec`). The comment at `jni_api.rs:483-488` explains why: JNI calls cannot happen from within `poll_next` on tokio threads. So `ShuffleScanExec` stores a `batch: Arc<Mutex<Option<InputBatch>>>` and `get_next_batch()` is called from `pull_input_batches` before each `poll_next`.

- [ ] **Step 1: Create shuffle_scan.rs**

Use `scan.rs` as the template. The key differences:
- `get_next_batch` calls `hasNext()`/`getBuffer()`/`getCurrentBlockLength()` on `CometShuffleBlockIterator` instead of Arrow FFI methods on `CometBatchIterator`
- After getting the `DirectByteBuffer`, call `read_ipc_compressed()` to decode
- No `arrow_ffi_safe` flag, no selection vectors, no `copy_or_unpack_array`
- Track `decode_time` metric

The core `get_next` method:

```rust
fn get_next(
    exec_context_id: i64,
    iter: &JObject,
    data_types: &[DataType],
) -> Result<InputBatch, CometError> {
    let mut env = JVMClasses::get_env()?;

    // Call hasNext() — returns block length or -1 for EOF
    let block_length: i32 = unsafe {
        jni_call!(&mut env, comet_shuffle_block_iterator(iter).has_next() -> i32)?
    };

    if block_length < 0 {
        return Ok(InputBatch::EOF);
    }

    // Get the DirectByteBuffer
    let buffer: JByteBuffer = unsafe {
        jni_call!(&mut env, comet_shuffle_block_iterator(iter).get_buffer() -> JObject)?
    }.into();

    // Get raw pointer to the buffer data
    let raw_pointer = env.get_direct_buffer_address(&buffer)?;
    let length = block_length as usize;
    let slice: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };

    // Decompress and decode the IPC block
    let batch = read_ipc_compressed(slice)?;

    // Convert RecordBatch columns to InputBatch
    let arrays: Vec<ArrayRef> = batch.columns().to_vec();
    let num_rows = batch.num_rows();

    Ok(InputBatch::new(arrays, Some(num_rows)))
}
```

For the `ExecutionPlan` trait implementation, follow `ScanExec` closely:
- `schema()` returns schema built from `data_types`
- `execute()` returns a `ScanStream` (reuse the same stream type from `scan.rs`)
- The `ScanStream` checks `self.batch` mutex on each `poll_next`, takes the batch if available

- [ ] **Step 2: Register the module**

In `native/core/src/execution/operators/mod.rs`, add:

```rust
mod shuffle_scan;
pub use shuffle_scan::ShuffleScanExec;
```

- [ ] **Step 3: Verify it compiles**

Run: `cd native && cargo build`
Expected: Successful build.

- [ ] **Step 4: Commit**

```bash
git add native/core/src/execution/operators/shuffle_scan.rs
git add native/core/src/execution/operators/mod.rs
git commit -m "feat: add ShuffleScanExec native operator for direct shuffle read"
```

---

### Task 6: Wire ShuffleScanExec into the native planner and pre-pull

**Files:**
- Modify: `native/core/src/execution/planner.rs`
- Modify: `native/core/src/execution/jni_api.rs`

**Design decision — separate scan vectors:** The planner's `create_plan` currently returns `(Vec<ScanExec>, Arc<SparkPlan>)`. Change the return type to include shuffle scans: `(Vec<ScanExec>, Vec<ShuffleScanExec>, Arc<SparkPlan>)`. All intermediate operators pass both vectors through. `ExecutionContext` gets a new `shuffle_scans: Vec<ShuffleScanExec>` field, and `pull_input_batches` iterates both.

- [ ] **Step 1: Update create_plan return type**

In `planner.rs`, change the `create_plan` return type (line 915):

```rust
) -> Result<(Vec<ScanExec>, Vec<ShuffleScanExec>, Arc<SparkPlan>), ExecutionError>
```

Update every match arm that calls `create_plan` recursively or returns results:
- Single-child operators (Filter, Project, Sort, etc.): destructure as `let (scans, shuffle_scans, child) = ...` and pass both through
- Multi-child operators (joins via `parse_join_parameters`): concatenate both scan vectors from left and right children
- `Scan` arm: returns `(vec![scan.clone()], vec![], ...)`
- Add `ShuffleScan` arm (see step 2)

This is a mechanical change across many match arms. Each `Ok((scans, ...))` becomes `Ok((scans, shuffle_scans, ...))`.

Also update `parse_join_parameters` return type similarly.

- [ ] **Step 2: Add ShuffleScan match arm**

```rust
OpStruct::ShuffleScan(scan) => {
    let data_types = scan.fields.iter().map(to_arrow_datatype).collect_vec();

    if self.exec_context_id != TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
        return Err(GeneralError("No input for shuffle scan".to_string()));
    }

    let input_source =
        if self.exec_context_id == TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
            None
        } else {
            Some(inputs.remove(0))
        };

    let shuffle_scan = ShuffleScanExec::new(
        self.exec_context_id,
        input_source,
        &scan.source,
        data_types,
    )?;

    Ok((
        vec![],
        vec![shuffle_scan.clone()],
        Arc::new(SparkPlan::new(spark_plan.plan_id, Arc::new(shuffle_scan), vec![])),
    ))
}
```

- [ ] **Step 3: Update ExecutionContext and pull_input_batches**

In `jni_api.rs`:

Add `shuffle_scans: Vec<ShuffleScanExec>` field to `ExecutionContext` struct (after `scans` on line 153). Initialize as `shuffle_scans: vec![]` in the constructor (line 313).

Where `create_plan` results are stored (line 542-550):

```rust
let (scans, shuffle_scans, root_op) = planner.create_plan(...)?;
exec_context.scans = scans;
exec_context.shuffle_scans = shuffle_scans;
```

Update `pull_input_batches` (line 490):

```rust
fn pull_input_batches(exec_context: &mut ExecutionContext) -> Result<(), CometError> {
    exec_context.scans.iter_mut().try_for_each(|scan| {
        scan.get_next_batch()?;
        Ok::<(), CometError>(())
    })?;
    exec_context.shuffle_scans.iter_mut().try_for_each(|scan| {
        scan.get_next_batch()?;
        Ok::<(), CometError>(())
    })
}
```

Also update the `exec_context.scans.is_empty()` check (line 563) to also check `shuffle_scans`:

```rust
if exec_context.scans.is_empty() && exec_context.shuffle_scans.is_empty() {
```

- [ ] **Step 4: Verify it compiles**

Run: `cd native && cargo build`
Expected: Successful build.

- [ ] **Step 5: Commit**

```bash
git add native/core/src/execution/planner.rs
git add native/core/src/execution/jni_api.rs
git commit -m "feat: wire ShuffleScanExec into planner and pre-pull mechanism"
```

---

### Task 7: Emit ShuffleScan from JVM serde

**Files:**
- Modify: `spark/src/main/scala/org/apache/comet/serde/operator/CometSink.scala`

The `CometExchangeSink.convert()` receives the outer operator (e.g., `ShuffleQueryStageExec`) not the inner `CometShuffleExchangeExec`. We must unwrap to check `shuffleType`.

- [ ] **Step 1: Override convert in CometExchangeSink**

Replace the `CometExchangeSink` object (lines 87-100) with:

```scala
object CometExchangeSink extends CometSink[SparkPlan] {

  override def isFfiSafe: Boolean = true

  override def convert(
      op: SparkPlan,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    if (shouldUseShuffleScan(op)) {
      convertToShuffleScan(op, builder)
    } else {
      super.convert(op, builder, childOp: _*)
    }
  }

  private def shouldUseShuffleScan(op: SparkPlan): Boolean = {
    if (!CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.get()) return false

    // Extract the CometShuffleExchangeExec from the wrapper
    val shuffleExec = op match {
      case ShuffleQueryStageExec(_, s: CometShuffleExchangeExec, _) => Some(s)
      case ShuffleQueryStageExec(_, ReusedExchangeExec(_, s: CometShuffleExchangeExec), _) =>
        Some(s)
      case s: CometShuffleExchangeExec => Some(s)
      case _ => None
    }

    shuffleExec.exists(_.shuffleType == CometNativeShuffle)
  }

  private def convertToShuffleScan(
      op: SparkPlan,
      builder: Operator.Builder): Option[OperatorOuterClass.Operator] = {
    val supportedTypes =
      op.output.forall(a => supportedDataType(a.dataType, allowComplex = true))

    if (!supportedTypes) {
      withInfo(op, "Unsupported data type for shuffle direct read")
      return None
    }

    val scanBuilder = OperatorOuterClass.ShuffleScan.newBuilder()
    val source = op.simpleStringWithNodeId()
    if (source.isEmpty) {
      scanBuilder.setSource(op.getClass.getSimpleName)
    } else {
      scanBuilder.setSource(source)
    }

    val scanTypes = op.output.flatMap { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == op.output.length) {
      scanBuilder.addAllFields(scanTypes.asJava)
      builder.clearChildren()
      Some(builder.setShuffleScan(scanBuilder).build())
    } else {
      withInfo(op, "unsupported data types for shuffle direct read")
      // Fall back to regular Scan
      None
    }
  }

  override def createExec(nativeOp: Operator, op: SparkPlan): CometNativeExec =
    CometSinkPlaceHolder(nativeOp, op, op)
}
```

Add necessary imports at the top of the file:
```scala
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.comet.CometConf
```

- [ ] **Step 2: Verify it compiles**

Run: `./mvnw compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add spark/src/main/scala/org/apache/comet/serde/operator/CometSink.scala
git commit -m "feat: emit ShuffleScan protobuf for native shuffle with direct read"
```

---

### Task 8: Wire CometShuffleBlockIterator into JVM execution path

**Files:**
- Modify: `spark/src/main/scala/org/apache/comet/Native.scala`
- Modify: `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/CometExecRDD.scala`
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/operators.scala`
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala`

This task connects the JVM plumbing so that `ShuffleScan` inputs get `CometShuffleBlockIterator` (wrapping raw `InputStream`) instead of `CometBatchIterator` (wrapping decoded `ColumnarBatch`).

**Key insight**: Currently all inputs flow through `RDD[ColumnarBatch]`. For shuffle direct read, we need the raw `InputStream` before decoding. The approach: add a parallel input channel for raw shuffle streams alongside the existing `ColumnarBatch` inputs.

- [ ] **Step 1: Change Native.scala createPlan signature**

In `spark/src/main/scala/org/apache/comet/Native.scala` (line 57), change:

```scala
iterators: Array[CometBatchIterator],
```
to:
```scala
iterators: Array[Object],
```

The JNI side (`jni_api.rs:190`) already uses `JObjectArray`, so no Rust changes needed.

- [ ] **Step 2: Add shuffle stream inputs to CometExecIterator**

In `spark/src/main/scala/org/apache/comet/CometExecIterator.scala`, add a parameter for shuffle block iterators that should be used instead of regular batch iterators at specific input positions:

```scala
class CometExecIterator(
    val id: Long,
    inputs: Seq[Iterator[ColumnarBatch]],
    numOutputCols: Int,
    protobufQueryPlan: Array[Byte],
    nativeMetrics: CometMetricNode,
    numParts: Int,
    partitionIndex: Int,
    broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
    encryptedFilePaths: Seq[String] = Seq.empty,
    shuffleBlockIterators: Map[Int, CometShuffleBlockIterator] = Map.empty)
```

Replace the `cometBatchIterators` construction (lines 81-83):

```scala
private val nativeIterators: Array[Object] = {
  val result = new Array[Object](inputs.size)
  inputs.zipWithIndex.foreach { case (iterator, idx) =>
    result(idx) = shuffleBlockIterators.getOrElse(
      idx,
      new CometBatchIterator(iterator, nativeUtil))
  }
  result
}
```

Change `nativeLib.createPlan(id, cometBatchIterators, ...)` (line 109) to use `nativeIterators`.

In the `close()` method, also close `CometShuffleBlockIterator` instances:
```scala
shuffleBlockIterators.values.foreach { iter =>
  try { iter.close() } catch { case _: Exception => }
}
```

- [ ] **Step 3: Add shuffle stream support to CometExecRDD**

In `spark/src/main/scala/org/apache/spark/sql/comet/CometExecRDD.scala`, add a parameter to carry shuffle block iterator factories:

```scala
private[spark] class CometExecRDD(
    sc: SparkContext,
    var inputRDDs: Seq[RDD[ColumnarBatch]],
    ...
    encryptedFilePaths: Seq[String] = Seq.empty,
    shuffleBlockIteratorFactories: Map[Int, (TaskContext, Partition) => CometShuffleBlockIterator] = Map.empty)
```

In the `compute` method (line 112), pass them to `CometExecIterator`:

```scala
// Create shuffle block iterators for this partition
val shuffleBlockIters = shuffleBlockIteratorFactories.map { case (idx, factory) =>
  idx -> factory(context, partition.inputPartitions(idx))
}

val it = new CometExecIterator(
  CometExec.newIterId,
  inputs,
  numOutputCols,
  actualPlan,
  nativeMetrics,
  numPartitions,
  partition.index,
  broadcastedHadoopConfForEncryption,
  encryptedFilePaths,
  shuffleBlockIters)
```

- [ ] **Step 4: Identify ShuffleScan inputs in operators.scala**

In `spark/src/main/scala/org/apache/spark/sql/comet/operators.scala`, in `CometNativeExec.doExecuteColumnar` (around line 480):

After `foreachUntilCometInput(this)(sparkPlans += _)`, determine which inputs correspond to `ShuffleScan` operators. Parse the serialized protobuf plan to find `ShuffleScan` leaf positions:

```scala
import org.apache.comet.serde.OperatorOuterClass

// Find which input indices correspond to ShuffleScan operators
val shuffleScanIndices: Set[Int] = {
  val plan = OperatorOuterClass.Operator.parseFrom(serializedPlanCopy)
  var scanIndex = 0
  val indices = scala.collection.mutable.Set.empty[Int]
  def walk(op: OperatorOuterClass.Operator): Unit = {
    if (op.hasShuffleScan) {
      indices += scanIndex
      scanIndex += 1
    } else if (op.hasScan) {
      scanIndex += 1
    } else {
      // Recurse into children in order
      (0 until op.getChildrenCount).foreach(i => walk(op.getChildren(i)))
    }
  }
  walk(plan)
  indices.toSet
}
```

Then in the `sparkPlans.zipWithIndex.foreach` loop (line 523), for plans at shuffle scan indices, create a factory that produces `CometShuffleBlockIterator`:

```scala
val shuffleBlockIteratorFactories = scala.collection.mutable.Map.empty[Int, (TaskContext, Partition) => CometShuffleBlockIterator]

sparkPlans.zipWithIndex.foreach { case (plan, idx) =>
  plan match {
    // ... existing cases ...
    case _ if shuffleScanIndices.contains(inputIndexForPlan(idx)) =>
      // Still add the RDD for partition tracking, but also register
      // a factory for the raw InputStream
      val rdd = plan.executeColumnar()
      inputs += rdd
      // The factory creates a CometShuffleBlockIterator from the raw shuffle stream
      // We need to get the raw InputStream - see Step 5
      shuffleBlockIteratorFactories(inputs.size - 1) = ...
    // ... remaining cases ...
  }
}
```

The tricky part is getting the raw `InputStream` from the shuffle read. See Step 5.

- [ ] **Step 5: Add raw InputStream mode to CometBlockStoreShuffleReader**

In `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala`:

The current `read()` method creates `NativeBatchDecoderIterator` which decodes blocks. For direct read, we need a mode that yields the raw `InputStream` wrapped in `CometShuffleBlockIterator`.

Add a method:

```scala
def readRawStreams(): Iterator[CometShuffleBlockIterator] = {
  fetchIterator.map { case (_, inputStream) =>
    new CometShuffleBlockIterator(inputStream)
  }
}
```

The challenge is that `CometShuffledBatchRDD` calls `reader.read()` which returns `Iterator[Product2[Int, ColumnarBatch]]`. For the direct read path, we need a different RDD that calls `readRawStreams()` instead.

**Approach**: Create `CometShuffledRawStreamRDD` — a simple RDD that wraps the shuffle reader and yields `CometShuffleBlockIterator` objects per block. Then in `operators.scala`, instead of using the ColumnarBatch RDD, create a `CometShuffledRawStreamRDD` and pass its iterator-producing factory to `CometExecRDD`.

Alternatively, since `CometShuffleBlockIterator` wraps a single `InputStream` that may contain multiple blocks, and `fetchIterator` yields one `InputStream` per shuffle block, the simplest approach is to **concatenate all InputStreams into one** per partition:

```scala
def readAsRawStream(): InputStream = {
  val streams = fetchIterator.map(_._2)
  new SequenceInputStream(java.util.Collections.enumeration(
    streams.toList.asJava))
}
```

Then in the factory: `(ctx, part) => new CometShuffleBlockIterator(reader.readAsRawStream())`

But the reader is created per-partition in `CometShuffledBatchRDD.compute()`. The factory approach means the reader creation must be deferred.

**Simplest concrete approach**: Instead of a factory, create a new RDD `CometShuffledRawRDD` that returns `Iterator[CometShuffleBlockIterator]`. Pass this as a separate input alongside the regular `ColumnarBatch` inputs:

```scala
// In CometExecRDD, add:
shuffleRawInputRDDs: Seq[(Int, RDD[CometShuffleBlockIterator])]
```

In `compute`, create iterators from these RDDs and pass them to `CometExecIterator` via the `shuffleBlockIterators` map.

This is the most invasive part of the implementation. The exact approach should be determined by reading the code at implementation time, as there are multiple valid paths. The key constraint: the raw `InputStream` from `fetchIterator` must reach `CometShuffleBlockIterator` without going through `NativeBatchDecoderIterator`.

- [ ] **Step 6: Verify it compiles**

Run: `./mvnw compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 7: Commit**

```bash
git add spark/src/main/scala/org/apache/comet/Native.scala
git add spark/src/main/scala/org/apache/comet/CometExecIterator.scala
git add spark/src/main/scala/org/apache/spark/sql/comet/CometExecRDD.scala
git add spark/src/main/scala/org/apache/spark/sql/comet/operators.scala
git add spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala
git commit -m "feat: wire CometShuffleBlockIterator into JVM execution path"
```

---

### Task 9: End-to-end testing

**Files:**
- Modify: Appropriate test suite (find the right suite by searching for existing shuffle tests)

- [ ] **Step 1: Build everything**

Run: `make`
Expected: Successful build of both native and JVM.

- [ ] **Step 2: Run existing shuffle tests**

Run: `./mvnw test -Dsuites="org.apache.comet.exec.CometShuffleSuite"`
Expected: All existing tests pass (they now use the new direct read path by default).

If tests fail, debug by setting `spark.comet.shuffle.directRead.enabled=false` to confirm the old path still works, then investigate the new path.

- [ ] **Step 3: Add comparison test**

Add a test that runs the same queries with direct read enabled and disabled:

```scala
test("shuffle direct read produces same results as FFI path") {
  Seq(true, false).foreach { directRead =>
    withSQLConf(
      CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> directRead.toString) {
      val df = spark.range(1000)
        .selectExpr("id", "id % 10 as key", "cast(id as string) as value")
        .repartition(4, col("key"))
        .groupBy("key")
        .agg(sum("id").as("total"), count("value").as("cnt"))
        .orderBy("key")
      checkSparkAnswer(df)
    }
  }
}
```

- [ ] **Step 4: Add Rust unit test for ShuffleScanExec**

In `native/core/src/execution/operators/shuffle_scan.rs`, add a `#[cfg(test)]` module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::shuffle::codec::{CompressionCodec, ShuffleBlockWriter};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::io::Cursor;
    use std::sync::Arc;

    #[test]
    fn test_read_compressed_ipc_block() {
        // Create a test RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        ).unwrap();

        // Write it as compressed IPC using ShuffleBlockWriter
        let writer = ShuffleBlockWriter::try_new(
            &batch.schema(), CompressionCodec::Zstd(1)
        ).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = datafusion::physical_plan::metrics::Time::new();
        writer.write_batch(&batch, &mut buf, &ipc_time).unwrap();

        // Read back the body (skip the 16-byte header)
        let bytes = buf.into_inner();
        let body = &bytes[16..]; // Skip compressed_length(8) + field_count(8)

        // Decode using read_ipc_compressed
        let decoded = read_ipc_compressed(body).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 2);
    }
}
```

- [ ] **Step 5: Run all tests**

Run: `make test`

- [ ] **Step 6: Run clippy**

Run: `cd native && cargo clippy --all-targets --workspace -- -D warnings`
Expected: No warnings.

- [ ] **Step 7: Format**

Run: `make format`

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "test: add shuffle direct read tests"
```

---

## Implementation Notes

### Task 8 is the hardest

The core challenge is routing raw `InputStream` from Spark's shuffle infrastructure through to `CometShuffleBlockIterator` without going through the decode path. The current RDD pipeline (`CometShuffledBatchRDD` → `CometBlockStoreShuffleReader.read()` → `NativeBatchDecoderIterator`) always decodes. You need to intercept before `NativeBatchDecoderIterator` is created.

The most surgical approach: in `CometBlockStoreShuffleReader`, add a `readRaw()` method that returns the raw `InputStream` (or a `CometShuffleBlockIterator` wrapping it) instead of decoded batches. Then create a parallel RDD (`CometShuffledRawRDD`) that calls `readRaw()` in its `compute` method and pass it through to `CometExecIterator`.

### Metrics

`ShuffleScanExec` should track `decode_time` using DataFusion's `Time` metric. Register it in `ShuffleScanExec::new` via `MetricBuilder` following the pattern in `ScanExec`.

### Order of tasks

Tasks 1-7 can be done sequentially. Task 8 depends on all previous tasks. Task 9 validates everything.
