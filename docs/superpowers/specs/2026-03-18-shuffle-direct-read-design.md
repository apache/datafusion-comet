# Shuffle Direct Read: Bypass FFI for Native Shuffle Read Path

## Problem

When a native shuffle exchange feeds into a downstream native operator, shuffle data crosses the JVM/native FFI boundary twice:

1. **Native to JVM**: `decodeShuffleBlock` JNI call decompresses Arrow IPC, creates a `RecordBatch`, and exports it via Arrow C Data Interface (per-column `FFI_ArrowArray` + `FFI_ArrowSchema` allocation, export, and import).
2. **JVM to Native**: `CometBatchIterator` re-exports the `ColumnarBatch` via Arrow C Data Interface back to native, where `ScanExec` imports and copies/unpacks the arrays.

Each crossing involves per-column schema serialization, struct allocation, and array copying. For queries with many shuffle stages or wide schemas, this overhead is significant.

## Solution

Introduce a direct read path where native code consumes compressed shuffle blocks directly, bypassing Arrow FFI entirely. The JVM reads raw bytes from Spark's shuffle infrastructure and hands them to native via a `DirectByteBuffer` (zero-copy pointer access). Native decompresses and decodes in-place, feeding `RecordBatch` directly into the execution plan.

### Data Flow Comparison

**Current path (double FFI):**

```
Shuffle stream
  -> NativeBatchDecoderIterator (JVM)
  -> JNI: decodeShuffleBlock
  -> FFI export: RecordBatch -> ArrowArray/Schema (native -> JVM)
  -> ColumnarBatch on JVM
  -> CometBatchIterator
  -> FFI export: ColumnarBatch -> ArrowArray/Schema (JVM -> native)
  -> ScanExec imports + copies arrays
  -> Native operators
```

**New path (zero FFI):**

```
Shuffle stream
  -> CometShuffleBlockIterator (JVM)
  -> reads header + compressed body into DirectByteBuffer
  -> holds bytes, waits for native pull

ShuffleScanExec (native, pull-based)
  -> JNI callback: iterator.hasNext()/getBuffer()
  -> read_ipc_compressed() -> RecordBatch
  -> feeds directly into native execution plan
```

## Scope

- Native shuffle (`CometNativeShuffle`) only. JVM columnar shuffle is excluded because its per-batch dictionary encoding decisions can change the schema between batches.
- Both paths (old and new) are retained. A config flag controls which is used.

## Components

### New JVM Components

#### `CometShuffleBlockIterator` (Java)

A new class that wraps a shuffle `InputStream` and exposes raw compressed blocks for native consumption. Absorbs the header-reading and buffer-management logic from `NativeBatchDecoderIterator`, but does not decode.

JNI-callable interface:

- `hasNext() -> int`: Reads the next block's header from the stream. The header is 16 bytes: 8-byte compressed length (includes the 8-byte field count but not itself) + 8-byte field count. The field count from the header is discarded — the schema is determined by the `ShuffleScan` protobuf's `fields` list, which is authoritative. Returns the compressed body length in bytes (i.e., `compressedLength - 8`, which includes the 4-byte codec prefix + compressed IPC data), or -1 for EOF.
- `getBuffer() -> ByteBuffer`: Returns the `DirectByteBuffer` containing the current block's compressed bytes (4-byte codec prefix + compressed IPC data). This buffer is only valid until the next `hasNext()` call — the caller must fully consume it (via `read_ipc_compressed()`, which decompresses into a new allocation) before pulling the next block.

Uses its own `DirectByteBuffer` instance (not shared with `NativeBatchDecoderIterator`) with the same pooling strategy: initial 128KB, grows as needed, reset on close.

**Lifecycle**: Implements `Closeable`. `close()` closes the underlying shuffle `InputStream` and resets the buffer. `CometBlockStoreShuffleReader` registers a task completion listener to close it, matching the existing pattern for `NativeBatchDecoderIterator`.

### New Native Components

#### `ShuffleScanExec` (Rust)

Location: `native/core/src/execution/operators/shuffle_scan.rs`

A new `ExecutionPlan` operator that replaces `ScanExec` at shuffle boundaries. On each `poll_next`:

1. Calls JNI into `CometShuffleBlockIterator.hasNext()` to get the next block's byte length (or -1 for EOF).
2. Calls `CometShuffleBlockIterator.getBuffer()` to get a `DirectByteBuffer`.
3. Obtains the buffer's raw pointer via `JNIEnv::get_direct_buffer_address()` and creates a slice over it (zero-copy, same pattern as `decodeShuffleBlock`).
4. Calls `read_ipc_compressed()` to decompress and decode into a `RecordBatch`. This allocates new memory for the decompressed data — the `DirectByteBuffer` can be safely reused afterward.
5. Returns the `RecordBatch` directly to the downstream native operator.

No `FFI_ArrowArray`, `FFI_ArrowSchema`, `ArrowImporter`, or `CometVector` involved.

Implements `on_close` for cleanup (releasing the JNI `GlobalRef`), matching the `ScanExec` pattern.

#### `ShuffleScan` Protobuf Message

Location: `native/proto/src/proto/operator.proto`

New message alongside existing `Scan`:

```protobuf
message ShuffleScan {
  repeated spark.spark_expression.DataType fields = 1;
  string source = 2;  // Informational label (e.g., "CometShuffleExchangeExec [id=5]")
}
```

The `Operator` message gains a new `shuffle_scan` field in its oneof.

### Modified JVM Components

#### `CometExchangeSink` / `CometExecRule`

The decision to use `ShuffleScan` vs `Scan` is made when `CometNativeExec` is constructed (not during the bottom-up conversion pass). At that point, the operator tree is already converted: `CometExecRule.convertBlock()` wraps a contiguous group of native operators into `CometNativeExec` and serializes the protobuf plan. The children (including `CometSinkPlaceHolder` wrapping shuffle exchanges) are already known. So the check is: when serializing a `CometSinkPlaceHolder` whose `originalPlan` is a `CometShuffleExchangeExec` with `shuffleType == CometNativeShuffle`, and the config flag is enabled, emit `ShuffleScan` instead of `Scan`.

Conditions for `ShuffleScan`:

1. Shuffle type is `CometNativeShuffle`
2. The sink is inside a `CometNativeExec` block (always true at serialization time — this is where sinks get serialized)
3. Config `spark.comet.shuffle.directRead.enabled` is true (default: true)

#### `CometNativeExec` (operators.scala)

When collecting input RDDs and creating iterators, distinguish the two cases:

- `ShuffleScan` input: Wrap the shuffle RDD's `Iterator[ColumnarBatch]` stream in `CometShuffleBlockIterator` — but note that `CometShuffleBlockIterator` wraps the raw `InputStream` from shuffle blocks, not decoded `ColumnarBatch`. This means the RDD must provide the raw shuffle `InputStream` rather than going through `NativeBatchDecoderIterator`. The `CometShuffledBatchRDD` / `CometBlockStoreShuffleReader` needs a mode where it yields raw `InputStream` objects per block instead of decoded batches.
- `Scan` input: Wrap in `CometBatchIterator` (existing behavior)

#### `CometExecIterator` — JNI Input Contract

Currently `CometExecIterator` wraps all inputs as `CometBatchIterator` and passes them to `Native.createPlan()` as `Array[CometBatchIterator]`. To support `CometShuffleBlockIterator`:

- Change the JNI parameter from `Array[CometBatchIterator]` to `Array[Object]`. On the native side in `createPlan`, the planner already knows from the protobuf whether each input is a `Scan` or `ShuffleScan`, so it knows which JNI methods to call on each `GlobalRef` — no type checking needed at runtime.
- `CometExecIterator` populates the array with either `CometBatchIterator` or `CometShuffleBlockIterator` based on whether the corresponding leaf in the protobuf plan is `Scan` or `ShuffleScan`.

### Native Planner Changes

In `planner.rs`, handle the `ShuffleScan` protobuf variant:

- Consume an input from `inputs.remove(0)` (same pattern as `Scan`)
- Create `ShuffleScanExec` instead of `ScanExec`
- The `GlobalRef` points to a `CometShuffleBlockIterator` Java object

## Fallback Behavior

The new path is used only when all conditions above are met. Otherwise, the existing path is used unchanged. The most common fallback case is a shuffle whose output is consumed by a non-native Spark operator (e.g., `collect()`, or an unsupported operator), where the JVM needs a materialized `ColumnarBatch`.

## Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `spark.comet.shuffle.directRead.enabled` | `true` | Use direct native read path for native shuffle when downstream operator is native |

## Error Handling

- `ShuffleScanExec` reuses `read_ipc_compressed()`, which handles corrupt data and unsupported codecs.
- JNI errors from `CometShuffleBlockIterator` (stream closed, EOF, I/O errors) propagate through the existing `try_unwrap_or_throw` pattern.
- If the JVM iterator throws, the exception surfaces as a Rust error and propagates through DataFusion's error handling.
- Empty batches (zero rows): `read_ipc_compressed()` calls `reader.next().unwrap()` which panics if the stream contains no batches. The shuffle writer never writes zero-row blocks (guarded by `if batch.num_rows() == 0 { return Ok(0) }` in `ShuffleBlockWriter.write_batch`), so this case does not arise.

## Metrics

`ShuffleScanExec` tracks and reports:

- `decodeTime`: Time spent in `read_ipc_compressed()` (decompression + IPC decode). Same metric as `NativeBatchDecoderIterator` reports today.
- Shuffle read metrics (`recordsRead`, `bytesRead`) continue to be reported by `CometBlockStoreShuffleReader` and the `ShuffleBlockFetcherIterator`, which are upstream of the new code and unchanged.

## Testing

- Existing shuffle tests (`CometShuffleSuite`) run with the config defaulting to true, automatically covering the new path.
- Add a test that runs the same queries with the config flag on and off, asserting identical results.
- Add a Rust unit test for `ShuffleScanExec` with pre-built compressed IPC blocks (no JNI), using the `TEST_EXEC_CONTEXT_ID` pattern from `ScanExec` tests.
