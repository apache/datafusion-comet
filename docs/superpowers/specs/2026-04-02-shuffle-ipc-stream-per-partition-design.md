# Shuffle Format: One IPC Stream Per Partition

**Issue:** [#3882](https://github.com/apache/datafusion-comet/issues/3882) — Current shuffle format has too much overhead with default batch size

**Date:** 2026-04-02

## Problem

The current shuffle format writes each batch as an independent Arrow IPC stream, repeating the schema for every batch. Each block also has a custom 20-byte header (8-byte length + 8-byte field count + 4-byte codec tag) and creates a new compression codec instance. With the default batch size of 8192 rows, Comet shuffle files are ~50% larger than Spark shuffle files and overall query performance is ~10% slower than Spark.

## Solution

Write one Arrow IPC stream per partition instead of one per batch. Use Arrow IPC's built-in per-buffer body compression instead of wrapping the entire IPC stream in an external compression codec. Move IPC stream parsing to Rust, with Rust pulling bytes from JVM's `InputStream` via JNI callbacks.

## Current Format (Per Partition)

```
[block 1: header(20 bytes) | compressed(schema + batch)]
[block 2: header(20 bytes) | compressed(schema + batch)]
...
[block N: header(20 bytes) | compressed(schema + batch)]
```

Each block is a self-contained compressed Arrow IPC stream with a custom header:
- 8 bytes: IPC block length (little-endian u64)
- 8 bytes: field count (little-endian u64)
- 4 bytes: compression codec tag (ASCII: `SNAP`, `LZ4_`, `ZSTD`, `NONE`)
- Variable: compressed Arrow IPC stream bytes (schema message + one record batch message)

The JVM reader parses the custom header, reads the exact byte blob, and passes it to Rust via JNI for decompression and decoding.

## New Format (Per Partition)

```
[standard Arrow IPC stream: schema | batch | batch | ... | EOS]
```

Each partition's data is a single standard Arrow IPC stream:
- One schema message (written once when the stream is created)
- N record batch messages (each batch's buffers individually compressed via Arrow IPC body compression)
- End-of-stream marker

No custom header. No external compression wrapper. Standard Arrow IPC framing handles message boundaries.

## Architecture Changes

### Write Side (Rust)

#### Immediate Mode (`ImmediateModePartitioner`)

`PartitionOutputStream` currently creates a new `StreamWriter` per `write_ipc_block()` call. Change to hold a persistent `StreamWriter` per partition:

- On first flush: create `StreamWriter` with `IpcWriteOptions::try_with_compression(...)`, which writes the schema message once
- On subsequent flushes: call `writer.write(batch)` which appends only the record batch message
- On `shuffle_write()`: call `writer.finish()` to write the EOS marker, then drain the buffer to the output file

The `StreamWriter` writes into the existing `Vec<u8>` buffer. Spill behavior is unchanged — when memory pressure triggers a spill, the buffer (now containing partial IPC stream bytes) is written to a spill file. On final write, spill files are concatenated before the remaining in-memory bytes. Since all bytes belong to the same IPC stream, concatenation produces a valid stream.

#### Buffered Mode (`MultiPartitionShuffleRepartitioner`)

`PartitionWriter` wraps `ShuffleBlockWriter`, which creates a new `StreamWriter` per `write_batch()` call. Change to hold a persistent `StreamWriter` per partition:

- Each `PartitionWriter` creates a `StreamWriter` on first batch write
- Subsequent batches append record batch messages only
- On final write: finish the stream and write to the output file

#### `ShuffleBlockWriter`

This struct is eliminated. Its responsibilities (compression codec selection, header writing, per-batch IPC encoding) are replaced by:
- `IpcWriteOptions` for compression codec selection
- Arrow `StreamWriter` for IPC encoding
- No custom headers needed

#### `SinglePartitionShufflePartitioner`

Same change — use a persistent `StreamWriter` for the single output partition.

### Read Side

#### New: `JniInputStream` (Rust)

A Rust struct that wraps a JVM `InputStream` object reference and implements `std::io::Read`:

```rust
struct JniInputStream {
    jvm: JavaVM,
    stream: GlobalRef,  // reference to JVM InputStream
    buffer: Vec<u8>,    // internal read-ahead buffer (e.g., 64KB)
    pos: usize,
    len: usize,
}

impl Read for JniInputStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // If internal buffer is exhausted, refill via JNI:
        //   byte[] jbuf = new byte[capacity];
        //   int n = inputStream.read(jbuf, 0, capacity);
        // Copy bytes from jbuf into self.buffer
        // Then copy from self.buffer into buf
    }
}
```

The internal buffer minimizes JNI boundary crossings. A 64KB buffer means one JNI call per 64KB of data, which keeps overhead negligible.

#### New: JNI Function `decodeShuffleStream`

```rust
pub unsafe extern "system" fn Java_org_apache_comet_Native_decodeShuffleStream(
    e: JNIEnv,
    _class: JClass,
    input_stream: JObject,      // JVM InputStream
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    tracing_enabled: jboolean,
) -> jlong {
    // 1. Wrap input_stream in JniInputStream
    // 2. Create Arrow StreamReader over it
    // 3. Read next batch from the stream
    // 4. Export batch via Arrow FFI (prepare_output)
    // 5. Return row count (or 0 if stream exhausted)
}
```

The `StreamReader` must be created with validation disabled (`with_skip_validation(true)`) since we control both the write and read sides and validation adds unnecessary overhead.

This function is called repeatedly by the JVM iterator until it returns 0 (stream exhausted). The `StreamReader` must persist across calls — either by storing it in a native handle that the JVM holds, or by restructuring the JVM iterator to make a single JNI call that iterates all batches.

**Recommended approach:** Use a native handle pattern:
1. `openShuffleStream(inputStream) -> long handle` — creates `JniInputStream` + `StreamReader`, returns opaque handle
2. `nextShuffleStreamBatch(handle, arrayAddrs, schemaAddrs) -> long rowCount` — reads next batch, exports via FFI
3. `closeShuffleStream(handle)` — drops the reader and stream

This avoids recreating the `StreamReader` per batch and cleanly manages the lifecycle.

#### JVM: `NativeBatchDecoderIterator`

Simplify to use the native handle pattern:

```scala
case class NativeBatchDecoderIterator(in: InputStream, ...) extends Iterator[ColumnarBatch] {
  // On construction: call nativeLib.openShuffleStream(in) to get handle
  private val handle = nativeLib.openShuffleStream(in)

  private def fetchNext(): Option[ColumnarBatch] = {
    // Call nativeLib.nextShuffleStreamBatch(handle, arrayAddrs, schemaAddrs)
    // Returns batch or None if stream exhausted
  }

  def close(): Unit = {
    nativeLib.closeShuffleStream(handle)
    in.close()
  }
}
```

Remove all manual header parsing (length bytes, field count, codec tag).

#### Removed

- `read_ipc_compressed()` in `ipc.rs` — no longer needed
- `decodeShuffleBlock` JNI function — replaced by the stream-based API
- Custom header format parsing in `NativeBatchDecoderIterator`

### Compression

#### Arrow IPC Body Compression

Arrow IPC supports per-buffer compression via `IpcWriteOptions`:

```rust
let options = IpcWriteOptions::try_with_compression(
    Some(CompressionType::LZ4_FRAME)  // or CompressionType::ZSTD
)?;
let writer = StreamWriter::try_new_with_options(output, &schema, options)?;
```

Each record batch's data buffers are individually compressed. The schema message and IPC framing metadata are not compressed (they're small). The `StreamReader` handles decompression transparently.

#### Supported Codecs

- **LZ4_FRAME** — fast compression/decompression, moderate ratio
- **ZSTD** — better compression ratio, slightly slower
- **None** — no compression

#### Dropped Codec

- **Snappy** — not supported by Arrow IPC body compression. This is acceptable because LZ4 provides similar speed characteristics with better compression ratios. The `CompressionCodec::Snappy` variant and all Snappy-related code paths are removed.

### Configuration

Map the existing `spark.comet.exec.shuffle.compression.codec` config (`COMET_EXEC_SHUFFLE_COMPRESSION_CODEC`) to Arrow IPC compression types:

| Config Value | Arrow IPC CompressionType |
|---|---|
| `lz4` | `CompressionType::LZ4_FRAME` |
| `zstd` | `CompressionType::ZSTD` |
| `none` | `None` |
| `snappy` | Error or fall back to `LZ4_FRAME` with warning |

### Format Compatibility

This is a **breaking change** to the shuffle format. The new format is not readable by old readers and vice versa. This is acceptable because:

- Shuffle data is ephemeral — it exists only for the duration of a job
- There is no cross-version shuffle data exchange
- All writers and readers within a single Comet deployment use the same version

### What Stays the Same

- **Index file format** — partition offset table written at end of shuffle write
- **Block fetching** — `ShuffleBlockFetcherIterator` and Spark's shuffle block resolution
- **Partition assignment** — hash/range partitioning logic in both modes
- **Spill file handling** — immediate mode spill/restore behavior (though spill files now contain partial IPC stream bytes)
- **`readAsRawStream()`** — still concatenates partition InputStreams; now each stream is a valid Arrow IPC stream

## Files Modified

### Rust (native/)

| File | Change |
|---|---|
| `shuffle/src/partitioners/immediate_mode.rs` | `PartitionOutputStream`: persistent `StreamWriter`, remove custom headers, use `IpcWriteOptions` compression |
| `shuffle/src/partitioners/multi_partition.rs` | `PartitionWriter`: persistent `StreamWriter` per partition |
| `shuffle/src/partitioners/single_partition.rs` | Same persistent `StreamWriter` change |
| `shuffle/src/writers/shuffle_block_writer.rs` | **Remove** — replaced by direct `StreamWriter` usage |
| `shuffle/src/ipc.rs` | **Remove** `read_ipc_compressed` — no longer needed |
| `core/src/execution/jni_api.rs` | Replace `decodeShuffleBlock` with `openShuffleStream`, `nextShuffleStreamBatch`, `closeShuffleStream` |
| `shuffle/src/lib.rs` | New `JniInputStream` struct, update exports |

### Scala (spark/)

| File | Change |
|---|---|
| `NativeBatchDecoderIterator.scala` | Replace header parsing with native handle pattern (`open`/`next`/`close`) |
| `Native.scala` (or equivalent) | Add new JNI method declarations |
| `CometConf.scala` | Update codec config to map to Arrow IPC types, deprecate Snappy |

### Removed

| File | Reason |
|---|---|
| `shuffle/src/writers/shuffle_block_writer.rs` | Replaced by persistent `StreamWriter` with `IpcWriteOptions` |
| `shuffle/src/ipc.rs` (partially) | `read_ipc_compressed` no longer needed |

## Testing

- Existing shuffle tests should pass with the new format (they test end-to-end behavior, not wire format)
- Add unit test for `JniInputStream` — mock a JVM `InputStream` and verify `Read` impl
- Add integration test verifying a partition with multiple batches produces a valid Arrow IPC stream
- Verify shuffle benchmark shows reduced file size and improved performance with default batch size
- Test with LZ4, ZSTD, and no compression
