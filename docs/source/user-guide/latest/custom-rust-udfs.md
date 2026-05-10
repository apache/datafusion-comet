<!---
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

# Custom Rust UDFs

Comet supports user-written **scalar UDFs in Rust**, distributed as a `cdylib` and dispatched directly inside Comet's native execution layer. There is no JNI round-trip per row — your UDF runs alongside Comet's built-in operators using `arrow-rs` arrays.

## When to use a Rust UDF vs. a Scala/Java `CometUDF`

Use a **Rust UDF** when you want native-speed compute and your function logic doesn't need to call into the JVM. Your `invoke` method receives `arrow-rs` `ArrayRef` inputs and returns an `ArrayRef`. The full `arrow-rs` compute kernels are available.

Use a **Scala/Java `CometUDF`** when you need access to JVM libraries or shared JVM state. The JVM `evaluate(Array[ValueVector])` callback runs on a JNI-attached thread, with one round trip per batch.

## Prerequisites

- Rust toolchain (matching Comet's MSRV — currently `1.88`).
- A working knowledge of `arrow-rs`'s `Array`, `ArrayRef`, and `DataType`.

The `comet-udf-sdk` crate (in this repository under `native/comet-udf-sdk/`) is the public surface. Until it is published to crates.io, depend on it via a path or git dep in your `Cargo.toml`.

## Project setup

`Cargo.toml`:

```toml
[package]
name = "my-comet-udfs"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
arrow = "58"
comet-udf-sdk = { path = "/path/to/datafusion-comet/native/comet-udf-sdk" }
```

The `[lib] crate-type = ["cdylib"]` is required — without it the `#[no_mangle]` symbols emitted by `comet_udf_sdk::export!` are not exported as dynamic symbols.

## Writing a UDF

Implement `CometScalarUdf` for each function, then register all of them with one `export!` invocation.

```rust
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;

use comet_udf_sdk::error::CometUdfError;
use comet_udf_sdk::types::{CometUdfSignature, Volatility};
use comet_udf_sdk::CometScalarUdf;

pub struct AddOne {
    sig: CometUdfSignature,
}

impl Default for AddOne {
    fn default() -> Self {
        Self {
            sig: CometUdfSignature {
                args: vec![DataType::Int64],
                return_type: DataType::Int64,
                volatility: Volatility::Immutable,
            },
        }
    }
}

impl CometScalarUdf for AddOne {
    fn name(&self) -> &str { "add_one" }
    fn signature(&self) -> &CometUdfSignature { &self.sig }
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
        let arr = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| CometUdfError::new("expected Int64Array"))?;
        let out: Int64Array = arr.iter().map(|v| v.map(|x| x + 1)).collect();
        Ok(Arc::new(out))
    }
}

comet_udf_sdk::export!(AddOne);
```

The `Default` impl is required — Comet constructs each UDF type via `Default::default()` once per process.

Implementations must be `Send + Sync` and stateless. Comet caches a single instance per UDF and may invoke it from many worker threads concurrently.

If you want to expose multiple UDFs from one library, list them all in the macro:

```rust
comet_udf_sdk::export!(AddOne, MultiplyByTwo, FormatTimestamp);
```

## Building per platform

```sh
cargo build --release
```

The output is `target/release/libmy_comet_udfs.{so,dylib}` (`.so` on Linux, `.dylib` on macOS, `.dll` on Windows). You need a separate build per executor architecture — typically Linux x86_64, Linux aarch64, and macOS arm64.

## Registering from Scala

Either declare each UDF's signature explicitly:

```scala
import org.apache.comet.udf.CometRustUDF
import org.apache.spark.sql.types.LongType

CometRustUDF.register(
  spark,
  name = "add_one",
  libraryPath = "/opt/comet/libmy_comet_udfs.so",
  inputTypes = Seq(LongType),
  returnType = LongType)
```

Or auto-discover every UDF the library exports:

```scala
val names = CometRustUDF.registerAll(spark, "/opt/comet/libmy_comet_udfs.so")
```

The `libraryPath` must be reachable on the **driver and every executor**. Distribute via Spark's `--files`, or pre-install the library on every host.

`register` validates the library on the driver immediately: it `dlopen`s the file, checks the SDK ABI version, finds the named UDF, and compares the declared signature to the one reported by the cdylib. Any mismatch raises a typed exception — no silent fallback.

## Using the UDF

Once registered, call it like any other SQL function:

```scala
spark.sql("SELECT add_one(id) AS y FROM range(0, 5)").show()
```

Comet's plan rule replaces the call with a native `RustUdfCall` that runs inside Comet's executor.

## Troubleshooting

- **`CometRustUdfLoadException`** — the file isn't loadable. Check the path on both driver and executors. Verify the library architecture matches the host (Linux x86_64 vs. arm64, etc.). The exception's cause carries the underlying OS error from `dlopen`.

- **`CometRustUdfAbiException`** — the library reports a different SDK ABI version than the host expects. Rebuild your UDF crate against a `comet-udf-sdk` version compatible with the Comet build you're running.

- **`CometRustUdfSignatureException`** — the `inputTypes` / `returnType` you passed to `register` don't match what the library reports. Either fix your declaration or use `registerAll` to skip explicit declaration.

- **`CometRustUdfNotEvaluatedException` at query time** — Comet's plan rule didn't replace the call, so the JVM stub fired. Confirm Comet is enabled (`spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions`) and the operator hosting the call (typically `Project` or `Filter`) is being converted to a `CometNativeExec`.

## Limitations

- **Scalar UDFs only** in v1. Aggregate, window, and table-valued UDFs are not yet supported.
- **No JVM fallback.** If the library can't load on an executor, the query fails. There is no automatic fall-through to a Scala implementation.
- **Library distribution is your responsibility.** Comet does not ship the `.so` to executors. Use Spark's `--files` or pre-install.
- **Loaded libraries are never unloaded** for the lifetime of the JVM process. The same path always returns the same handle — re-deploying a UDF requires restarting the executor JVMs.
- **Nested type signatures.** The auto type-name parser used by `registerAll` covers primitives only in v1. For UDFs taking or returning complex types (struct, list, decimal with precision/scale), use explicit `register` with `inputTypes` / `returnType` declared as Spark `DataType`s.
- **Rust ABI is not stable.** The `comet-udf-sdk` ABI version (currently `1`) protects against the SDK's own surface changing, but a UDF compiled against a different rustc / std / arrow major version than Comet's host can produce undefined behavior. Use the same toolchain and `arrow` version Comet uses for safety.
- **Process-level abort isolation.** A user UDF that calls `std::process::abort()` or triggers a stack overflow that bypasses the panic handler will kill the executor JVM. Same as any other native code Comet links against.

## Compatibility

Your UDF binary keeps working across Comet upgrades unless we bump the SDK ABI version. ABI bumps are rare and announced in the `comet-udf-sdk` release notes.
