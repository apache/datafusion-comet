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

Comet can load scalar user-defined functions written in Rust from a shared library and run them
natively, inside the Comet pipeline, with no JVM round trip per row.

This is different from [Scala UDF and Java UDF Support](scala_java_udfs.md), where the user
function stays on the JVM and Comet dispatches into it. Here the function is compiled Rust that
operates directly on Arrow arrays.

> **Experimental.** This feature and the ABI it depends on are experimental. Neither
> `comet-udf-sdk` nor `CometNativeUDF` is part of Comet's supported API: they fall under
> [everything else is internal](../../about/versioning_policy.md#everything-else-is-internal) in the
> [versioning policy](../../about/versioning_policy.md), so they may change or be removed in any
> release, including a patch release, with no deprecation cycle. Expect to rebuild your UDF library
> against the SDK from each Comet release you upgrade to. It is not yet recommended for production
> use. See [Limitations](#limitations) before adopting it.

## Writing a UDF

A UDF library is an ordinary Rust `cdylib` that depends on `comet-udf-sdk` and `arrow`:

```toml
[package]
name = "my-comet-udfs"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
arrow = "58"
comet-udf-sdk = { git = "https://github.com/apache/datafusion-comet" }
```

Implement the `CometCScalarUdf` trait and export it:

```rust
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use comet_udf_sdk::c_abi::CometCScalarUdf;
use comet_udf_sdk::comet_c_udf_export;

#[derive(Default)]
pub struct AddOne;

impl CometCScalarUdf for AddOne {
    /// The name the function is registered and called under.
    fn name(&self) -> &str {
        "add_one"
    }

    /// Validate the argument types and declare the output type. Called once
    /// per execution, before `invoke`. Returning `Err` fails the query with
    /// your message.
    fn return_field(&self, args: &[Field]) -> Result<Field, String> {
        if args.len() != 1 || args[0].data_type() != &DataType::Int64 {
            return Err("add_one expects (Int64) -> Int64".into());
        }
        Ok(Field::new("add_one", DataType::Int64, true))
    }

    /// Evaluate one batch. `args` holds one Arrow array per argument.
    fn invoke(&self, args: &[ArrayRef], _n_rows: usize) -> Result<ArrayRef, String> {
        let a = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected an Int64Array")?;
        Ok(Arc::new(
            a.iter().map(|v| v.map(|x| x + 1)).collect::<Int64Array>(),
        ))
    }
}

comet_c_udf_export!(AddOne);
```

Each type passed to `comet_c_udf_export!` must implement `CometCScalarUdf` and `Default`. One
library may export any number of functions.

Build it:

```sh
cargo build --release
# target/release/libmy_comet_udfs.so   (Linux)
# target/release/libmy_comet_udfs.dylib (macOS)
```

Note that the library depends only on `arrow` and the SDK, not on DataFusion. The ABI is built
purely on the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html),
which keeps your library decoupled from the DataFusion version Comet happens to use.

Nothing in that ABI is Rust-specific — it is C structs of function pointers carrying Arrow C Data
Interface arrays — so a library written in C or C++ could implement it. That is not supported or
tested today, though: no C header is published, the struct layouts are only defined in the Rust
source and are not stable across Comet releases, and the SDK's panic guards, which keep a bug in
your UDF from taking down the executor, have no automatic equivalent in another language. Treat the
Rust SDK as the way to write a UDF for now.

## Registering and calling a UDF

Register the function on the driver, giving its name, the library path, and its signature:

```scala
import org.apache.comet.udf.CometNativeUDF
import org.apache.spark.sql.types.LongType

CometNativeUDF.register(
  spark,
  name = "add_one",
  libraryPath = "/opt/udfs/libmy_comet_udfs.so",
  inputTypes = Seq(LongType),
  returnType = LongType)
```

Registration loads the library on the driver and verifies that a function with that name exists, so
a bad path or a missing function fails immediately with a clear error rather than at execution time.

The function is then callable from SQL or the DataFrame API like any other:

```scala
spark.range(0, 5).selectExpr("add_one(id) AS y").show()
```

Your function must be a pure function of its arguments. Comet plans every Rust UDF as immutable,
which lets the optimizer fold a call over constants, evaluate it once and reuse the result, or drop
a repeated call as a common subexpression. `register` therefore rejects `deterministic = false`
rather than accept a function whose volatility it would go on to ignore.

`libraryPath` is passed to the platform's dynamic loader. An absolute path is what you want in
practice, and it is what the rest of this page assumes, but a bare library name resolves the same
way it would for any other shared object, through `LD_LIBRARY_PATH` on Linux and
`DYLD_LIBRARY_PATH` on macOS. That is a convenience, not a sandbox: Comet does not restrict which
paths may be loaded, so it makes no difference to the trust decision described under
[Limitations](#limitations).

## Return types

A UDF does not have one fixed return type. Its `return_field` is called with the actual argument
types and computes the output type on demand, so a single kernel can serve many signatures: the
`echo_c` UDF in Comet's own test library returns whatever type it is given, for every type in the
table below.

What is fixed is the type you declare to `register`, because Spark needs a concrete `DataType` at
analysis time in order to plan the query. That declaration is per-registration, not per-kernel:
re-registering the same function under different types is supported, and the kernel computes the
matching return type each time.

The two must agree. Comet checks the declared type against what `return_field` reports at planning
time and fails with both types named if they differ, rather than letting it surface as a type
assertion partway through execution. Nested nullability (`containsNull`, struct field nullability)
is not part of that comparison, since Spark and Arrow disagree about it harmlessly, but everything
that changes how bytes are read is: decimal precision and scale, timestamp unit, and struct field
names and order.

Watch for Spark's own type promotion when declaring: `cast(id as decimal(10,2)) + 0.25` has type
`decimal(11,2)`, not `decimal(10,2)`, so registering the latter is a mismatch.

## Supported types

Arguments and return values may be any of:

| Spark type          | Arrow type               |
| ------------------- | ------------------------ |
| `BooleanType`       | `Boolean`                |
| `ByteType`          | `Int8`                   |
| `ShortType`         | `Int16`                  |
| `IntegerType`       | `Int32`                  |
| `LongType`          | `Int64`                  |
| `FloatType`         | `Float32`                |
| `DoubleType`        | `Float64`                |
| `DecimalType(p, s)` | `Decimal128(p, s)`       |
| `StringType`        | `Utf8`                   |
| `BinaryType`        | `Binary`                 |
| `DateType`          | `Date32`                 |
| `TimestampType`     | `Timestamp(Microsecond)` |
| `TimestampNTZType`  | `Timestamp(Microsecond)` |

Complex types are supported and may be nested arbitrarily:

| Spark type                | Arrow type            |
| ------------------------- | --------------------- |
| `ArrayType(t)`            | `List(t)`             |
| `StructType(f1, f2, ...)` | `Struct(f1, f2, ...)` |
| `MapType(k, v)`           | `Map(k, v)`           |

Nulls are preserved in both directions; a null input row arrives as a null slot in the Arrow array
and your output nulls come back to Spark as nulls.

Not yet supported: `CalendarIntervalType`, `NullType`, `UserDefinedType`, and Arrow extension types
such as Variant and Geometry.

## Error handling

Returning `Err(String)` from `return_field` or `invoke` fails the query with your message attached.
This is the intended way to reject bad input.

Panics in your code are caught at the FFI boundary and converted into query errors, so an `unwrap`
on `None` fails that query rather than taking down the executor. Do not rely on this as a control
flow mechanism: prefer returning `Err`, which produces a much better message.

## Limitations

This feature is at an early stage. The current limitations are:

- **Scalar functions only.** Aggregate, window, and table functions are not supported.
- **Immutable functions only.** A UDF must return the same output for the same input. Comet plans
  every Rust UDF with DataFusion's `Volatility::Immutable`, so a function that reads a clock, draws
  from an RNG, or carries state across batches may be folded at plan time, evaluated once and
  reused, or eliminated as a common subexpression. `register` rejects `deterministic = false`.
- **The library must already be present on every executor**, at the same absolute path given to
  `register`. Comet does not distribute it for you: stage it with your image, a mounted volume, or
  your cluster's own file distribution, and pass a path that is valid cluster-wide. A path that
  exists only on the driver will fail at execution time.
- **Up to 4 arguments** per function.
- **No type coercion.** Arguments arrive as the types the query produces; `return_field` should
  reject anything it does not handle. Downcast defensively in `invoke` and return a clear `Err`
  rather than assuming a particular array layout.
- **Loading a library is loading native code.** It runs with the full privileges of the executor
  process and Comet cannot sandbox it: a bug in a UDF can corrupt memory or crash the executor.
  Only register libraries you trust and control.
- Once loaded, a library stays loaded for the life of the process. Replacing the file on disk has
  no effect until the executors restart.
- The ABI is versioned and checked strictly at load time. A library built against a different
  Comet's SDK is refused with an explicit ABI-mismatch error rather than being loaded unsafely.
  Rebuild your UDF library when upgrading Comet.
