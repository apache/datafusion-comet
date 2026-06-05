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

//! Arrow version bridge for the kernel-read path (Phase 1b).
//!
//! `delta_kernel` 0.19 pins **arrow-57** internally; Comet's execution plans run on
//! **arrow-58**. The two never exchange typed Arrow values on the existing scan path -- only
//! plain Rust types (`ScanFile`, `HashMap`, ...) cross the boundary. The kernel-read path is
//! the first place we must hand a kernel-produced `RecordBatch` (arrow-57) back into a Comet
//! plan (arrow-58).
//!
//! The bridge is the **Arrow C Data Interface**. `FFI_ArrowArray` / `FFI_ArrowSchema` are
//! `#[repr(C)]` mirrors of the Arrow C Data Interface spec, byte-identical across arrow
//! versions, so we export each column from arrow-57 to those FFI structs and re-import on the
//! arrow-58 side. It is zero-copy: arrow-58 takes ownership of arrow-57's buffers and invokes
//! arrow-57's `release` callback when done (both crates are statically linked into `libcomet`,
//! so the callback is always resident).

use arrow::array::{make_array, ArrayRef as ArrayRef58, RecordBatch as RecordBatch58};
use arrow::datatypes::{Field as Field58, Schema as Schema58};
use arrow::error::ArrowError;
use delta_kernel::arrow::array::{Array as _, RecordBatch as RecordBatch57};
use delta_kernel::arrow::datatypes::{Field as Field57, Schema as Schema57};
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use delta_kernel::schema::{SchemaRef, StructType};
use delta_kernel::{DeltaResult, Error};
use std::sync::Arc;

/// Convert a Comet (arrow-58) `Schema` into a kernel (arrow-57) `StructType`, which is what
/// `read_file_via_kernel` needs for the physical/logical schemas. Each field crosses the
/// arrow-version boundary via the C Data Interface schema struct (metadata only, no buffers),
/// then kernel's own `TryFromArrow` does the arrow->kernel mapping -- so nested/decimal/
/// timestamp fidelity matches kernel exactly rather than a hand-rolled type match.
///
/// This is the executor-side source of the kernel schema: it comes from the proto's
/// `required_schema` (already converted to an arrow-58 `Schema` upstream), NOT from a kernel
/// `Snapshot` -- no executor-side log replay.
pub fn comet_schema_to_kernel(schema: &Schema58) -> DeltaResult<SchemaRef> {
    let mut kernel_fields: Vec<Field57> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let ffi_58 = arrow::ffi::FFI_ArrowSchema::try_from(field.as_ref())
            .map_err(|e| Error::generic(format!("schema bridge export failed: {e}")))?;
        // SAFETY: identical C Data Interface ABI across arrow 57/58 (size asserted in
        // `kernel_batch_to_comet`). The arrow-58-installed `release` callback travels with the
        // struct, so dropping the arrow-57 view still releases correctly.
        let ffi_57: delta_kernel::arrow::ffi::FFI_ArrowSchema =
            unsafe { std::mem::transmute(ffi_58) };
        let field_57 = Field57::try_from(&ffi_57)
            .map_err(|e| Error::generic(format!("schema bridge import failed: {e}")))?;
        kernel_fields.push(field_57);
    }
    let arrow_57_schema = Schema57::new(kernel_fields);
    let struct_type = StructType::try_from_arrow(&arrow_57_schema)
        .map_err(|e| Error::generic(format!("arrow->kernel schema conversion failed: {e}")))?;
    Ok(Arc::new(struct_type))
}

/// Bridge a kernel (arrow-57) `RecordBatch` to a Comet (arrow-58) `RecordBatch` over the Arrow
/// C Data Interface. Column names + nullability are carried from the arrow-57 schema; the
/// arrow-58 data type is whatever `from_ffi` reconstructs (the C Data Interface is the source
/// of truth for type, so the two always agree).
pub fn kernel_batch_to_comet(batch: &RecordBatch57) -> Result<RecordBatch58, ArrowError> {
    let kernel_schema = batch.schema();
    let mut fields58: Vec<Field58> = Vec::with_capacity(batch.num_columns());
    let mut arrays58: Vec<ArrayRef58> = Vec::with_capacity(batch.num_columns());

    for (i, col) in batch.columns().iter().enumerate() {
        // Export the arrow-57 array to the C Data Interface structs.
        let data57 = col.to_data();
        let (ffi_array_57, ffi_schema_57) = delta_kernel::arrow::ffi::to_ffi(&data57)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        // The FFI structs are byte-identical across arrow versions (same C Data Interface
        // ABI), so reinterpret them as arrow-58's FFI types. SAFETY: `FFI_ArrowArray` and
        // `FFI_ArrowSchema` are `#[repr(C)]` over the frozen Arrow C Data Interface layout;
        // arrow 57 and 58 declare the identical struct. The compile-time size asserts below
        // guard against an unexpected layout change.
        const _: () = assert!(
            std::mem::size_of::<delta_kernel::arrow::ffi::FFI_ArrowArray>()
                == std::mem::size_of::<arrow::ffi::FFI_ArrowArray>()
        );
        const _: () = assert!(
            std::mem::size_of::<delta_kernel::arrow::ffi::FFI_ArrowSchema>()
                == std::mem::size_of::<arrow::ffi::FFI_ArrowSchema>()
        );
        let ffi_array_58: arrow::ffi::FFI_ArrowArray =
            unsafe { std::mem::transmute(ffi_array_57) };
        let ffi_schema_58: arrow::ffi::FFI_ArrowSchema =
            unsafe { std::mem::transmute(ffi_schema_57) };

        // Re-import on the arrow-58 side. SAFETY: the structs were produced by a valid
        // `to_ffi` export and are consumed exactly once here; arrow-58 owns them afterward.
        let data58 = unsafe { arrow::ffi::from_ffi(ffi_array_58, &ffi_schema_58) }?;
        let array58 = make_array(data58);

        let kf = kernel_schema.field(i);
        fields58.push(Field58::new(
            kf.name(),
            array58.data_type().clone(),
            kf.is_nullable(),
        ));
        arrays58.push(array58);
    }

    RecordBatch58::try_new(Arc::new(Schema58::new(fields58)), arrays58)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array as _, Int64Array as Int64Array58, StringArray as StringArray58};
    use delta_kernel::arrow::array::{Int64Array as Int64Array57, StringArray as StringArray57};
    use delta_kernel::arrow::datatypes::{
        DataType as DataType57, Field as Field57, Schema as Schema57,
    };

    // Build an arrow-57 batch, bridge it to arrow-58, and confirm the values + schema survive.
    // The two columns carry different types (Int64 nullable, Utf8 with a null) so a bridge that
    // dropped a buffer, mis-mapped a column, or lost the null mask would fail here.
    #[test]
    fn bridges_arrow57_batch_to_arrow58() {
        let schema57 = Arc::new(Schema57::new(vec![
            Field57::new("id", DataType57::Int64, true),
            Field57::new("name", DataType57::Utf8, true),
        ]));
        let batch57 = RecordBatch57::try_new(
            schema57,
            vec![
                Arc::new(Int64Array57::from(vec![Some(10), None, Some(30)])),
                Arc::new(StringArray57::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap();

        let batch58 = kernel_batch_to_comet(&batch57).unwrap();

        assert_eq!(batch58.num_rows(), 3);
        assert_eq!(batch58.num_columns(), 2);
        assert_eq!(batch58.schema().field(0).name(), "id");
        assert_eq!(batch58.schema().field(1).name(), "name");

        let ids = batch58
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array58>()
            .unwrap();
        assert_eq!(ids.value(0), 10);
        assert!(ids.is_null(1));
        assert_eq!(ids.value(2), 30);

        let names = batch58
            .column(1)
            .as_any()
            .downcast_ref::<StringArray58>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "b");
        assert!(names.is_null(2));
    }

    // Comet (arrow-58) Schema -> kernel StructType, with names + types + nullability preserved.
    #[test]
    fn comet_schema_bridges_to_kernel() {
        use arrow::datatypes::{DataType as DataType58, Field as F58};
        use delta_kernel::schema::DataType as KDataType;

        let schema58 = Schema58::new(vec![
            F58::new("id", DataType58::Int64, true),
            F58::new("name", DataType58::Utf8, false),
            F58::new("score", DataType58::Float64, true),
        ]);

        let kernel = comet_schema_to_kernel(&schema58).unwrap();
        let fields: Vec<_> = kernel.fields().collect();
        assert_eq!(fields.len(), 3);

        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[0].data_type(), &KDataType::LONG);
        assert!(fields[0].is_nullable());

        assert_eq!(fields[1].name(), "name");
        assert_eq!(fields[1].data_type(), &KDataType::STRING);
        assert!(!fields[1].is_nullable());

        assert_eq!(fields[2].name(), "score");
        assert_eq!(fields[2].data_type(), &KDataType::DOUBLE);
    }
}
