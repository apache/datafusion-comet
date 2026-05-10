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

//! Type tags, signatures, and the wire-format descriptor used across the
//! `comet-udf-sdk` C ABI.

use arrow::datatypes::{DataType, Field};

/// Function volatility, mirroring DataFusion's `Volatility`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    /// Always returns the same output for the same input.
    Immutable,
    /// May vary across queries but not within a single query.
    Stable,
    /// Free to vary on every call.
    Volatile,
}

/// Signature declared by a UDF.
#[derive(Debug, Clone)]
pub struct CometUdfSignature {
    /// Argument types in declaration order.
    pub args: Vec<DataType>,
    /// Return type.
    pub return_type: DataType,
    /// Volatility.
    pub volatility: Volatility,
}

/// Stable, version-pinned tag for Arrow data types used in the wire-format
/// descriptor. Primitive types map 1:1; complex types (`Struct`, `List`,
/// `Decimal`, timestamp-with-tz, etc.) use `Field` and carry their schema
/// as IPC bytes — see `UdfDescriptor` (added in Task 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ArrowTypeTag {
    /// Null type.
    Null = 0,
    /// Boolean type.
    Boolean = 1,
    /// Signed 8-bit integer.
    Int8 = 2,
    /// Signed 16-bit integer.
    Int16 = 3,
    /// Signed 32-bit integer.
    Int32 = 4,
    /// Signed 64-bit integer.
    Int64 = 5,
    /// Unsigned 8-bit integer.
    UInt8 = 6,
    /// Unsigned 16-bit integer.
    UInt16 = 7,
    /// Unsigned 32-bit integer.
    UInt32 = 8,
    /// Unsigned 64-bit integer.
    UInt64 = 9,
    /// 32-bit floating point.
    Float32 = 10,
    /// 64-bit floating point.
    Float64 = 11,
    /// UTF-8 encoded string (32-bit offsets).
    Utf8 = 12,
    /// UTF-8 encoded string (64-bit offsets).
    LargeUtf8 = 13,
    /// Variable-length binary (32-bit offsets).
    Binary = 14,
    /// Variable-length binary (64-bit offsets).
    LargeBinary = 15,
    /// Date represented as days since UNIX epoch (32-bit).
    Date32 = 16,
    /// Date represented as milliseconds since UNIX epoch (64-bit).
    Date64 = 17,
    /// Anything else — accompanied by IPC-encoded `Field` bytes.
    Field = 255,
}

impl ArrowTypeTag {
    /// Tag for a primitive `DataType`. Returns `None` for non-primitive
    /// types — those must be carried as `Field` IPC bytes by the caller.
    pub fn primitive(dt: &DataType) -> Option<Self> {
        Some(match dt {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            _ => return None,
        })
    }

    /// Inverse of [`ArrowTypeTag::primitive`] for the subset of tags representing a
    /// complete `DataType` without auxiliary bytes. Returns `None` for
    /// `Field`.
    pub fn to_data_type(self) -> Option<DataType> {
        Some(match self {
            Self::Null => DataType::Null,
            Self::Boolean => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::LargeUtf8 => DataType::LargeUtf8,
            Self::Binary => DataType::Binary,
            Self::LargeBinary => DataType::LargeBinary,
            Self::Date32 => DataType::Date32,
            Self::Date64 => DataType::Date64,
            Self::Field => return None,
        })
    }
}

/// Encode a `Field` as raw FlatBuffer-encoded Arrow `Schema` message bytes.
///
/// This is **not** a framed Arrow IPC stream (no continuation marker, no
/// length prefix). The receiver must decode with [`field_from_ipc_bytes`]
/// or `arrow::ipc::convert::try_schema_from_flatbuffer_bytes` — *not*
/// `try_schema_from_ipc_buffer` or `StreamReader`. The format is stable
/// across `arrow-rs` versions because Arrow's FlatBuffer schema layout
/// is part of the Arrow specification.
pub fn field_to_ipc_bytes(field: &Field) -> Vec<u8> {
    use arrow::datatypes::Schema;
    use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    let schema = Schema::new(vec![field.clone()]);
    let opts = IpcWriteOptions::default();
    let gen = IpcDataGenerator {};
    let mut tracker = DictionaryTracker::new(false);
    let encoded = gen.schema_to_bytes_with_dictionary_tracker(&schema, &mut tracker, &opts);
    encoded.ipc_message
}

/// Decode a `Field` from raw FlatBuffer-encoded Arrow `Schema` message bytes
/// produced by [`field_to_ipc_bytes`].
///
/// These bytes are **not** a framed Arrow IPC stream; use this function (or
/// `arrow::ipc::convert::try_schema_from_flatbuffer_bytes`) to decode them —
/// *not* `try_schema_from_ipc_buffer` or `StreamReader`.
///
/// Errors if the bytes cannot be parsed or do not contain exactly one field.
pub fn field_from_ipc_bytes(bytes: &[u8]) -> Result<Field, String> {
    use arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
    let schema = try_schema_from_flatbuffer_bytes(bytes)
        .map_err(|e| format!("invalid IPC schema bytes: {e}"))?;
    if schema.fields().len() != 1 {
        return Err(format!(
            "expected exactly 1 field, got {}",
            schema.fields().len()
        ));
    }
    Ok(schema.field(0).as_ref().clone())
}

use std::ffi::c_char;

/// Wire-format description of a UDF, populated by `comet_udf_describe`.
///
/// Field/return-type encoding:
/// - For primitive types, `*_tag` is the corresponding [`ArrowTypeTag`]
///   and `*_field_ipc_*` are zero.
/// - For complex types, `*_tag` is `ArrowTypeTag::Field as u32` and
///   `*_field_ipc_ptr` / `_len` point to IPC-encoded `Field` bytes
///   owned by the cdylib (lifetime: process-static).
///
/// `name_ptr` / `name_len` reference a static UTF-8 string in the cdylib.
#[repr(C)]
pub struct UdfDescriptor {
    /// Pointer to the UDF name (UTF-8, not null-terminated).
    pub name_ptr: *const c_char,
    /// Length of the name in bytes.
    pub name_len: u32,
    /// Volatility tag: 0 = Immutable, 1 = Stable, 2 = Volatile.
    pub volatility: u32,
    /// Number of arguments.
    pub n_args: u32,
    /// Pointer to an array of `n_args` u32 type tags.
    pub arg_tags: *const u32,
    /// Pointer to an array of `n_args` IPC pointers (null for primitive args).
    pub arg_field_ipc_ptrs: *const *const u8,
    /// Pointer to an array of `n_args` IPC byte-lengths (zero for primitive args).
    pub arg_field_ipc_lens: *const u32,
    /// Return type tag.
    pub return_tag: u32,
    /// IPC-encoded Field bytes for non-primitive return types (null otherwise).
    pub return_field_ipc_ptr: *const u8,
    /// IPC byte-length for non-primitive return types (zero otherwise).
    pub return_field_ipc_len: u32,
    /// Reserved for future ABI minor versions; zero today.
    pub _reserved: [u64; 4],
}

impl UdfDescriptor {
    /// Construct a fully zeroed descriptor. Used by the host before
    /// calling `comet_udf_describe`.
    pub fn zeroed() -> Self {
        Self {
            name_ptr: std::ptr::null(),
            name_len: 0,
            volatility: 0,
            n_args: 0,
            arg_tags: std::ptr::null(),
            arg_field_ipc_ptrs: std::ptr::null(),
            arg_field_ipc_lens: std::ptr::null(),
            return_tag: 0,
            return_field_ipc_ptr: std::ptr::null(),
            return_field_ipc_len: 0,
            _reserved: [0; 4],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields};

    #[test]
    fn primitive_tag_roundtrip() {
        for dt in [
            DataType::Boolean,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
        ] {
            let tag = ArrowTypeTag::primitive(&dt).unwrap();
            assert_eq!(tag.to_data_type().unwrap(), dt);
        }
    }

    #[test]
    fn struct_type_via_ipc() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, false),
        ]);
        let f = Field::new("root", DataType::Struct(fields), true);
        let bytes = field_to_ipc_bytes(&f);
        let f2 = field_from_ipc_bytes(&bytes).unwrap();
        assert_eq!(f, f2);
    }
}
