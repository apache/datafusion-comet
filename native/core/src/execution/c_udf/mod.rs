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

//! Loader and adapters for user-supplied native UDF cdylibs registered
//! through `CometNativeUDF` on the JVM side.
//!
//! The ABI is built only on Arrow's stable FFI (the C Data Interface):
//! `comet_c_udf_list_v1` returns sedona-style `CometCScalarKernel`
//! factory structs. No DataFusion type appears in the FFI surface, so a
//! user library is not coupled to Comet's DataFusion version, and nothing
//! here depends on the library having been written in Rust. Producing one
//! from C or C++ is possible in principle but unsupported and untested:
//! no header is shipped, and the SDK's panic guards have no equivalent.
//!
//! See `comet_udf_sdk` for the rationale behind not exposing
//! `datafusion-ffi` here.

pub mod cache;
pub mod imported_c;
pub mod loader;

#[cfg(test)]
pub(crate) mod test_support;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};

/// Rewrite every nested field of `dt` as nullable.
///
/// Spark carries `containsNull` / field nullability inside the declared type, so
/// `array<int>` with `containsNull = false` converts to an Arrow `List` whose child field is
/// non-nullable. The arrays Comet actually hands to a UDF normalize those child fields to
/// nullable, so the declared type and the delivered type disagree on nested nullability even when
/// the UDF is behaving perfectly.
///
/// Used only to compare two types; never to build a type that gets handed to Arrow, which matters
/// because some nested types (map keys) require a non-nullable child.
fn erase_nested_nullability(dt: &DataType) -> DataType {
    fn erase(f: &FieldRef) -> FieldRef {
        Arc::new(Field::new(
            f.name(),
            erase_nested_nullability(f.data_type()),
            true,
        ))
    }
    match dt {
        DataType::List(f) => DataType::List(erase(f)),
        DataType::LargeList(f) => DataType::LargeList(erase(f)),
        DataType::ListView(f) => DataType::ListView(erase(f)),
        DataType::LargeListView(f) => DataType::LargeListView(erase(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(erase(f), *n),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(erase).collect()),
        DataType::Map(f, sorted) => DataType::Map(erase(f), *sorted),
        other => other.clone(),
    }
}

/// True if two types agree once nested nullability is disregarded.
///
/// This is the check applied between the return type registered on the JVM side and the type the
/// UDF's own `return_field` reports. It stays strict about everything that changes how bytes are
/// read (decimal precision and scale, timestamp unit, child ordering and names) while tolerating
/// the nested-nullability drift described on [`erase_nested_nullability`].
pub fn return_types_compatible(declared: &DataType, actual: &DataType) -> bool {
    declared == actual || erase_nested_nullability(declared) == erase_nested_nullability(actual)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Fields, TimeUnit};

    fn list(inner: DataType, nullable: bool) -> DataType {
        DataType::List(Arc::new(Field::new("item", inner, nullable)))
    }

    #[test]
    fn identical_types_are_compatible() {
        assert!(return_types_compatible(&DataType::Int32, &DataType::Int32));
    }

    #[test]
    fn nested_nullability_is_disregarded() {
        assert!(return_types_compatible(
            &list(DataType::Int32, false),
            &list(DataType::Int32, true)
        ));
    }

    #[test]
    fn nested_nullability_is_disregarded_through_several_levels() {
        let non_null = list(
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)])),
            false,
        );
        let nullable = list(
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
            true,
        );
        assert!(return_types_compatible(&non_null, &nullable));
    }

    #[test]
    fn differing_value_types_are_incompatible() {
        assert!(!return_types_compatible(
            &list(DataType::Int32, true),
            &list(DataType::Int64, true)
        ));
    }

    /// The case that motivated the check: Spark widens decimal arithmetic, so a UDF registered
    /// with the un-widened precision must still be rejected.
    #[test]
    fn differing_decimal_precision_is_incompatible() {
        assert!(!return_types_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(11, 2)
        ));
    }

    #[test]
    fn differing_timestamp_unit_is_incompatible() {
        assert!(!return_types_compatible(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        ));
    }

    #[test]
    fn differing_struct_field_names_are_incompatible() {
        let a = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));
        let b = DataType::Struct(Fields::from(vec![Field::new("b", DataType::Int32, true)]));
        assert!(!return_types_compatible(&a, &b));
    }
}
