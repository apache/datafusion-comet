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

/// Canonical name Comet gives a list's element field.
const LIST_ELEMENT_NAME: &str = "item";
/// Canonical name Comet gives a map's entries field.
const MAP_ENTRIES_NAME: &str = "entries";
/// Canonical names Comet gives a map's key and value fields.
const MAP_KEY_NAME: &str = "key";
const MAP_VALUE_NAME: &str = "value";

/// Rewrite `dt` into the form used to compare a declared type against a delivered one.
///
/// Two normalizations happen here, both because Spark's type and the Arrow type a UDF hands back
/// disagree in ways that do not change a single byte of data.
///
/// **Nested nullability.** Spark carries `containsNull` / field nullability inside the declared
/// type, so `array<int>` with `containsNull = false` converts to an Arrow `List` whose child field
/// is non-nullable. The arrays Comet actually hands to a UDF normalize those child fields to
/// nullable, so the two types disagree even when the UDF is behaving perfectly. Every nested field
/// is therefore rewritten as nullable.
///
/// **List and map child field names.** The Arrow columnar format leaves these names unconstrained:
/// a list's single child and a map's entries struct (and its two children) are addressed by
/// position, not by name, and Comet's own JVM side reads them positionally too. arrow-rs's most
/// natural constructors do not agree with Comet on them either, since `MapBuilder::new(None, ..)`
/// names them `entries` / `keys` / `values` where Comet's Spark conversion emits `entries` /
/// `key` / `value`. Rejecting a UDF over that would be rejecting it for a spelling. They are
/// rewritten to Comet's names so the comparison ignores the difference.
///
/// Struct field names are *not* normalized: those are part of the Spark type and a caller reading
/// `row.getAs[Row]("x").getAs[Int]("a")` depends on them.
///
/// Used only to compare two types; never to build a type that gets handed to Arrow, which matters
/// because some nested types (map keys) require a non-nullable child.
fn normalize_for_comparison(dt: &DataType) -> DataType {
    fn rename(f: &FieldRef, name: &str) -> FieldRef {
        Arc::new(Field::new(
            name,
            normalize_for_comparison(f.data_type()),
            true,
        ))
    }
    fn keep_name(f: &FieldRef) -> FieldRef {
        rename(f, f.name())
    }
    fn element(f: &FieldRef) -> FieldRef {
        rename(f, LIST_ELEMENT_NAME)
    }
    /// Normalize a map's entries field: its name and the names of the key and value fields
    /// inside it are all positional.
    fn entries(f: &FieldRef) -> FieldRef {
        let entry_type = match f.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => DataType::Struct(
                [
                    rename(&fields[0], MAP_KEY_NAME),
                    rename(&fields[1], MAP_VALUE_NAME),
                ]
                .into_iter()
                .collect(),
            ),
            // Not a well-formed map entries struct. Leave it alone and let the comparison fail
            // on the shape rather than papering over it here.
            other => normalize_for_comparison(other),
        };
        Arc::new(Field::new(MAP_ENTRIES_NAME, entry_type, true))
    }
    match dt {
        DataType::List(f) => DataType::List(element(f)),
        DataType::LargeList(f) => DataType::LargeList(element(f)),
        DataType::ListView(f) => DataType::ListView(element(f)),
        DataType::LargeListView(f) => DataType::LargeListView(element(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(element(f), *n),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(keep_name).collect()),
        DataType::Map(f, sorted) => DataType::Map(entries(f), *sorted),
        other => other.clone(),
    }
}

/// True if two types agree once the differences described on [`normalize_for_comparison`] are
/// disregarded.
///
/// This is the check applied between the return type registered on the JVM side and the type the
/// UDF's own `return_field` reports. It stays strict about everything that changes how bytes are
/// read or how Spark addresses them: decimal precision and scale, timestamp unit *and timezone*,
/// child ordering, and struct field names.
pub fn return_types_compatible(declared: &DataType, actual: &DataType) -> bool {
    declared == actual || normalize_for_comparison(declared) == normalize_for_comparison(actual)
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

    fn map(entries_name: &str, key_name: &str, value_name: &str) -> DataType {
        let entries = DataType::Struct(Fields::from(vec![
            Field::new(key_name, DataType::Utf8, false),
            Field::new(value_name, DataType::Int32, true),
        ]));
        DataType::Map(Arc::new(Field::new(entries_name, entries, false)), false)
    }

    /// The Arrow columnar format does not constrain these names and Comet's JVM side reads a map
    /// positionally, so a UDF that builds its output with arrow-rs's `MapBuilder::new(None, ..)`
    /// defaults (`entries` / `keys` / `values`) must not be rejected against Comet's own
    /// `entries` / `key` / `value`.
    #[test]
    fn map_child_field_names_are_disregarded() {
        assert!(return_types_compatible(
            &map("entries", "key", "value"),
            &map("entries", "keys", "values")
        ));
        assert!(return_types_compatible(
            &map("entries", "key", "value"),
            &map("some_other_name", "k", "v")
        ));
    }

    /// Names are ignored, but the types under them are not.
    #[test]
    fn differing_map_value_types_are_incompatible() {
        let declared = map("entries", "key", "value");
        let actual = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        );
        assert!(!return_types_compatible(&declared, &actual));
    }

    /// A list's element field name is positional in the same way.
    #[test]
    fn list_element_field_name_is_disregarded() {
        assert!(return_types_compatible(
            &list(DataType::Int32, true),
            &DataType::List(Arc::new(Field::new("element", DataType::Int32, true)))
        ));
    }

    /// A timestamp's timezone is part of how Spark reads the value, so it stays strict. Comet maps
    /// `TimestampType` to `Timestamp(Microsecond, Some("UTC"))` and `TimestampNTZType` to
    /// `Timestamp(Microsecond, None)`, and those are different Spark types.
    #[test]
    fn differing_timestamp_timezone_is_incompatible() {
        assert!(!return_types_compatible(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
    }

    #[test]
    fn differing_struct_field_names_are_incompatible() {
        let a = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));
        let b = DataType::Struct(Fields::from(vec![Field::new("b", DataType::Int32, true)]));
        assert!(!return_types_compatible(&a, &b));
    }
}
