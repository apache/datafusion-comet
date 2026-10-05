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

//! Holds a JVM UDF's result to the return type the plan declared for it.
//!
//! The result is imported with whatever schema the JVM exported, and an `FFI_ArrowArray` carries
//! no type of its own, so nothing else checks that a `CometUDF` built the vector it was declared
//! to build. A UDF declared as `LongType` that returns an `IntVector` would otherwise hand
//! DataFusion an `Int32` column where the schema promises `Int64`.

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DFResult};

/// Return `array` with exactly the type `declared`, or fail naming both types.
///
/// Three differences are tolerated, because none of them changes a value:
///
/// - **List and map child field names.** The Arrow format addresses a list's element and a map's
///   entries, key and value by position, and Arrow Java's own vectors do not use Comet's names: a
///   `ListVector` names its element `$data$`. Rejecting a UDF over that would reject it for a
///   spelling.
/// - **Nested nullability and field metadata.** Spark declares `containsNull` and struct field
///   nullability inside the type, and a vector built without that in mind usually marks every
///   child nullable. If a child declared non-nullable does hold a null, relabelling it fails.
/// - **`Null` in place of a type.** An array of type `Null` holds only nulls, which an array of
///   any type can hold. Arrow Java produces one wherever a vector was never given a child: a
///   `ListVector` whose writer only wrote empty lists has a `Null` element.
///
/// In each case the result is relabelled to `declared`. Everything else must match: the value
/// types, decimal precision and scale, a timestamp's unit and timezone, and struct field names,
/// which Spark reads fields by.
pub(super) fn conform_to_declared_type(
    class_name: &str,
    array: ArrayRef,
    declared: &DataType,
) -> DFResult<ArrayRef> {
    if array.data_type() == declared {
        return Ok(array);
    }
    if !relabels_to(array.data_type(), declared) {
        return Err(DataFusionError::Execution(format!(
            "JVM UDF {class_name} returned {} but its declared return type is {declared}. The \
             vector a CometUDF returns must have the Arrow type of the Spark return type it was \
             registered with. List and map child field names and nested nullability may \
             differ, but a decimal's precision and scale, a timestamp's timezone (UTC for \
             TimestampType, none for TimestampNTZType) and struct field names must match exactly.",
            array.data_type()
        )));
    }
    // A relabelling cast reuses the buffers, except where a `Null` array becomes a typed one.
    arrow::compute::cast(&array, declared).map_err(|e| {
        DataFusionError::Execution(format!(
            "JVM UDF {class_name} returned a {} that does not fit its declared return type \
             {declared}: {e}",
            array.data_type()
        ))
    })
}

/// Whether an array of type `actual` can be relabelled as `declared` without changing a value,
/// under the rules on [`conform_to_declared_type`]. `declared` comes from a Spark type, so the
/// only nested types it can be are `List`, `Struct` and `Map`.
fn relabels_to(actual: &DataType, declared: &DataType) -> bool {
    match (actual, declared) {
        (DataType::Null, _) => true,
        (DataType::List(a), DataType::List(d)) => relabels_to(a.data_type(), d.data_type()),
        (DataType::Struct(a), DataType::Struct(d)) => {
            a.len() == d.len()
                && a.iter()
                    .zip(d.iter())
                    .all(|(a, d)| a.name() == d.name() && relabels_to(a.data_type(), d.data_type()))
        }
        // A map's entries field and the key and value fields inside it are positional. Its sorted
        // flag has to match, because Arrow's cast cannot change it.
        (DataType::Map(a, a_sorted), DataType::Map(d, d_sorted)) if a_sorted == d_sorted => {
            match (a.data_type(), d.data_type()) {
                (DataType::Struct(a), DataType::Struct(d)) if a.len() == 2 && d.len() == 2 => a
                    .iter()
                    .zip(d.iter())
                    .all(|(a, d)| relabels_to(a.data_type(), d.data_type())),
                _ => false,
            }
        }
        _ => actual == declared,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        new_empty_array, Array, AsArray, Int32Array, Int64Array, ListArray, MapArray, NullArray,
        StringArray, StructArray, TimestampMicrosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, Int64Type, TimeUnit};

    const CLASS: &str = "com.example.Udf";

    fn list_type(element_name: &str, element_nullable: bool) -> DataType {
        DataType::List(Arc::new(Field::new(
            element_name,
            DataType::Int64,
            element_nullable,
        )))
    }

    /// `[[1, 2], [3]]`, or `[[1, null], [3]]` with `with_null`, built the way Arrow Java's
    /// `ListVector` exports a list: its element named `$data$` and nullable.
    fn arrow_java_list(with_null: bool) -> ArrayRef {
        let values = if with_null {
            Int64Array::from(vec![Some(1), None, Some(3)])
        } else {
            Int64Array::from(vec![1, 2, 3])
        };
        Arc::new(ListArray::new(
            Arc::new(Field::new("$data$", DataType::Int64, true)),
            OffsetBuffer::from_lengths([2, 1]),
            Arc::new(values),
            None,
        ))
    }

    #[test]
    fn a_result_of_the_declared_type_is_returned_as_is() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let out = conform_to_declared_type(CLASS, Arc::clone(&array), &DataType::Int64).unwrap();
        assert!(Arc::ptr_eq(&array, &out));
    }

    #[test]
    fn a_different_value_type_names_both_types() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let err = conform_to_declared_type(CLASS, array, &DataType::Int64)
            .unwrap_err()
            .to_string();
        assert!(err.contains(CLASS), "{err}");
        assert!(
            err.contains("returned Int32 but its declared return type is Int64"),
            "{err}"
        );
    }

    /// Arrow Java's `ListVector` names its element `$data$` and marks it nullable, while Spark's
    /// `ArrayType(LongType, containsNull = false)` declares a non-nullable `item`.
    #[test]
    fn an_arrow_java_list_is_relabelled_to_the_declared_type() {
        let declared = list_type("item", false);
        let out = conform_to_declared_type(CLASS, arrow_java_list(false), &declared).unwrap();
        assert_eq!(out.data_type(), &declared);
        let values = out.as_list::<i32>().values().as_primitive::<Int64Type>();
        assert_eq!(values.values(), &[1, 2, 3]);
    }

    #[test]
    fn a_null_in_a_child_declared_non_nullable_is_an_error() {
        let declared = list_type("item", false);
        let err = conform_to_declared_type(CLASS, arrow_java_list(true), &declared)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not fit its declared return type"),
            "{err}"
        );
    }

    #[test]
    fn struct_field_names_must_match() {
        let array: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("b", DataType::Int64, true)]),
            vec![Arc::new(Int64Array::from(vec![1]))],
            None,
        ));
        let declared = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)]));
        assert!(conform_to_declared_type(CLASS, array, &declared).is_err());
    }

    #[test]
    fn a_list_nested_in_a_struct_is_relabelled() {
        let array: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("xs", list_type("$data$", true), true)]),
            vec![arrow_java_list(false)],
            None,
        ));
        let declared = DataType::Struct(Fields::from(vec![Field::new(
            "xs",
            list_type("item", true),
            true,
        )]));
        let out = conform_to_declared_type(CLASS, array, &declared).unwrap();
        assert_eq!(out.data_type(), &declared);
    }

    #[test]
    fn map_child_field_names_are_relabelled() {
        let entries = |key: &str, value: &str| {
            Fields::from(vec![
                Field::new(key, DataType::Utf8, false),
                Field::new(value, DataType::Int64, true),
            ])
        };
        let array: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new(
                "kv",
                DataType::Struct(entries("keys", "values")),
                false,
            )),
            OffsetBuffer::from_lengths([1]),
            StructArray::new(
                entries("keys", "values"),
                vec![
                    Arc::new(StringArray::from(vec!["k"])),
                    Arc::new(Int64Array::from(vec![7])),
                ],
                None,
            ),
            None,
            false,
        ));
        let declared = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entries("key", "value")),
                false,
            )),
            false,
        );
        let out = conform_to_declared_type(CLASS, array, &declared).unwrap();
        assert_eq!(out.data_type(), &declared);
    }

    /// A `ListVector` whose writer only wrote empty lists never creates its element vector, so the
    /// element arrives as `Null`.
    #[test]
    fn a_null_element_type_is_relabelled_to_the_declared_one() {
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("$data$", DataType::Null, true)),
            OffsetBuffer::from_lengths([0, 0]),
            new_empty_array(&DataType::Null),
            None,
        ));
        let declared = list_type("item", false);
        let out = conform_to_declared_type(CLASS, array, &declared).unwrap();
        assert_eq!(out.data_type(), &declared);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn a_null_result_becomes_nulls_of_the_declared_type() {
        let array: ArrayRef = Arc::new(NullArray::new(3));
        let out = conform_to_declared_type(CLASS, array, &DataType::Int64).unwrap();
        assert_eq!(out.data_type(), &DataType::Int64);
        assert_eq!(out.null_count(), 3);
    }

    /// `Null` stands in for a type, not for a name: struct field names stay strict.
    #[test]
    fn a_null_field_does_not_excuse_a_different_name() {
        let array: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("b", DataType::Null, true)]),
            vec![Arc::new(NullArray::new(1))],
            None,
        ));
        let declared = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)]));
        assert!(conform_to_declared_type(CLASS, array, &declared).is_err());
    }

    /// Spark has no sorted maps and Comet declares every map unsorted. A map that claims sorted keys
    /// is refused with both types named, since Arrow's cast cannot drop the claim.
    #[test]
    fn a_sorted_map_is_refused() {
        let entries = Fields::from(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]);
        let map = |sorted| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(entries.clone()),
                    false,
                )),
                sorted,
            )
        };
        let array: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entries.clone()),
                false,
            )),
            OffsetBuffer::from_lengths([1]),
            StructArray::new(
                entries.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![2])),
                ],
                None,
            ),
            None,
            true,
        ));
        assert_eq!(array.data_type(), &map(true));
        let err = conform_to_declared_type(CLASS, array, &map(false))
            .unwrap_err()
            .to_string();
        assert!(err.contains("but its declared return type is"), "{err}");
    }

    #[test]
    fn a_timestamp_timezone_must_match() {
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1]));
        let declared = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert!(conform_to_declared_type(CLASS, array, &declared).is_err());
    }
}
