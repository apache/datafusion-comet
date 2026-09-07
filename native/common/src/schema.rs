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

//! Helpers for reconciling the Arrow type an operator actually produced with the type Spark
//! catalyst declared.
//!
//! Arrow treats nested field nullability as part of `DataType` identity, so a column whose nested
//! `nullable` flags are narrower than the declared type is rejected by
//! `RecordBatch::try_new_with_options` even though the data is fine — a non-null child is a strict
//! subset of a nullable one. Every Comet boundary that stamps a declared schema onto a runtime
//! array therefore normalizes first instead of asserting. See
//! <https://github.com/apache/datafusion-comet/issues/5137> for the boundaries and
//! <https://github.com/apache/datafusion-comet/issues/4515> for the upstream drift itself.

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use datafusion::common::DataFusionError;
use std::sync::Arc;

/// Builds a `RecordBatch` with `schema` from `columns`, casting any column whose Arrow type
/// differs from the declared field type. `operator` names the caller and is used only in error
/// messages.
///
/// This is the normalizing counterpart to stamping `schema` on directly: it absorbs the
/// return-type drift — most commonly a nested `nullable` flag — that native kernels and non-Parquet
/// sources introduce, the same way `ScanExec` absorbs it at the FFI boundary.
///
/// Reconciliation follows `schema` in both directions, so it will also narrow a nullable nested
/// child to non-null when that is what `schema` declares. That is not silently lossy: arrow's
/// `StructArray::try_new` rejects unmasked nulls under a non-nullable field, so data that cannot
/// survive the narrowing errors here rather than producing an array that misreports itself.
/// Callers that must not narrow should widen `schema` first — see [`widen_nested_nullability`],
/// which is what `SchemaAlignExec` and `ExpandExec` do.
pub fn cast_and_stamp_schema(
    operator: &str,
    schema: &SchemaRef,
    mut columns: Vec<ArrayRef>,
    num_rows: usize,
) -> Result<RecordBatch, DataFusionError> {
    if columns.len() != schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "{operator} produced {} columns but its schema declares {}",
            columns.len(),
            schema.fields().len()
        )));
    }

    for (idx, (column, field)) in columns.iter_mut().zip(schema.fields()).enumerate() {
        if column.data_type() != field.data_type() {
            *column = cast_with_options(column, field.data_type(), &CastOptions::default())
                .map_err(|e| cast_error(operator, schema, idx, column.data_type(), e))?;
        }
    }

    // Every column's type now matches its declared field — `cast_with_options` returns either an
    // error or an array of exactly the requested type — so the stamp can only fail on row counts.
    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|e| {
        DataFusionError::Context(
            format!("{operator} cannot build a batch of {num_rows} rows with its declared schema"),
            Box::new(DataFusionError::from(e)),
        )
    })
}

/// Names the operator and the dotted path of the column that could not be reconciled, since
/// arrow's own message reports only `at column index N` and the two printed types may differ by a
/// single flag hundreds of characters in.
fn cast_error(
    operator: &str,
    schema: &SchemaRef,
    idx: usize,
    actual: &DataType,
    source: arrow::error::ArrowError,
) -> DataFusionError {
    let field = schema.field(idx);
    let detail = describe_type_mismatch(field.name(), field.data_type(), actual)
        .unwrap_or_else(|| format!("{}: expected {}", field.name(), field.data_type()));
    DataFusionError::Context(
        format!("{operator} cannot reconcile col[{idx}] with its declared schema at {detail}"),
        Box::new(DataFusionError::from(source)),
    )
}

/// Describes where `expected` and `actual` first diverge, as a dotted path rooted at `path`.
/// Returns `None` when the two types are equal.
///
/// The nested arms cover the shapes Comet actually builds — see `make_all_fields_nullable` in the
/// planner and `to_arrow_datatype` in the serde layer, which walk the same set.
fn describe_type_mismatch(path: &str, expected: &DataType, actual: &DataType) -> Option<String> {
    if expected == actual {
        return None;
    }
    match (expected, actual) {
        (DataType::List(e), DataType::List(a))
        | (DataType::LargeList(e), DataType::LargeList(a)) => {
            describe_field_mismatch(&format!("{path}.element"), e, a)
        }
        (DataType::FixedSizeList(e, e_len), DataType::FixedSizeList(a, a_len))
            if e_len == a_len =>
        {
            describe_field_mismatch(&format!("{path}.element"), e, a)
        }
        (DataType::Map(e, e_sorted), DataType::Map(a, a_sorted)) if e_sorted == a_sorted => {
            describe_field_mismatch(&format!("{path}.entries"), e, a)
        }
        (DataType::Struct(e), DataType::Struct(a)) if e.len() == a.len() => e
            .iter()
            .zip(a.iter())
            .find_map(|(e, a)| describe_field_mismatch(&format!("{path}.{}", e.name()), e, a)),
        _ => Some(format!("{path}: expected {expected}, found {actual}")),
    }
}

fn describe_field_mismatch(path: &str, expected: &FieldRef, actual: &FieldRef) -> Option<String> {
    if expected.name() != actual.name() {
        return Some(format!(
            "{path}: expected field name '{}', found '{}'",
            expected.name(),
            actual.name()
        ));
    }
    if expected.is_nullable() != actual.is_nullable() {
        return Some(format!(
            "{path}: expected {}, found {}",
            nullability(expected),
            nullability(actual)
        ));
    }
    describe_type_mismatch(path, expected.data_type(), actual.data_type())
}

fn nullability(field: &FieldRef) -> String {
    let qualifier = if field.is_nullable() {
        "nullable"
    } else {
        "non-null"
    };
    format!("{qualifier} {}", field.data_type())
}

/// Returns `base` with nested field nullability widened to also cover `other`, so that arrays of
/// either type can be stamped with the result. Shapes that do not line up (different struct field
/// counts, different list flavours, ...) are left as `base`; those are real type differences and
/// are handled by the cast in [`cast_and_stamp_schema`].
///
/// Related but not interchangeable: `make_all_fields_nullable` in the planner widens one type
/// unconditionally, and arrow's `Field::try_merge` unions two but errors on a leaf type difference
/// instead of tolerating it and does not recurse into maps or fixed-size lists.
pub fn widen_nested_nullability(base: &DataType, other: &DataType) -> DataType {
    match (base, other) {
        (DataType::List(b), DataType::List(o)) => DataType::List(widen_field(b, o)),
        (DataType::LargeList(b), DataType::LargeList(o)) => DataType::LargeList(widen_field(b, o)),
        (DataType::FixedSizeList(b, b_len), DataType::FixedSizeList(o, o_len))
            if b_len == o_len =>
        {
            DataType::FixedSizeList(widen_field(b, o), *b_len)
        }
        (DataType::Map(b, b_sorted), DataType::Map(o, o_sorted)) if b_sorted == o_sorted => {
            DataType::Map(widen_field(b, o), *b_sorted)
        }
        (DataType::Struct(b), DataType::Struct(o)) if b.len() == o.len() => DataType::Struct(
            b.iter()
                .zip(o.iter())
                .map(|(b, o)| widen_field(b, o))
                .collect(),
        ),
        _ => base.clone(),
    }
}

/// Widens a single field, keeping `base`'s name and metadata. A map's `entries` field must stay
/// non-nullable in Arrow, so only the nullability of fields that are already nullable on either
/// side is propagated — which is exactly `base.nullable || other.nullable`.
fn widen_field(base: &FieldRef, other: &FieldRef) -> FieldRef {
    let data_type = widen_nested_nullability(base.data_type(), other.data_type());
    let nullable = base.is_nullable() || other.is_nullable();
    if nullable == base.is_nullable() && &data_type == base.data_type() {
        return Arc::clone(base);
    }
    Arc::new(
        base.as_ref()
            .clone()
            .with_data_type(data_type)
            .with_nullable(nullable),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, ListArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, Schema};

    fn struct_field(nullable_child: bool) -> FieldRef {
        Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("flag", DataType::Boolean, nullable_child),
            ])),
            true,
        ))
    }

    /// The drift from the issue: `List(Struct(..non-null Boolean))` where catalyst declared the
    /// child nullable. Only the child's `nullable` flag differs, so the path must pin it down.
    #[test]
    fn describes_nested_nullability_path() {
        let expected = DataType::List(struct_field(true));
        let actual = DataType::List(struct_field(false));
        assert_eq!(
            describe_type_mismatch("c0", &expected, &actual).unwrap(),
            "c0.element.flag: expected nullable Boolean, found non-null Boolean"
        );
    }

    #[test]
    fn describes_leaf_type_difference() {
        let expected = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        let actual = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        assert_eq!(
            describe_type_mismatch("c0", &expected, &actual).unwrap(),
            "c0.element: expected Int64, found Int32"
        );
    }

    #[test]
    fn describes_nothing_for_equal_types() {
        let dt = DataType::List(struct_field(true));
        assert_eq!(describe_type_mismatch("c0", &dt, &dt), None);
    }

    #[test]
    fn widens_nested_nullability_in_both_directions() {
        let nullable_child = DataType::List(struct_field(true));
        let non_null_child = DataType::List(struct_field(false));
        assert_eq!(
            widen_nested_nullability(&non_null_child, &nullable_child),
            nullable_child
        );
        assert_eq!(
            widen_nested_nullability(&nullable_child, &non_null_child),
            nullable_child
        );
    }

    #[test]
    fn widening_leaves_real_type_differences_alone() {
        let int64 = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false)));
        let int32 = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        // The element nullability still widens; the leaf type stays `base`'s for the cast to fix.
        assert_eq!(
            widen_nested_nullability(&int64, &int32),
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
        );
    }

    /// A `List(Struct(non-null Boolean))` array stamped with a schema declaring the child nullable
    /// must be absorbed rather than rejected, and the values must survive unchanged.
    #[test]
    fn stamps_nested_nullability_drift() {
        let actual = drifting_list_of_struct();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c0",
            DataType::List(struct_field(true)),
            true,
        )]));

        // Pin the reason this helper exists: stamping the declared schema on directly rejects the
        // batch on the child's `nullable` flag alone. If arrow ever relaxes that, this assertion
        // flips and every `cast_and_stamp_schema` call site can go back to a plain stamp.
        let options = RecordBatchOptions::new().with_row_count(Some(2));
        assert!(
            RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                vec![Arc::clone(&actual)],
                &options
            )
            .is_err(),
            "arrow no longer treats nested field nullability as part of DataType identity"
        );

        let batch = cast_and_stamp_schema("TestExec", &schema, vec![actual], 2).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema(), schema);
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value(0).len(), 2);
        assert_eq!(list.value(1).len(), 1);
        let entries = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let ids = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    /// `[[{1,true},{2,false}], [{3,true}]]` with a non-null `flag` child.
    fn drifting_list_of_struct() -> ArrayRef {
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("flag", DataType::Boolean, false),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![true, false, true])),
            ],
            None,
        );
        Arc::new(ListArray::new(
            struct_field(false),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(entries),
            None,
        ))
    }

    /// The narrowing direction of the same reconciliation. Rewriting a nullable child as non-null
    /// is only safe when the child holds no nulls, so the flag alone must not decide the outcome:
    /// null-free data narrows, and data with a real null errors instead of producing an array whose
    /// nullability misreports its own contents.
    #[test]
    fn narrowing_a_child_errors_only_when_it_holds_a_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c0",
            DataType::List(struct_field(false)),
            true,
        )]));

        let batch =
            cast_and_stamp_schema("TestExec", &schema, vec![nullable_flag_list(None)], 2).unwrap();
        assert_eq!(batch.schema(), schema);

        let err = cast_and_stamp_schema("TestExec", &schema, vec![nullable_flag_list(Some(1))], 2)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("TestExec"), "{msg}");
        assert!(msg.contains("col[0]"), "{msg}");
        assert!(
            msg.contains("c0.element.flag: expected non-null Boolean, found nullable Boolean"),
            "{msg}"
        );
    }

    /// `[[{1,_},{2,_}], [{3,_}]]` with a *nullable* `flag` child, null at `null_at` if given.
    fn nullable_flag_list(null_at: Option<usize>) -> ArrayRef {
        let flags: Vec<Option<bool>> = (0..3)
            .map(|i| if Some(i) == null_at { None } else { Some(true) })
            .collect();
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("flag", DataType::Boolean, true),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(flags)),
            ],
            None,
        );
        Arc::new(ListArray::new(
            struct_field(true),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(entries),
            None,
        ))
    }

    #[test]
    fn stamps_equal_types_without_copying() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, true)]));
        let column: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch =
            cast_and_stamp_schema("TestExec", &schema, vec![Arc::clone(&column)], 3).unwrap();
        assert!(Arc::ptr_eq(batch.column(0), &column));
    }

    /// An unreconcilable difference must still name the operator and the column, not just an index.
    #[test]
    fn error_names_operator_and_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int64, true)])),
            true,
        )]));
        let column: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let err = cast_and_stamp_schema("TestExec", &schema, vec![column], 2).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("TestExec"), "{msg}");
        assert!(msg.contains("col[0]"), "{msg}");
        assert!(msg.contains("payload: expected Struct"), "{msg}");
    }

    #[test]
    fn error_on_column_count_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, true)]));
        let err = cast_and_stamp_schema("TestExec", &schema, vec![], 0).unwrap_err();
        assert!(
            err.to_string()
                .contains("produced 0 columns but its schema declares 1"),
            "{err}"
        );
    }
}
