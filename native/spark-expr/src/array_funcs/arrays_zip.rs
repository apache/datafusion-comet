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

use arrow::array::RecordBatch;
use arrow::array::{
    new_null_array, Array, ArrayRef, Capacities, ListArray, MutableArrayData, NullBufferBuilder,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::cast::{as_fixed_size_list_array, as_large_list_array, as_list_array};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
// TODO: replace the local copy of `make_scalar_function` below with
// `datafusion::functions_nested::utils::make_scalar_function` if it is ever made public.

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct SparkArraysZipFunc {
    values: Vec<Arc<dyn PhysicalExpr>>,
    names: Vec<String>,
}

impl SparkArraysZipFunc {
    pub fn new(values: Vec<Arc<dyn PhysicalExpr>>, names: Vec<String>) -> Self {
        Self { values, names }
    }
    fn fields(&self, schema: &Schema) -> Result<Vec<Field>> {
        let mut fields: Vec<Field> = Vec::with_capacity(self.values.len());
        for (i, v) in self.values.iter().enumerate() {
            let element_type = match (*v).as_ref().data_type(schema)? {
                List(field) | LargeList(field) | FixedSizeList(field, _) => {
                    field.data_type().clone()
                }
                Null => Null,
                dt => {
                    return exec_err!("arrays_zip expects array arguments, got {dt}");
                }
            };
            fields.push(Field::new(self.names[i].to_string(), element_type, true));
        }

        Ok(fields)
    }
}

impl Display for SparkArraysZipFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArraysZip [values: {:?}, names: {:?}]",
            self.values, self.names
        )
    }
}

impl PhysicalExpr for SparkArraysZipFunc {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let fields = self.fields(input_schema)?;
        Ok(List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(fields)),
            false,
        ))))
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self
            .values
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        make_scalar_function(|arr| arrays_zip_inner(arr, self.names.clone()))(&values)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.values.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(SparkArraysZipFunc::new(
            children.clone(),
            self.names.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// This function is copied from https://github.com/apache/datafusion/blob/53.0.0/datafusion/spark/src/function/functions_nested_utils.rs#L23
/// b/c the original function is public to crate only
pub fn make_scalar_function<F>(inner: F) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

/// Per-row element ranges of a list column, borrowed from the input array so that
/// no per-row offset vector has to be materialized.
enum ListOffsets<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
    /// Fixed-size lists have implicit offsets of `row * size`.
    Fixed(usize),
}

impl ListOffsets<'_> {
    #[inline]
    fn range(&self, row: usize) -> (usize, usize) {
        match self {
            ListOffsets::I32(offsets) => (offsets[row] as usize, offsets[row + 1] as usize),
            ListOffsets::I64(offsets) => (offsets[row] as usize, offsets[row + 1] as usize),
            ListOffsets::Fixed(size) => (row * size, (row + 1) * size),
        }
    }
}

/// Type-erased view of a list column, covering List, LargeList and FixedSizeList.
///
/// Derived from the equivalent crate-private struct in Apache DataFusion's
/// `datafusion-functions-nested`; the representation has since diverged so that the
/// input's offsets and null buffer are borrowed rather than re-materialized per row.
struct ListColumnView<'a> {
    /// The flat values array backing this list column.
    values: &'a ArrayRef,
    offsets: ListOffsets<'a>,
    nulls: Option<&'a NullBuffer>,
}

impl ListColumnView<'_> {
    #[inline]
    fn is_null(&self, row: usize) -> bool {
        self.nulls.is_some_and(|nulls| nulls.is_null(row))
    }
}

/// Zips N list arrays into a list of structs: field `i` of the struct at position `j` of a
/// row holds element `j` of argument `i`. Within a row, shorter arrays are padded with nulls
/// to the longest array's length, and a row that is null in every argument produces a null
/// row. `names` supplies the struct field names, which Spark derives from the argument
/// expressions.
///
/// Derived from the equivalent crate-private function in Apache DataFusion's
/// `datafusion-functions-nested`, with the added names argument and a different copying
/// strategy.
pub fn arrays_zip_inner(args: &[ArrayRef], names: Vec<String>) -> Result<ArrayRef> {
    if args.is_empty() {
        return exec_err!("arrays_zip requires at least one argument");
    }

    let num_rows = args[0].len();

    // Build a type-erased ListColumnView for each argument.
    // None means the argument is Null-typed (all nulls, no backing data).
    let mut views: Vec<Option<ListColumnView>> = Vec::with_capacity(args.len());
    let mut element_types: Vec<DataType> = Vec::with_capacity(args.len());

    for (i, arg) in args.iter().enumerate() {
        match arg.data_type() {
            List(field) => {
                let arr = as_list_array(arg)?;
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: arr.values(),
                    offsets: ListOffsets::I32(arr.value_offsets()),
                    nulls: arr.nulls(),
                }));
            }
            LargeList(field) => {
                let arr = as_large_list_array(arg)?;
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: arr.values(),
                    offsets: ListOffsets::I64(arr.value_offsets()),
                    nulls: arr.nulls(),
                }));
            }
            FixedSizeList(field, size) => {
                let arr = as_fixed_size_list_array(arg)?;
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: arr.values(),
                    offsets: ListOffsets::Fixed(*size as usize),
                    nulls: arr.nulls(),
                }));
            }
            Null => {
                element_types.push(Null);
                views.push(None);
            }
            dt => {
                return exec_err!("arrays_zip argument {i} expected list type, got {dt}");
            }
        }
    }

    let struct_fields: Fields = element_types
        .iter()
        .enumerate()
        .map(|(i, dt)| Field::new(names[i].to_string(), dt.clone(), true))
        .collect::<Vec<_>>()
        .into();

    // First pass: the output length of each row is the longest of that row's arrays; rows
    // where every array is null produce a null. Computing the offsets up front gives the
    // builders an exact capacity and lets the copy pass below run one column at a time.
    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(num_rows);
    let mut total_values: usize = 0;

    for row_idx in 0..num_rows {
        let mut max_len: usize = 0;
        let mut all_null = true;

        for view in views.iter().flatten() {
            if !view.is_null(row_idx) {
                all_null = false;
                let (start, end) = view.offsets.range(row_idx);
                max_len = max_len.max(end - start);
            }
        }

        if all_null {
            nulls.append_null();
        } else {
            nulls.append_non_null();
            total_values += max_len;
        }
        offsets.push(total_values as i32);
    }

    // The builders below borrow this data, so it has to outlive them.
    let values_data: Vec<_> = views
        .iter()
        .map(|v| v.as_ref().map(|view| view.values.to_data()))
        .collect();

    // One MutableArrayData builder per column. Null-typed args have no backing data, so they
    // get no builder; they are materialized as an all-null column when assembling the struct.
    let mut builders: Vec<Option<MutableArrayData>> = values_data
        .iter()
        .map(|vd| {
            vd.as_ref().map(|data| {
                MutableArrayData::with_capacities(vec![data], true, Capacities::Array(total_values))
            })
        })
        .collect();

    // Second pass: copy values one column at a time, merging the runs of rows that need no
    // padding into a single `extend` call. Rows of equal length across all arrays (the common
    // case) therefore cost one bulk copy per column rather than one call per row.
    fn flush(pending: &mut Option<(usize, usize)>, builder: &mut MutableArrayData<'_>) {
        if let Some((start, end)) = pending.take() {
            builder.extend(0, start, end);
        }
    }

    for (view, builder) in views.iter().zip(builders.iter_mut()) {
        let (Some(view), Some(builder)) = (view.as_ref(), builder.as_mut()) else {
            continue;
        };
        let mut pending: Option<(usize, usize)> = None;
        for row_idx in 0..num_rows {
            // A null output row, or one whose arrays are all empty, contributes nothing.
            let max_len = (offsets[row_idx + 1] - offsets[row_idx]) as usize;
            if max_len == 0 {
                continue;
            }

            if view.is_null(row_idx) {
                flush(&mut pending, builder);
                builder.extend_nulls(max_len);
                continue;
            }

            let (start, end) = view.offsets.range(row_idx);
            match pending {
                // Rows this column is null for break the run, since they consume no values.
                Some((pending_start, pending_end)) if pending_end == start => {
                    pending = Some((pending_start, end))
                }
                _ => {
                    flush(&mut pending, builder);
                    pending = Some((start, end));
                }
            }

            // Padding has to be appended right after this row's values, so the pending run
            // cannot be carried any further.
            if end - start < max_len {
                flush(&mut pending, builder);
                builder.extend_nulls(max_len - (end - start));
            }
        }
        flush(&mut pending, builder);
    }

    // Assemble struct columns from builders. A column without a builder is a Null-typed arg.
    let struct_columns: Vec<ArrayRef> = builders
        .into_iter()
        .map(|builder| match builder {
            Some(b) => arrow::array::make_array(b.freeze()),
            None => new_null_array(&Null, total_values),
        })
        .collect();

    let struct_array = StructArray::try_new(struct_fields, struct_columns, None)?;

    let result = ListArray::try_new(
        Arc::new(Field::new_list_field(
            struct_array.data_type().clone(),
            false,
        )),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        nulls.finish(),
    )?;

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        new_null_array, Int32Array, Int64Array, LargeListArray, StringArray, StructArray,
    };
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("{i}")).collect()
    }

    /// Renders the zipped list array row by row so tests can assert on the
    /// exact output, including padding and nulls.
    fn zip_to_strings(args: &[ArrayRef]) -> Vec<Option<String>> {
        let result = arrays_zip_inner(args, names(args.len())).unwrap();
        let options = FormatOptions::default().with_null("null");
        let formatter = ArrayFormatter::try_new(&result, &options).unwrap();
        (0..result.len())
            .map(|row| {
                if result.is_null(row) {
                    None
                } else {
                    Some(formatter.value(row).to_string())
                }
            })
            .collect()
    }

    fn int_list(rows: Vec<Option<Vec<Option<i32>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<
            arrow::datatypes::Int32Type,
            _,
            _,
        >(rows))
    }

    #[test]
    fn test_equal_lengths() {
        let a = int_list(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ]);
        let b: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("c"),
                Some("d"),
            ])),
            None,
        ));
        assert_eq!(
            zip_to_strings(&[a, b]),
            vec![
                Some("[{0: 1, 1: a}, {0: 2, 1: null}]".to_string()),
                Some("[{0: 3, 1: c}, {0: 4, 1: d}]".to_string()),
            ]
        );
    }

    #[test]
    fn test_ragged_lengths_are_padded_with_nulls() {
        let a = int_list(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![]),
            Some(vec![Some(9)]),
        ]);
        let b = int_list(vec![
            Some(vec![Some(10)]),
            Some(vec![Some(20)]),
            Some(vec![Some(30), Some(40)]),
        ]);
        assert_eq!(
            zip_to_strings(&[a, b]),
            vec![
                Some("[{0: 1, 1: 10}, {0: 2, 1: null}, {0: 3, 1: null}]".to_string()),
                Some("[{0: null, 1: 20}]".to_string()),
                Some("[{0: 9, 1: 30}, {0: null, 1: 40}]".to_string()),
            ]
        );
    }

    #[test]
    fn test_null_rows() {
        // Row 1 is null in every column, so the whole output row is null.
        let a = int_list(vec![Some(vec![Some(1)]), None, None]);
        let b = int_list(vec![None, None, Some(vec![Some(2), Some(3)])]);
        assert_eq!(
            zip_to_strings(&[a, b]),
            vec![
                Some("[{0: 1, 1: null}]".to_string()),
                None,
                Some("[{0: null, 1: 2}, {0: null, 1: 3}]".to_string()),
            ]
        );
    }

    #[test]
    fn test_large_list_and_null_typed_argument() {
        let a: ArrayRef = Arc::new(LargeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            OffsetBuffer::new(vec![0i64, 2, 2].into()),
            Arc::new(Int64Array::from(vec![7, 8])),
            Some(NullBuffer::from(vec![true, false])),
        ));
        let b: ArrayRef = new_null_array(&DataType::Null, 2);
        assert_eq!(
            zip_to_strings(&[a, b]),
            vec![
                Some("[{0: 7, 1: null}, {0: 8, 1: null}]".to_string()),
                // The Null-typed argument carries no length, so a row whose only
                // list argument is null is itself null.
                None,
            ]
        );
    }

    #[test]
    fn test_sliced_input() {
        // A sliced list array has a non-zero first offset, exercising the run
        // coalescing on a value range that does not start at zero.
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 2, 4, 6].into()),
            values,
            None,
        ));
        let sliced = list.slice(1, 2);
        let other = int_list(vec![Some(vec![Some(0)]), Some(vec![Some(0)])]);
        assert_eq!(
            zip_to_strings(&[sliced, other]),
            vec![
                Some("[{0: 3, 1: 0}, {0: 4, 1: null}]".to_string()),
                Some("[{0: 5, 1: 0}, {0: 6, 1: null}]".to_string()),
            ]
        );
    }

    #[test]
    fn test_struct_field_names() {
        let a = int_list(vec![Some(vec![Some(1)])]);
        let result = arrays_zip_inner(&[a], vec!["x".to_string()]).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(values.fields().len(), 1);
        assert_eq!(values.fields()[0].name(), "x");
    }
}
