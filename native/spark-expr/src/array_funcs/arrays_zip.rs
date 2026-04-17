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
    new_null_array, Array, ArrayRef, Capacities, ListArray, MutableArrayData, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::cast::{as_fixed_size_list_array, as_large_list_array, as_list_array};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
// TODO: Reuse functions from DF
// use datafusion::functions_nested::utils::make_scalar_function;
// use datafusion::functions_nested::arrays_zip::arrays_zip_inner;

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

/// This struct is copied from https://github.com/apache/datafusion/blob/53.0.0/datafusion/functions-nested/src/arrays_zip.rs#L40
struct ListColumnView {
    /// The flat values array backing this list column.
    values: ArrayRef,
    /// Pre-computed per-row start offsets (length = num_rows + 1).
    offsets: Vec<usize>,
    /// Pre-computed null bitmap: true means the row is null.
    is_null: Vec<bool>,
}

/// This function is copied from https://github.com/apache/datafusion/blob/53.0.0/datafusion/functions-nested/src/arrays_zip.rs#L159
/// with an additional names argument to parameterized struct keys like Spark does
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
                let raw_offsets = arr.value_offsets();
                let offsets: Vec<usize> = raw_offsets.iter().map(|&o| o as usize).collect();
                let is_null = (0..num_rows).map(|row| arr.is_null(row)).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    is_null,
                }));
            }
            LargeList(field) => {
                let arr = as_large_list_array(arg)?;
                let raw_offsets = arr.value_offsets();
                let offsets: Vec<usize> = raw_offsets.iter().map(|&o| o as usize).collect();
                let is_null = (0..num_rows).map(|row| arr.is_null(row)).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    is_null,
                }));
            }
            FixedSizeList(field, size) => {
                let arr = as_fixed_size_list_array(arg)?;
                let size = *size as usize;
                let offsets: Vec<usize> = (0..=num_rows).map(|row| row * size).collect();
                let is_null = (0..num_rows).map(|row| arr.is_null(row)).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    is_null,
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

    // Collect per-column values data for MutableArrayData builders.
    let values_data: Vec<_> = views
        .iter()
        .map(|v| v.as_ref().map(|view| view.values.to_data()))
        .collect();

    let struct_fields: Fields = element_types
        .iter()
        .enumerate()
        .map(|(i, dt)| Field::new(names[i].to_string(), dt.clone(), true))
        .collect::<Vec<_>>()
        .into();

    // Create a MutableArrayData builder per column. For None (Null-typed)
    // args we only need extend_nulls, so we track them separately.
    let mut builders: Vec<Option<MutableArrayData>> = values_data
        .iter()
        .map(|vd| {
            vd.as_ref().map(|data| {
                MutableArrayData::with_capacities(vec![data], true, Capacities::Array(0))
            })
        })
        .collect();

    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    offsets.push(0);
    let mut null_mask: Vec<bool> = Vec::with_capacity(num_rows);
    let mut total_values: usize = 0;

    // Process each row: compute per-array lengths, then copy values
    // and pad shorter arrays with NULLs.
    for row_idx in 0..num_rows {
        let mut max_len: usize = 0;
        let mut all_null = true;

        for view in views.iter().flatten() {
            if !view.is_null[row_idx] {
                all_null = false;
                let len = view.offsets[row_idx + 1] - view.offsets[row_idx];
                max_len = max_len.max(len);
            }
        }

        if all_null {
            null_mask.push(true);
            offsets.push(*offsets.last().unwrap());
            continue;
        }
        null_mask.push(false);

        // Extend each column builder for this row.
        for (col_idx, view) in views.iter().enumerate() {
            match view {
                Some(v) if !v.is_null[row_idx] => {
                    let start = v.offsets[row_idx];
                    let end = v.offsets[row_idx + 1];
                    let len = end - start;
                    let builder = builders[col_idx].as_mut().unwrap();
                    builder.extend(0, start, end);
                    if len < max_len {
                        builder.extend_nulls(max_len - len);
                    }
                }
                _ => {
                    // Null list entry or None (Null-typed) arg — all nulls.
                    if let Some(builder) = builders[col_idx].as_mut() {
                        builder.extend_nulls(max_len);
                    }
                }
            }
        }

        total_values += max_len;
        let last = *offsets.last().unwrap();
        offsets.push(last + max_len as i32);
    }

    // Assemble struct columns from builders.
    let struct_columns: Vec<ArrayRef> = builders
        .into_iter()
        .zip(element_types.iter())
        .map(|(builder, elem_type)| match builder {
            Some(b) => arrow::array::make_array(b.freeze()),
            None => new_null_array(
                if elem_type.is_null() {
                    &Null
                } else {
                    elem_type
                },
                total_values,
            ),
        })
        .collect();

    let struct_array = StructArray::try_new(struct_fields, struct_columns, None)?;

    let null_buffer = if null_mask.iter().any(|&v| v) {
        Some(NullBuffer::from(
            null_mask.iter().map(|v| !v).collect::<Vec<bool>>(),
        ))
    } else {
        None
    };

    let result = ListArray::try_new(
        Arc::new(Field::new_list_field(
            struct_array.data_type().clone(),
            true,
        )),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        null_buffer,
    )?;

    Ok(Arc::new(result))
}
