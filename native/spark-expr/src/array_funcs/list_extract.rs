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

use arrow::array::{
    new_null_array, Array, BooleanArray, GenericListArray, Int32Array, OffsetSizeTrait,
};
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::{array::MutableArrayData, datatypes::ArrowNativeType, record_batch::RecordBatch};
use datafusion::common::{
    cast::{as_int32_array, as_large_list_array, as_list_array},
    internal_err, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

use crate::SparkError;

#[derive(Debug, Clone)]
pub struct ListExtract {
    child: Arc<dyn PhysicalExpr>,
    ordinal: Arc<dyn PhysicalExpr>,
    default_value: Option<Arc<dyn PhysicalExpr>>,
    one_based: bool,
    fail_on_error: bool,
    expr_id: Option<u64>,
    registry: Arc<crate::QueryContextMap>,
}

impl Hash for ListExtract {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
        self.default_value.hash(state);
        self.one_based.hash(state);
        self.fail_on_error.hash(state);
        self.expr_id.hash(state);
        // Exclude registry from hash
    }
}

impl PartialEq for ListExtract {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.ordinal.eq(&other.ordinal)
            && self.default_value.eq(&other.default_value)
            && self.one_based.eq(&other.one_based)
            && self.fail_on_error.eq(&other.fail_on_error)
            && self.expr_id.eq(&other.expr_id)
        // Exclude registry from equality check
    }
}

impl Eq for ListExtract {}

impl ListExtract {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        ordinal: Arc<dyn PhysicalExpr>,
        default_value: Option<Arc<dyn PhysicalExpr>>,
        one_based: bool,
        fail_on_error: bool,
        expr_id: Option<u64>,
        registry: Arc<crate::QueryContextMap>,
    ) -> Self {
        Self {
            child,
            ordinal,
            default_value,
            one_based,
            fail_on_error,
            expr_id,
            registry,
        }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.child.data_type(input_schema)? {
            DataType::List(field) | DataType::LargeList(field) => Ok(field),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in ListExtract: {data_type:?}"
            ))),
        }
    }

    /// Wrap a SparkError with QueryContext if expr_id is available
    fn wrap_error_with_context(&self, error: SparkError) -> DataFusionError {
        if let Some(expr_id) = self.expr_id {
            if let Some(query_ctx) = self.registry.get(expr_id) {
                let wrapped = crate::SparkErrorWithContext::with_context(error, query_ctx);
                return DataFusionError::External(Box::new(wrapped));
            }
        }
        DataFusionError::from(error)
    }
}

impl PhysicalExpr for ListExtract {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        // The result is null if any of the following holds:
        //  - fail_on_error is disabled (an out-of-bounds index yields null), or
        //  - the array itself is null (a null array row yields null), or
        //  - the ordinal is null (a null index yields null), or
        //  - the extracted element is itself nullable.
        // It is only non-nullable when fail_on_error is enabled and none of the inputs
        // nor the element can be null.
        Ok(!self.fail_on_error
            || self.child.nullable(input_schema)?
            || self.ordinal.nullable(input_schema)?
            || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let element_type = self.data_type(&batch.schema())?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(ColumnarValue::Array(new_null_array(&element_type, 0)));
        }

        // Spark evaluates the array once, then evaluates the index only for non-null arrays.
        // Keep that order in every mode: an index expression can throw even without ANSI, and
        // evaluating stateful children on a different set of rows changes their results.
        let child_value = self.child.evaluate(batch)?.into_array(num_rows)?;
        if child_value.null_count() == num_rows {
            return Ok(ColumnarValue::Array(new_null_array(
                &element_type,
                num_rows,
            )));
        }
        let ordinal_value = if child_value.null_count() == 0 {
            self.ordinal.evaluate(batch)?
        } else {
            let selection = BooleanArray::new(child_value.nulls().unwrap().inner().clone(), None);
            self.ordinal.evaluate_selection(batch, &selection)?
        }
        .into_array(num_rows)?;

        let default_value = self
            .default_value
            .as_ref()
            .map(|d| {
                d.evaluate(batch).map(|value| match value {
                    ColumnarValue::Scalar(scalar)
                        if !scalar.data_type().equals_datatype(&element_type) =>
                    {
                        scalar.cast_to(&element_type)
                    }
                    ColumnarValue::Scalar(scalar) => Ok(scalar),
                    v => Err(DataFusionError::Execution(format!(
                        "Expected scalar default value for ListExtract, got {v:?}"
                    ))),
                })
            })
            .transpose()?
            .unwrap_or(element_type.try_into())?;

        // Create error wrapper closure that has access to self
        let error_wrapper = |error: SparkError| self.wrap_error_with_context(error);

        let adjust_index: Box<dyn Fn(i32, usize) -> DataFusionResult<Option<usize>>> =
            if self.one_based {
                Box::new(|idx, len| one_based_index(idx, len, &error_wrapper))
            } else {
                Box::new(|idx, len| zero_based_index(idx, len, &error_wrapper))
            };

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    self.one_based,
                    adjust_index,
                    &error_wrapper,
                )
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    self.one_based,
                    adjust_index,
                    &error_wrapper,
                )
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected child type for ListExtract: {data_type:?}"
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child, &self.ordinal]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            2 => Ok(Arc::new(ListExtract::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.default_value.clone(),
                self.one_based,
                self.fail_on_error,
                self.expr_id,
                Arc::clone(&self.registry),
            ))),
            _ => internal_err!("ListExtract should have exactly two children"),
        }
    }
}

fn one_based_index(
    index: i32,
    len: usize,
    error_wrapper: &impl Fn(SparkError) -> DataFusionError,
) -> DataFusionResult<Option<usize>> {
    if index == 0 {
        return Err(error_wrapper(SparkError::InvalidIndexOfZero));
    }

    let abs_index = index.abs().as_usize();
    if abs_index <= len {
        if index > 0 {
            Ok(Some(abs_index - 1))
        } else {
            Ok(Some(len - abs_index))
        }
    } else {
        Ok(None)
    }
}

fn zero_based_index(
    index: i32,
    len: usize,
    _error_wrapper: &impl Fn(SparkError) -> DataFusionError,
) -> DataFusionResult<Option<usize>> {
    if index < 0 {
        Ok(None)
    } else {
        let positive_index = index.as_usize();
        if positive_index < len {
            Ok(Some(positive_index))
        } else {
            Ok(None)
        }
    }
}

fn list_extract<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    index_array: &Int32Array,
    default_value: &ScalarValue,
    fail_on_error: bool,
    one_based: bool,
    adjust_index: impl Fn(i32, usize) -> DataFusionResult<Option<usize>>,
    error_wrapper: &impl Fn(SparkError) -> DataFusionError,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array.values();
    let offsets = list_array.offsets();

    let data = values.to_data();

    let default_data = default_value.to_array()?.to_data();

    let mut mutable = MutableArrayData::new(vec![&data, &default_data], true, index_array.len());

    for (row, (offset_window, index)) in offsets.windows(2).zip(index_array.iter()).enumerate() {
        let start = offset_window[0].as_usize();
        let len = offset_window[1].as_usize() - start;

        if list_array.is_null(row) {
            mutable.extend_nulls(1);
        } else if let Some(index) = index {
            if let Some(i) = adjust_index(index, len)? {
                mutable.extend(0, start + i, start + i + 1);
            } else if fail_on_error {
                // Throw appropriate error based on whether this is element_at (one_based=true)
                // or GetArrayItem (one_based=false)
                let error = if one_based {
                    // element_at function
                    SparkError::InvalidElementAtIndex {
                        index_value: index,
                        array_size: len as i32,
                    }
                } else {
                    // GetArrayItem (arr[index])
                    SparkError::InvalidArrayIndex {
                        index_value: index,
                        array_size: len as i32,
                    }
                };
                return Err(error_wrapper(error));
            } else {
                mutable.extend(1, 0, 1);
            }
        } else {
            // index is NULL → result is NULL
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    Ok(ColumnarValue::Array(arrow::array::make_array(data)))
}

impl Display for ListExtract {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ListExtract [child: {:?}, ordinal: {:?}, default_value: {:?}, one_based: {:?}, fail_on_error: {:?}]",
            self.child, self.ordinal,  self.default_value, self.one_based, self.fail_on_error
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int32Array, LargeListArray, ListArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion::common::{Result, ScalarValue};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
    use datafusion::physical_plan::ColumnarValue;

    use crate::nondetermenistic_funcs::monotonically_increasing_id::MonotonicallyIncreasingId;

    #[test]
    fn test_list_extract_skips_throwing_index_for_null_array() -> Result<()> {
        for one_based in [false, true] {
            for fail_on_error in [false, true] {
                let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    None,
                    Some(vec![Some(99)]),
                ]);
                let index_list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(i32::from(one_based))]),
                    Some(vec![Some(i32::from(one_based))]),
                ]);
                let batch = RecordBatch::try_from_iter(vec![
                    ("arr", Arc::new(list) as ArrayRef),
                    ("index_arr", Arc::new(index_list) as ArrayRef),
                    ("index", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
                ])?;
                // Index zero throws even without ANSI, but belongs to the null outer array.
                let ordinal = Arc::new(ListExtract::new(
                    Arc::new(Column::new("index_arr", 1)),
                    Arc::new(Column::new("index", 2)),
                    None,
                    true,
                    false,
                    None,
                    crate::create_query_context_map(),
                ));
                assert!(ordinal.evaluate(&batch).is_err());
                let expr = ListExtract::new(
                    Arc::new(Column::new("arr", 0)),
                    ordinal,
                    None,
                    one_based,
                    fail_on_error,
                    None,
                    crate::create_query_context_map(),
                );
                let result = expr.evaluate(&batch)?.into_array(2)?;
                assert_eq!(
                    result.to_data(),
                    Int32Array::from(vec![None, Some(99)]).to_data()
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_list_extract_skips_index_for_null_and_empty_input() -> Result<()> {
        let lists: Vec<ArrayRef> = vec![
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                None::<Vec<Option<i32>>>,
                None,
            ])),
            Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![None::<Vec<Option<i32>>>, None],
            )),
        ];
        for list in lists {
            let batch = RecordBatch::try_from_iter(vec![("arr", Arc::clone(&list))])?;
            let throwing_index: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::Divide,
                Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
            ));
            assert!(throwing_index.evaluate(&batch).is_err());
            let children: Vec<Arc<dyn PhysicalExpr>> = vec![
                Arc::new(Column::new("arr", 0)),
                Arc::new(Literal::new(ScalarValue::try_new_null(list.data_type())?)),
            ];
            for child in children {
                for fail_on_error in [false, true] {
                    let expr = ListExtract::new(
                        Arc::clone(&child),
                        Arc::clone(&throwing_index),
                        None,
                        true,
                        fail_on_error,
                        None,
                        crate::create_query_context_map(),
                    );
                    for input in [&batch, &batch.slice(0, 0)] {
                        let result = expr.evaluate(input)?.into_array(input.num_rows())?;
                        assert_eq!(result.data_type(), &DataType::Int32);
                        assert_eq!(result.null_count(), input.num_rows());
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_list_extract_advances_stateful_index_only_for_non_null_arrays() -> Result<()> {
        for fail_on_error in [false, true] {
            let ordinal = Arc::new(CastExpr::new(
                Arc::new(MonotonicallyIncreasingId::from_offset(0)),
                DataType::Int32,
                None,
            ));
            let expr = ListExtract::new(
                Arc::new(Column::new("arr", 0)),
                ordinal,
                None,
                false,
                fail_on_error,
                None,
                crate::create_query_context_map(),
            );
            for (values, expected) in [([10, 11], 10), ([20, 21], 21)] {
                let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    None,
                    Some(values.map(Some).to_vec()),
                ]);
                let batch = RecordBatch::try_from_iter(vec![("arr", Arc::new(list) as ArrayRef)])?;
                // Neither an all-null batch nor an empty batch may advance the index counter.
                expr.evaluate(&batch.slice(0, 1))?;
                expr.evaluate(&batch.slice(0, 0))?;
                let result = expr.evaluate(&batch)?.into_array(2)?;
                assert_eq!(
                    result.to_data(),
                    Int32Array::from(vec![None, Some(expected)]).to_data()
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_list_extract_short_circuits_sliced_large_list() -> Result<()> {
        let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(99)]),
            None,
            Some(vec![Some(10)]),
            None,
            Some(vec![Some(20)]),
        ]);
        let batch = RecordBatch::try_from_iter(vec![
            ("arr", Arc::new(list) as ArrayRef),
            (
                "denominator",
                Arc::new(Int32Array::from(vec![1, 0, 1, 0, 1])) as ArrayRef,
            ),
        ])?
        .slice(1, 4);
        assert_eq!(batch.column(0).nulls().unwrap().inner().offset(), 1);
        let ordinal: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            Operator::Divide,
            Arc::new(Column::new("denominator", 1)),
        ));
        assert!(ordinal.evaluate(&batch).is_err());
        for fail_on_error in [false, true] {
            let expr = ListExtract::new(
                Arc::new(Column::new("arr", 0)),
                Arc::clone(&ordinal),
                None,
                true,
                fail_on_error,
                None,
                crate::create_query_context_map(),
            );
            let result = expr.evaluate(&batch)?.into_array(4)?;
            assert_eq!(
                result.to_data(),
                Int32Array::from(vec![None, Some(10), None, Some(20)]).to_data()
            );
        }
        Ok(())
    }

    #[test]
    fn test_nullable_when_array_is_nullable() -> Result<()> {
        // Regression test for SPARK-55747: GetArrayItem over a nullable array (e.g. the
        // result of split() on a null input) must report nullable=true even with
        // fail_on_error enabled (ANSI), because a null array row yields a null result.
        // The list element field is non-nullable here, mirroring split()'s output.
        let element_field = Arc::new(Field::new("item", DataType::Int32, false));
        let schema = Schema::new(vec![
            Field::new("arr", DataType::List(element_field), true),
            Field::new("idx", DataType::Int32, false),
        ]);

        let list_extract = ListExtract::new(
            Arc::new(Column::new("arr", 0)),
            Arc::new(Column::new("idx", 1)),
            None,
            false, // one_based (GetArrayItem)
            true,  // fail_on_error (ANSI enabled)
            None,
            crate::create_query_context_map(),
        );

        assert!(
            list_extract.nullable(&schema)?,
            "ListExtract over a nullable array must be nullable even with fail_on_error"
        );
        Ok(())
    }

    #[test]
    fn test_list_extract_default_value() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            None,
            Some(vec![]),
        ]);
        let indices = Int32Array::from(vec![0, 0, 0]);

        let null_default = ScalarValue::Int32(None);

        // Simple error wrapper for tests - just converts SparkError to DataFusionError
        let error_wrapper = |error: SparkError| DataFusionError::from(error);

        let ColumnarValue::Array(result) = list_extract(
            &list,
            &indices,
            &null_default,
            false,
            false,
            |idx, len| zero_based_index(idx, len, &error_wrapper),
            &error_wrapper,
        )?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, None]).to_data()
        );

        let zero_default = ScalarValue::Int32(Some(0));

        let ColumnarValue::Array(result) = list_extract(
            &list,
            &indices,
            &zero_default,
            false,
            false,
            |idx, len| zero_based_index(idx, len, &error_wrapper),
            &error_wrapper,
        )?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, Some(0)]).to_data()
        );
        Ok(())
    }

    #[test]
    fn test_list_extract_null_index() -> Result<()> {
        // GetArrayItem returns incorrect results with dynamic (column) index containing nulls
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10), Some(20), Some(30)]),
            Some(vec![Some(10), Some(20), Some(30)]),
            Some(vec![Some(10), Some(20), Some(30)]),
            Some(vec![Some(1)]),
            None,
            Some(vec![Some(10), Some(20)]),
        ]);
        let indices = Int32Array::from(vec![Some(0), Some(1), Some(2), Some(0), Some(0), None]);

        let null_default = ScalarValue::Int32(None);
        let error_wrapper = |error: SparkError| DataFusionError::from(error);

        let ColumnarValue::Array(result) = list_extract(
            &list,
            &indices,
            &null_default,
            false,
            false,
            |idx, len| zero_based_index(idx, len, &error_wrapper),
            &error_wrapper,
        )?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(10), Some(20), Some(30), Some(1), None, None]).to_data()
        );
        Ok(())
    }
}
