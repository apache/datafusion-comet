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

use super::case_when::is_infallible;
use arrow::array::{new_null_array, Array, BooleanArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_datafusion_err, internal_err, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Evaluates a function's arguments the way Spark evaluates those of a null-intolerant
/// `BinaryExpression` or `TernaryExpression`: the first for every row, and each later one only for
/// the rows where no earlier argument is NULL.
///
/// Spark returns NULL as soon as an argument is NULL, without evaluating the ones after it.
/// DataFusion evaluates every argument over the whole batch before it calls a function, so an
/// argument that can fail, such as an ANSI cast of a malformed string, would fail on a row that
/// Spark answers with NULL, and a nondeterministic argument would see rows that it does not see in
/// Spark. An argument that can be evaluated for any row, such as a column, is evaluated for all of
/// them.
///
/// The function still sees every row, with NULL for each argument skipped there, so it has to
/// return NULL wherever an argument other than the last is NULL. Its arguments have to be its
/// children, in the order Spark evaluates them, and it has to compute its result from them alone.
/// `ArrayInsert` evaluates its own arguments the same way.
#[derive(Debug)]
pub struct NullShortCircuit {
    /// The function over its arguments
    inner: Arc<dyn PhysicalExpr>,
    /// The function over the evaluated arguments, reading each one that is not a literal from a
    /// column of the batch that holds them
    body: Arc<dyn PhysicalExpr>,
    args: Vec<Arg>,
}

impl NullShortCircuit {
    /// Wraps `inner` if evaluating one of its arguments after the first for a row that Spark skips
    /// could change the outcome, and returns it unchanged otherwise.
    pub fn wrap(
        inner: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let masked = inner
            .children()
            .iter()
            .enumerate()
            .map(|(i, arg)| i > 0 && !is_infallible(arg, input_schema))
            .collect::<Vec<_>>();
        if !masked.contains(&true) {
            return Ok(inner);
        }
        let plans = inner
            .children()
            .into_iter()
            .zip(masked)
            .map(|(arg, masked)| {
                if arg.is::<Literal>() {
                    return Ok((None, masked));
                }
                // A masked argument is NULL on the rows it is skipped for
                let field = arg.return_field(input_schema)?;
                Ok((
                    Some(Arc::new(field.as_ref().clone().with_nullable(true))),
                    masked,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self::try_new(inner, plans)?))
    }

    /// `plans` holds each argument's [`Arg::field`] and whether it is masked.
    fn try_new(inner: Arc<dyn PhysicalExpr>, plans: Vec<(Option<FieldRef>, bool)>) -> Result<Self> {
        let children = inner.children();
        if children.len() != plans.len() {
            return internal_err!(
                "NullShortCircuit has {} plans for {} arguments",
                plans.len(),
                children.len()
            );
        }
        let args = children
            .into_iter()
            .zip(plans)
            .map(|(expr, (field, masked))| Arg::try_new(expr, field, masked))
            .collect::<Result<Vec<_>>>()?;
        let mut body_children = Vec::with_capacity(args.len());
        let mut column = 0;
        for arg in &args {
            if let Some(field) = &arg.field {
                body_children.push(Arc::new(Column::new(field.name(), column)) as _);
                column += 1;
            } else {
                body_children.push(Arc::clone(&arg.expr));
            }
        }
        let body = Arc::clone(&inner).with_new_children(body_children)?;
        Ok(Self { inner, body, args })
    }
}

impl Hash for NullShortCircuit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl PartialEq for NullShortCircuit {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for NullShortCircuit {}

impl Display for NullShortCircuit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NullShortCircuit({})", self.inner)
    }
}

impl PhysicalExpr for NullShortCircuit {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        // The rows where an argument evaluated so far is NULL, or `None` while there are none
        let mut nulls: Option<NullBuffer> = None;
        let mut fields = vec![];
        let mut columns = vec![];
        let last = self.args.len() - 1;
        for (i, arg) in self.args.iter().enumerate() {
            let value = arg.evaluate(batch, nulls.as_ref())?;
            if i < last {
                let value_nulls = match &value {
                    ColumnarValue::Array(array) if array.len() != num_rows => {
                        return internal_err!(
                            "{} returned {} rows for a batch of {num_rows}",
                            arg.expr,
                            array.len()
                        );
                    }
                    ColumnarValue::Array(array) => array.logical_nulls(),
                    ColumnarValue::Scalar(scalar) if scalar.is_null() => {
                        Some(NullBuffer::new_null(num_rows))
                    }
                    ColumnarValue::Scalar(_) => None,
                };
                nulls = NullBuffer::union(nulls.as_ref(), value_nulls.as_ref())
                    .filter(|n| n.null_count() > 0);
                if nulls.as_ref().is_some_and(|n| n.null_count() == num_rows) {
                    // Every row is NULL, and no later argument is evaluated for any of them
                    let data_type = self.inner.data_type(batch.schema_ref())?;
                    return Ok(ColumnarValue::Array(new_null_array(&data_type, num_rows)));
                }
            }
            if let Some(field) = &arg.field {
                let column = value.into_array(num_rows)?;
                // The batch is checked against the types the arguments have at run time, which
                // can differ from their planned types in the names and nullability of nested fields
                fields.push(if field.data_type() == column.data_type() {
                    Arc::clone(field)
                } else {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(column.data_type().clone()),
                    )
                });
                columns.push(column);
            }
        }
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let args =
            RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), columns, &options)?;
        self.body.evaluate(&args)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let inner = Arc::clone(&self.inner).with_new_children(children)?;
        let plans = self
            .args
            .iter()
            .map(|arg| (arg.field.clone(), arg.masked.is_some()))
            .collect();
        Ok(Arc::new(Self::try_new(inner, plans)?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }

    fn is_volatile_node(&self) -> bool {
        self.inner.is_volatile_node()
    }
}

/// An argument, and how it is evaluated.
#[derive(Debug)]
struct Arg {
    expr: Arc<dyn PhysicalExpr>,
    /// The field of the column that `body` reads the argument from, or `None` for a literal,
    /// which `body` keeps
    field: Option<FieldRef>,
    /// For an argument evaluated for only the rows where no earlier argument is NULL, the input
    /// columns it reads, and the argument reading them from a batch of just those columns, so
    /// that skipping rows filters no other column
    masked: Option<(Vec<usize>, Arc<dyn PhysicalExpr>)>,
}

impl Arg {
    fn try_new(
        expr: &Arc<dyn PhysicalExpr>,
        field: Option<FieldRef>,
        masked: bool,
    ) -> Result<Self> {
        let masked = masked.then(|| project(expr)).transpose()?;
        Ok(Self {
            expr: Arc::clone(expr),
            field,
            masked,
        })
    }

    /// Evaluates the argument, for just the rows outside `nulls` if it is masked, returning a
    /// value for every row of the batch.
    fn evaluate(&self, batch: &RecordBatch, nulls: Option<&NullBuffer>) -> Result<ColumnarValue> {
        match (&self.masked, nulls) {
            (Some((projection, projected)), Some(nulls)) => {
                let rows = BooleanArray::new(nulls.inner().clone(), None);
                projected.evaluate_selection(&batch.project(projection)?, &rows)
            }
            _ => self.expr.evaluate(batch),
        }
    }
}

/// The input columns that `expr` reads, and `expr` reading them from a batch of just those columns.
fn project(expr: &Arc<dyn PhysicalExpr>) -> Result<(Vec<usize>, Arc<dyn PhysicalExpr>)> {
    let mut projection = collect_columns(expr)
        .iter()
        .map(Column::index)
        .collect::<Vec<_>>();
    projection.sort_unstable();
    projection.dedup();
    let projected = Arc::clone(expr)
        .transform_down(|e| {
            let Some(column) = e.downcast_ref::<Column>() else {
                return Ok(Transformed::no(e));
            };
            let index = projection
                .binary_search(&column.index())
                .map_err(|_| internal_datafusion_err!("{column} is not projected"))?;
            if index == column.index() {
                return Ok(Transformed::no(e));
            }
            Ok(Transformed::yes(
                Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
            ))
        })?
        .data;
    Ok((projection, projected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        create_query_context_map, Cast, EvalMode, ListExtract, SparkArrayPositionFunc,
        SparkArraySlice, SparkCastOptions,
    };
    use arrow::array::{ArrayRef, Int32Array, Int64Array, ListArray, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_expr::expressions::{col, lit, CaseExpr, IsNotNullExpr};
    use datafusion::physical_expr::ScalarFunctionExpr;
    use std::sync::Mutex;

    /// An array per row, a string that is not an integer on most rows whose array is NULL, and
    /// the integer it is elsewhere.
    fn test_batch() -> RecordBatch {
        let arrays = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            None,
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            None,
            Some(vec![Some(5), None, Some(6)]),
            None,
        ]);
        let ints = Int32Array::from(vec![7, 2, 1, 7, 6, 7]);
        let strings = StringArray::from(vec!["bad", "2", "3", "worse", "3", "x"]);
        let lengths = Int64Array::from(vec![Some(1), Some(1), None, Some(1), Some(2), Some(1)]);
        let schema = Schema::new(vec![
            Field::new("a", arrays.data_type().clone(), true),
            Field::new("i", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("len", DataType::Int64, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrays),
                Arc::new(ints),
                Arc::new(strings),
                Arc::new(lengths),
            ],
        )
        .unwrap()
    }

    fn ansi_cast(expr: Arc<dyn PhysicalExpr>, to: DataType) -> Arc<dyn PhysicalExpr> {
        Arc::new(Cast::new(
            expr,
            to,
            SparkCastOptions::new_without_timezone(EvalMode::Ansi, false),
            None,
            None,
        ))
    }

    fn function(
        udf: ScalarUDF,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
    ) -> Arc<dyn PhysicalExpr> {
        let name = udf.name().to_string();
        Arc::new(ScalarFunctionExpr::new(
            &name,
            Arc::new(udf),
            args,
            Arc::new(Field::new(&name, return_type, true)),
            Arc::new(ConfigOptions::default()),
        ))
    }

    fn array_position(
        array: Arc<dyn PhysicalExpr>,
        element: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        let udf = ScalarUDF::new_from_impl(SparkArrayPositionFunc::new());
        function(udf, vec![array, element], DataType::Int64)
    }

    fn slice(args: Vec<Arc<dyn PhysicalExpr>>, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let udf = ScalarUDF::new_from_impl(SparkArraySlice::new());
        function(udf, args, schema.field(0).data_type().clone())
    }

    fn evaluate(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<ArrayRef> {
        expr.evaluate(batch)?.into_array(batch.num_rows())
    }

    /// `array_position(a, CAST(s AS INT))` over `test_batch`
    fn expected_positions() -> ArrayRef {
        Arc::new(Int64Array::from(vec![
            None,
            Some(2),
            Some(1),
            None,
            Some(0),
            None,
        ]))
    }

    /// `slice(a, CAST(s AS INT), len)` over `test_batch`
    fn expected_slices() -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            None,
            Some(vec![Some(2)]),
            None,
            None,
            Some(vec![Some(6)]),
            None,
        ]))
    }

    #[test]
    fn later_argument_is_not_evaluated_where_the_array_is_null() {
        let batch = test_batch();
        let schema = batch.schema();
        let array = Arc::new(RowRecorder::new(col("a", &schema).unwrap()));
        let element = ansi_cast(col("s", &schema).unwrap(), DataType::Int32);
        let unguarded = array_position(Arc::clone(&array) as _, element);
        // The cast fails on "bad", whose array is NULL
        let error = evaluate(&unguarded, &batch).unwrap_err().to_string();
        assert!(error.contains("CAST_INVALID_INPUT"), "{error}");
        array.seen.lock().unwrap().clear();

        let guarded = NullShortCircuit::wrap(unguarded, &schema).unwrap();
        assert!(guarded.is::<NullShortCircuit>());
        let actual = evaluate(&guarded, &batch).unwrap();
        assert_eq!(actual.as_ref(), expected_positions().as_ref());
        // The array is evaluated once, over the whole batch
        assert_eq!(*array.seen.lock().unwrap(), vec![(6, 4)]);
        // An offset into the batch reaches the arrays' buffers and the filter
        let actual = evaluate(&guarded, &batch.slice(1, 4)).unwrap();
        assert_eq!(actual.as_ref(), expected_positions().slice(1, 4).as_ref());
    }

    #[test]
    fn later_argument_still_fails_where_the_array_is_not_null() {
        let batch = test_batch();
        let schema = batch.schema();
        let element = ansi_cast(col("s", &schema).unwrap(), DataType::Int32);
        let guarded =
            NullShortCircuit::wrap(array_position(col("a", &schema).unwrap(), element), &schema)
                .unwrap();
        let arrays =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![None, Some(vec![Some(1)])]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrays),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["bad", "x"])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        // "bad" is skipped, but the array of "x" is not NULL
        let error = evaluate(&guarded, &batch).unwrap_err().to_string();
        assert!(error.contains("'x'"), "{error}");
    }

    #[test]
    fn infallible_arguments_are_not_wrapped() {
        let batch = test_batch();
        let schema = batch.schema();
        let column = array_position(col("a", &schema).unwrap(), col("i", &schema).unwrap());
        let wrapped = NullShortCircuit::wrap(Arc::clone(&column), &schema).unwrap();
        assert!(Arc::ptr_eq(&column, &wrapped));
        let widened = slice(
            vec![
                col("a", &schema).unwrap(),
                ansi_cast(col("i", &schema).unwrap(), DataType::Int64),
                lit(1i64),
            ],
            &schema,
        );
        let wrapped = NullShortCircuit::wrap(Arc::clone(&widened), &schema).unwrap();
        assert!(Arc::ptr_eq(&widened, &wrapped));
    }

    #[test]
    fn each_argument_waits_for_every_earlier_one() {
        let batch = test_batch();
        let schema = batch.schema();
        // The length is only evaluated where neither the array nor the start is NULL
        let start = ansi_cast(
            ansi_cast(col("s", &schema).unwrap(), DataType::Int32),
            DataType::Int64,
        );
        let length = Arc::new(RowRecorder::new(col("len", &schema).unwrap()));
        let args = vec![col("a", &schema).unwrap(), start, Arc::clone(&length) as _];
        let guarded = NullShortCircuit::wrap(slice(args, &schema), &schema).unwrap();
        let actual = evaluate(&guarded, &batch).unwrap();
        assert_eq!(actual.as_ref(), expected_slices().as_ref());
        // The length saw the three rows that have an array, and only the column it reads
        assert_eq!(*length.seen.lock().unwrap(), vec![(3, 1)]);
    }

    #[test]
    fn infallible_later_argument_is_evaluated_for_every_row() {
        let batch = test_batch();
        let schema = batch.schema();
        let start = ansi_cast(
            ansi_cast(col("s", &schema).unwrap(), DataType::Int32),
            DataType::Int64,
        );
        let args = vec![
            col("a", &schema).unwrap(),
            start,
            col("len", &schema).unwrap(),
        ];
        let guarded = NullShortCircuit::wrap(slice(args, &schema), &schema).unwrap();
        // Only the start, which can fail, skips the rows whose array is NULL
        let masked = guarded
            .downcast_ref::<NullShortCircuit>()
            .unwrap()
            .args
            .iter()
            .map(|arg| arg.masked.is_some())
            .collect::<Vec<_>>();
        assert_eq!(masked, vec![false, true, false]);
        let actual = evaluate(&guarded, &batch).unwrap();
        assert_eq!(actual.as_ref(), expected_slices().as_ref());
    }

    #[test]
    fn null_literal_array_evaluates_no_later_argument() {
        let batch = test_batch();
        let schema = batch.schema();
        let null_array = lit(ScalarValue::try_new_null(schema.field(0).data_type()).unwrap());
        let element = Arc::new(RowRecorder::new(ansi_cast(
            col("s", &schema).unwrap(),
            DataType::Int32,
        )));
        let guarded = NullShortCircuit::wrap(
            array_position(null_array, Arc::clone(&element) as _),
            &schema,
        )
        .unwrap();
        let actual = evaluate(&guarded, &batch).unwrap();
        assert_eq!(actual.data_type(), &DataType::Int64);
        assert_eq!(actual.null_count(), batch.num_rows());
        assert!(element.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn list_extract_is_guarded() {
        let batch = test_batch();
        let schema = batch.schema();
        // element_at(a, CAST(s AS INT)), with a NULL for an index past the end
        let extract = Arc::new(ListExtract::new(
            col("a", &schema).unwrap(),
            ansi_cast(col("s", &schema).unwrap(), DataType::Int32),
            None,
            true,
            false,
            None,
            create_query_context_map(),
        ));
        let guarded = NullShortCircuit::wrap(extract, &schema).unwrap();
        let actual = evaluate(&guarded, &batch).unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            Some(2),
            None,
            None,
            Some(6),
            None,
        ]));
        assert_eq!(actual.as_ref(), expected.as_ref());
    }

    #[test]
    fn column_indices_can_be_rewritten() {
        let batch = test_batch();
        let schema = batch.schema();
        let element = ansi_cast(col("s", &schema).unwrap(), DataType::Int32);
        let guarded =
            NullShortCircuit::wrap(array_position(col("a", &schema).unwrap(), element), &schema)
                .unwrap();
        // DataFusion's CASE projects the batch down to the columns that it reads, and rewrites
        // their indices in its branches through `with_new_children`
        let when = Arc::new(IsNotNullExpr::new(col("len", &schema).unwrap()));
        let case: Arc<dyn PhysicalExpr> =
            Arc::new(CaseExpr::try_new(None, vec![(when, guarded)], None).unwrap());
        let actual = evaluate(&case, &batch).unwrap();
        let expected: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(2),
            None,
            None,
            Some(0),
            None,
        ]));
        assert_eq!(actual.as_ref(), expected.as_ref());
    }

    /// Evaluates `child`, recording the rows and columns of each batch that it is evaluated for.
    #[derive(Debug)]
    struct RowRecorder {
        child: Arc<dyn PhysicalExpr>,
        /// Shared with the copies that `with_new_children` makes
        seen: Arc<Mutex<Vec<(usize, usize)>>>,
    }

    impl RowRecorder {
        fn new(child: Arc<dyn PhysicalExpr>) -> Self {
            Self {
                child,
                seen: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl Hash for RowRecorder {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.child.hash(state);
        }
    }

    impl PartialEq for RowRecorder {
        fn eq(&self, other: &Self) -> bool {
            self.child.eq(&other.child)
        }
    }

    impl Eq for RowRecorder {}

    impl Display for RowRecorder {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RowRecorder({})", self.child)
        }
    }

    impl PhysicalExpr for RowRecorder {
        fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
            self.child.data_type(input_schema)
        }

        fn nullable(&self, input_schema: &Schema) -> Result<bool> {
            self.child.nullable(input_schema)
        }

        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            self.seen
                .lock()
                .unwrap()
                .push((batch.num_rows(), batch.num_columns()));
            self.child.evaluate(batch)
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.child]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(Self {
                child: Arc::clone(&children[0]),
                seen: Arc::clone(&self.seen),
            }))
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.child.fmt_sql(f)
        }
    }
}
