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
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, internal_err, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use std::collections::BTreeSet;
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
/// Spark.
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
    /// The fields of those columns
    fields: Vec<FieldRef>,
    args: Vec<Arg>,
}

impl NullShortCircuit {
    /// Wraps `inner` if evaluating one of its arguments after the first for a row that Spark skips
    /// could change the outcome, and returns it unchanged otherwise.
    pub fn wrap(
        inner: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let children = inner.children();
        if children
            .iter()
            .skip(1)
            .all(|arg| is_infallible(arg, input_schema))
        {
            return Ok(inner);
        }
        // An argument after the first is NULL on the rows it is skipped for
        let fields = children
            .iter()
            .filter(|arg| !arg.is::<Literal>())
            .map(|arg| {
                let field = arg.return_field(input_schema)?;
                Ok(Arc::new(field.as_ref().clone().with_nullable(true)) as FieldRef)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self::try_new(inner, fields)?))
    }

    fn try_new(inner: Arc<dyn PhysicalExpr>, fields: Vec<FieldRef>) -> Result<Self> {
        let args = inner
            .children()
            .into_iter()
            .map(Arg::try_new)
            .collect::<Result<Vec<_>>>()?;
        let mut columns = fields.iter().enumerate();
        let body_children = args
            .iter()
            .map(|arg| {
                if arg.is_literal {
                    return Ok(Arc::clone(&arg.expr));
                }
                let (index, field) = columns
                    .next()
                    .ok_or_else(|| internal_datafusion_err!("NullShortCircuit lacks a field"))?;
                Ok(Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>)
            })
            .collect::<Result<Vec<_>>>()?;
        if columns.next().is_some() {
            return internal_err!("NullShortCircuit has a field for no argument");
        }
        let body = Arc::clone(&inner).with_new_children(body_children)?;
        Ok(Self {
            inner,
            body,
            fields,
            args,
        })
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
        // The rows where no argument evaluated so far is NULL, or `None` while that is all of them
        let mut rows: Option<BooleanArray> = None;
        let mut columns = Vec::with_capacity(self.fields.len());
        let last = self.args.len() - 1;
        for (i, arg) in self.args.iter().enumerate() {
            let value = match &rows {
                Some(rows) if !arg.is_literal => arg.evaluate_rows(batch, rows)?,
                _ => arg.expr.evaluate(batch)?,
            };
            if i < last {
                let valid = match &value {
                    ColumnarValue::Array(array) if array.len() != num_rows => {
                        return internal_err!(
                            "{} returned {} rows for a batch of {num_rows}",
                            arg.expr,
                            array.len()
                        );
                    }
                    ColumnarValue::Array(array) => array
                        .logical_nulls()
                        .filter(|nulls| nulls.null_count() > 0)
                        .map(|nulls| nulls.into_inner()),
                    ColumnarValue::Scalar(scalar) if scalar.is_null() => {
                        Some(BooleanBuffer::new_unset(num_rows))
                    }
                    ColumnarValue::Scalar(_) => None,
                };
                if let Some(valid) = valid {
                    let selected = match rows.take() {
                        Some(rows) => rows.values() & &valid,
                        None => valid,
                    };
                    if selected.count_set_bits() == 0 {
                        // Every row is NULL, and no later argument is evaluated for any of them
                        let data_type = self.inner.data_type(batch.schema_ref())?;
                        return Ok(ColumnarValue::Array(new_null_array(&data_type, num_rows)));
                    }
                    rows = Some(BooleanArray::new(selected, None));
                }
            }
            if !arg.is_literal {
                columns.push(value.into_array(num_rows)?);
            }
        }
        // The batch is checked against the types the arguments have at run time, which can differ
        // from their planned types in the names and nullability of nested fields
        let fields = self
            .fields
            .iter()
            .zip(&columns)
            .map(|(field, column)| {
                if field.data_type() == column.data_type() {
                    Arc::clone(field)
                } else {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(column.data_type().clone()),
                    )
                }
            })
            .collect::<Vec<_>>();
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
        Ok(Arc::new(Self::try_new(inner, self.fields.clone())?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }

    fn is_volatile_node(&self) -> bool {
        self.inner.is_volatile_node()
    }
}

/// An argument, with how to evaluate it for only some of the rows.
#[derive(Debug)]
struct Arg {
    expr: Arc<dyn PhysicalExpr>,
    is_literal: bool,
    /// The input columns that `expr` reads
    projection: Vec<usize>,
    /// `expr` reading those columns from a batch of just them, so that evaluating it for some of
    /// the rows filters no other column
    projected: Arc<dyn PhysicalExpr>,
}

impl Arg {
    fn try_new(expr: &Arc<dyn PhysicalExpr>) -> Result<Self> {
        let mut projection = BTreeSet::new();
        expr.apply(|e| {
            if let Some(column) = e.downcast_ref::<Column>() {
                projection.insert(column.index());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let projection = projection.into_iter().collect::<Vec<_>>();
        let projected = Arc::clone(expr)
            .transform_down(|e| {
                let Some(column) = e.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(e));
                };
                let index = projection
                    .binary_search(&column.index())
                    .map_err(|_| internal_datafusion_err!("{column} is not projected"))?;
                Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
                ))
            })?
            .data;
        Ok(Self {
            expr: Arc::clone(expr),
            is_literal: expr.is::<Literal>(),
            projection,
            projected,
        })
    }

    /// Evaluates the argument for just `rows`, returning a value for every row of the batch.
    fn evaluate_rows(&self, batch: &RecordBatch, rows: &BooleanArray) -> Result<ColumnarValue> {
        self.projected
            .evaluate_selection(&batch.project(&self.projection)?, rows)
    }
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
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            None,
            Some(vec![Some(2)]),
            None,
            None,
            Some(vec![Some(6)]),
            None,
        ]);
        assert_eq!(actual.as_ref(), &expected as &dyn Array);
        // The length saw the three rows that have an array, and only the column it reads
        assert_eq!(*length.seen.lock().unwrap(), vec![(3, 1)]);
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
