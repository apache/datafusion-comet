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

//! Bounds the rows per batch under an operator that expands a large string scalar.
//!
//! An aggregate, a hash partitioner and most string kernels expand a scalar argument to the
//! batch's length: a 256 KiB string over the default 8192-row batch needs 2 GiB of value bytes,
//! past the 32-bit offsets of Arrow's `Utf8` and `Binary` arrays. Spark evaluates such scalars
//! row by row and never materializes the column. See
//! <https://github.com/apache/datafusion-comet/issues/7247>.
//!
//! The bound is enforced right under the operator that evaluates the expressions, with a
//! [`BatchSplitExec`], because the operators below do not keep the batch size they were handed:
//! a filter coalesces its output up to its own size and a window emits its whole partition.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{Column, InListExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_operators::BatchSplitExec;
use datafusion_comet_spark_expr::jvm_udf::JvmScalarUdfExpr;
use datafusion_comet_spark_expr::Subquery;

use super::{PhysicalPlanner, TEST_EXEC_CONTEXT_ID};

/// The most value bytes the string and binary scalars of one operator may take once expanded to
/// a batch's length. Arrow's 32-bit offsets make 2 GiB the hard limit; half of it leaves room for
/// kernels that append to the expanded value, such as `concat`, and matches what an Arrow Java
/// vector can hold before its reallocation fails.
const SCALAR_EXPANSION_BYTES: usize = 1 << 30;

/// The rows per batch under which `scalar_bytes`, expanded to the batch's length, stay within
/// `SCALAR_EXPANSION_BYTES`.
fn scalar_batch_rows(scalar_bytes: usize, batch_size: usize) -> usize {
    if scalar_bytes == 0 {
        return batch_size;
    }
    batch_size.min((SCALAR_EXPANSION_BYTES / scalar_bytes).max(1))
}

impl PhysicalPlanner {
    /// Wraps `input` so that the batches it emits keep the string and binary scalars in `exprs`,
    /// expanded to a batch's length, within `SCALAR_EXPANSION_BYTES`. Returns `input` as is when
    /// the session's batch size already does.
    pub(crate) fn bound_scalar_expansion<'a>(
        &self,
        input: Arc<dyn ExecutionPlan>,
        exprs: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let bytes: usize = exprs
            .into_iter()
            .map(|expr| self.expanded_scalar_bytes(expr, &schema))
            .sum();
        let batch_size = self.session_ctx.copied_config().batch_size();
        let rows = scalar_batch_rows(bytes, batch_size);
        if rows < batch_size {
            Arc::new(BatchSplitExec::new(input, rows))
        } else {
            input
        }
    }

    /// The bytes of the string and binary scalars that evaluating `expr` expands to the batch's
    /// length, added up, so that a scalar used twice counts twice.
    ///
    /// A scalar is any subtree without a column reference, evaluated here on a one-row batch:
    /// `repeat(<subquery>, 8)` is wider than the subquery result it is built from. A subtree
    /// that fails to evaluate counts nothing, so that its error surfaces where Spark's would,
    /// at execution. The members of an `IN` list are compared and never expanded.
    fn expanded_scalar_bytes(&self, expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> usize {
        if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
            return self.expanded_scalar_bytes(in_list.expr(), schema);
        }
        if !is_column_free(expr) {
            return expr
                .children()
                .into_iter()
                .map(|child| self.expanded_scalar_bytes(child, schema))
                .sum();
        }
        if !matches!(
            expr.data_type(schema),
            Ok(DataType::Utf8 | DataType::Binary)
        ) {
            return 0;
        }
        // A JVM UDF is the user's code, which may have side effects or be slow, and a planner
        // unit test has no JVM to read a subquery result from.
        if contains(expr, &|e| e.is::<JvmScalarUdfExpr>())
            || (self.exec_context_id == TEST_EXEC_CONTEXT_ID
                && contains(expr, &|e| e.is::<Subquery>()))
        {
            return 0;
        }
        // One row, so that a kernel that expands its scalar arguments still yields one value.
        let Ok(one_row) = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        ) else {
            return 0;
        };
        let value = match expr.evaluate(&one_row) {
            Ok(ColumnarValue::Scalar(value)) => value,
            Ok(ColumnarValue::Array(array)) if array.len() == 1 => {
                match ScalarValue::try_from_array(&array, 0) {
                    Ok(value) => value,
                    Err(_) => return 0,
                }
            }
            _ => return 0,
        };
        match value {
            ScalarValue::Utf8(Some(s)) => s.len(),
            ScalarValue::Binary(Some(b)) => b.len(),
            _ => 0,
        }
    }
}

fn is_column_free(expr: &Arc<dyn PhysicalExpr>) -> bool {
    !expr.is::<Column>() && expr.children().iter().all(|child| is_column_free(child))
}

fn contains(expr: &Arc<dyn PhysicalExpr>, is: &dyn Fn(&Arc<dyn PhysicalExpr>) -> bool) -> bool {
    is(expr) || expr.children().iter().any(|child| contains(child, is))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, in_list, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ])
    }

    fn bytes(expr: Arc<dyn PhysicalExpr>) -> usize {
        PhysicalPlanner::default().expanded_scalar_bytes(&expr, &schema())
    }

    #[test]
    fn scalar_batch_rows_keeps_the_batch_size_unless_a_scalar_would_overflow_it() {
        assert_eq!(scalar_batch_rows(0, 8192), 8192);
        assert_eq!(scalar_batch_rows(1024, 8192), 8192);
        // A 256 KiB scalar expanded to 8192 rows needs 2 GiB; 4096 rows keep it at 1 GiB.
        assert_eq!(scalar_batch_rows(256 * 1024, 8192), 4096);
        // A scalar past 1 GiB still gets one row per batch.
        assert_eq!(scalar_batch_rows(2 * 1024 * 1024 * 1024, 8192), 1);
    }

    #[test]
    fn counts_string_scalars_and_not_columns() {
        assert_eq!(bytes(lit("abcd")), 4);
        assert_eq!(bytes(lit(7)), 0);
        assert_eq!(bytes(col("s", &schema()).unwrap()), 0);
    }

    #[test]
    fn counts_a_scalar_built_by_an_expression_at_its_final_width() {
        let schema = schema();
        let built = binary(lit("abcd"), Operator::StringConcat, lit("efgh"), &schema).unwrap();
        assert_eq!(bytes(Arc::clone(&built)), 8);
        // Under a column reference the scalar subtree still counts, once per use.
        let over_column = binary(
            Arc::clone(&built),
            Operator::StringConcat,
            col("s", &schema).unwrap(),
            &schema,
        )
        .unwrap();
        assert_eq!(bytes(Arc::clone(&over_column)), 8);
        let twice = binary(over_column, Operator::StringConcat, built, &schema).unwrap();
        assert_eq!(bytes(twice), 16);
    }

    #[test]
    fn skips_the_members_of_an_in_list() {
        let schema = schema();
        let expr = in_list(
            col("s", &schema).unwrap(),
            vec![lit("abcd"), lit("efgh")],
            &false,
            &schema,
        )
        .unwrap();
        assert_eq!(bytes(expr), 0);
    }

    #[test]
    fn wraps_the_input_only_when_the_scalars_need_a_smaller_batch() {
        use datafusion::physical_plan::empty::EmptyExec;
        let planner = PhysicalPlanner::default();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(schema())));
        let small = planner.bound_scalar_expansion(Arc::clone(&input), [&lit("abcd")]);
        assert_eq!("EmptyExec", small.name());
        let wide = lit("x".repeat(256 * 1024));
        let bounded = planner.bound_scalar_expansion(input, [&wide, &wide]);
        let split = bounded
            .downcast_ref::<BatchSplitExec>()
            .expect("a split under the expanding operator");
        assert_eq!(2048, split.batch_size());
    }
}
