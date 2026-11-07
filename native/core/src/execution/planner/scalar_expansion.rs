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
//! [`BatchSplitExec`], because the operators below do not keep the batch size they were
//! handed: a filter coalesces its output back up to the session's batch size, a window emits
//! its whole partition and a broadcast arrives as a few wide batches. No width can be assumed,
//! so the split is installed whenever the operator expands a string or binary scalar at all.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::physical_expr::expressions::{Column, InListExpr};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_operators::BatchSplitExec;
use datafusion_comet_spark_expr::jvm_udf::JvmScalarUdfExpr;
use datafusion_comet_spark_expr::monotonically_increasing_id::MonotonicallyIncreasingId;
use datafusion_comet_spark_expr::{
    RandExpr, RandStrExpr, RandnExpr, ShuffleExpr, Subquery, UuidExpr,
};

use super::{PhysicalPlanner, TEST_EXEC_CONTEXT_ID};

/// The most value bytes the string and binary scalars of one operator may take once expanded to
/// a batch's length. Arrow's 32-bit offsets make 2 GiB the hard limit; half of it leaves room for
/// kernels that append to the expanded value, such as `concat`, and matches what an Arrow Java
/// vector can hold before its reallocation fails.
const SCALAR_EXPANSION_BYTES: usize = 1 << 30;

/// The most rows per batch at which `scalar_bytes`, expanded to the batch's length, stay within
/// `SCALAR_EXPANSION_BYTES`; unbounded when there is nothing to expand.
fn scalar_rows_cap(scalar_bytes: usize) -> usize {
    if scalar_bytes == 0 {
        return usize::MAX;
    }
    (SCALAR_EXPANSION_BYTES / scalar_bytes).max(1)
}

impl PhysicalPlanner {
    /// Wraps `input` so that the batches it emits keep the string and binary scalars in `exprs`,
    /// expanded to a batch's length, within `SCALAR_EXPANSION_BYTES` and the session's batch
    /// size. Returns `input` as is when `exprs` expand no scalar.
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
        if bytes == 0 {
            return input;
        }
        let batch_size = self.session_ctx.copied_config().batch_size();
        Arc::new(BatchSplitExec::new(
            input,
            scalar_rows_cap(bytes).min(batch_size),
        ))
    }

    /// The bytes of the string and binary scalars that evaluating `expr` expands to the batch's
    /// length, added up, so that a scalar used twice counts twice.
    ///
    /// A scalar is any subtree without a column reference or a volatile function, evaluated
    /// here on a one-row batch: `repeat(<subquery>, 8)` is wider than the subquery result it is
    /// built from. A subtree that is not a scalar is evaluated per row, so only the scalars
    /// among its children count. A subtree that fails to evaluate counts nothing, so that its
    /// error surfaces where Spark's would, at execution. A constant member of an `IN` list is
    /// compared and never expanded; a member computed per row counts the scalars it contains.
    fn expanded_scalar_bytes(&self, expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> usize {
        if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
            // A member that IN compares as a constant is not expanded. One computed per row is,
            // along with the scalars it contains.
            let members: usize = in_list
                .list()
                .iter()
                .filter(|member| !self.is_scalar(member))
                .map(|member| self.expanded_scalar_bytes(member, schema))
                .sum();
            return self.expanded_scalar_bytes(in_list.expr(), schema) + members;
        }
        if !self.is_scalar(expr) {
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

impl PhysicalPlanner {
    /// Whether `expr` yields one value for a batch that can be computed at plan time. A column
    /// reference and a volatile function yield a value per row, and evaluating a volatile
    /// function advances its seeded generator. A JVM UDF is the user's code, which may have
    /// side effects or be slow, and a planner unit test has no JVM to read a subquery result
    /// from.
    fn is_scalar(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        let per_row = expr.is::<Column>()
            || is_volatile(expr)
            || expr.is::<JvmScalarUdfExpr>()
            || (self.exec_context_id == TEST_EXEC_CONTEXT_ID && expr.is::<Subquery>());
        !per_row && expr.children().iter().all(|child| self.is_scalar(child))
    }
}

fn is_volatile(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.is::<UuidExpr>()
        || expr.is::<RandStrExpr>()
        || expr.is::<RandExpr>()
        || expr.is::<RandnExpr>()
        || expr.is::<ShuffleExpr>()
        || expr.is::<MonotonicallyIncreasingId>()
        || expr
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|f| f.fun().signature().volatility == Volatility::Volatile)
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
    fn scalar_rows_cap_bounds_the_rows_by_the_expanded_bytes() {
        assert_eq!(scalar_rows_cap(0), usize::MAX);
        assert_eq!(scalar_rows_cap(1024), 1024 * 1024);
        // A 256 KiB scalar expanded to 8192 rows needs 2 GiB; 4096 rows keep it at 1 GiB.
        assert_eq!(scalar_rows_cap(256 * 1024), 4096);
        // A scalar past 1 GiB still gets one row per batch.
        assert_eq!(scalar_rows_cap(2 * 1024 * 1024 * 1024), 1);
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
    fn measuring_a_seeded_generator_does_not_advance_it() {
        use datafusion_comet_spark_expr::UuidExpr;
        let one_row = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let first_draw =
            |expr: &Arc<dyn PhysicalExpr>| expr.evaluate(&one_row).unwrap().into_array(1).unwrap();
        let measured: Arc<dyn PhysicalExpr> = Arc::new(UuidExpr::new(0));
        assert_eq!(bytes(Arc::clone(&measured)), 0);
        let fresh: Arc<dyn PhysicalExpr> = Arc::new(UuidExpr::new(0));
        assert_eq!(*first_draw(&measured), *first_draw(&fresh));
    }

    #[test]
    fn wraps_the_input_at_a_session_batch_size_the_scalar_fits() {
        use datafusion::execution::context::SessionContext;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::prelude::SessionConfig;
        let session = SessionContext::new_with_config(SessionConfig::new().with_batch_size(256));
        let planner = PhysicalPlanner::new(Arc::new(session), 0);
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(schema())));
        // A 256 KiB scalar allows 4096 rows, more than the session's 256, which the operators
        // below do not keep to.
        let wide = lit("x".repeat(256 * 1024));
        let bounded = planner.bound_scalar_expansion(input, [&wide]);
        let split = bounded
            .downcast_ref::<BatchSplitExec>()
            .expect("a split under the expanding operator");
        assert_eq!(256, split.batch_size());
    }

    #[test]
    fn counts_a_scalar_beside_a_volatile_sibling_without_evaluating_the_sibling() {
        use datafusion_comet_spark_expr::UuidExpr;
        let schema = schema();
        let one_row = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let first_draw =
            |expr: &Arc<dyn PhysicalExpr>| expr.evaluate(&one_row).unwrap().into_array(1).unwrap();
        let uuid: Arc<dyn PhysicalExpr> = Arc::new(UuidExpr::new(0));
        let mixed = binary(
            lit("x".repeat(256 * 1024)),
            Operator::StringConcat,
            Arc::clone(&uuid),
            &schema,
        )
        .unwrap();
        assert_eq!(bytes(mixed), 256 * 1024);
        let fresh: Arc<dyn PhysicalExpr> = Arc::new(UuidExpr::new(0));
        assert_eq!(*first_draw(&uuid), *first_draw(&fresh));
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
    fn counts_the_scalar_in_a_list_member_computed_per_row() {
        use datafusion_comet_spark_expr::spark_in_list;
        let schema = schema();
        let per_row = binary(
            lit("x".repeat(256 * 1024)),
            Operator::StringConcat,
            col("s", &schema).unwrap(),
            &schema,
        )
        .unwrap();
        // The per-row member expands its scalar for every input row; the literal is compared.
        let expr = spark_in_list(
            col("s", &schema).unwrap(),
            vec![per_row, lit("foo")],
            false,
            &schema,
        )
        .unwrap();
        assert!(expr.is::<InListExpr>());
        assert_eq!(bytes(expr), 256 * 1024);
    }

    #[test]
    fn wraps_the_input_at_the_session_batch_size_or_the_scalar_cap() {
        use datafusion::physical_plan::empty::EmptyExec;
        let planner = PhysicalPlanner::default();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(schema())));
        let none = planner.bound_scalar_expansion(Arc::clone(&input), [&lit(7)]);
        assert_eq!("EmptyExec", none.name());
        let split_at = |exprs: &[Arc<dyn PhysicalExpr>]| {
            planner
                .bound_scalar_expansion(Arc::clone(&input), exprs)
                .downcast_ref::<BatchSplitExec>()
                .expect("a split under the expanding operator")
                .batch_size()
        };
        // A small scalar is bounded by the session's batch size, whatever arrives from below.
        assert_eq!(8192, split_at(&[lit("abcd")]));
        let wide = lit("x".repeat(256 * 1024));
        assert_eq!(2048, split_at(&[Arc::clone(&wide), wide]));
    }
}
