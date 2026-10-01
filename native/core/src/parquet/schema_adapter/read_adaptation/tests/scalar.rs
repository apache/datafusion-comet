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

use super::*;
use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::TimeUnit;
use datafusion::physical_expr::expressions::lit;
use datafusion_comet_spark_expr::SparkCastOptions;

#[test]
fn literals_and_resolved_columns_need_no_conversion() {
    let physical = Schema::new(vec![Field::new("s", DataType::Int32, true)]);
    assert!(is_infallible_read_adaptation(&lit(7_i32), &physical));
    let resolved: Arc<dyn PhysicalExpr> = Arc::new(Column::new("s", usize::MAX));
    assert!(is_infallible_read_adaptation(&resolved, &physical));
    let missing: Arc<dyn PhysicalExpr> = Arc::new(Column::new("missing", 0));
    assert!(!is_infallible_read_adaptation(&missing, &physical));
}

#[test]
fn accepted_int32_promotion_handles_boundaries_and_nulls() {
    for eval_mode in [EvalMode::Legacy, EvalMode::Ansi] {
        let mut options = options();
        options.eval_mode = eval_mode;
        options.allow_type_promotion = true;
        let (schema, expr) = adapt(
            Field::new("s", DataType::Int32, true),
            Field::new("s", DataType::Int64, true),
            options,
        );
        assert!(is_infallible_read_adaptation(&expr, &schema));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(i32::MAX),
            ]))],
        )
        .unwrap();
        let output = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![
                Some(i64::from(i32::MIN)),
                None,
                Some(i64::from(i32::MAX))
            ]),
        );
    }
}

#[test]
fn promotion_rejected_by_spark_remains_unsafe() {
    let mut options = options();
    options.allow_type_promotion = false;
    let (schema, expr) = adapt(
        Field::new("s", DataType::Int32, true),
        Field::new("s", DataType::Int64, true),
        options,
    );
    assert!(!is_infallible_read_adaptation(&expr, &schema));
    assert!(expr
        .evaluate(&RecordBatch::new_empty(Arc::clone(&schema)))
        .is_ok());
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    assert!(expr.evaluate(&batch).is_err());
}

#[test]
fn scalar_casts_without_a_read_adaptation_proof_remain_unsafe() {
    let schema = Schema::new(vec![Field::new("s", DataType::Int32, true)]);
    let ordinary_cast: Arc<dyn PhysicalExpr> = Arc::new(Cast::new(
        Arc::new(Column::new("s", 0)),
        DataType::Int64,
        SparkCastOptions::new(EvalMode::Ansi, "UTC", false),
        None,
        None,
    ));
    assert!(!is_infallible_read_adaptation(&ordinary_cast, &schema));
    let df_cast: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
        Arc::new(Column::new("s", 0)),
        DataType::Int64,
        None,
    ));
    assert!(!is_infallible_read_adaptation(&df_cast, &schema));

    let (schema, timestamp) = adapt(
        Field::new("s", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("s", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        options(),
    );
    assert!(!is_infallible_read_adaptation(&timestamp, &schema));
}
