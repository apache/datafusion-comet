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

mod resolution;

use super::*;
use arrow::array::{BooleanArray, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;

fn schemas_and_batch() -> (SchemaRef, RecordBatch) {
    let logical_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        physical_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    (logical_schema, batch)
}

#[test]
fn conversion_guard_resolves_static_columns_by_name() {
    // Predicate indices can refer to an earlier projection: index 0 names the
    // wrong file column, and usize::MAX is outside this file's schema entirely.
    for index in [0, usize::MAX] {
        let (logical_schema, batch) = schemas_and_batch();
        let inner: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(DefaultPhysicalExprAdapterFactory);
        let column = Column::new("payload", index);
        let static_filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(column.clone()),
            Operator::Gt,
            lit(15_i64),
        ));
        let original = inner
            .create(Arc::clone(&logical_schema), batch.schema())
            .unwrap();
        let expected = original
            .rewrite(Arc::clone(&static_filter))
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        assert_eq!(
            expected.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![false, true])
        );

        let adapter = RuntimeFilterSchemaAdapterFactory::new(inner, vec![column])
            .create(logical_schema, batch.schema())
            .unwrap();
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("key", 0))],
            lit(false),
        ));
        let combined: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            static_filter,
            Operator::And,
            dynamic_filter,
        ));
        let actual = adapter
            .rewrite(combined)
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        // The converting payload disables only the dynamic filter. The static
        // comparison must still select the second row using the named payload.
        assert_eq!(
            actual.as_any().downcast_ref::<BooleanArray>().unwrap(),
            expected.as_any().downcast_ref::<BooleanArray>().unwrap(),
            "stale column index {index}"
        );
    }
}

#[test]
fn unchanged_column_with_stale_index_keeps_live_filter() {
    let (logical_schema, batch) = schemas_and_batch();
    let adapter = RuntimeFilterSchemaAdapterFactory::new(
        Arc::new(DefaultPhysicalExprAdapterFactory),
        vec![Column::new("key", usize::MAX)],
    )
    .create(logical_schema, batch.schema())
    .unwrap();
    let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("key", 0))],
        lit(true),
    ));
    let expr = Arc::clone(&dynamic_filter);
    let adapted = adapter.rewrite(expr).unwrap();
    assert!(adapted.is::<DynamicFilterPhysicalExpr>());

    dynamic_filter.update(lit(false)).unwrap();
    let actual = adapted
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    assert_eq!(
        actual.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(vec![false, false])
    );
}
