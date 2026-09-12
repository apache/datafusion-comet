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
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{lit, BinaryExpr};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};

#[test]
fn rejects_unsupported_sort_shapes_types_and_configuration() {
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("other", DataType::Int32, false),
    ]));
    let input_batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(Int32Array::from(vec![3, 1])),
            Arc::new(Int32Array::from(vec![2, 4])),
        ],
    )
    .unwrap();
    let input: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[vec![input_batch]], input_schema, None).unwrap();
    let config = ConfigOptions::default();
    let base = sort(Arc::clone(&input), 1, SortOptions::default());
    assert!(DynamicFilterTopKExec::try_new(&base, &config)
        .unwrap()
        .is_some());
    for fetch in [None, Some(0)] {
        assert!(
            DynamicFilterTopKExec::try_new(&base.with_fetch(fetch), &config)
                .unwrap()
                .is_none()
        );
    }
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let computed: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&column),
        Operator::Plus,
        lit(1i32),
    ));
    for expressions in [
        vec![PhysicalSortExpr::new_default(computed)],
        vec![
            PhysicalSortExpr::new_default(Arc::clone(&column)),
            PhysicalSortExpr::new_default(Arc::new(Column::new("other", 1))),
        ],
    ] {
        let plan = SortExec::new(LexOrdering::new(expressions).unwrap(), Arc::clone(&input))
            .with_fetch(Some(1));
        assert!(DynamicFilterTopKExec::try_new(&plan, &config)
            .unwrap()
            .is_none());
    }
    for key_type in [
        DataType::UInt32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
    ] {
        let plan = sort(
            memory_input(vec![Some(3), Some(1)], &key_type),
            1,
            SortOptions::default(),
        );
        assert!(
            DynamicFilterTopKExec::try_new(&plan, &config)
                .unwrap()
                .is_none(),
            "{key_type:?}"
        );
    }
    let batch = batch(vec![Some(3), Some(1)], &DataType::Int32);
    let partitions = MemorySourceConfig::try_new_exec(
        &[vec![batch.clone()], vec![batch.clone()]],
        batch.schema(),
        None,
    )
    .unwrap();
    let plan = sort(partitions, 1, SortOptions::default());
    assert!(DynamicFilterTopKExec::try_new(&plan, &config)
        .unwrap()
        .is_none());
    for topk_enabled in [false, true] {
        let mut disabled = config.clone();
        disabled.optimizer.enable_dynamic_filter_pushdown = topk_enabled;
        disabled.optimizer.enable_topk_dynamic_filter_pushdown = !topk_enabled;
        assert!(DynamicFilterTopKExec::try_new(&base, &disabled)
            .unwrap()
            .is_none());
    }
}

#[test]
fn wrapper_preserves_sort_properties_and_statistics() {
    let session = session(2);
    let plain = sort(
        memory_input(vec![Some(3), Some(1), Some(2)], &DataType::Int32),
        2,
        SortOptions::default(),
    );
    let plan = wrapper(&plain, &session);
    assert_eq!(plan.schema(), plain.schema());
    assert_eq!(
        plan.properties().output_partitioning().partition_count(),
        plain.properties().output_partitioning().partition_count()
    );
    assert_eq!(
        plan.properties().output_ordering(),
        plain.properties().output_ordering()
    );
    assert_eq!(plan.maintains_input_order(), plain.maintains_input_order());
    for partition in [None, Some(0)] {
        let context = StatisticsContext::new();
        let args = StatisticsArgs::new().with_partition(partition);
        assert_eq!(
            context.compute(&plan, &args).unwrap(),
            context.compute(&plain, &args).unwrap()
        );
    }
    assert!(plan.dynamic_expressions_produced().is_empty());
}
