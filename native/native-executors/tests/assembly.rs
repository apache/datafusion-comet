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

//! Runs assembled stage graphs on DataFusion alone, without Ballista: each exchange runs as a
//! DataFusion repartition in this process.

mod common;

use comet_native_executors::assembly::build_plan;
use common::*;
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::physical_plan::{collect, ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_expression::{
    expr::ExprStruct, literal::Value, Expr, IfExpr, Literal, Subquery,
};
use datafusion_comet_proto::spark_native_executors::{QueryRequest, Stage};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, NativeScan, NativeScanCommon, Operator, Projection, SparkFilePartition,
    SparkPartitionedFile,
};
use prost::Message;
use std::sync::Arc;

async fn assemble(request: QueryRequest) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    tokio::task::spawn_blocking(move || build_plan(request))
        .await
        .unwrap()
}

/// Runs `request` and returns its single column, sorted.
async fn run(request: QueryRequest) -> Vec<i64> {
    let plan = assemble(request).await.unwrap();
    let batches = collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap();
    sorted_values::<Int64Type>(&batches)
}

#[tokio::test(flavor = "multi_thread")]
async fn leaf_stage_runs_every_partition() {
    let request = query(vec![range_stage(100, 4)]);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn hash_exchange_moves_every_row() {
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, hash_on_first_column(3), 3),
    ]);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn single_partition_exchange_collects_every_row() {
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, single_partition(), 1),
    ]);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn range_exchange_is_read_as_one_partition() {
    // The driver asks for the exchange's partition count.
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, range_partitioning(4), 4),
    ]);
    let plan = assemble(request.clone()).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    assert_eq!(run(request).await, (0..100).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_plans_must_match_the_partitions() {
    let mut stage = range_stage(100, 4);
    let block = stage.plan.as_ref().unwrap().encode_to_vec();
    stage.partition_plans = vec![block.clone(), block];
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has 4 partitions but 2 partition plans"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reading_stage_has_as_many_partitions_as_its_exchange() {
    // The partition count a driver gives a stage that reads an exchange doesn't matter.
    let request = query(vec![
        range_stage(100, 4),
        reading_stage(0, hash_on_first_column(3), 5),
    ]);
    let plan = assemble(request).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn output_is_cast_to_the_types_the_stage_declares() {
    // A range produces longs. A stage declaring an int column gets ints, as a Spark task's
    // shuffle writer casts a column whose native type drifted from the one Spark declared.
    let mut stage = range_stage(100, 4);
    stage.output_schema = vec![field(int_type())];
    let plan = assemble(query(vec![stage])).await.unwrap();
    assert_eq!(plan.schema().field(0).data_type(), &DataType::Int32);
    let batches = collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert_eq!(
        sorted_values::<Int32Type>(&batches),
        (0..100).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_have_the_schema_the_stage_declares() {
    // `if(true, id, null)` has no nulls, but the block's output leaves it nullable. Spark can
    // declare such a column not null, as it does a join key that a filter has checked, and a
    // distributed engine checks each batch a stage sends against the stage's schema.
    let literal = |value: Option<Value>, data_type| Expr {
        expr_struct: Some(ExprStruct::Literal(Literal {
            is_null: value.is_none(),
            value,
            datatype: Some(data_type),
        })),
        ..Default::default()
    };
    let column = Expr {
        expr_struct: Some(ExprStruct::If(Box::new(IfExpr {
            if_expr: Some(Box::new(literal(Some(Value::BoolVal(true)), bool_type()))),
            true_expr: Some(Box::new(bound(0, long_type()))),
            false_expr: Some(Box::new(literal(None, long_type()))),
        }))),
        ..Default::default()
    };
    let mut stage = range_stage(100, 4);
    stage.plan = Some(Operator {
        op_struct: Some(OpStruct::Projection(Projection {
            project_list: vec![column],
        })),
        children: vec![stage.plan.take().unwrap()],
        ..Default::default()
    });
    stage.output_schema[0].nullable = false;
    let plan = assemble(query(vec![stage])).await.unwrap();
    let batches = collect(Arc::clone(&plan), SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert!(batches.iter().all(|batch| batch.schema() == plan.schema()));
    assert_eq!(
        sorted_values::<Int64Type>(&batches),
        (0..100).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_columns_must_match_the_block() {
    let mut stage = range_stage(100, 4);
    stage.output_schema = vec![field(long_type()), field(long_type())];
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("produces 1 columns but the stage declares 2"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_without_partitions_is_rejected() {
    let mut stage = range_stage(100, 4);
    stage.num_partitions = 0;
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(error.to_string().contains("no partitions"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn block_that_calls_into_the_jvm_is_rejected() {
    // A scalar subquery reads its value from the JVM, which a native executor doesn't have.
    let subquery = Expr {
        expr_struct: Some(ExprStruct::Subquery(Subquery {
            id: 1,
            datatype: Some(long_type()),
        })),
        ..Default::default()
    };
    let mut stage = range_stage(100, 4);
    stage.plan = Some(Operator {
        op_struct: Some(OpStruct::Projection(Projection {
            project_list: vec![subquery],
        })),
        children: vec![stage.plan.take().unwrap()],
        ..Default::default()
    });
    let error = assemble(query(vec![stage])).await.unwrap_err();
    assert!(error.to_string().contains("calls into the JVM"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_stage_is_checked_without_its_object_store() {
    // The scheduler checks a stage without reading its files, so it builds no object store and
    // uses no storage credentials. Nothing can build a store for this URL, so only the stage's
    // run finds that out.
    let scan = |partitioned_file| Operator {
        op_struct: Some(OpStruct::NativeScan(NativeScan {
            common: Some(NativeScanCommon {
                required_schema: vec![field(long_type())],
                data_schema: vec![field(long_type())],
                projection_vector: vec![0],
                session_timezone: "UTC".to_string(),
                ..Default::default()
            }),
            file_partition: Some(SparkFilePartition { partitioned_file }),
            ..Default::default()
        })),
        ..Default::default()
    };
    let file = SparkPartitionedFile {
        file_path: "no-such-store://bucket/part-0.parquet".to_string(),
        length: 16,
        file_size: 16,
        ..Default::default()
    };
    let stage = Stage {
        plan: Some(scan(vec![])),
        partition_plans: vec![scan(vec![file]).encode_to_vec()],
        num_partitions: 1,
        output_schema: vec![field(long_type())],
        ..Default::default()
    };
    let plan = assemble(query(vec![stage])).await.unwrap();
    let error = collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no-such-store"), "{error}");
}
