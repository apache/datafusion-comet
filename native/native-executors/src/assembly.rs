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

//! Turns a driver's stage graph into a Ballista physical plan.

use std::collections::HashMap;
use std::sync::Arc;

use comet::execution::native_executors::{
    block_schema, partition_descriptor, CometFragmentExec, COMET_VERSION,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{plan_datafusion_err, plan_err, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr, RangePartitioning};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_comet_proto::spark_expression::expr::ExprStruct;
use datafusion_comet_proto::spark_expression::{Expr, NullOrdering, SortDirection};
use datafusion_comet_proto::spark_native_executors::{Fragment, QueryRequest, Stage, StageInput};
use datafusion_comet_proto::spark_partitioning::partitioning::PartitioningStruct;
use datafusion_comet_proto::PROTO_FINGERPRINT;
use log::{info, warn};
use prost::Message;

use crate::range::{sample_size_per_partition, split_points, SampleExec};

/// Runs a plan to completion and returns the rows of all its partitions: how the scheduler
/// samples the input of a range exchange.
pub type Sampler<'a> = dyn FnMut(Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> + 'a;

/// Checks that `request` comes from a driver running this Comet build. The plan format is not
/// yet stable across versions, and an executor would ignore any field it doesn't know.
pub fn check_version(request: &QueryRequest) -> Result<()> {
    if request.comet_version != COMET_VERSION || request.proto_fingerprint != PROTO_FINGERPRINT {
        return plan_err!(
            "This Comet Native Executor cluster runs Comet {COMET_VERSION} with plan format \
             {PROTO_FINGERPRINT}, but the query comes from Comet {} with plan format {}. The \
             driver and the cluster must run the same Comet build.",
            request.comet_version,
            request.proto_fingerprint
        );
    }
    Ok(())
}

/// Builds the physical plan of `request`: a [`CometFragmentExec`] per stage, with a node that
/// Ballista's planner turns into a stage boundary on each edge. The root is the last stage. Each
/// range exchange is range-partitioned by split points found by sampling its input with
/// `sampler`, as Spark does.
///
/// Each stage's block is planned with Comet's planner to check it before anything runs. That can
/// block, so don't call this on an async runtime's thread. A stage that several stages read runs
/// once for each of them, since Ballista's planner doesn't share a stage between consumers yet.
pub fn build_plan(request: QueryRequest, sampler: &mut Sampler) -> Result<Arc<dyn ExecutionPlan>> {
    let mut stages: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(request.stages.len());
    for (index, stage) in request.stages.into_iter().enumerate() {
        let fragment = build_stage(stage, &stages, &request.conf, sampler)
            .map_err(|e| e.context(format!("Stage {index} of the query")))?;
        stages.push(fragment);
    }
    stages
        .pop()
        .ok_or_else(|| plan_datafusion_err!("The query has no stages"))
}

fn build_stage(
    stage: Stage,
    producers: &[Arc<dyn ExecutionPlan>],
    conf: &HashMap<String, String>,
    sampler: &mut Sampler,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = stage
        .plan
        .as_ref()
        .ok_or_else(|| plan_datafusion_err!("The stage has no plan"))?;
    let mut inputs = Vec::with_capacity(stage.inputs.len());
    for input in &stage.inputs {
        let producer = producers.get(input.producer as usize).ok_or_else(|| {
            plan_datafusion_err!(
                "The stage reads stage {}, which doesn't come before it",
                input.producer
            )
        })?;
        inputs.push(exchange(input, Arc::clone(producer), sampler)?);
    }
    // A stage that reads exchanges has as many partitions as they deliver, and they must agree.
    let mut partitioned = stage
        .inputs
        .iter()
        .zip(&inputs)
        .filter(|(input, _)| !input.broadcast)
        .map(|(_, exchange)| exchange.output_partitioning().partition_count());
    let num_partitions = partitioned.next().unwrap_or(stage.num_partitions as usize);
    if let Some(other) = partitioned.find(|count| *count != num_partitions) {
        return plan_err!(
            "The stage reads exchanges of {num_partitions} and {other} partitions, which must agree"
        );
    }
    if num_partitions == 0 {
        return plan_err!("The stage has no partitions");
    }
    if !stage.partition_plans.is_empty() && stage.partition_plans.len() != num_partitions {
        return plan_err!(
            "The stage has {num_partitions} partitions but {} partition plans",
            stage.partition_plans.len()
        );
    }
    // A stage that reads files runs each partition's own copy of the block, so the block itself
    // is only needed for a stage that doesn't. Checking one of those copies reads none of its
    // files.
    let block = if stage.partition_plans.is_empty() {
        plan.encode_to_vec()
    } else {
        vec![]
    };
    let planned = block_schema(stage.partition_plans.first().unwrap_or(&block), conf)?;
    if planned.fields().len() != stage.output_schema.len() {
        return plan_err!(
            "The stage's block produces {} columns but the stage declares {}",
            planned.fields().len(),
            stage.output_schema.len()
        );
    }

    let mut children = vec![partition_descriptor(
        num_partitions,
        &stage.partition_plans,
    )?];
    children.extend(inputs);
    let fragment = Fragment {
        block,
        broadcast: stage.inputs.iter().map(|input| input.broadcast).collect(),
        output_schema: stage.output_schema,
        conf: conf.clone(),
    };
    Ok(Arc::new(CometFragmentExec::try_new(
        Arc::new(fragment),
        children,
    )?))
}

/// The exchange that carries `producer`'s output to a stage, as a node Ballista's planner turns
/// into a stage boundary.
///
/// A hash exchange partitions with DataFusion's hash function, not Spark's Murmur3. That is
/// consistent within a query, which is all joins, aggregates and windows need, but a plan that
/// relies on Spark's own placement of rows, such as a join with a bucketed table, would be wrong
/// (https://github.com/apache/datafusion-comet/issues/7368).
fn exchange(
    input: &StageInput,
    producer: Arc<dyn ExecutionPlan>,
    sampler: &mut Sampler,
) -> Result<Arc<dyn ExecutionPlan>> {
    if input.broadcast {
        return Ok(one_partition(producer));
    }
    let partitioning = input
        .partitioning
        .as_ref()
        .and_then(|p| p.partitioning_struct.as_ref());
    match partitioning {
        Some(PartitioningStruct::HashPartition(hash)) => {
            let schema = producer.schema();
            let keys = hash
                .hash_expression
                .iter()
                .map(|expr| {
                    let index = bound_column(expr, schema.fields().len()).ok_or_else(|| {
                        plan_datafusion_err!(
                            "Hash partitioning keys must be columns of the stage's input"
                        )
                    })?;
                    Ok(column(&schema, index))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(RepartitionExec::try_new(
                producer,
                Partitioning::Hash(keys, hash.num_partitions as usize),
            )?))
        }
        Some(PartitioningStruct::RangePartition(range)) => {
            Ok(match range_exchange(range, &producer, sampler)? {
                Some(partitioning) => Arc::new(RepartitionExec::try_new(producer, partitioning)?),
                // Read as one partition, the stage sorts correctly in a single task.
                None => one_partition(producer),
            })
        }
        Some(PartitioningStruct::SinglePartition(_)) => Ok(one_partition(producer)),
        other => plan_err!("Comet Native Executors don't support this exchange yet: {other:?}"),
    }
}

/// `producer`'s output gathered into one partition.
fn one_partition(producer: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalescePartitionsExec::new(producer))
}

fn column(schema: &SchemaRef, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(schema.field(index).name(), index))
}

/// The index of the column `expr` reads, if it reads a column of an input with `num_fields`.
fn bound_column(expr: &Expr, num_fields: usize) -> Option<usize> {
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::Bound(bound)) if (bound.index as usize) < num_fields => {
            Some(bound.index as usize)
        }
        _ => None,
    }
}

/// Whether Arrow orders values of `data_type` as Spark does, so that a range shuffle, which
/// compares raw values, puts rows in the ranges that Comet's sort orders them by. Comet's sort
/// normalizes floats first (a NaN of either sign sorts last), so a float key, or a nested one
/// that may hold floats, can't be range-partitioned this way
/// (https://github.com/apache/datafusion-comet/issues/7366).
fn orders_like_spark(data_type: &DataType) -> bool {
    data_type.is_integer()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        )
}

/// The range partitioning of a range exchange over `producer`, with split points found by
/// sampling it. `None` when the exchange should be read as one partition instead: when a sort
/// key is not a column, or of a type a range shuffle doesn't order as Spark does, or the sample
/// yields no split points.
///
/// The sampling job runs the stages below the exchange once more
/// (https://github.com/apache/datafusion-comet/issues/7367).
fn range_exchange(
    range: &datafusion_comet_proto::spark_partitioning::RangePartition,
    producer: &Arc<dyn ExecutionPlan>,
    sampler: &mut Sampler,
) -> Result<Option<Partitioning>> {
    let partitions = range.num_partitions.max(1) as usize;
    if partitions == 1 {
        return Ok(None);
    }
    let schema = producer.schema();
    let mut keys = Vec::with_capacity(range.sort_orders.len());
    let mut sort_exprs = Vec::with_capacity(range.sort_orders.len());
    for order in &range.sort_orders {
        let key = match order.expr_struct.as_ref() {
            Some(ExprStruct::SortOrder(order)) => order.child.as_deref().and_then(|child| {
                bound_column(child, schema.fields().len()).map(|index| (index, order))
            }),
            _ => None,
        };
        let Some((index, order)) = key else {
            warn!("A range exchange sorts by an expression that is not a column, so it runs in one task");
            return Ok(None);
        };
        let data_type = schema.field(index).data_type();
        if !orders_like_spark(data_type) {
            warn!("A range exchange sorts by a {data_type} column, so it runs in one task");
            return Ok(None);
        }
        keys.push(index);
        sort_exprs.push(PhysicalSortExpr::new(
            column(&schema, index),
            SortOptions {
                descending: order.direction == SortDirection::Descending as i32,
                nulls_first: order.null_ordering == NullOrdering::NullsFirst as i32,
            },
        ));
    }
    let options: Vec<SortOptions> = sort_exprs.iter().map(|expr| expr.options).collect();
    let Some(ordering) = LexOrdering::new(sort_exprs) else {
        return Ok(None);
    };

    let per_partition =
        sample_size_per_partition(partitions, producer.output_partitioning().partition_count());
    let sample = Arc::new(SampleExec::try_new(
        Arc::clone(producer),
        keys,
        per_partition,
    )?);
    let sample_schema = sample.schema();
    let samples = sampler(sample)?;
    let points = split_points(&sample_schema, &samples, &options, partitions)?;
    if points.is_empty() {
        return Ok(None);
    }
    info!(
        "Range-partitioning an exchange into {} partitions by a sample of {} rows",
        points.len() + 1,
        samples.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    Ok(Some(Partitioning::Range(RangePartitioning::try_new(
        ordering, points,
    )?)))
}
