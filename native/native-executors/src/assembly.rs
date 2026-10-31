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
use datafusion::arrow::datatypes::DataType;
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
/// Ballista's planner turns into a stage boundary on each edge. The root is the last stage.
///
/// Each stage's block is planned with Comet's planner to check it before anything runs. That can
/// block, so don't call this on an async runtime's thread. A stage that several stages read runs
/// once for each of them, since Ballista's planner doesn't share a stage between consumers yet.
///
/// A range exchange is read as one partition, so the stage that sorts it runs in a single task.
/// [`build_plan_with_sampler`] range-partitions it instead.
pub fn build_plan(request: QueryRequest) -> Result<Arc<dyn ExecutionPlan>> {
    build(request, None)
}

/// Builds the physical plan of `request` like [`build_plan`], but range-partitions each range
/// exchange by split points found by sampling its input with `sampler`, as Spark does.
pub fn build_plan_with_sampler(
    request: QueryRequest,
    sampler: &mut Sampler,
) -> Result<Arc<dyn ExecutionPlan>> {
    build(request, Some(sampler))
}

fn build(
    request: QueryRequest,
    mut sampler: Option<&mut Sampler>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut stages: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(request.stages.len());
    for (index, stage) in request.stages.into_iter().enumerate() {
        let fragment = build_stage(stage, &stages, &request.conf, sampler.as_deref_mut())
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
    mut sampler: Option<&mut Sampler>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let block = stage
        .plan
        .as_ref()
        .ok_or_else(|| plan_datafusion_err!("The stage has no plan"))?
        .encode_to_vec();
    let mut inputs = Vec::with_capacity(stage.inputs.len());
    for input in &stage.inputs {
        let producer = producers.get(input.producer as usize).ok_or_else(|| {
            plan_datafusion_err!(
                "The stage reads stage {}, which doesn't come before it",
                input.producer
            )
        })?;
        inputs.push(exchange(
            input,
            Arc::clone(producer),
            sampler.as_deref_mut(),
        )?);
    }
    // A stage that reads exchanges has as many partitions as they deliver, which
    // `CometFragmentExec` checks they agree on.
    let num_partitions = stage
        .inputs
        .iter()
        .zip(&inputs)
        .find(|(input, _)| !input.broadcast)
        .map(|(_, exchange)| exchange.output_partitioning().partition_count())
        .unwrap_or(stage.num_partitions as usize);
    if num_partitions == 0 {
        return plan_err!("The stage has no partitions");
    }
    if !stage.partition_plans.is_empty() && stage.partition_plans.len() != num_partitions {
        return plan_err!(
            "The stage has {num_partitions} partitions but {} partition plans",
            stage.partition_plans.len()
        );
    }
    // A stage that reads files carries its scans' schemas and options only in each partition's
    // copy of the block, so check one of those. Checking reads none of its files.
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
        // A stage that reads files runs each partition's own copy of the block instead.
        block: if stage.partition_plans.is_empty() {
            block
        } else {
            vec![]
        },
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
/// relies on Spark's own placement of rows, such as a join with a bucketed table, would be wrong.
fn exchange(
    input: &StageInput,
    producer: Arc<dyn ExecutionPlan>,
    sampler: Option<&mut Sampler>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if input.broadcast {
        return Ok(Arc::new(CoalescePartitionsExec::new(producer)));
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
                    Ok(Arc::new(Column::new(schema.field(index).name(), index))
                        as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(RepartitionExec::try_new(
                producer,
                Partitioning::Hash(keys, hash.num_partitions as usize),
            )?))
        }
        Some(PartitioningStruct::RangePartition(range)) => match sampler {
            Some(sampler) => range_exchange(range, producer, sampler),
            None => Ok(Arc::new(CoalescePartitionsExec::new(producer))),
        },
        Some(PartitioningStruct::SinglePartition(_)) => {
            Ok(Arc::new(CoalescePartitionsExec::new(producer)))
        }
        other => plan_err!("Comet Native Executors don't support this exchange yet: {other:?}"),
    }
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
/// that may hold floats, can't be range-partitioned this way.
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

/// A range exchange as a range shuffle (`RepartitionExec` with a range partitioning), with
/// split points found by sampling `producer`. It falls back to reading the exchange as one
/// partition, which sorts correctly in a single task, when a sort key is not a column, or of a
/// type a range shuffle doesn't order as Spark does, or the sample yields no split points.
fn range_exchange(
    range: &datafusion_comet_proto::spark_partitioning::RangePartition,
    producer: Arc<dyn ExecutionPlan>,
    sampler: &mut Sampler,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = producer.schema();
    let mut keys = Vec::with_capacity(range.sort_orders.len());
    let mut options = Vec::with_capacity(range.sort_orders.len());
    for order in &range.sort_orders {
        let key = match order.expr_struct.as_ref() {
            Some(ExprStruct::SortOrder(order)) => order.child.as_deref().and_then(|child| {
                bound_column(child, schema.fields().len()).map(|index| (index, order))
            }),
            _ => None,
        };
        let Some((index, order)) = key else {
            warn!("A range exchange sorts by an expression that is not a column, so it runs in one task");
            return Ok(Arc::new(CoalescePartitionsExec::new(producer)));
        };
        let data_type = schema.field(index).data_type();
        if !orders_like_spark(data_type) {
            warn!("A range exchange sorts by a {data_type} column, so it runs in one task");
            return Ok(Arc::new(CoalescePartitionsExec::new(producer)));
        }
        keys.push(index);
        options.push(SortOptions {
            descending: order.direction == SortDirection::Descending as i32,
            nulls_first: order.null_ordering == NullOrdering::NullsFirst as i32,
        });
    }
    let partitions = range.num_partitions.max(1) as usize;
    if keys.is_empty() || partitions == 1 {
        return Ok(Arc::new(CoalescePartitionsExec::new(producer)));
    }

    let per_partition =
        sample_size_per_partition(partitions, producer.output_partitioning().partition_count());
    let sample = Arc::new(SampleExec::try_new(
        Arc::clone(&producer),
        keys.clone(),
        per_partition,
    )?);
    let sample_schema = sample.schema();
    let samples = sampler(sample)?;
    let points = split_points(&sample_schema, &samples, &options, partitions)?;
    if points.is_empty() {
        return Ok(Arc::new(CoalescePartitionsExec::new(producer)));
    }
    info!(
        "Range-partitioning an exchange into {} partitions by a sample of {} rows",
        points.len() + 1,
        samples.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    let ordering = LexOrdering::new(keys.iter().zip(&options).map(|(index, options)| {
        PhysicalSortExpr::new(
            Arc::new(Column::new(schema.field(*index).name(), *index)),
            *options,
        )
    }))
    .ok_or_else(|| plan_datafusion_err!("A range exchange has no sort keys"))?;
    Ok(Arc::new(RepartitionExec::try_new(
        producer,
        Partitioning::Range(RangePartitioning::try_new(ordering, points)?),
    )?))
}
