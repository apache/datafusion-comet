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
use datafusion::common::{plan_datafusion_err, plan_err, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_comet_proto::spark_expression::expr::ExprStruct;
use datafusion_comet_proto::spark_native_executors::{Fragment, QueryRequest, Stage, StageInput};
use datafusion_comet_proto::spark_partitioning::partitioning::PartitioningStruct;
use datafusion_comet_proto::PROTO_FINGERPRINT;
use prost::Message;

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
pub fn build_plan(request: QueryRequest) -> Result<Arc<dyn ExecutionPlan>> {
    let mut stages: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(request.stages.len());
    for (index, stage) in request.stages.into_iter().enumerate() {
        let fragment = build_stage(stage, &stages, &request.conf)
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
) -> Result<Arc<dyn ExecutionPlan>> {
    let block = stage
        .plan
        .as_ref()
        .ok_or_else(|| plan_datafusion_err!("The stage has no plan"))?
        .encode_to_vec();
    // Spark places the rows of a bucketed scan by Murmur3 and shuffles only the other side of a
    // join to match. The hash exchange below would place the other side by DataFusion's hash, so
    // partition `p` of one side would no longer meet the rows of bucket `p` of the other, and the
    // join would return wrong rows without an error.
    if !stage.partition_plans.is_empty()
        && stage.inputs.iter().any(|input| {
            !input.broadcast
                && matches!(
                    input
                        .partitioning
                        .as_ref()
                        .and_then(|p| p.partitioning_struct.as_ref()),
                    Some(PartitioningStruct::HashPartition(_))
                )
        })
    {
        return plan_err!(
            "A stage that reads files can't also read a hash exchange: Spark's hash places rows \
             differently from DataFusion's, so the two sides of a bucketed join wouldn't meet"
        );
    }
    let mut inputs = Vec::with_capacity(stage.inputs.len());
    for input in &stage.inputs {
        let producer = producers.get(input.producer as usize).ok_or_else(|| {
            plan_datafusion_err!(
                "The stage reads stage {}, which doesn't come before it",
                input.producer
            )
        })?;
        inputs.push(exchange(input, Arc::clone(producer))?);
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
/// relies on Spark's own placement of rows, such as a join with a bucketed table, would be wrong,
/// which `build_stage` refuses.
fn exchange(
    input: &StageInput,
    producer: Arc<dyn ExecutionPlan>,
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
                .map(|expr| match expr.expr_struct.as_ref() {
                    Some(ExprStruct::Bound(bound))
                        if (bound.index as usize) < schema.fields().len() =>
                    {
                        let index = bound.index as usize;
                        Ok(Arc::new(Column::new(schema.field(index).name(), index))
                            as Arc<dyn PhysicalExpr>)
                    }
                    _ => plan_err!("Hash partitioning keys must be columns of the stage's input"),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(RepartitionExec::try_new(
                producer,
                Partitioning::Hash(keys, hash.num_partitions as usize),
            )?))
        }
        // Ballista can't plan a range shuffle for a physical plan yet
        // (https://github.com/apache/datafusion-ballista/issues/2680), so the consuming stage
        // reads a range exchange as one partition too. That sorts correctly, in a single task.
        Some(PartitioningStruct::SinglePartition(_) | PartitioningStruct::RangePartition(_)) => {
            Ok(Arc::new(CoalescePartitionsExec::new(producer)))
        }
        other => plan_err!("Comet Native Executors don't support this exchange yet: {other:?}"),
    }
}
