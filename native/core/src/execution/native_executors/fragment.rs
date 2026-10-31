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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryArray, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_datafusion_err, DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    InputDistributionRequirements, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_native_executors::Fragment;
use datafusion_comet_proto::spark_operator::{Operator, SparkStructField};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::execution::jni_api::{comet_runtime_env, comet_session_context};
use crate::execution::planner::{
    align_shuffle_writer_input, convert_spark_types_to_arrow_schema, PhysicalPlanner,
};
use crate::execution::serde::deserialize_op;
use crate::execution::spark_config::{
    SparkConfig, COMET_BATCH_SIZE, COMET_STRICT_TIMESTAMP_LABELS,
};

const DEFAULT_BATCH_SIZE: usize = 8192;

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt32, false),
        Field::new("plan", DataType::Binary, true),
    ]))
}

/// The partition descriptor of a fragment with `num_partitions` partitions.
///
/// Partition `i` is a single row holding `i`, the partition's index in its stage, and
/// `plans[i]`, the partition's own copy of the block, when `plans` is not empty. A stage that
/// reads files has one, naming the partition's files. A distributed engine that splits a stage's
/// partitions between tasks slices the descriptor along with the stage's other leaves, so a task
/// still learns each partition's index in the stage: Spark seeds `rand`,
/// `monotonically_increasing_id` and similar expressions with it.
pub fn partition_descriptor(
    num_partitions: usize,
    plans: &[Vec<u8>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = descriptor_schema();
    let partitions = (0..num_partitions)
        .map(|index| {
            let plan = plans.get(index).map(Vec::as_slice);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(UInt32Array::from(vec![index as u32])),
                    Arc::new(BinaryArray::from(vec![plan])),
                ],
            )?;
            Ok(vec![batch])
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(MemorySourceConfig::try_new_exec(&partitions, schema, None)?)
}

/// Runs a Comet native block for each partition of a stage.
///
/// The first child is the partition descriptor (see [`partition_descriptor`]), and each other
/// child is an input of the block, matched to the block's input leaves in the order Comet's
/// planner meets them. Partition `p` of the fragment reads partition `p` of each partitioned
/// input, and the single partition of each broadcast input.
#[derive(Debug)]
pub struct CometFragmentExec {
    fragment: Arc<Fragment>,
    /// The partition descriptor, then the inputs.
    children: Vec<Arc<dyn ExecutionPlan>>,
    props: Arc<PlanProperties>,
}

impl CometFragmentExec {
    pub fn try_new(fragment: Arc<Fragment>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        if fragment
            .output_schema
            .iter()
            .any(|field| field.data_type.is_none())
        {
            return Err(internal_datafusion_err!(
                "CometFragmentExec's output schema has a field without a type"
            ));
        }
        let Some((descriptor, inputs)) = children.split_first() else {
            return Err(internal_datafusion_err!(
                "CometFragmentExec needs a partition descriptor"
            ));
        };
        if inputs.len() != fragment.broadcast.len() {
            return Err(internal_datafusion_err!(
                "CometFragmentExec has {} inputs but {} broadcast flags",
                inputs.len(),
                fragment.broadcast.len()
            ));
        }
        let partitions = descriptor.output_partitioning().partition_count();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(fragment_schema(&fragment.output_schema)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            fragment,
            children,
            props,
        })
    }

    pub fn fragment(&self) -> &Arc<Fragment> {
        &self.fragment
    }

    /// Checks that each input's partitions line up with the descriptor's. This is checked when the
    /// plan runs rather than when it is built, since an optimizer can rebuild the node with an
    /// input whose exchange it has taken away and is about to put back.
    fn check_inputs(&self) -> Result<()> {
        let partitions = self.props.output_partitioning().partition_count();
        for (index, (input, broadcast)) in self.children[1..]
            .iter()
            .zip(&self.fragment.broadcast)
            .enumerate()
        {
            let expected = if *broadcast { 1 } else { partitions };
            let actual = input.output_partitioning().partition_count();
            if actual != expected {
                return Err(internal_datafusion_err!(
                    "CometFragmentExec input {index} has {actual} partitions, expected {expected}"
                ));
            }
        }
        Ok(())
    }
}

/// The schema of a fragment's output: the types Spark declared for it, every column nullable.
/// The cast to those types keeps a column nullable wherever the block's output is, even where
/// Spark knows it has no nulls, such as a join key a filter has checked, so a fragment can't
/// promise more. Nothing between stages relies on a column having no nulls.
fn fragment_schema(output_schema: &[SparkStructField]) -> SchemaRef {
    let declared = convert_spark_types_to_arrow_schema(output_schema);
    let fields: Vec<Field> = declared
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect();
    Arc::new(Schema::new(fields))
}

impl DisplayAs for CometFragmentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CometFragmentExec: block={} bytes, broadcast={:?}",
            self.fragment.block.len(),
            self.fragment.broadcast
        )
    }
}

impl ExecutionPlan for CometFragmentExec {
    fn name(&self) -> &str {
        "CometFragmentExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    /// The descriptor's and the partitioned inputs' partitions line up with this plan's, while a
    /// broadcast input is read whole by every partition, so a distributed engine must not split
    /// it between tasks. A stage with one partition reads every input as one partition, which
    /// keeps an optimizer such as `EnsureRequirements` from removing the coalesce that gathers
    /// it.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        let single = self.props.output_partitioning().partition_count() == 1;
        let inputs = self.fragment.broadcast.iter().map(|broadcast| {
            if *broadcast || single {
                Distribution::SinglePartition
            } else {
                Distribution::UnspecifiedDistribution
            }
        });
        InputDistributionRequirements::new(
            std::iter::once(Distribution::UnspecifiedDistribution)
                .chain(inputs)
                .collect(),
        )
    }

    /// Repartitioning an input for parallelism would break its alignment with the descriptor.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children.len()]
    }

    /// The block's expressions are in its serialized plan, so there are none to visit here.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            Arc::clone(&self.fragment),
            children,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.check_inputs()?;
        let descriptor = Arc::clone(&self.children[0]);
        let inputs = self.children[1..]
            .iter()
            .zip(&self.fragment.broadcast)
            .map(|(input, broadcast)| {
                let input_partition = if *broadcast { 0 } else { partition };
                fragment_input(Arc::clone(input), input_partition, Arc::clone(&context))
            })
            .collect::<Result<Vec<_>>>()?;
        let fragment = Arc::clone(&self.fragment);
        let schema = self.schema();
        let stream = futures::stream::once(async move {
            let (index, own_block) = read_descriptor(&descriptor, partition, &context).await?;
            // Planning can block on Comet's own runtime, which panics on a runtime thread.
            tokio::task::spawn_blocking(move || {
                let block = own_block.as_deref().unwrap_or(&fragment.block);
                run_block(block, index, inputs, &fragment, schema, &context)
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Reads partition `partition` of a fragment's descriptor: the partition's index in its stage,
/// and the partition's own copy of the block, if it has one.
async fn read_descriptor(
    descriptor: &Arc<dyn ExecutionPlan>,
    partition: usize,
    context: &Arc<TaskContext>,
) -> Result<(u32, Option<Vec<u8>>)> {
    let batches: Vec<RecordBatch> = descriptor
        .execute(partition, Arc::clone(context))?
        .try_collect()
        .await?;
    let batch = match batches.as_slice() {
        [batch] if batch.num_rows() == 1 => batch,
        _ => {
            return Err(internal_datafusion_err!(
                "A fragment's partition descriptor must have one row per partition"
            ))
        }
    };
    let index = batch.column(0).as_primitive::<UInt32Type>().value(0);
    let plans = batch.column(1).as_binary::<i32>();
    let plan = (!plans.is_null(0)).then(|| plans.value(0).to_vec());
    Ok((index, plan))
}

/// Plans `block` for the partition with index `index` in its stage, feeding its input leaves from
/// `inputs`, and starts it.
///
/// Its output is cast to the fragment's output schema, the types Spark declared for it, before it
/// leaves the stage, as a Spark task's shuffle writer casts its input: a hash exchange places a
/// row by the types of its key values as well as by the values. A wrong timezone label on a
/// timestamp fails instead in strict test runs, as it does there. Each batch then carries
/// `schema`, the fragment's, which a distributed engine checks it against.
fn run_block(
    block: &[u8],
    index: u32,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    fragment: &Fragment,
    schema: SchemaRef,
    context: &TaskContext,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let spark_plan = deserialize_op(block)?;
    let session = fragment_session(
        &spark_plan,
        &fragment.conf,
        Arc::clone(context.memory_pool()),
        Some(Arc::clone(&context.runtime_env().disk_manager)),
    )?;
    let planner =
        PhysicalPlanner::new(Arc::clone(&session), index as i32).with_native_inputs(inputs);
    let root = plan_block(&spark_plan, planner)?;
    let root = align_shuffle_writer_input(
        root,
        &fragment.output_schema,
        fragment.conf.get_bool(COMET_STRICT_TIMESTAMP_LABELS),
    )?;
    let stream = root.execute(0, session.task_ctx())?;
    // The session holds state that the block's operators may use while they run.
    Ok(stream
        .map(move |batch| {
            let _session = &session;
            let batch = batch?;
            // The cast made the names and types the declared ones, so this changes only
            // nullability and metadata.
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                batch.columns().to_vec(),
                &options,
            )?)
        })
        .boxed())
}

/// The Comet session a block runs in. It reserves memory from `memory_pool` and spills through
/// `disk_manager`, the host's, so that spill files go where the host keeps them.
fn fragment_session(
    spark_plan: &Operator,
    conf: &HashMap<String, String>,
    memory_pool: Arc<dyn MemoryPool>,
    disk_manager: Option<Arc<DiskManager>>,
) -> Result<Arc<SessionContext>> {
    let mut runtime = comet_runtime_env(memory_pool);
    runtime.disk_manager = disk_manager;
    let session = comet_session_context(
        runtime,
        conf.get_usize(COMET_BATCH_SIZE, DEFAULT_BATCH_SIZE),
        1,
        conf,
        spark_plan,
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(session))
}

fn plan_block(spark_plan: &Operator, planner: PhysicalPlanner) -> Result<Arc<dyn ExecutionPlan>> {
    let planner = planner.with_sql_text_pool(spark_plan);
    let (_, _, root) = planner.create_plan(spark_plan, &mut vec![], 1)?;
    Ok(Arc::clone(&root.native_plan))
}

/// The output schema of `block`, found by planning it without its data: each input leaf is empty,
/// and each file scan builds no object store for its files, since nothing reads them, so this
/// uses no storage credentials.
///
/// Fails where running the block would fail at planning, such as for an operator or expression
/// this build doesn't support, or one that calls into the JVM. A problem with the files or their
/// store shows only when the block runs. Planning can block on Comet's own runtime, so don't call
/// this on an async runtime's thread.
pub fn block_schema(block: &[u8], conf: &HashMap<String, String>) -> Result<SchemaRef> {
    let spark_plan = deserialize_op(block)?;
    let session = fragment_session(
        &spark_plan,
        conf,
        Arc::new(UnboundedMemoryPool::default()),
        None,
    )?;
    let planner = PhysicalPlanner::new(session, 0).without_data();
    Ok(plan_block(&spark_plan, planner)?.schema())
}

/// One partition of a host plan, read with the host's task context: what a block's input leaf
/// reads. Comet's planner sees it as a single-partition leaf.
fn fragment_input(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let stream = FragmentInput {
        input,
        partition,
        context,
        schema: Arc::clone(&schema),
    };
    Ok(Arc::new(StreamingTableExec::try_new(
        schema,
        vec![Arc::new(stream)],
        None,
        vec![],
        false,
        None,
    )?))
}

#[derive(Debug)]
struct FragmentInput {
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    schema: SchemaRef,
}

impl PartitionStream for FragmentInput {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Ignores the block's task context: the input belongs to the host's plan.
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        match self
            .input
            .execute(self.partition, Arc::clone(&self.context))
        {
            Ok(stream) => stream,
            Err(e) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::once(async { Err(e) }),
            )),
        }
    }
}
