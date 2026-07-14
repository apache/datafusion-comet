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

//! Native Lance table scan operator.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::future::{BoxFuture, FutureExt};
use futures::Stream;
use lance::dataset::builder::DatasetBuilder;
use lance::deps::datafusion::execution::SendableRecordBatchStream as LanceSendableRecordBatchStream;

#[derive(Debug)]
pub struct LanceScanConfig {
    pub dataset_uri: String,
    pub resolved_version: i64,
    pub storage_options: HashMap<String, String>,
    pub output_schema: SchemaRef,
    pub filter_sql: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub batch_size: u32,
    pub spark_partition_index: u32,
    pub fragment_ids: Vec<u32>,
}

#[derive(Debug)]
pub struct LanceScanExec {
    dataset_uri: String,
    resolved_version: u64,
    storage_options: HashMap<String, String>,
    output_schema: SchemaRef,
    projection_names: Vec<String>,
    filter_sql: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    batch_size: Option<usize>,
    spark_partition_index: u32,
    fragment_ids: Vec<u32>,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl LanceScanExec {
    pub fn try_new(config: LanceScanConfig) -> DFResult<Self> {
        let LanceScanConfig {
            dataset_uri,
            resolved_version,
            storage_options,
            output_schema,
            filter_sql,
            limit,
            offset,
            batch_size,
            spark_partition_index,
            fragment_ids,
        } = config;
        let resolved_version = resolved_version.try_into().map_err(|_| {
            DataFusionError::Execution(format!(
                "LanceScan resolved_version must be non-negative, got {resolved_version}"
            ))
        })?;

        let projection_names = output_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let plan_properties = Self::compute_properties(Arc::clone(&output_schema));
        let metrics = ExecutionPlanMetricsSet::new();
        let batch_size = (batch_size != 0).then_some(batch_size as usize);

        Ok(Self {
            dataset_uri,
            resolved_version,
            storage_options,
            output_schema,
            projection_names,
            filter_sql,
            limit,
            offset,
            batch_size,
            spark_partition_index,
            fragment_ids,
            plan_properties,
            metrics,
        })
    }

    fn compute_properties(schema: SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    async fn open_stream(
        dataset_uri: String,
        resolved_version: u64,
        storage_options: HashMap<String, String>,
        projection_names: Vec<String>,
        filter_sql: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
        batch_size: Option<usize>,
        fragment_ids: Vec<u32>,
    ) -> DFResult<SendableRecordBatchStream> {
        let dataset = DatasetBuilder::from_uri(dataset_uri)
            .with_version(resolved_version)
            .with_storage_options(storage_options)
            .load()
            .await
            .map_err(lance_error)?;

        let file_fragments = dataset.get_frags_from_ordered_ids(&fragment_ids);
        let mut fragments = Vec::with_capacity(file_fragments.len());
        for (fragment_id, fragment) in fragment_ids.iter().zip(file_fragments.into_iter()) {
            let fragment = fragment.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "LanceScan requested missing fragment id {fragment_id}"
                ))
            })?;
            fragments.push(fragment.metadata().clone());
        }

        let mut scanner = dataset.scan();
        scanner.with_fragments(fragments);
        if projection_names.is_empty() {
            scanner.project(&[] as &[&str]).map_err(lance_error)?;
        } else {
            scanner.project(&projection_names).map_err(lance_error)?;
        }
        if let Some(filter) = filter_sql {
            scanner.filter(&filter).map_err(lance_error)?;
        }
        if limit.is_some() || offset.is_some() {
            scanner.limit(limit, offset).map_err(lance_error)?;
        }
        if let Some(batch_size) = batch_size {
            scanner.batch_size(batch_size);
        }

        let lance_stream: LanceSendableRecordBatchStream =
            scanner.try_into_stream().await.map_err(lance_error)?.into();
        Ok(Box::pin(LanceRecordBatchStreamAdapter {
            inner: lance_stream,
        }))
    }
}

impl ExecutionPlan for LanceScanExec {
    fn name(&self) -> &str {
        "LanceScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LanceScanExec has one native partition, got partition {partition}"
            )));
        }

        let metrics = LanceScanMetrics::new(&self.metrics);
        metrics.fragment_count.add(self.fragment_ids.len());

        let open_future = Self::open_stream(
            self.dataset_uri.clone(),
            self.resolved_version,
            self.storage_options.clone(),
            self.projection_names.clone(),
            self.filter_sql.clone(),
            self.limit,
            self.offset,
            self.batch_size,
            self.fragment_ids.clone(),
        )
        .boxed();

        Ok(Box::pin(LanceScanStream {
            state: LanceScanStreamState::Opening(open_future),
            schema: Arc::clone(&self.output_schema),
            baseline_metrics: metrics.baseline,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for LanceScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LanceScanExec: dataset_uri={}, version={}, spark_partition={}, fragments={}",
            self.dataset_uri,
            self.resolved_version,
            self.spark_partition_index,
            self.fragment_ids.len()
        )
    }
}

struct LanceRecordBatchStreamAdapter {
    inner: LanceSendableRecordBatchStream,
}

impl Stream for LanceRecordBatchStreamAdapter {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner
            .as_mut()
            .poll_next(cx)
            .map(|poll| poll.map(|result| result.map_err(lance_error)))
    }
}

impl RecordBatchStream for LanceRecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

struct LanceScanMetrics {
    baseline: BaselineMetrics,
    fragment_count: Count,
}

impl LanceScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, 0),
            fragment_count: MetricBuilder::new(metrics).counter("fragment_count", 0),
        }
    }
}

enum LanceScanStreamState {
    Opening(BoxFuture<'static, DFResult<SendableRecordBatchStream>>),
    Scanning(SendableRecordBatchStream),
    Done,
}

struct LanceScanStream {
    state: LanceScanStreamState,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl Stream for LanceScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                LanceScanStreamState::Opening(open_future) => match open_future.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        this.state = LanceScanStreamState::Scanning(stream);
                    }
                    Poll::Ready(Err(err)) => {
                        this.state = LanceScanStreamState::Done;
                        return this
                            .baseline_metrics
                            .record_poll(Poll::Ready(Some(Err(err))));
                    }
                    Poll::Pending => return this.baseline_metrics.record_poll(Poll::Pending),
                },
                LanceScanStreamState::Scanning(stream) => {
                    let poll = stream.as_mut().poll_next(cx);
                    if matches!(poll, Poll::Ready(None)) {
                        this.state = LanceScanStreamState::Done;
                    }
                    return this.baseline_metrics.record_poll(poll);
                }
                LanceScanStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for LanceScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn lance_error(error: impl fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("Lance scan error: {error}"))
}
