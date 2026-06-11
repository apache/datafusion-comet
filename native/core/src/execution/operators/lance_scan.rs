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

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
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

use crate::execution::operators::ExecutionError;

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
    pub fn try_new(
        dataset_uri: String,
        resolved_version: i64,
        storage_options: HashMap<String, String>,
        output_schema: SchemaRef,
        filter_sql: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
        batch_size: u32,
        spark_partition_index: u32,
        fragment_ids: Vec<u32>,
    ) -> Result<Self, ExecutionError> {
        if dataset_uri.is_empty() {
            return Err(ExecutionError::GeneralError(
                "LanceScan missing dataset_uri".to_string(),
            ));
        }

        let resolved_version = resolved_version.try_into().map_err(|_| {
            ExecutionError::GeneralError(format!(
                "LanceScan resolved_version must be non-negative, got {resolved_version}"
            ))
        })?;

        validate_optional_non_negative("limit", limit)?;
        validate_optional_non_negative("offset", offset)?;
        validate_ordered_fragment_ids(&fragment_ids)?;

        let projection_names = output_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let plan_properties = Self::compute_properties(Arc::clone(&output_schema));
        let metrics = ExecutionPlanMetricsSet::new();
        let filter_sql = filter_sql.filter(|filter| !filter.is_empty());
        let limit = limit.filter(|limit| *limit != 0);
        let offset = offset.filter(|offset| *offset != 0);
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

        Ok(scanner.try_into_stream().await.map_err(lance_error)?.into())
    }
}

impl ExecutionPlan for LanceScanExec {
    fn name(&self) -> &str {
        "LanceScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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

fn validate_optional_non_negative(name: &str, value: Option<i64>) -> Result<(), ExecutionError> {
    if matches!(value, Some(value) if value < 0) {
        return Err(ExecutionError::GeneralError(format!(
            "LanceScan {name} must be non-negative, got {}",
            value.unwrap()
        )));
    }
    Ok(())
}

fn validate_ordered_fragment_ids(fragment_ids: &[u32]) -> Result<(), ExecutionError> {
    if fragment_ids.windows(2).any(|window| window[0] >= window[1]) {
        return Err(ExecutionError::GeneralError(
            "LanceScan fragment_ids must be strictly increasing".to_string(),
        ));
    }
    Ok(())
}

fn lance_error(error: impl fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("Lance scan error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn rejects_invalid_descriptor_values() {
        struct TestCase {
            name: &'static str,
            resolved_version: i64,
            limit: Option<i64>,
            offset: Option<i64>,
            fragment_ids: Vec<u32>,
            expected_error: &'static str,
        }

        let cases = vec![
            TestCase {
                name: "negative version",
                resolved_version: -1,
                limit: None,
                offset: None,
                fragment_ids: vec![1],
                expected_error: "resolved_version must be non-negative",
            },
            TestCase {
                name: "negative limit",
                resolved_version: 1,
                limit: Some(-1),
                offset: None,
                fragment_ids: vec![1],
                expected_error: "limit must be non-negative",
            },
            TestCase {
                name: "negative offset",
                resolved_version: 1,
                limit: None,
                offset: Some(-1),
                fragment_ids: vec![1],
                expected_error: "offset must be non-negative",
            },
            TestCase {
                name: "unordered fragments",
                resolved_version: 1,
                limit: None,
                offset: None,
                fragment_ids: vec![2, 1],
                expected_error: "fragment_ids must be strictly increasing",
            },
        ];

        for case in cases {
            let err = LanceScanExec::try_new(
                "file:///tmp/table.lance".to_string(),
                case.resolved_version,
                HashMap::new(),
                test_schema(),
                None,
                case.limit,
                case.offset,
                0,
                0,
                case.fragment_ids,
            )
            .expect_err(case.name);
            assert!(
                err.to_string().contains(case.expected_error),
                "{}: expected error containing {:?}, got {:?}",
                case.name,
                case.expected_error,
                err
            );
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]))
    }
}
