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

use crate::{
    errors::CometError,
    execution::{
        operators::ExecutionError, planner::TEST_EXEC_CONTEXT_ID,
        shuffle::codec::read_ipc_compressed,
    },
    jvm_bridge::{jni_call, JVMClasses},
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{arrow_datafusion_err, Result as DataFusionResult};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::{
    execution::TaskContext,
    physical_expr::*,
    physical_plan::{ExecutionPlan, *},
};
use futures::Stream;
use jni::objects::{GlobalRef, JByteBuffer, JObject};
use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use super::scan::InputBatch;

/// ShuffleScanExec reads compressed shuffle blocks from JVM via JNI and decodes them natively.
/// Unlike ScanExec which receives Arrow arrays via FFI, ShuffleScanExec receives raw compressed
/// bytes from CometShuffleBlockIterator and decodes them using read_ipc_compressed().
#[derive(Debug, Clone)]
pub struct ShuffleScanExec {
    /// The ID of the execution context that owns this subquery.
    pub exec_context_id: i64,
    /// The input source: a global reference to a JVM CometShuffleBlockIterator object.
    pub input_source: Option<Arc<GlobalRef>>,
    /// The data types of columns in the shuffle output.
    pub data_types: Vec<DataType>,
    /// Schema of the shuffle output.
    pub schema: SchemaRef,
    /// The current input batch, populated by get_next_batch() before poll_next().
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    /// Cache of plan properties.
    cache: PlanProperties,
    /// Metrics collector.
    metrics: ExecutionPlanMetricsSet,
    /// Baseline metrics.
    baseline_metrics: BaselineMetrics,
    /// Time spent decoding compressed shuffle blocks.
    decode_time: Time,
}

impl ShuffleScanExec {
    pub fn new(
        exec_context_id: i64,
        input_source: Option<Arc<GlobalRef>>,
        data_types: Vec<DataType>,
    ) -> Result<Self, CometError> {
        let metrics_set = ExecutionPlanMetricsSet::default();
        let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
        let decode_time = MetricBuilder::new(&metrics_set).subset_time("decode_time", 0);

        let schema = schema_from_data_types(&data_types);

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            exec_context_id,
            input_source,
            data_types,
            batch: Arc::new(Mutex::new(None)),
            cache,
            metrics: metrics_set,
            baseline_metrics,
            schema,
            decode_time,
        })
    }

    /// Feeds input batch into this scan. Only used in unit tests.
    pub fn set_input_batch(&mut self, input: InputBatch) {
        *self.batch.try_lock().unwrap() = Some(input);
    }

    /// Pull next input batch from JVM. Called externally before poll_next()
    /// because JNI calls cannot happen from within poll_next on tokio threads.
    pub fn get_next_batch(&mut self) -> Result<(), CometError> {
        if self.input_source.is_none() {
            // Unit test mode - no JNI calls needed.
            return Ok(());
        }
        let mut timer = self.baseline_metrics.elapsed_compute().timer();

        let mut current_batch = self.batch.try_lock().unwrap();
        if current_batch.is_none() {
            let next_batch = Self::get_next(
                self.exec_context_id,
                self.input_source.as_ref().unwrap().as_obj(),
                &self.data_types,
                &self.decode_time,
            )?;
            *current_batch = Some(next_batch);
        }

        timer.stop();

        Ok(())
    }

    /// Invokes JNI calls to get the next compressed shuffle block and decode it.
    fn get_next(
        exec_context_id: i64,
        iter: &JObject,
        data_types: &[DataType],
        decode_time: &Time,
    ) -> Result<InputBatch, CometError> {
        if exec_context_id == TEST_EXEC_CONTEXT_ID {
            return Ok(InputBatch::EOF);
        }

        if iter.is_null() {
            return Err(CometError::from(ExecutionError::GeneralError(format!(
                "Null shuffle block iterator object. Plan id: {exec_context_id}"
            ))));
        }

        let mut env = JVMClasses::get_env()?;

        // has_next() reads the next block and returns its length, or -1 if EOF
        let block_length: i32 = unsafe {
            jni_call!(&mut env,
                comet_shuffle_block_iterator(iter).has_next() -> i32)?
        };

        if block_length == -1 {
            return Ok(InputBatch::EOF);
        }

        // Get the DirectByteBuffer containing the compressed shuffle block
        let buffer: JObject = unsafe {
            jni_call!(&mut env,
                comet_shuffle_block_iterator(iter).get_buffer() -> JObject)?
        };

        let byte_buffer = JByteBuffer::from(buffer);
        let raw_pointer = env.get_direct_buffer_address(&byte_buffer)?;
        let length = block_length as usize;
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };

        // Decode the compressed IPC data
        let mut timer = decode_time.timer();
        let batch = read_ipc_compressed(slice)?;
        timer.stop();

        let num_rows = batch.num_rows();

        // The read_ipc_compressed already produces owned arrays, so we skip the
        // header (field count + codec) that was already consumed by read_ipc_compressed.
        // Extract column arrays from the RecordBatch.
        let columns: Vec<ArrayRef> = batch.columns().to_vec();

        debug_assert_eq!(
            columns.len(),
            data_types.len(),
            "Shuffle block column count mismatch: got {} but expected {}",
            columns.len(),
            data_types.len()
        );

        Ok(InputBatch::new(columns, Some(num_rows)))
    }
}

fn schema_from_data_types(data_types: &[DataType]) -> SchemaRef {
    let fields = data_types
        .iter()
        .enumerate()
        .map(|(idx, dt)| Field::new(format!("col_{idx}"), dt.clone(), true))
        .collect::<Vec<Field>>();

    Arc::new(Schema::new(fields))
}

impl ExecutionPlan for ShuffleScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(ShuffleScanStream::new(
            self.clone(),
            self.schema(),
            partition,
            self.baseline_metrics.clone(),
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn name(&self) -> &str {
        "ShuffleScanExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for ShuffleScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let fields: Vec<String> = self
                    .data_types
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| format!("col_{idx}: {dt}"))
                    .collect();
                write!(f, "ShuffleScanExec: schema=[{}]", fields.join(", "))?;
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
        Ok(())
    }
}

/// An async stream that feeds decoded shuffle batches into the DataFusion plan.
struct ShuffleScanStream {
    /// The ShuffleScanExec producing input batches.
    shuffle_scan: ShuffleScanExec,
    /// Schema of the output.
    schema: SchemaRef,
    /// Metrics.
    baseline_metrics: BaselineMetrics,
}

impl ShuffleScanStream {
    pub fn new(
        shuffle_scan: ShuffleScanExec,
        schema: SchemaRef,
        _partition: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            shuffle_scan,
            schema,
            baseline_metrics,
        }
    }
}

impl Stream for ShuffleScanStream {
    type Item = DataFusionResult<arrow::array::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        let mut scan_batch = self.shuffle_scan.batch.try_lock().unwrap();

        let input_batch = &*scan_batch;
        let input_batch = if let Some(batch) = input_batch {
            batch
        } else {
            timer.stop();
            return Poll::Pending;
        };

        let result = match input_batch {
            InputBatch::EOF => Poll::Ready(None),
            InputBatch::Batch(columns, num_rows) => {
                self.baseline_metrics.record_output(*num_rows);
                let options =
                    arrow::array::RecordBatchOptions::new().with_row_count(Some(*num_rows));
                let maybe_batch = arrow::array::RecordBatch::try_new_with_options(
                    Arc::clone(&self.schema),
                    columns.clone(),
                    &options,
                )
                .map_err(|e| arrow_datafusion_err!(e));
                Poll::Ready(Some(maybe_batch))
            }
        };

        *scan_batch = None;

        timer.stop();

        result
    }
}

impl RecordBatchStream for ShuffleScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::shuffle::codec::{CompressionCodec, ShuffleBlockWriter};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::metrics::Time;
    use std::io::Cursor;
    use std::sync::Arc;

    use crate::execution::shuffle::codec::read_shuffle_block;

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call FFI functions (zstd)
    fn test_read_compressed_block() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Write as compressed raw batch
        let writer =
            ShuffleBlockWriter::try_new(&batch.schema(), CompressionCodec::Zstd(1)).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = Time::new();
        writer.write_batch(&batch, &mut buf, &ipc_time).unwrap();

        // Read back (skip 16-byte header: 8 compressed_length + 8 field_count)
        let bytes = buf.into_inner();
        let body = &bytes[16..];

        let decoded = read_shuffle_block(body, &schema).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 2);

        // Verify data
        let col0 = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col0.value(0), 1);
        assert_eq!(col0.value(1), 2);
        assert_eq!(col0.value(2), 3);
    }
}
