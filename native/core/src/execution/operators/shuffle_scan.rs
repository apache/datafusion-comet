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
        operators::ExecutionError,
        planner::TEST_EXEC_CONTEXT_ID,
        shuffle::{read_ipc_compressed, read_ipc_compressed_validated, validate_remote_schema},
    },
    jvm_bridge::{jni_call, JVMClasses},
};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DataFusionResult;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::{
    execution::TaskContext,
    physical_expr::*,
    physical_plan::{ExecutionPlan, *},
};
use datafusion_comet_common::cast_and_stamp_schema;
use futures::Stream;
use jni::objects::{Global, JByteBuffer, JObject};
use std::{
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
    pub input_source: Option<Arc<Global<JObject<'static>>>>,
    /// The data types of columns in the shuffle output.
    pub data_types: Vec<DataType>,
    /// Schema of the shuffle output.
    pub schema: SchemaRef,
    /// The current input batch, populated by get_next_batch() before poll_next().
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    /// Cache of plan properties.
    cache: Arc<PlanProperties>,
    /// Metrics collector.
    metrics: ExecutionPlanMetricsSet,
    /// Baseline metrics.
    baseline_metrics: BaselineMetrics,
    /// Time spent decoding compressed shuffle blocks.
    decode_time: Time,
    /// Remote inputs require Arrow array and logical schema validation; queried once at construction.
    requires_validation: bool,
}

impl ShuffleScanExec {
    pub fn new(
        exec_context_id: i64,
        input_source: Option<Arc<Global<JObject<'static>>>>,
        data_types: Vec<DataType>,
    ) -> Result<Self, CometError> {
        let requires_validation = if exec_context_id == TEST_EXEC_CONTEXT_ID {
            false
        } else if let Some(input) = &input_source {
            JVMClasses::with_env(|env| unsafe {
                jni_call!(env,
                    comet_shuffle_block_iterator(input.as_obj()).requires_validation() -> bool)
            })?
        } else {
            false
        };
        let metrics_set = ExecutionPlanMetricsSet::default();
        let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
        let decode_time = MetricBuilder::new(&metrics_set).subset_time("decode_time", 0);

        let schema = schema_from_data_types(&data_types);

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

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
            requires_validation,
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
                self.requires_validation,
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
        requires_validation: bool,
    ) -> Result<InputBatch, CometError> {
        if exec_context_id == TEST_EXEC_CONTEXT_ID {
            return Ok(InputBatch::EOF);
        }

        if iter.is_null() {
            return Err(CometError::from(ExecutionError::GeneralError(format!(
                "Null shuffle block iterator object. Plan id: {exec_context_id}"
            ))));
        }

        JVMClasses::with_env(|env| {
            // has_next() reads the next block and returns its length, or -1 if EOF
            let block_length: i32 = unsafe {
                jni_call!(env,
                    comet_shuffle_block_iterator(iter).has_next() -> i32)?
            };

            if block_length == -1 {
                return Ok(InputBatch::EOF);
            }

            // Get the DirectByteBuffer containing the compressed shuffle block
            let buffer: JObject = unsafe {
                jni_call!(env,
                    comet_shuffle_block_iterator(iter).get_buffer() -> JObject)?
            };

            let byte_buffer = unsafe { JByteBuffer::from_raw(env, buffer.into_raw()) };
            let raw_pointer = env.get_direct_buffer_address(&byte_buffer)?;
            let length = block_length as usize;
            let slice: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };

            // Decode the compressed IPC data
            let mut timer = decode_time.timer();
            let batch = match decode_shuffle_batch(slice, data_types, requires_validation) {
                Ok(batch) => batch,
                Err(failure) => {
                    // Remote inputs must invalidate the failed shuffle generation even when
                    // fetching bytes succeeded and only IPC/codec decoding or logical schema
                    // validation detects the corruption. JNI preserves FetchFailedException.
                    // Local inputs leave this callback as a no-op and keep the native error.
                    let message = env.new_string(failure.to_string())?;
                    unsafe {
                        jni_call!(env,
                            comet_shuffle_block_iterator(iter).on_decode_failure(&message) -> ())?;
                    }
                    return Err(failure.into());
                }
            };
            timer.stop();

            let num_rows = batch.num_rows();

            // Extract column arrays, unpacking any dictionary-encoded columns.
            // Native shuffle may dictionary-encode string/binary columns for efficiency,
            // but downstream DataFusion operators expect the value types declared in the
            // schema (e.g. Utf8, not Dictionary<Int32, Utf8>).
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| unpack_dictionary(col))
                .collect();

            unsafe {
                jni_call!(env,
                    comet_shuffle_block_iterator(iter).inc_records_read(num_rows as i64) -> ())?
            };
            Ok(InputBatch::new(columns, Some(num_rows)))
        })
    }
}

fn decode_shuffle_batch(
    bytes: &[u8],
    expected_types: &[DataType],
    requires_validation: bool,
) -> DataFusionResult<RecordBatch> {
    if requires_validation {
        let batch = read_ipc_compressed_validated(bytes)?;
        // Validate before unpack_dictionary or cast_and_stamp_schema can hide an incompatible
        // wire type by changing values. Keep failures inside get_next's recovery callback.
        validate_remote_schema(&batch, expected_types)?;
        Ok(batch)
    } else {
        check_column_count(read_ipc_compressed(bytes)?, expected_types.len())
    }
}

fn check_column_count(batch: RecordBatch, expected: usize) -> DataFusionResult<RecordBatch> {
    if batch.num_columns() != expected {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "Shuffle block column count mismatch: got {} but expected {expected}",
            batch.num_columns()
        )));
    }
    Ok(batch)
}

/// If `array` is dictionary-encoded, cast it to the value type. Otherwise return as-is.
fn unpack_dictionary(array: &ArrayRef) -> ArrayRef {
    if let DataType::Dictionary(_, value_type) = array.data_type() {
        arrow::compute::cast(array, value_type.as_ref()).expect("failed to unpack dictionary array")
    } else {
        Arc::clone(array)
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
            partition,
            self.baseline_metrics.clone(),
        )))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
    /// Metrics.
    baseline_metrics: BaselineMetrics,
}

impl ShuffleScanStream {
    pub fn new(
        shuffle_scan: ShuffleScanExec,
        _partition: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            shuffle_scan,
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
                // Reconcile the decoded block with the catalyst-declared schema rather than
                // stamping it on, so that nested field nullability drift is absorbed here the way
                // `ScanExec` absorbs it at the FFI boundary.
                // See https://github.com/apache/datafusion-comet/issues/5137.
                let maybe_batch = cast_and_stamp_schema(
                    self.shuffle_scan.name(),
                    &self.shuffle_scan.schema,
                    columns.clone(),
                    *num_rows,
                );
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
        self.shuffle_scan.schema()
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::shuffle::{CompressionCodec, ShuffleBlockWriter};
    use arrow::array::{Int32Array, RecordBatchOptions, StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::CompressionContext;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::metrics::Time;
    use std::io::Cursor;
    use std::sync::Arc;

    use crate::execution::shuffle::ipc::read_ipc_compressed;

    fn uncompressed_shuffle_payload(batch: &RecordBatch) -> Vec<u8> {
        let writer = ShuffleBlockWriter::try_new(&batch.schema(), CompressionCodec::None).unwrap();
        let mut output = Cursor::new(Vec::new());
        writer
            .write_batch(
                batch,
                &mut output,
                &mut CompressionContext::default(),
                &Time::new(),
            )
            .unwrap();
        // The iterator has already checked the 8-byte frame length and 8-byte field count.
        output.into_inner()[16..].to_vec()
    }

    #[test]
    fn remote_shuffle_rejects_schema_signedness_flip_before_casting() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![-1]))]).unwrap();
        let mut payload = uncompressed_shuffle_payload(&batch);

        // Corrupt only the schema's signed flag, leaving the field count, row count and all
        // value buffers valid. The same -1 bytes now represent UINT_MAX, which a later cast to
        // the Spark IntegerType would silently turn into null.
        let schema_start = 4 + 8; // codec, then IPC continuation marker and metadata length
        let signed_flag = {
            let message = arrow::ipc::root_as_message(&payload[schema_start..]).unwrap();
            let integer = message
                .header_as_schema()
                .unwrap()
                .fields()
                .unwrap()
                .get(0)
                .type_as_int()
                .unwrap();
            assert!(integer.is_signed());
            let offset = integer._tab.vtable().get(arrow::ipc::Int::VT_IS_SIGNED);
            assert_ne!(offset, 0);
            schema_start + integer._tab.loc() + usize::from(offset)
        };
        payload[signed_flag] = 0;

        let decoded = super::read_ipc_compressed_validated(&payload).unwrap();
        assert_eq!(decoded.num_columns(), 1);
        assert_eq!(decoded.column(0).data_type(), &DataType::UInt32);
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values(),
            &[u32::MAX]
        );
        let error = super::decode_shuffle_batch(&payload, &[DataType::Int32], true)
            .unwrap_err()
            .to_string();
        assert!(error.contains("type mismatch at column 0"), "{error}");
        assert!(error.contains("UInt32"), "{error}");
        assert!(error.contains("Int32"), "{error}");

        // The new logical validation is confined to remote inputs.
        assert_eq!(
            super::decode_shuffle_batch(&payload, &[DataType::Int32], false)
                .unwrap()
                .column(0)
                .data_type(),
            &DataType::UInt32
        );
    }

    #[test]
    fn remote_shuffle_preserves_rows_for_zero_column_batches() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let payload = uncompressed_shuffle_payload(&batch);
        let decoded = super::decode_shuffle_batch(&payload, &[], true).unwrap();
        assert_eq!(decoded.num_columns(), 0);
        assert_eq!(decoded.num_rows(), 3);
    }

    #[test]
    fn test_shuffle_column_count_mismatch_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        assert!(super::check_column_count(batch.clone(), 1).is_ok());
        for expected in [0, 2] {
            let error = super::check_column_count(batch.clone(), expected)
                .unwrap_err()
                .to_string();
            assert!(error.contains("column count mismatch"), "{error}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call FFI functions (zstd)
    fn test_read_compressed_ipc_block() {
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

        // Write as compressed IPC
        let writer =
            ShuffleBlockWriter::try_new(&batch.schema(), CompressionCodec::Zstd(1)).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = Time::new();
        writer
            .write_batch(
                &batch,
                &mut buf,
                &mut CompressionContext::default(),
                &ipc_time,
            )
            .unwrap();

        // Read back (skip 16-byte header: 8 compressed_length + 8 field_count)
        let bytes = buf.into_inner();
        let body = &bytes[16..];

        let decoded = read_ipc_compressed(body).unwrap();
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

    /// Tests that ShuffleScanExec correctly unpacks dictionary-encoded columns.
    /// Native shuffle may dictionary-encode string/binary columns, but the schema
    /// declares value types (e.g. Utf8). Without unpacking, RecordBatch creation
    /// fails with a schema mismatch.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_dictionary_encoded_shuffle_block_is_unpacked() {
        use super::*;
        use arrow::array::StringDictionaryBuilder;
        use arrow::datatypes::Int32Type;
        use datafusion::physical_plan::ExecutionPlan;
        use futures::StreamExt;

        // Build a batch with a dictionary-encoded string column (simulating what
        // the native shuffle writer produces for string columns).
        let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        dict_builder.append_value("hello");
        dict_builder.append_value("world");
        dict_builder.append_value("hello"); // repeated value, good for dictionary
        let dict_array = dict_builder.finish();

        // The IPC schema includes the dictionary type
        let dict_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let dict_batch = RecordBatch::try_new(
            Arc::clone(&dict_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(dict_array),
            ],
        )
        .unwrap();

        // Write as compressed IPC (preserves dictionary encoding)
        let writer =
            ShuffleBlockWriter::try_new(&dict_batch.schema(), CompressionCodec::Zstd(1)).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = Time::new();
        writer
            .write_batch(
                &dict_batch,
                &mut buf,
                &mut CompressionContext::default(),
                &ipc_time,
            )
            .unwrap();
        let bytes = buf.into_inner();
        let body = &bytes[16..];

        // Remote schema validation must preserve the writer's dictionary representation.
        let decoded =
            super::decode_shuffle_batch(body, &[DataType::Int32, DataType::Utf8], true).unwrap();
        assert!(
            matches!(decoded.column(1).data_type(), DataType::Dictionary(_, _)),
            "Expected dictionary-encoded column from IPC, got {:?}",
            decoded.column(1).data_type()
        );

        // Create ShuffleScanExec with value types (Utf8, not Dictionary) — this is
        // what the protobuf schema provides.
        let mut scan = ShuffleScanExec::new(
            super::super::super::planner::TEST_EXEC_CONTEXT_ID,
            None,
            vec![DataType::Int32, DataType::Utf8],
        )
        .unwrap();

        // Feed the decoded batch through unpack_dictionary (simulating get_next)
        let columns: Vec<ArrayRef> = decoded
            .columns()
            .iter()
            .map(|col| super::unpack_dictionary(col))
            .collect();
        let input = InputBatch::new(columns, Some(decoded.num_rows()));
        scan.set_input_batch(input);

        // Execute and verify the output RecordBatch has the expected schema
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Arc::new(TaskContext::default());
            let mut stream = scan.execute(0, ctx).unwrap();
            let result_batch = stream.next().await.unwrap().unwrap();

            // Schema should have Utf8, not Dictionary
            assert_eq!(
                *result_batch.schema().field(1).data_type(),
                DataType::Utf8,
                "Expected Utf8 after dictionary unpacking"
            );

            // Verify data integrity
            let col1 = result_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Column should be StringArray after unpacking");
            assert_eq!(col1.value(0), "hello");
            assert_eq!(col1.value(1), "world");
            assert_eq!(col1.value(2), "hello");
        });
    }

    /// A decoded shuffle block whose nested field nullability is narrower than the catalyst-declared
    /// type must be reconciled, not rejected. `ShuffleScanExec` used to stamp the declared schema
    /// straight onto the block, which aborted the task on a single nested `nullable` flag even
    /// though a non-null child is a strict subset of a nullable one.
    /// See <https://github.com/apache/datafusion-comet/issues/5137>.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_nested_nullability_drift_is_reconciled() {
        use super::*;
        use crate::execution::operators::nested_nullability_fixture::{
            list_of_struct, list_of_struct_type,
        };
        use arrow::array::Array;
        use datafusion::physical_plan::ExecutionPlan;
        use futures::StreamExt;

        // The block carries `List(Struct("id": Int64, "flag": non-null Boolean))` while catalyst
        // declared the `flag` child nullable.
        let block_column = list_of_struct(false);
        let declared = list_of_struct_type(true);
        let block = RecordBatch::try_from_iter([("payload", block_column)]).unwrap();
        let payload = uncompressed_shuffle_payload(&block);
        let decoded =
            super::decode_shuffle_batch(&payload, std::slice::from_ref(&declared), true).unwrap();
        let mut scan = ShuffleScanExec::new(
            super::super::super::planner::TEST_EXEC_CONTEXT_ID,
            None,
            vec![declared.clone()],
        )
        .unwrap();
        scan.set_input_batch(InputBatch::new(decoded.columns().to_vec(), Some(2)));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Arc::new(TaskContext::default());
            let mut stream = scan.execute(0, ctx).unwrap();
            let batch = stream.next().await.unwrap().unwrap();

            assert_eq!(batch.schema().field(0).data_type(), &declared);
            assert_eq!(batch.num_rows(), 2);
            // The values must survive the reconciliation untouched. `ArrayData` equality is
            // logical, so this holds regardless of whether the cast materialized an all-valid
            // null buffer for the widened child.
            assert_eq!(batch.column(0).to_data(), list_of_struct(true).to_data());
        });
    }

    /// An unreconcilable column must name the operator and the column, since arrow's own message
    /// reports only `at column index N`.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_unreconcilable_column_error_names_operator() {
        use super::*;
        use arrow::datatypes::Fields;
        use datafusion::physical_plan::ExecutionPlan;
        use futures::StreamExt;

        let declared =
            DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int64, true)]));
        let mut scan = ShuffleScanExec::new(
            super::super::super::planner::TEST_EXEC_CONTEXT_ID,
            None,
            vec![declared],
        )
        .unwrap();
        let column: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        scan.set_input_batch(InputBatch::new(vec![column], Some(2)));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Arc::new(TaskContext::default());
            let mut stream = scan.execute(0, ctx).unwrap();
            let err = stream.next().await.unwrap().unwrap_err().to_string();
            assert!(err.contains("ShuffleScanExec"), "{err}");
            assert!(err.contains("col[0]"), "{err}");
            assert!(err.contains("col_0: expected Struct"), "{err}");
        });
    }
}
