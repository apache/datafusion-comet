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

//! Builders and planning helpers for native shuffle operators.

use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    row::{OwnedRow, RowConverter, SortField},
};
use datafusion::{
    common::ScalarValue,
    physical_expr::{expressions::Literal, LexOrdering, PhysicalExpr, PhysicalSortExpr},
    physical_plan::ExecutionPlan,
};
use datafusion_comet_proto::spark_operator::{
    self, CompressionCodec as SparkCompressionCodec, Operator,
};
use datafusion_comet_proto::spark_partitioning::{
    partitioning::PartitioningStruct, Partitioning as SparkPartitioning,
};
use datafusion_comet_spark_expr::normalize_floats;
use jni::objects::{Global, JObject};

use super::{
    convert_spark_types_to_arrow_schema, operator_registry::OperatorBuilder, PhysicalPlanner,
    PlanCreationResult, TEST_EXEC_CONTEXT_ID,
};
use crate::{
    execution::{
        operators::{ExecutionError, ShuffleScanExec},
        serde::to_arrow_datatype,
        shuffle::{
            CometPartitioning, CompressionCodec, PartitionOffsets, RoundRobinStrategy,
            SchemaAlignExec, ShuffleWriterDestination, ShuffleWriterExec,
        },
        spark_plan::SparkPlan,
    },
    extract_op,
    jvm_bridge::ShufflePartitionPusher,
};

use ExecutionError::GeneralError;

/// Builder for native shuffle writers.
pub struct ShuffleWriterBuilder;

impl OperatorBuilder for ShuffleWriterBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let writer = extract_op!(spark_plan, ShuffleWriter);
        let children = &spark_plan.children;

        assert_eq!(children.len(), 1);
        let (scans, shuffle_scans, child) =
            planner.create_plan(&children[0], inputs, partition_count)?;

        let writer_input = align_shuffle_writer_input(
            Arc::clone(&child.native_plan),
            &writer.expected_output_schema,
        )?;

        let partitioning = planner
            .create_partitioning(writer.partitioning.as_ref().unwrap(), writer_input.schema())?;

        let codec = match writer.codec.try_into() {
            Ok(SparkCompressionCodec::None) => Ok(CompressionCodec::None),
            Ok(SparkCompressionCodec::Snappy) => Ok(CompressionCodec::Snappy),
            Ok(SparkCompressionCodec::Zstd) => Ok(CompressionCodec::Zstd(writer.compression_level)),
            Ok(SparkCompressionCodec::Lz4) => Ok(CompressionCodec::Lz4Frame),
            _ => Err(GeneralError(format!(
                "Unsupported shuffle compression codec: {:?}",
                writer.codec
            ))),
        }?;

        let destination =
            shuffle_writer_destination(writer, planner.shuffle_partition_pusher.as_ref())?;
        let write_buffer_size = writer.write_buffer_size as usize;
        // Zero on the wire means the limit is disabled; normalize it here so the writer
        // only ever sees a real limit or none at all.
        let max_buffer_bytes =
            (writer.max_buffer_bytes > 0).then_some(writer.max_buffer_bytes as usize);
        let shuffle_writer = Arc::new(ShuffleWriterExec::try_new_with_destination(
            writer_input,
            partitioning,
            codec,
            destination,
            writer.tracing_enabled,
            write_buffer_size,
            max_buffer_bytes,
        )?);

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                shuffle_writer,
                vec![Arc::clone(&child)],
            )),
        ))
    }
}

impl PhysicalPlanner {
    /// Create a DataFusion physical partitioning from Spark physical partitioning.
    pub(super) fn create_partitioning(
        &self,
        spark_partitioning: &SparkPartitioning,
        input_schema: SchemaRef,
    ) -> Result<CometPartitioning, ExecutionError> {
        match spark_partitioning.partitioning_struct.as_ref().unwrap() {
            PartitioningStruct::HashPartition(hash_partition) => {
                let exprs: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = hash_partition
                    .hash_expression
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .collect();
                Ok(CometPartitioning::Hash(
                    exprs?,
                    hash_partition.num_partitions as usize,
                ))
            }
            PartitioningStruct::RangePartition(range_partition) => {
                // Generate the lexical ordering for comparisons
                let exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = range_partition
                    .sort_orders
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, Arc::clone(&input_schema)))
                    .collect();
                let lex_ordering = LexOrdering::new(exprs?).unwrap();

                // Generate the row converter for comparing incoming batches to boundary rows
                let sort_fields: Vec<SortField> = lex_ordering
                    .iter()
                    .map(|sort_expr| {
                        sort_expr
                            .expr
                            .data_type(input_schema.as_ref())
                            .map(|dt| SortField::new_with_options(dt, sort_expr.options))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Deserialize the literals to columnar collections of ScalarValues
                let mut scalar_values: Vec<Vec<ScalarValue>> = vec![vec![]; lex_ordering.len()];
                for boundary_row in &range_partition.boundary_rows {
                    // For each serialized expr in a boundary row, convert to a Literal
                    // expression, then extract the ScalarValue from the Literal and push it
                    // into the collection of ScalarValues
                    for (col_idx, col_values) in scalar_values
                        .iter_mut()
                        .enumerate()
                        .take(lex_ordering.len())
                    {
                        let expr = self.create_expr(
                            &boundary_row.partition_bounds[col_idx],
                            Arc::clone(&input_schema),
                        )?;
                        let literal_expr = expr.downcast_ref::<Literal>().expect("Literal");
                        col_values.push(literal_expr.value().clone());
                    }
                }

                // Normalize boundary arrays just like the incoming sort keys, so equal NaNs
                // and signed zeros are assigned to the same range partition.
                let arrays: Vec<ArrayRef> = scalar_values
                    .iter()
                    .map(|scalar_vec| {
                        ScalarValue::iter_to_array(scalar_vec.iter().cloned())
                            .map(|array| normalize_floats(&array))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Create a RowConverter and use to create OwnedRows from the Arrays
                let converter = RowConverter::new(sort_fields)?;
                let boundary_rows = converter.convert_columns(&arrays)?;
                // Rows are only a view into Arrow Arrays. We need to create OwnedRows with their
                // own internal memory ownership to pass as our boundary values to the partitioner.
                let boundary_owned_rows: Vec<OwnedRow> =
                    boundary_rows.iter().map(|row| row.owned()).collect();

                Ok(CometPartitioning::RangePartitioning(
                    lex_ordering,
                    range_partition.num_partitions as usize,
                    Arc::new(converter),
                    boundary_owned_rows,
                ))
            }
            PartitioningStruct::SinglePartition(_) => Ok(CometPartitioning::SinglePartition),
            PartitioningStruct::RoundRobinPartition(rr_partition) => {
                // Treat negative max_hash_columns as 0 (no limit).
                let max_hash_columns = rr_partition.max_hash_columns.max(0) as usize;
                let strategy = if rr_partition.positional {
                    // Resolved on the driver and frozen with the shuffle dependency. Deriving it
                    // here from the executor's batch size would let a retried task use a
                    // different group size, and so a different placement, than the attempt it
                    // replaces.
                    if rr_partition.positional_group_rows <= 0 {
                        return Err(GeneralError(format!(
                            "Positional round robin needs a positive group size, got {}",
                            rr_partition.positional_group_rows
                        )));
                    }
                    RoundRobinStrategy::RowGroups {
                        // Computed per task on the JVM, where the Spark map partition id is in
                        // scope. See `CometShuffleExchangeExec.positionalStartPartition`.
                        start_partition: rr_partition.positional_start_partition.max(0) as usize,
                        group_rows: rr_partition.positional_group_rows as usize,
                        // Kept for the case where the schema rules positional placement out.
                        max_hash_columns,
                    }
                } else {
                    RoundRobinStrategy::HashAll { max_hash_columns }
                };
                Ok(CometPartitioning::RoundRobin(
                    rr_partition.num_partitions as usize,
                    strategy,
                ))
            }
        }
    }
}

/// Builder for native shuffle scans.
pub struct ShuffleScanBuilder;

impl OperatorBuilder for ShuffleScanBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        _partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let scan = extract_op!(spark_plan, ShuffleScan);
        let data_types = scan.fields.iter().map(to_arrow_datatype).collect();

        let exec_context_id = planner.exec_context_id;
        if exec_context_id != TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
            return Err(GeneralError("No input for shuffle scan".to_string()));
        }

        let input_source = if exec_context_id == TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
            None
        } else {
            Some(inputs.remove(0))
        };

        let shuffle_scan = ShuffleScanExec::new(exec_context_id, input_source, data_types)?;

        Ok((
            vec![],
            vec![shuffle_scan.clone()],
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                Arc::new(shuffle_scan),
                vec![],
            )),
        ))
    }
}

/// Wrap `child` in a `SchemaAlignExec` when its output drifts from what Spark catalyst
/// declared. See <https://github.com/apache/datafusion-comet/issues/4515>.
fn align_shuffle_writer_input(
    child: Arc<dyn ExecutionPlan>,
    expected_proto: &[spark_operator::SparkStructField],
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    if expected_proto.is_empty() {
        return Ok(child);
    }
    let expected = convert_spark_types_to_arrow_schema(expected_proto);
    SchemaAlignExec::try_new_or_passthrough(child, &expected)
        .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
}

/// Resolves a native shuffle writer's destination and binds its task-owned callback, if any.
///
/// Plans serialized before partition-writer descriptors were introduced only contain the
/// top-level paths. New local plans populate both representations so older native binaries can
/// still consume them; when both are present they must agree to avoid ambiguous destinations.
/// RSS plans must not contain local output paths and require the callback supplied by their own
/// Spark task. Supplying a remote callback for a local destination is also rejected.
fn shuffle_writer_destination(
    writer: &spark_operator::ShuffleWriter,
    shuffle_partition_pusher: Option<&Arc<dyn ShufflePartitionPusher>>,
) -> Result<ShuffleWriterDestination, ExecutionError> {
    let Some(partition_writer) = writer.partition_writer.as_ref() else {
        if shuffle_partition_pusher.is_some() {
            return Err(GeneralError(
                "Local shuffle partition writer cannot use a remote shuffle callback".to_string(),
            ));
        }

        return Ok(ShuffleWriterDestination::Local {
            output_data_file: writer.output_data_file.clone(),
            partition_offsets: Arc::new(PartitionOffsets::default()),
        });
    };

    match partition_writer.writer.as_ref() {
        Some(spark_operator::partition_writer::Writer::Local(local)) => {
            if local.output_data_file.is_empty() {
                return Err(GeneralError(
                    "Local shuffle partition writer is missing its output data file".to_string(),
                ));
            }

            if !writer.output_data_file.is_empty()
                && writer.output_data_file != local.output_data_file
            {
                return Err(GeneralError(
                    "Local shuffle partition writer output data file conflicts with the legacy \
                     shuffle output data file"
                        .to_string(),
                ));
            }

            if shuffle_partition_pusher.is_some() {
                return Err(GeneralError(
                    "Local shuffle partition writer cannot use a remote shuffle callback"
                        .to_string(),
                ));
            }

            Ok(ShuffleWriterDestination::Local {
                output_data_file: local.output_data_file.clone(),
                partition_offsets: Arc::new(PartitionOffsets::default()),
            })
        }
        Some(spark_operator::partition_writer::Writer::Rss(_)) => {
            if !writer.output_data_file.is_empty() {
                return Err(GeneralError(
                    "RSS shuffle partition writer cannot have local output files".to_string(),
                ));
            }

            let pusher = shuffle_partition_pusher.ok_or_else(|| {
                GeneralError(
                    "RSS shuffle partition writer requires a task-owned remote shuffle callback"
                        .to_string(),
                )
            })?;

            Ok(ShuffleWriterDestination::Rss {
                pusher: Arc::clone(pusher),
                max_frame_size: pusher.max_frame_size(),
            })
        }
        None => Err(GeneralError(
            "Shuffle partition writer has no destination".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::Ordering, task::Poll};

    use datafusion_comet_proto::{
        spark_expression,
        spark_operator::operator::OpStruct,
        spark_partitioning::{partitioning::PartitioningStruct, Partitioning, SinglePartition},
    };
    use futures::{poll, StreamExt};

    use super::*;
    use crate::{execution::operators::InputBatch, jvm_bridge::JavaShufflePartitionPusher};

    #[derive(Default)]
    struct RecordingShufflePartitionPusher {
        pushes: std::sync::atomic::AtomicUsize,
    }

    impl ShufflePartitionPusher for RecordingShufflePartitionPusher {
        fn push_partition_data(
            &self,
            _partition_id: i32,
            _data: &[u8],
        ) -> datafusion::common::Result<()> {
            self.pushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct BoundedShufflePartitionPusher {
        max_frame_size: usize,
    }

    impl ShufflePartitionPusher for BoundedShufflePartitionPusher {
        fn push_partition_data(
            &self,
            _partition_id: i32,
            _data: &[u8],
        ) -> datafusion::common::Result<()> {
            Ok(())
        }

        fn max_frame_size(&self) -> usize {
            self.max_frame_size
        }
    }

    fn local_shuffle_partition_writer(output_data_file: &str) -> spark_operator::PartitionWriter {
        spark_operator::PartitionWriter {
            writer: Some(spark_operator::partition_writer::Writer::Local(
                spark_operator::LocalPartitionWriter {
                    output_data_file: output_data_file.to_string(),
                },
            )),
        }
    }

    fn rss_shuffle_partition_writer() -> spark_operator::PartitionWriter {
        spark_operator::PartitionWriter {
            writer: Some(spark_operator::partition_writer::Writer::Rss(
                spark_operator::RssPartitionWriter {},
            )),
        }
    }

    fn assert_local_shuffle_destination(
        writer: &spark_operator::ShuffleWriter,
        expected_data_file: &str,
    ) {
        match shuffle_writer_destination(writer, None).unwrap() {
            ShuffleWriterDestination::Local {
                output_data_file,
                partition_offsets,
            } => {
                assert_eq!(output_data_file, expected_data_file);
                // A fresh destination has not run a writer yet, so nothing is published.
                assert!(partition_offsets.get().is_none());
            }
            destination => panic!("expected a local shuffle destination, got {destination:?}"),
        }
    }

    #[test]
    fn shuffle_partition_writer_legacy_paths_remain_supported() {
        let writer = spark_operator::ShuffleWriter {
            output_data_file: "legacy.data".to_string(),
            ..Default::default()
        };

        assert_local_shuffle_destination(&writer, "legacy.data");
    }

    #[test]
    fn shuffle_partition_writer_uses_nested_local_paths() {
        let writer = spark_operator::ShuffleWriter {
            partition_writer: Some(local_shuffle_partition_writer("shuffle.data")),
            ..Default::default()
        };

        assert_local_shuffle_destination(&writer, "shuffle.data");
    }

    #[test]
    fn shuffle_partition_writer_accepts_matching_legacy_paths() {
        let writer = spark_operator::ShuffleWriter {
            output_data_file: "shuffle.data".to_string(),
            partition_writer: Some(local_shuffle_partition_writer("shuffle.data")),
            ..Default::default()
        };

        assert_local_shuffle_destination(&writer, "shuffle.data");
    }

    #[test]
    fn shuffle_partition_writer_rejects_conflicting_legacy_data_path() {
        let writer = spark_operator::ShuffleWriter {
            output_data_file: "legacy.data".to_string(),
            partition_writer: Some(local_shuffle_partition_writer("shuffle.data")),
            ..Default::default()
        };

        let error = shuffle_writer_destination(&writer, None).unwrap_err();
        assert!(
            error.to_string().contains("output data file conflicts"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn shuffle_partition_writer_rejects_empty_local_data_path() {
        let writer = spark_operator::ShuffleWriter {
            output_data_file: "legacy.data".to_string(),
            partition_writer: Some(local_shuffle_partition_writer("")),
            ..Default::default()
        };

        let error = shuffle_writer_destination(&writer, None).unwrap_err();
        assert!(
            error.to_string().contains("missing its output data file"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn shuffle_partition_writer_rejects_missing_destination() {
        let writer = spark_operator::ShuffleWriter {
            partition_writer: Some(spark_operator::PartitionWriter { writer: None }),
            ..Default::default()
        };

        let error = shuffle_writer_destination(&writer, None).unwrap_err();
        assert!(
            error.to_string().contains("has no destination"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn shuffle_partition_writer_requires_a_task_callback_for_rss() {
        let writer = spark_operator::ShuffleWriter {
            partition_writer: Some(rss_shuffle_partition_writer()),
            ..Default::default()
        };

        let error = shuffle_writer_destination(&writer, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires a task-owned remote shuffle callback"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn shuffle_partition_writer_binds_its_rss_task_callback() {
        let writer = spark_operator::ShuffleWriter {
            partition_writer: Some(rss_shuffle_partition_writer()),
            ..Default::default()
        };
        let callback: Arc<dyn ShufflePartitionPusher> =
            Arc::new(RecordingShufflePartitionPusher::default());

        match shuffle_writer_destination(&writer, Some(&callback)).unwrap() {
            ShuffleWriterDestination::Rss {
                pusher,
                max_frame_size,
            } => {
                assert!(Arc::ptr_eq(&pusher, &callback));
                assert_eq!(max_frame_size, JavaShufflePartitionPusher::MAX_PAYLOAD_SIZE);
            }
            destination => panic!("expected an RSS shuffle destination, got {destination:?}"),
        }
    }

    #[test]
    fn shuffle_partition_writer_uses_its_task_callback_frame_limit() {
        let writer = spark_operator::ShuffleWriter {
            partition_writer: Some(rss_shuffle_partition_writer()),
            ..Default::default()
        };
        let callback: Arc<dyn ShufflePartitionPusher> = Arc::new(BoundedShufflePartitionPusher {
            max_frame_size: 4096,
        });

        match shuffle_writer_destination(&writer, Some(&callback)).unwrap() {
            ShuffleWriterDestination::Rss { max_frame_size, .. } => {
                assert_eq!(max_frame_size, 4096);
            }
            destination => panic!("expected an RSS shuffle destination, got {destination:?}"),
        }
    }

    #[test]
    fn shuffle_partition_writer_rejects_rss_with_legacy_data_path() {
        let writer = spark_operator::ShuffleWriter {
            output_data_file: "legacy.data".to_string(),
            partition_writer: Some(rss_shuffle_partition_writer()),
            ..Default::default()
        };
        let callback: Arc<dyn ShufflePartitionPusher> =
            Arc::new(RecordingShufflePartitionPusher::default());

        let error = shuffle_writer_destination(&writer, Some(&callback)).unwrap_err();
        assert!(
            error.to_string().contains("cannot have local output files"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn shuffle_partition_writer_rejects_callback_for_legacy_local_destination() {
        let writer = spark_operator::ShuffleWriter {
            output_data_file: "legacy.data".to_string(),
            ..Default::default()
        };
        let callback: Arc<dyn ShufflePartitionPusher> =
            Arc::new(RecordingShufflePartitionPusher::default());

        let error = shuffle_writer_destination(&writer, Some(&callback)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot use a remote shuffle callback"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn shuffle_partition_writer_rejects_callback_for_explicit_local_destination() {
        let writer = spark_operator::ShuffleWriter {
            partition_writer: Some(local_shuffle_partition_writer("shuffle.data")),
            ..Default::default()
        };
        let callback: Arc<dyn ShufflePartitionPusher> =
            Arc::new(RecordingShufflePartitionPusher::default());

        let error = shuffle_writer_destination(&writer, Some(&callback)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot use a remote shuffle callback"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn shuffle_partition_writer_callbacks_remain_isolated_between_tasks() {
        let writer = spark_operator::ShuffleWriter {
            partition_writer: Some(rss_shuffle_partition_writer()),
            ..Default::default()
        };
        let first_recorder = Arc::new(RecordingShufflePartitionPusher::default());
        let second_recorder = Arc::new(RecordingShufflePartitionPusher::default());
        let first_callback: Arc<dyn ShufflePartitionPusher> =
            Arc::<RecordingShufflePartitionPusher>::clone(&first_recorder);
        let second_callback: Arc<dyn ShufflePartitionPusher> =
            Arc::<RecordingShufflePartitionPusher>::clone(&second_recorder);
        let first_planner = PhysicalPlanner::default()
            .with_shuffle_partition_pusher(Some(Arc::clone(&first_callback)));
        let second_planner = PhysicalPlanner::default()
            .with_shuffle_partition_pusher(Some(Arc::clone(&second_callback)));

        let ShuffleWriterDestination::Rss {
            pusher: first_pusher,
            ..
        } = shuffle_writer_destination(&writer, first_planner.shuffle_partition_pusher.as_ref())
            .unwrap()
        else {
            panic!("expected an RSS shuffle destination for the first task");
        };
        let ShuffleWriterDestination::Rss {
            pusher: second_pusher,
            ..
        } = shuffle_writer_destination(&writer, second_planner.shuffle_partition_pusher.as_ref())
            .unwrap()
        else {
            panic!("expected an RSS shuffle destination for the second task");
        };

        assert!(Arc::ptr_eq(&first_pusher, &first_callback));
        assert!(Arc::ptr_eq(&second_pusher, &second_callback));
        assert!(!Arc::ptr_eq(&first_pusher, &second_pusher));

        first_pusher.push_partition_data(0, b"first").unwrap();
        assert_eq!(first_recorder.pushes.load(Ordering::Relaxed), 1);
        assert_eq!(second_recorder.pushes.load(Ordering::Relaxed), 0);

        second_pusher.push_partition_data(0, b"second").unwrap();
        assert_eq!(first_recorder.pushes.load(Ordering::Relaxed), 1);
        assert_eq!(second_recorder.pushes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn shuffle_partition_writer_plans_and_executes_rss_with_its_task_callback() {
        let scan = Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![],
                source: "rss-task-input".to_string(),
            })),
        };
        let shuffle = Operator {
            plan_id: 1,
            sql_text_pool: vec![],
            children: vec![scan],
            op_struct: Some(OpStruct::ShuffleWriter(spark_operator::ShuffleWriter {
                partitioning: Some(Partitioning {
                    partitioning_struct: Some(PartitioningStruct::SinglePartition(
                        SinglePartition {},
                    )),
                }),
                partition_writer: Some(rss_shuffle_partition_writer()),
                write_buffer_size: 1024,
                ..Default::default()
            })),
        };
        let recorder = Arc::new(RecordingShufflePartitionPusher::default());
        let callback: Arc<dyn ShufflePartitionPusher> =
            Arc::<RecordingShufflePartitionPusher>::clone(&recorder);
        let planner = PhysicalPlanner::default().with_shuffle_partition_pusher(Some(callback));
        let (mut scans, shuffle_scans, plan) =
            planner.create_plan(&shuffle, &mut vec![], 1).unwrap();

        assert!(shuffle_scans.is_empty());
        scans[0].set_input_batch(InputBatch::Batch(vec![], 17));
        let mut stream = plan
            .native_plan
            .execute(0, planner.session_ctx().task_ctx())
            .unwrap();

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let mut eof_sent = false;

                loop {
                    match poll!(stream.next()) {
                        Poll::Ready(Some(result)) => {
                            let result = result.unwrap();
                            panic!("shuffle writer must not produce output batches: {result:?}");
                        }
                        Poll::Ready(None) => break,
                        Poll::Pending if !eof_sent => {
                            scans[0].set_input_batch(InputBatch::EOF);
                            eof_sent = true;
                        }
                        Poll::Pending => {
                            panic!("shuffle writer remained pending after end of input")
                        }
                    }
                }
            });

        assert_eq!(recorder.pushes.load(Ordering::Relaxed), 1);
    }

    fn shuffle_scan_operator() -> Operator {
        Operator {
            plan_id: 1,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::ShuffleScan(spark_operator::ShuffleScan {
                fields: vec![spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }],
                ..Default::default()
            })),
        }
    }

    #[test]
    fn shuffle_scan_plans_without_input_in_test_context() {
        let planner = PhysicalPlanner::default();
        let (scans, shuffle_scans, plan) = planner
            .create_plan(&shuffle_scan_operator(), &mut vec![], 1)
            .unwrap();

        assert!(scans.is_empty());
        assert_eq!(shuffle_scans.len(), 1);
        assert!(plan.native_plan.is::<ShuffleScanExec>());
    }

    #[test]
    fn shuffle_scan_requires_input_outside_test_context() {
        let planner = PhysicalPlanner::default().with_exec_id(1);
        let error = planner
            .create_plan(&shuffle_scan_operator(), &mut vec![], 1)
            .unwrap_err();

        assert!(error.to_string().contains("No input for shuffle scan"));
    }
}
