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

use crate::metrics::ShufflePartitionerMetrics;
use crate::writers::partition_writer::PartitionWriter;
use crate::ShuffleBlockWriter;
use arrow::array::RecordBatch;
use arrow::ipc::writer::CompressionContext;
use datafusion::common::{DataFusionError, Result};
use datafusion_comet_jni_bridge::ShufflePartitionPusher;
use std::io::Cursor;
use std::sync::Arc;

/// Sends complete Comet shuffle blocks to a task-owned remote shuffle pusher.
///
/// Each callback receives exactly one self-contained, length-prefixed Arrow
/// IPC block. Keeping block boundaries intact lets remote shuffle services
/// concatenate or retrieve the resulting payloads without repairing partially
/// encoded blocks. The encoded frame is checked against `max_frame_size`
/// before it is exposed to the pusher.
///
/// Partition data may arrive in any order until its partition is finalized.
/// Finalization follows the same ascending-partition contract as the existing
/// local shuffle writer.
pub struct RssPartitionWriter {
    block_writer: ShuffleBlockWriter,
    pusher: Arc<dyn ShufflePartitionPusher>,
    num_partitions: usize,
    max_frame_size: usize,
    compression_context: CompressionContext,
    frame: Vec<u8>,
    next_partition_to_finish: usize,
    finished: bool,
}

impl RssPartitionWriter {
    /// Creates a remote writer without retaining any thread-local JNI state.
    ///
    /// `num_partitions` must be nonzero and each partition identifier must fit
    /// in a JVM `int`. `max_frame_size` is the maximum complete encoded shuffle
    /// block passed to a callback and must also be nonzero.
    pub fn try_new(
        block_writer: ShuffleBlockWriter,
        pusher: Arc<dyn ShufflePartitionPusher>,
        num_partitions: usize,
        max_frame_size: usize,
    ) -> Result<Self> {
        if num_partitions == 0 {
            return Err(DataFusionError::Execution(
                "Remote shuffle output requires at least one partition".to_string(),
            ));
        }

        if num_partitions - 1 > i32::MAX as usize {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partition count {num_partitions} exceeds the JVM partition limit"
            )));
        }

        if max_frame_size == 0 {
            return Err(DataFusionError::Execution(
                "Remote shuffle maximum frame size must be greater than zero".to_string(),
            ));
        }

        Ok(Self {
            block_writer,
            pusher,
            num_partitions,
            max_frame_size,
            compression_context: CompressionContext::default(),
            frame: Vec::new(),
            next_partition_to_finish: 0,
            finished: false,
        })
    }

    fn validate_writable_partition(&self, partition_id: usize) -> Result<i32> {
        if self.finished {
            return Err(DataFusionError::Execution(
                "Remote shuffle writer has already finished".to_string(),
            ));
        }

        if partition_id >= self.num_partitions {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partition {partition_id} is outside the configured range 0..{}",
                self.num_partitions
            )));
        }

        if partition_id < self.next_partition_to_finish {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partition {partition_id} has already been finalized"
            )));
        }

        i32::try_from(partition_id).map_err(|_| {
            DataFusionError::Execution(format!(
                "Remote shuffle partition {partition_id} exceeds the JVM partition limit"
            ))
        })
    }

    fn push_batches<I>(
        &mut self,
        partition_id: i32,
        batches: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        for batch in batches.by_ref() {
            let batch = batch?;
            self.frame.clear();

            let encoded_size = self.block_writer.write_batch(
                &batch,
                &mut Cursor::new(&mut self.frame),
                &mut self.compression_context,
                &metrics.encode_time,
            )?;

            if encoded_size == 0 {
                continue;
            }

            if encoded_size > self.max_frame_size {
                return Err(DataFusionError::Execution(format!(
                    "Remote shuffle frame size {encoded_size} exceeds the configured maximum {}",
                    self.max_frame_size
                )));
            }

            let mut write_timer = metrics.write_time.timer();
            let result = self.pusher.push_partition_data(partition_id, &self.frame);
            write_timer.stop();
            result?;
        }

        Ok(())
    }
}

impl PartitionWriter for RssPartitionWriter {
    fn write<I>(
        &mut self,
        partition_id: usize,
        batches: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        let jvm_partition_id = self.validate_writable_partition(partition_id)?;
        self.push_batches(jvm_partition_id, batches, metrics)
    }

    fn finish_partition<I>(
        &mut self,
        partition_id: usize,
        batches: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        let jvm_partition_id = self.validate_writable_partition(partition_id)?;
        if partition_id != self.next_partition_to_finish {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partitions must be finalized in order: expected {}, got {partition_id}",
                self.next_partition_to_finish
            )));
        }

        self.push_batches(jvm_partition_id, batches, metrics)?;
        self.next_partition_to_finish += 1;
        Ok(())
    }

    fn finish_all(&mut self, _metrics: &ShufflePartitionerMetrics) -> Result<()> {
        if self.finished {
            return Err(DataFusionError::Execution(
                "Remote shuffle writer has already finished".to_string(),
            ));
        }

        if self.next_partition_to_finish != self.num_partitions {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle writer cannot finish before all partitions are finalized: \
                 finalized {} of {}",
                self.next_partition_to_finish, self.num_partitions
            )));
        }

        self.finished = true;
        Ok(())
    }
}
