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
use crate::partitioners::partitioned_batch_iterator::{
    PartitionedBatchIterator, PartitionedBatchesProducer,
};
use crate::partitioners::scratch::ScratchSpace;
use crate::partitioners::ShufflePartitioner;
use crate::writers::{BufBatchWriter, PartitionWriter};
use crate::{CometPartitioning, CompressionCodec, ShuffleBlockWriter};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_common::tracing::{with_trace, with_trace_async};
use itertools::Itertools;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, Write};
use std::sync::Arc;
use tokio::time::Instant;

/// A partitioner that uses a hash function to partition data into multiple partitions
pub(crate) struct MultiPartitionShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    buffered_batches: Vec<RecordBatch>,
    partition_indices: Vec<Vec<(u32, u32)>>,
    partition_writers: Vec<PartitionWriter>,
    shuffle_block_writer: ShuffleBlockWriter,
    /// Partitioning scheme to use
    partitioning: CometPartitioning,
    runtime: Arc<RuntimeEnv>,
    metrics: ShufflePartitionerMetrics,
    /// Reused scratch space for computing partition indices
    scratch: ScratchSpace,
    /// The configured batch size
    batch_size: usize,
    /// Reservation for repartitioning
    reservation: MemoryReservation,
    tracing_enabled: bool,
    /// Size of the write buffer in bytes
    write_buffer_size: usize,
}

impl MultiPartitionShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        partition: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: CometPartitioning,
        metrics: ShufflePartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        codec: CompressionCodec,
        tracing_enabled: bool,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let num_output_partitions = partitioning.partition_count();
        assert_ne!(
            num_output_partitions, 1,
            "Use SinglePartitionShufflePartitioner for 1 output partition."
        );

        let scratch = ScratchSpace::new(&partitioning, batch_size, num_output_partitions);

        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;

        let partition_writers = (0..num_output_partitions)
            .map(|_| PartitionWriter::try_new(shuffle_block_writer.clone()))
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let reservation = MemoryConsumer::new(format!("ShuffleRepartitioner[{partition}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        Ok(Self {
            output_data_file,
            output_index_file,
            buffered_batches: vec![],
            partition_indices: vec![vec![]; num_output_partitions],
            partition_writers,
            shuffle_block_writer,
            partitioning,
            runtime,
            metrics,
            scratch,
            batch_size,
            reservation,
            tracing_enabled,
            write_buffer_size,
        })
    }

    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function first calculates hashes for rows and then takes rows in same
    /// partition as a record batch which is appended into partition buffer.
    /// This should not be called directly. Use `insert_batch` instead.
    async fn partitioning_batch(&mut self, input: RecordBatch) -> datafusion::common::Result<()> {
        if input.num_rows() == 0 {
            // skip empty batch
            return Ok(());
        }

        if input.num_rows() > self.batch_size {
            return Err(DataFusionError::Internal(
                "Input batch size exceeds configured batch size. Call `insert_batch` instead."
                    .to_string(),
            ));
        }

        // Update data size metric
        self.metrics.data_size.add(input.get_array_memory_size());

        // NOTE: in shuffle writer exec, the output_rows metrics represents the
        // number of rows those are written to output data file.
        self.metrics.baseline.record_output(input.num_rows());

        let mut scratch = std::mem::take(&mut self.scratch);
        {
            let mut timer = self.metrics.repart_time.timer();
            scratch.compute_partition_ids(&self.partitioning, &input)?;
            timer.stop();
        }

        self.buffer_partitioned_batch_may_spill(
            input,
            &scratch.partition_row_indices,
            &scratch.partition_starts,
        )
        .await?;
        self.scratch = scratch;
        Ok(())
    }

    async fn buffer_partitioned_batch_may_spill(
        &mut self,
        input: RecordBatch,
        partition_row_indices: &[u32],
        partition_starts: &[u32],
    ) -> datafusion::common::Result<()> {
        let mut mem_growth: usize = input.get_array_memory_size();
        let buffered_partition_idx = self.buffered_batches.len() as u32;
        self.buffered_batches.push(input);

        // partition_starts conceptually slices partition_row_indices into smaller slices,
        // each slice contains the indices of rows in input that will go into the corresponding
        // partition. The following loop iterates over the slices and put the row indices into
        // the indices array of the corresponding partition.
        for (partition_id, (&start, &end)) in partition_starts
            .iter()
            .tuple_windows()
            .enumerate()
            .filter(|(_, (start, end))| start < end)
        {
            let row_indices = &partition_row_indices[start as usize..end as usize];

            // Put row indices for the current partition into the indices array of that partition.
            // This indices array will be used for calling interleave_record_batch to produce
            // shuffled batches.
            let indices = &mut self.partition_indices[partition_id];
            let before_size = indices.allocated_size();
            indices.reserve(row_indices.len());
            for row_idx in row_indices {
                indices.push((buffered_partition_idx, *row_idx));
            }
            let after_size = indices.allocated_size();
            mem_growth += after_size.saturating_sub(before_size);
        }

        if self.reservation.try_grow(mem_growth).is_err() {
            self.spill()?;
        }

        Ok(())
    }

    fn shuffle_write_partition(
        partition_iter: &mut PartitionedBatchIterator,
        shuffle_block_writer: &mut ShuffleBlockWriter,
        output_data: &mut BufWriter<File>,
        encode_time: &Time,
        write_time: &Time,
        write_buffer_size: usize,
        batch_size: usize,
    ) -> datafusion::common::Result<()> {
        let mut buf_batch_writer = BufBatchWriter::new(
            shuffle_block_writer,
            output_data,
            write_buffer_size,
            batch_size,
        );
        for batch in partition_iter {
            let batch = batch?;
            buf_batch_writer.write(&batch, encode_time, write_time)?;
        }
        buf_batch_writer.flush(encode_time, write_time)?;
        Ok(())
    }

    fn used(&self) -> usize {
        self.reservation.size()
    }

    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes.value()
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count.value()
    }

    fn data_size(&self) -> usize {
        self.metrics.data_size.value()
    }

    /// This function transfers the ownership of the buffered batches and partition indices from the
    /// ShuffleRepartitioner to a new PartitionedBatches struct. The returned PartitionedBatches struct
    /// can be used to produce shuffled batches.
    fn partitioned_batches(&mut self) -> PartitionedBatchesProducer {
        let num_output_partitions = self.partition_indices.len();
        let buffered_batches = std::mem::take(&mut self.buffered_batches);
        // let indices = std::mem::take(&mut self.partition_indices);
        let indices = std::mem::replace(
            &mut self.partition_indices,
            vec![vec![]; num_output_partitions],
        );
        PartitionedBatchesProducer::new(buffered_batches, indices, self.batch_size)
    }

    pub(crate) fn spill(&mut self) -> datafusion::common::Result<()> {
        log::info!(
            "ShuffleRepartitioner spilling shuffle data of {} to disk while inserting ({} time(s) so far)",
            self.used(),
            self.spill_count()
        );

        // we could always get a chance to free some memory as long as we are holding some
        if self.buffered_batches.is_empty() {
            return Ok(());
        }

        with_trace("shuffle_spill", self.tracing_enabled, || {
            let num_output_partitions = self.partition_writers.len();
            let mut partitioned_batches = self.partitioned_batches();
            let mut spilled_bytes = 0;

            for partition_id in 0..num_output_partitions {
                let partition_writer = &mut self.partition_writers[partition_id];
                let mut iter = partitioned_batches.produce(partition_id);
                spilled_bytes += partition_writer.spill(
                    &mut iter,
                    &self.runtime,
                    &self.metrics,
                    self.write_buffer_size,
                    self.batch_size,
                )?;
            }

            self.reservation.free();
            self.metrics.spill_count.add(1);
            self.metrics.spilled_bytes.add(spilled_bytes);
            Ok(())
        })
    }

    #[cfg(test)]
    pub(crate) fn partition_writers(&self) -> &[PartitionWriter] {
        &self.partition_writers
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for MultiPartitionShuffleRepartitioner {
    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function will slice input batch according to configured batch size and then
    /// shuffle rows into corresponding partition buffer.
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        with_trace_async("shuffle_insert_batch", self.tracing_enabled, || async {
            let start_time = Instant::now();
            let mut start = 0;
            while start < batch.num_rows() {
                let end = (start + self.batch_size).min(batch.num_rows());
                let batch = batch.slice(start, end - start);
                self.partitioning_batch(batch).await?;
                start = end;
            }
            self.metrics.input_batches.add(1);
            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());
            Ok(())
        })
        .await
    }

    /// Writes buffered shuffled record batches into Arrow IPC bytes.
    fn shuffle_write(&mut self) -> datafusion::common::Result<()> {
        with_trace("shuffle_write", self.tracing_enabled, || {
            let start_time = Instant::now();

            let mut partitioned_batches = self.partitioned_batches();
            let num_output_partitions = self.partition_indices.len();
            let mut offsets = vec![0; num_output_partitions + 1];

            let data_file = self.output_data_file.clone();
            let index_file = self.output_index_file.clone();

            let output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(data_file)
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

            let mut output_data = BufWriter::new(output_data);

            #[allow(clippy::needless_range_loop)]
            for i in 0..num_output_partitions {
                offsets[i] = output_data.stream_position()?;

                // if we wrote a spill file for this partition then copy the
                // contents into the shuffle file
                if let Some(spill_path) = self.partition_writers[i].path() {
                    let mut spill_file = BufReader::new(File::open(spill_path)?);
                    let mut write_timer = self.metrics.write_time.timer();
                    std::io::copy(&mut spill_file, &mut output_data)?;
                    write_timer.stop();
                }

                // Write in memory batches to output data file
                let mut partition_iter = partitioned_batches.produce(i);
                Self::shuffle_write_partition(
                    &mut partition_iter,
                    &mut self.shuffle_block_writer,
                    &mut output_data,
                    &self.metrics.encode_time,
                    &self.metrics.write_time,
                    self.write_buffer_size,
                    self.batch_size,
                )?;
            }

            let mut write_timer = self.metrics.write_time.timer();
            output_data.flush()?;
            write_timer.stop();

            // add one extra offset at last to ease partition length computation
            offsets[num_output_partitions] = output_data.stream_position()?;

            let mut write_timer = self.metrics.write_time.timer();
            crate::writers::write_index_file(&index_file, &offsets)?;
            write_timer.stop();

            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());

            Ok(())
        })
    }
}

impl Debug for MultiPartitionShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleRepartitioner")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .field("data_size", &self.data_size())
            .finish()
    }
}
