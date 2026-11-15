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

//! A seam onto the shuffle partitioners for `benches/`, which compiles as its own crate and so
//! can only reach `pub` items.
//!
//! The partitioners are `pub(crate)` on purpose: `native/core` drives them through
//! [`ShuffleWriterExec`](crate::ShuffleWriterExec) and nothing else should. Rather than widen
//! their visibility so a benchmark can name them, this exposes one opaque handle that does
//! exactly what the partitioning benchmark needs — place rows, then gather them back out —
//! and keeps every type it is built from private.
//!
//! Not an API, and not used outside `benches/`.

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::{MultiPartitionShuffleRepartitioner, ShufflePartitioner};
use crate::writers::PartitionWriter;
use crate::CometPartitioning;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use std::sync::Arc;

/// Drops every batch it is handed, so a flush measures the gather out of the partition index
/// and nothing downstream of it: no IPC encoding, no compression, no file write.
struct DiscardingPartitionWriter;

impl PartitionWriter for DiscardingPartitionWriter {
    fn write<I>(
        &mut self,
        _pid: usize,
        iter: &mut I,
        _metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        for batch in iter {
            std::hint::black_box(batch?);
        }
        Ok(())
    }

    fn finish_partition<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        self.write(pid, iter, metrics)
    }

    fn finish_all(&mut self, _metrics: &ShufflePartitionerMetrics) -> Result<()> {
        Ok(())
    }
}

/// One shuffle map task's worth of partitioning, with the write side stubbed out.
#[doc(hidden)]
pub struct BenchRepartitioner {
    inner: MultiPartitionShuffleRepartitioner<DiscardingPartitionWriter>,
}

impl BenchRepartitioner {
    /// Builds a repartitioner over an unbounded memory pool and no buffer limit, so nothing
    /// spills and every strategy is measured over the same work.
    pub fn try_new(partitioning: CometPartitioning, batch_size: usize) -> Result<Self> {
        let runtime = Arc::new(RuntimeEnvBuilder::new().build()?);
        Ok(Self {
            inner: MultiPartitionShuffleRepartitioner::try_new(
                0,
                DiscardingPartitionWriter,
                partitioning,
                ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                runtime,
                batch_size,
                false,
                None,
            )?,
        })
    }

    /// Places every row of every batch and buffers its index, which is everything a shuffle
    /// write does before it flushes. The batches are cloned, so a caller can reuse its fixture
    /// across iterations.
    ///
    /// Nothing below here awaits, so a bare executor is enough and the measurement carries no
    /// tokio runtime.
    pub fn place(&mut self, batches: &[RecordBatch]) -> Result<()> {
        futures::executor::block_on(async {
            for batch in batches {
                self.inner.insert_batch(batch.clone()).await?;
            }
            Ok(())
        })
    }

    /// Gathers the buffered rows back into output batches, one output partition at a time, and
    /// discards them.
    pub fn gather(&mut self) -> Result<()> {
        self.inner.shuffle_write()
    }
}
