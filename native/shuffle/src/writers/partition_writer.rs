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
use arrow::record_batch::RecordBatch;

/// Storage backend abstraction for shuffle partition output.
///
/// Decouples partitioning from storage: partitioners only produce partitioned
/// `RecordBatch` streams, while implementations of this trait own how those
/// batches are stored and finalized. [`LocalPartitionWriter`] implements the
/// local file behavior; other backends (e.g. a remote shuffle writer) can be
/// added without changing the partitioners.
///
/// A partitioner drives a writer as: any number of
/// [`write`](PartitionWriter::write) calls to stage batches, then one
/// [`finish_partition`](PartitionWriter::finish_partition) per partition in
/// ascending id order, then a single [`finish_all`](PartitionWriter::finish_all).
///
/// [`LocalPartitionWriter`]: crate::writers::local::local_partition_writer::LocalPartitionWriter
pub(crate) trait PartitionWriter: Send + Sync {
    /// Stages the batches from `iter` for partition `pid` without finalizing it.
    ///
    /// Used to stream single-partition output and to stage multi-partition
    /// spilled batches. A partition may be written multiple times and in any
    /// order; staged data is only guaranteed visible after
    /// [`finish_partition`](PartitionWriter::finish_partition).
    fn write<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>;

    /// Finalizes partition `pid`, writing any remaining batches from `iter` and
    /// combining them with data previously staged via
    /// [`write`](PartitionWriter::write).
    ///
    /// Must be called exactly once per partition, in ascending id order, so the
    /// writer can lay partitions out contiguously and record their offsets.
    fn finish_partition<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>;

    /// Completes the shuffle write, flushing output and emitting the partition
    /// index. Called exactly once, after the last
    /// [`finish_partition`](PartitionWriter::finish_partition).
    fn finish_all(&mut self, metrics: &ShufflePartitionerMetrics)
        -> datafusion::common::Result<()>;
}
