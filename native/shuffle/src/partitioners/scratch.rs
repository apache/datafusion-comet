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

//! Shared scratch buffers and partition ID computation for shuffle partitioners.

use crate::{comet_partitioning, CometPartitioning};
use arrow::array::{ArrayRef, RecordBatch};
use datafusion::common::DataFusionError;
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::sync::Arc;

/// Reusable scratch buffers for computing row-to-partition assignments.
#[derive(Default)]
pub(crate) struct ScratchSpace {
    /// Hashes for each row in the current batch.
    pub(crate) hashes_buf: Vec<u32>,
    /// Partition ids for each row in the current batch.
    pub(crate) partition_ids: Vec<u32>,
    /// The row indices of the rows in each partition. This array is conceptually divided into
    /// partitions, where each partition contains the row indices of the rows in that partition.
    /// The length of this array is the same as the number of rows in the batch.
    pub(crate) partition_row_indices: Vec<u32>,
    /// The start indices of partitions in partition_row_indices. partition_starts[K] and
    /// partition_starts[K + 1] are the start and end indices of partition K in partition_row_indices.
    /// The length of this array is 1 + the number of partitions.
    pub(crate) partition_starts: Vec<u32>,
}

impl ScratchSpace {
    /// Create a new ScratchSpace with pre-allocated buffers for the given partitioning scheme.
    pub(crate) fn new(
        partitioning: &CometPartitioning,
        batch_size: usize,
        num_output_partitions: usize,
    ) -> Self {
        Self {
            hashes_buf: match partitioning {
                CometPartitioning::Hash(_, _) | CometPartitioning::RoundRobin(_, _) => {
                    vec![0; batch_size]
                }
                _ => vec![],
            },
            partition_ids: vec![0; batch_size],
            partition_row_indices: vec![0; batch_size],
            partition_starts: vec![0; num_output_partitions + 1],
        }
    }

    pub(crate) fn map_partition_ids_to_starts_and_indices(
        &mut self,
        num_output_partitions: usize,
        num_rows: usize,
    ) {
        let partition_ids = &mut self.partition_ids[..num_rows];

        // count each partition size, while leaving the last extra element as 0
        let partition_counters = &mut self.partition_starts;
        partition_counters.resize(num_output_partitions + 1, 0);
        partition_counters.fill(0);
        partition_ids
            .iter()
            .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

        // accumulate partition counters into partition ends
        // e.g. partition counter: [1, 3, 2, 1, 0] => [1, 4, 6, 7, 7]
        let partition_ends = partition_counters;
        let mut accum = 0;
        partition_ends.iter_mut().for_each(|v| {
            *v += accum;
            accum = *v;
        });

        // calculate partition row indices and partition starts
        // e.g. partition ids: [3, 1, 1, 1, 2, 2, 0] will produce the following partition_row_indices
        // and partition_starts arrays:
        //
        //  partition_row_indices: [6, 1, 2, 3, 4, 5, 0]
        //  partition_starts: [0, 1, 4, 6, 7]
        //
        // partition_starts conceptually splits partition_row_indices into smaller slices.
        // Each slice partition_row_indices[partition_starts[K]..partition_starts[K + 1]] contains the
        // row indices of the input batch that are partitioned into partition K. For example,
        // first partition 0 has one row index [6], partition 1 has row indices [1, 2, 3], etc.
        let partition_row_indices = &mut self.partition_row_indices;
        partition_row_indices.resize(num_rows, 0);
        for (index, partition_id) in partition_ids.iter().enumerate().rev() {
            partition_ends[*partition_id as usize] -= 1;
            let end = partition_ends[*partition_id as usize];
            partition_row_indices[end as usize] = index as u32;
        }

        // after calculating, partition ends become partition starts
    }

    /// Compute partition IDs for the given batch and populate `partition_starts` and
    /// `partition_row_indices`. Returns the number of rows processed.
    pub(crate) fn compute_partition_ids(
        &mut self,
        partitioning: &CometPartitioning,
        input: &RecordBatch,
    ) -> datafusion::common::Result<()> {
        match partitioning {
            CometPartitioning::Hash(exprs, num_partitions) => {
                let arrays = exprs
                    .iter()
                    .map(|expr| expr.evaluate(input)?.into_array(input.num_rows()))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                let num_rows = arrays[0].len();
                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                let partition_ids = &mut self.partition_ids[..num_rows];
                create_murmur3_hashes(&arrays, hashes_buf)?
                    .iter()
                    .enumerate()
                    .for_each(|(idx, hash)| {
                        partition_ids[idx] =
                            comet_partitioning::pmod(*hash, *num_partitions) as u32;
                    });
                self.map_partition_ids_to_starts_and_indices(*num_partitions, num_rows);
            }
            CometPartitioning::RangePartitioning(
                lex_ordering,
                num_partitions,
                row_converter,
                bounds,
            ) => {
                let arrays = lex_ordering
                    .iter()
                    .map(|expr| expr.expr.evaluate(input)?.into_array(input.num_rows()))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                let num_rows = arrays[0].len();
                let row_batch = row_converter.convert_columns(arrays.as_slice())?;
                let partition_ids = &mut self.partition_ids[..num_rows];
                row_batch.iter().enumerate().for_each(|(row_idx, row)| {
                    partition_ids[row_idx] = bounds
                        .as_slice()
                        .partition_point(|bound| bound.row() <= row)
                        as u32
                });
                self.map_partition_ids_to_starts_and_indices(*num_partitions, num_rows);
            }
            CometPartitioning::RoundRobin(num_partitions, max_hash_columns) => {
                let num_rows = input.num_rows();
                let num_columns_to_hash = if *max_hash_columns == 0 {
                    input.num_columns()
                } else {
                    (*max_hash_columns).min(input.num_columns())
                };
                let columns_to_hash: Vec<ArrayRef> = (0..num_columns_to_hash)
                    .map(|i| Arc::clone(input.column(i)))
                    .collect();
                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&columns_to_hash, hashes_buf)?;
                let partition_ids = &mut self.partition_ids[..num_rows];
                hashes_buf.iter().enumerate().for_each(|(idx, hash)| {
                    partition_ids[idx] = comet_partitioning::pmod(*hash, *num_partitions) as u32;
                });
                self.map_partition_ids_to_starts_and_indices(*num_partitions, num_rows);
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported shuffle partitioning scheme {other:?}"
                )));
            }
        }
        Ok(())
    }
}
