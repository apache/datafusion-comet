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

use arrow::row::{OwnedRow, RowConverter};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use std::ops::Range;
use std::sync::Arc;

/// How [`CometPartitioning::RoundRobin`] decides which output partition a row belongs to.
///
/// What each strategy trades, and why positional placement is only used where it is, is written
/// up once in the contributor guide's `native_shuffle.md`, under "Round Robin Partitioning".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoundRobinStrategy {
    /// Hash each row over its leading `max_hash_columns` columns (`0` meaning all of them) and
    /// place it at `pmod(hash, num_partitions)`. A pure function of the row, so it reproduces
    /// whatever order a re-executed map task sees its input in.
    HashAll { max_hash_columns: usize },

    /// Place the row at task-global ordinal `i` at
    /// `(start_partition + i / group_rows) % num_partitions`. The ordinal counts rows across
    /// input batch boundaries, so placement does not depend on how the input was framed, but it
    /// does depend on row order: only reproducible when the map task replays its rows in the same
    /// order, which the planner establishes before choosing this.
    RowGroups {
        /// Output partition the task's first group goes to, chosen per map task by
        /// `CometShuffleExchangeExec.positionalStartPartition`.
        start_partition: usize,
        /// Rows per group. Resolved on the driver and frozen with the shuffle dependency, so that a
        /// re-executed task uses the same group size whatever batch size its executor runs with.
        group_rows: usize,
        /// What [`Self::HashAll`] hashes if `create_repartitioner` rules positional placement out
        /// for the schema, so that the fallback honours the configured column cap.
        max_hash_columns: usize,
    },
}

impl Default for RoundRobinStrategy {
    /// Hashing every column, which is what Comet's round robin did before `RowGroups` existed.
    fn default() -> Self {
        Self::HashAll {
            max_hash_columns: 0,
        }
    }
}

/// Splits the rows `[row_seq, row_seq + num_rows)` of a task's input into the runs that
/// [`RoundRobinStrategy::RowGroups`] placement produces, in row order, each as an output
/// partition and the batch-relative rows bound for it.
///
/// `row_seq` is the count of rows the task has already placed, which is what makes the split
/// independent of where batch boundaries happen to fall: a group straddling two input batches
/// comes back as a trailing run of the first and a leading run of the second, and the rows land
/// on the same partition either way.
pub(crate) fn positional_runs(
    row_seq: u64,
    num_rows: usize,
    start_partition: usize,
    group_rows: usize,
    num_partitions: usize,
) -> impl Iterator<Item = (usize, Range<u32>)> {
    let group_rows = group_rows.max(1) as u64;
    let num_partitions = num_partitions.max(1) as u64;
    let num_rows = num_rows as u64;
    let mut offset = 0u64;
    std::iter::from_fn(move || {
        if offset >= num_rows {
            return None;
        }
        let global = row_seq + offset;
        // Rows left in the group `global` falls into, so the first run of a batch picks up a
        // group that a previous batch left part-way through.
        let len = (group_rows - global % group_rows).min(num_rows - offset);
        let partition = (start_partition as u64 + global / group_rows) % num_partitions;
        let rows = offset as u32..(offset + len) as u32;
        offset += len;
        Some((partition as usize, rows))
    })
}

/// Partitioning scheme for distributing rows across shuffle output partitions.
#[derive(Debug, Clone)]
pub enum CometPartitioning {
    SinglePartition,
    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions. Args are 1) the expression to hash on, and 2) the number of partitions.
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Allocate rows based on the lexical order of one of more expressions and the specified number of
    /// partitions. Args are 1) the LexOrdering to use to compare values and split into partitions,
    /// 2) the number of partitions, 3) the RowConverter used to view incoming RecordBatches as Arrow
    /// Rows for comparing to 4) OwnedRows that represent the boundaries of each partition, used with
    /// LexOrdering to bin each value in the RecordBatch to a partition.
    RangePartitioning(LexOrdering, usize, Arc<RowConverter>, Vec<OwnedRow>),
    /// Round robin partitioning. Args are 1) the number of partitions and 2) the strategy that
    /// decides where each row goes. See [`RoundRobinStrategy`] for the trade-offs.
    RoundRobin(usize, RoundRobinStrategy),
}

impl CometPartitioning {
    pub fn partition_count(&self) -> usize {
        use CometPartitioning::*;
        match self {
            SinglePartition => 1,
            Hash(_, n) | RangePartitioning(_, n, _, _) | RoundRobin(n, _) => *n,
        }
    }
}

pub(crate) fn pmod(hash: u32, n: usize) -> usize {
    let hash = hash as i32;
    let n = n as i32;
    let r = hash % n;
    let result = if r < 0 { (r + n) % n } else { r };
    result as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pmod() {
        let i: Vec<u32> = vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb];
        let result = i.into_iter().map(|i| pmod(i, 200)).collect::<Vec<usize>>();

        // expected partition from Spark with n=200
        let expected = vec![69, 5, 193, 171, 115];
        assert_eq!(result, expected);
    }

    /// A group that a previous batch left part-way through is finished by the next batch, rather
    /// than restarting at a group boundary.
    #[test]
    fn positional_runs_resume_a_partial_group() {
        // Rows 2..4 of the task finish group 0; rows 4..8 are group 1.
        assert_eq!(
            positional_runs(2, 6, 0, 4, 3).collect::<Vec<_>>(),
            vec![(0, 0..2), (1, 2..6)]
        );
    }

    #[test]
    fn positional_runs_group_larger_than_batch_yields_one_run() {
        assert_eq!(
            positional_runs(0, 100, 3, 8192, 200).collect::<Vec<_>>(),
            vec![(3, 0..100)]
        );
    }
}
