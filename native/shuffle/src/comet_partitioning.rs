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
use std::sync::Arc;

/// How [`CometPartitioning::RoundRobin`] decides which output partition a row belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoundRobinStrategy {
    /// Hash each row over its leading `max_hash_columns` columns (`0` meaning all of them) and
    /// place it at `pmod(hash, num_partitions)`.
    ///
    /// Placement is a pure function of a row's contents, so a re-executed map task reproduces it
    /// no matter what its input does. The price is a murmur3 pass per row that recurses into
    /// every struct child, plus a per-row gather on flush because adjacent rows scatter across
    /// every partition. It is also not really round robin: identical rows always hash to the same
    /// partition, so low-cardinality input skews where Spark's round robin spreads evenly.
    HashAll { max_hash_columns: usize },

    /// Place rows positionally, in contiguous groups of `group_rows` rows, counting rows across
    /// input batch boundaries: the row at task-global ordinal `i` goes to output partition
    /// `(start_partition + i / group_rows) % num_partitions`.
    ///
    /// This is Spark's own round robin at a coarser granularity — Spark seeds a counter with
    /// `XORShiftRandom(partitionId)` and bumps it per row, which is the `group_rows == 1` case —
    /// and it inherits Spark's determinism condition exactly: placement is reproducible when the
    /// upstream operator replays rows in the same *order*. It deliberately does not depend on how
    /// those rows are framed into batches, because no Spark contract covers framing;
    /// `DeterministicLevel::DETERMINATE` promises the same rows in the same order and says
    /// nothing about how a downstream operator chunks them, so an operator that spills can reframe
    /// under different memory pressure while still honouring it. Keying on a row ordinal rather
    /// than a batch ordinal is what lets this strategy rely on the level Spark already publishes
    /// instead of an assumption nothing checks.
    ///
    /// `start_partition` must be the Spark map partition id. It has to be distinct across mappers,
    /// or every task starts at partition 0 and a task emitting fewer groups than there are output
    /// partitions leaves the tail empty stage-wide; and it has to be a pure function of the map
    /// partition, or a re-executed task does not reproduce its own placement. Spark seeds
    /// `XORShiftRandom(partitionId)` for the same two reasons.
    ///
    /// `group_rows` trades balance against copying. Imbalance between any two output partitions is
    /// bounded by `group_rows` rows regardless of how the reader frames batches, so small groups
    /// balance better; large groups produce fewer, longer runs to copy on flush, and a group as
    /// large as the batch size lets a whole input batch pass through to one partition untouched.
    /// [`Self::AUTO_GROUP_ROWS`] picks a value from the batch size and partition count.
    RowGroups {
        start_partition: usize,
        group_rows: usize,
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

impl RoundRobinStrategy {
    /// `group_rows` sentinel asking for a value derived from the batch size and partition count.
    pub const AUTO_GROUP_ROWS: usize = 0;

    /// Smallest automatically chosen group. A multiple of 8 so that a run starts on a byte
    /// boundary of a validity bitmap, which keeps the per-run copy a memcpy rather than a
    /// bit-shift for every column.
    const MIN_AUTO_GROUP_ROWS: usize = 64;

    /// Resolves [`Self::AUTO_GROUP_ROWS`] against the runtime batch size and partition count.
    ///
    /// One batch spread over `num_partitions` groups is the finest split that still gives every
    /// output partition a run, so `batch_size / num_partitions` balances without fragmenting the
    /// copy any further than it has to. An explicit request is taken as given, including one
    /// larger than a batch, which sends several consecutive input batches to the same partition.
    pub fn resolve_group_rows(
        group_rows: usize,
        batch_size: usize,
        num_partitions: usize,
    ) -> usize {
        let batch_size = batch_size.max(1);
        if group_rows != Self::AUTO_GROUP_ROWS {
            return group_rows;
        }
        (batch_size / num_partitions.max(1))
            .clamp(Self::MIN_AUTO_GROUP_ROWS.min(batch_size), batch_size)
    }
}

/// A contiguous span of rows within one input batch, bound for one output partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PositionalRun {
    pub partition: usize,
    pub start: u32,
    pub len: u32,
}

/// Splits the rows `[row_seq, row_seq + num_rows)` of a task's input into the runs that
/// [`RoundRobinStrategy::RowGroups`] placement produces, appending them to `out` in row order.
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
    out: &mut Vec<PositionalRun>,
) {
    out.clear();
    let group_rows = group_rows.max(1) as u64;
    let num_partitions = num_partitions.max(1) as u64;
    let num_rows = num_rows as u64;
    let mut offset = 0u64;
    while offset < num_rows {
        let global = row_seq + offset;
        // Rows left in the group `global` falls into, so the first run of a batch picks up a
        // group that a previous batch left part-way through.
        let remaining_in_group = group_rows - (global % group_rows);
        let len = remaining_in_group.min(num_rows - offset);
        out.push(PositionalRun {
            partition: ((start_partition as u64 + global / group_rows) % num_partitions) as usize,
            start: offset as u32,
            len: len as u32,
        });
        offset += len;
    }
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

    /// Collects the partition of every row in `[row_seq, row_seq + num_rows)` by expanding the
    /// runs, which is the property the runs are a compressed encoding of.
    fn placement(
        row_seq: u64,
        num_rows: usize,
        group_rows: usize,
        num_partitions: usize,
    ) -> Vec<usize> {
        let mut runs = vec![];
        positional_runs(row_seq, num_rows, 0, group_rows, num_partitions, &mut runs);
        runs.iter()
            .flat_map(|run| std::iter::repeat_n(run.partition, run.len as usize))
            .collect()
    }

    #[test]
    fn positional_runs_cover_every_row_once_in_order() {
        let mut runs = vec![];
        positional_runs(0, 10, 0, 4, 3, &mut runs);
        assert_eq!(
            runs,
            vec![
                PositionalRun {
                    partition: 0,
                    start: 0,
                    len: 4
                },
                PositionalRun {
                    partition: 1,
                    start: 4,
                    len: 4
                },
                PositionalRun {
                    partition: 2,
                    start: 8,
                    len: 2
                },
            ]
        );
    }

    /// The point of counting rows rather than batches: however the reader frames the same rows,
    /// each row lands on the same partition.
    #[test]
    fn positional_placement_is_independent_of_batch_framing() {
        let group_rows = 7;
        let num_partitions = 5;
        let total = 100;

        let whole = placement(0, total, group_rows, num_partitions);

        for framing in [
            vec![100],
            vec![1; 100],
            vec![8; 12].into_iter().chain([4]).collect::<Vec<_>>(),
            vec![64, 36],
            vec![7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 2],
        ] {
            assert_eq!(framing.iter().sum::<usize>(), total, "bad framing fixture");
            let mut row_seq = 0u64;
            let mut refrained = vec![];
            for rows in framing.iter() {
                refrained.extend(placement(row_seq, *rows, group_rows, num_partitions));
                row_seq += *rows as u64;
            }
            assert_eq!(
                refrained, whole,
                "framing {framing:?} placed rows differently"
            );
        }
    }

    /// A group that a previous batch left part-way through is finished by the next batch, rather
    /// than restarting at a group boundary.
    #[test]
    fn positional_runs_resume_a_partial_group() {
        let mut runs = vec![];
        positional_runs(2, 6, 0, 4, 3, &mut runs);
        assert_eq!(
            runs,
            vec![
                // rows 2..4 finish group 0
                PositionalRun {
                    partition: 0,
                    start: 0,
                    len: 2
                },
                PositionalRun {
                    partition: 1,
                    start: 2,
                    len: 4
                },
            ]
        );
    }

    #[test]
    fn positional_runs_wrap_and_offset_by_start_partition() {
        let mut runs = vec![];
        positional_runs(0, 6, 2, 2, 3, &mut runs);
        assert_eq!(
            runs.iter().map(|r| r.partition).collect::<Vec<_>>(),
            vec![2, 0, 1],
            "start_partition offsets the sequence and it wraps at num_partitions"
        );
    }

    #[test]
    fn positional_runs_group_larger_than_batch_yields_one_run() {
        let mut runs = vec![];
        positional_runs(0, 100, 3, 8192, 200, &mut runs);
        assert_eq!(
            runs,
            vec![PositionalRun {
                partition: 3,
                start: 0,
                len: 100
            }]
        );
    }

    #[test]
    fn resolve_group_rows_auto_splits_a_batch_across_partitions() {
        use RoundRobinStrategy as S;
        // One batch spread over the output partitions, floored at the 64-row minimum.
        assert_eq!(S::resolve_group_rows(S::AUTO_GROUP_ROWS, 8192, 16), 512);
        assert_eq!(S::resolve_group_rows(S::AUTO_GROUP_ROWS, 8192, 200), 64);
        assert_eq!(S::resolve_group_rows(S::AUTO_GROUP_ROWS, 8192, 10_000), 64);
        // A batch smaller than the minimum group still resolves to something usable.
        assert_eq!(S::resolve_group_rows(S::AUTO_GROUP_ROWS, 32, 200), 32);
        // An explicit request is taken as given. A group longer than a batch is meaningful: it
        // sends several consecutive input batches to the same output partition.
        assert_eq!(S::resolve_group_rows(1, 8192, 200), 1);
        assert_eq!(S::resolve_group_rows(100_000, 8192, 200), 100_000);
    }
}
