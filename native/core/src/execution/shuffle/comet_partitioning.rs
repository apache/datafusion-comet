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
    /// Round robin partitioning. Distributes rows across partitions by sorting them by hash
    /// (computed from columns) and then assigning partitions sequentially. Args are:
    /// 1) number of partitions, 2) max columns to hash (0 means no limit).
    RoundRobin(usize, usize),
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
