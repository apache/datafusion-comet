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

use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalExpr};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CometPartitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Allocate rows based on lexical order of one of more expressions and the specified number of
    /// partitions
    RangePartitioning(LexOrdering, usize),
    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl CometPartitioning {
    pub fn partition_count(&self) -> usize {
        use CometPartitioning::*;
        match self {
            RoundRobinBatch(n) | Hash(_, n) | UnknownPartitioning(n) | RangePartitioning(_, n) => {
                *n
            }
        }
    }
}

impl From<Partitioning> for CometPartitioning {
    fn from(df_partitioning: Partitioning) -> Self {
        match df_partitioning {
            Partitioning::RoundRobinBatch(partitions) => {
                CometPartitioning::RoundRobinBatch(partitions)
            }
            Partitioning::Hash(exprs, partitions) => CometPartitioning::Hash(exprs, partitions),
            Partitioning::UnknownPartitioning(partitions) => {
                CometPartitioning::UnknownPartitioning(partitions)
            }
        }
    }
}

impl From<CometPartitioning> for Partitioning {
    fn from(val: CometPartitioning) -> Self {
        match val {
            CometPartitioning::RoundRobinBatch(partitions) => {
                Partitioning::RoundRobinBatch(partitions)
            }
            CometPartitioning::Hash(exprs, partitions) => Partitioning::Hash(exprs, partitions),
            CometPartitioning::UnknownPartitioning(partitions) => {
                Partitioning::UnknownPartitioning(partitions)
            }
            CometPartitioning::RangePartitioning(_lex_ordering, usize) => {
                Partitioning::UnknownPartitioning(usize)
            }
        }
    }
}
