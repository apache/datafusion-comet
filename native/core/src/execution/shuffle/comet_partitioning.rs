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
    /// partitions
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Allocate rows based on the lexical order of one of more expressions and the specified number of
    /// partitions
    RangePartitioning(LexOrdering, usize, Arc<RowConverter>, Vec<OwnedRow>),
}

impl CometPartitioning {
    pub fn partition_count(&self) -> usize {
        use CometPartitioning::*;
        match self {
            SinglePartition => 1,
            Hash(_, n) | RangePartitioning(_, n, _, _) => *n,
        }
    }
}
