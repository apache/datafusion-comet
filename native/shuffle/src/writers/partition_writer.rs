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
use datafusion::execution::runtime_env::RuntimeEnv;

#[async_trait::async_trait]
pub(crate) trait PartitionWriter: Send + Sync {
    fn spill<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>;

    fn write<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>;

    fn finish_partition<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>;

    fn finish_all(&mut self, metrics: &ShufflePartitionerMetrics)
        -> datafusion::common::Result<()>;
}
