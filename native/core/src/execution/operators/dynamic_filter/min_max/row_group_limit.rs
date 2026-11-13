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

//! Limit the work performed by DataFusion's live row-group pruner.

use std::sync::Arc;

use datafusion::datasource::physical_plan::FileSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;

use super::super::parquet_reader::concrete_parquet_source;
use crate::parquet::parquet_exec::METADATA_SIZE_HINT;

// DataFusion 55.2 rechecks the entire unread tail at every row-group boundary,
// even when the predicate has not changed. Until that work is bounded upstream,
// cap each eligible file rather than merely throttling aggregate updates.
pub(super) const MAX_ROW_GROUPS_PER_FILE: usize = 64;

pub(super) async fn within_row_group_limit(
    input: &Arc<dyn ExecutionPlan>,
    partition: usize,
) -> bool {
    let Some(scan) = input.downcast_ref::<DataSourceExec>() else {
        return false;
    };
    let Some((config, source, _)) = concrete_parquet_source(scan) else {
        return false;
    };
    let Some(factory) = source.parquet_file_reader_factory() else {
        return false;
    };
    let Some(group) = config.file_groups.get(partition) else {
        return false;
    };
    // Use the native reader's footer cache where available. If any file is
    // oversized, keep ordinary aggregation for this execution. Let normal
    // decoding report metadata errors in its original order and context.
    for file in group.files() {
        let Ok(mut reader) = factory.create_reader(
            partition,
            file.clone(),
            file.metadata_size_hint.or(Some(METADATA_SIZE_HINT)),
            source.metrics(),
        ) else {
            return false;
        };
        match reader.get_metadata(None).await {
            Ok(metadata) if metadata.num_row_groups() <= MAX_ROW_GROUPS_PER_FILE => {}
            _ => return false,
        }
    }
    true
}
