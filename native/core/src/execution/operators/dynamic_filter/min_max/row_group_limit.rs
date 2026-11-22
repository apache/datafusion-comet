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
use crate::parquet::file_error_context::ParquetErrorContext;
use crate::parquet::parquet_exec::METADATA_SIZE_HINT;

mod reader;
use reader::{PreflightFileIndex, PreloadedReaderFactory};

// DataFusion 55.2 rechecks the entire unread tail at every row-group boundary,
// even when the predicate has not changed. Until that work is bounded upstream,
// cap each eligible file rather than merely throttling aggregate updates.
pub(super) const MAX_ROW_GROUPS_PER_FILE: usize = 64;

// Retaining metadata must not turn the preflight into an unbounded cache. Stop
// after crossing this budget, retaining that last footer for the ordinary scan.
pub(super) const MAX_PREFLIGHT_METADATA_BYTES: usize = 8 * 1024 * 1024;

pub(super) async fn prepare_scan(
    input: &Arc<dyn ExecutionPlan>,
    filtered_input: &Arc<dyn ExecutionPlan>,
    partition: usize,
) -> (Arc<dyn ExecutionPlan>, bool) {
    let Some(scan) = input.downcast_ref::<DataSourceExec>() else {
        return (Arc::clone(input), false);
    };
    let Some(original) = concrete_parquet_source(scan) else {
        return (Arc::clone(input), false);
    };
    let Some(filtered_scan) = filtered_input.downcast_ref::<DataSourceExec>() else {
        return (Arc::clone(input), false);
    };
    let Some(filtered) = concrete_parquet_source(filtered_scan) else {
        return (Arc::clone(input), false);
    };
    let (config, source, _) = original;
    // Preflight has no decryption properties and cannot reuse a failed encrypted
    // footer load. Native scans advertise their key factory through factory_id;
    // other configured sources can supply file_decryption directly.
    let crypto = &source.table_parquet_options().crypto;
    if crypto.file_decryption.is_some() || crypto.factory_id.is_some() {
        return (Arc::clone(input), false);
    }
    let Some(factory) = source.parquet_file_reader_factory() else {
        return (Arc::clone(input), false);
    };
    let Some(group) = config.file_groups.get(partition) else {
        return (Arc::clone(input), false);
    };
    let mut readers = PreloadedReaderFactory::new(Arc::clone(factory));
    let mut metadata_bytes = 0;
    let mut within_limit = true;
    // Hand successful reads to either scan, including when a later file makes
    // us fall back. Let normal decoding report metadata errors in its original
    // order and context. Keeping each reader also preserves its validation state
    // and avoids starting its next-file prefetch a second time.
    for file in group.files() {
        let Ok(mut reader) = factory.create_reader(
            partition,
            file.clone(),
            file.metadata_size_hint.or(Some(METADATA_SIZE_HINT)),
            source.metrics(),
        ) else {
            within_limit = false;
            break;
        };
        let Ok(metadata) = reader.get_metadata(None).await else {
            within_limit = false;
            break;
        };
        metadata_bytes += metadata.memory_size();
        within_limit = metadata.num_row_groups() <= MAX_ROW_GROUPS_PER_FILE
            && metadata_bytes <= MAX_PREFLIGHT_METADATA_BYTES;
        readers.push(reader, metadata);
        if !within_limit {
            break;
        }
    }
    let (scan, (config, source, wrapped)) = if within_limit {
        (filtered_scan, filtered)
    } else {
        (scan, original)
    };
    let source: Arc<dyn FileSource> = Arc::new(
        source
            .clone()
            .with_parquet_file_reader_factory(Arc::new(readers)),
    );
    let mut config = config.clone();
    let group = &mut config.file_groups[partition];
    for index in 0..group.len() {
        group[index].extensions.insert(PreflightFileIndex(index));
    }
    config.file_source = if wrapped {
        ParquetErrorContext::wrap(source)
    } else {
        source
    };
    (
        Arc::new(scan.clone().with_data_source(Arc::new(config))),
        within_limit,
    )
}
