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

use crate::execution::operators::ExecutionError;
use arrow::datatypes::SchemaRef;
use datafusion::common::config::CsvOptions as DFCsvOptions;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::CsvSource;
use datafusion_comet_proto::spark_operator::CsvOptions;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use std::sync::Arc;

pub fn init_csv_datasource_exec(
    object_store_url: ObjectStoreUrl,
    file_groups: Vec<Vec<PartitionedFile>>,
    data_schema: SchemaRef,
    _partition_schema: SchemaRef,
    projection_vector: Vec<usize>,
    csv_options: &CsvOptions,
) -> Result<Arc<DataSourceExec>, ExecutionError> {
    let csv_source = build_csv_source(data_schema, csv_options)?;

    let file_groups = file_groups
        .iter()
        .map(|files| FileGroup::new(files.clone()))
        .collect();

    let file_scan_config = FileScanConfigBuilder::new(object_store_url, csv_source)
        .with_file_groups(file_groups)
        .with_projection_indices(Some(projection_vector))?
        .build();

    Ok(DataSourceExec::from_data_source(file_scan_config))
}

fn build_csv_source(schema: SchemaRef, options: &CsvOptions) -> Result<Arc<CsvSource>> {
    let delimiter = string_to_u8(&options.delimiter, "delimiter")?;
    let quote = string_to_u8(&options.quote, "quote")?;
    let escape = string_to_u8(&options.escape, "escape")?;
    let terminator = string_to_u8(&options.terminator, "terminator")?;
    let comment = options
        .comment
        .as_ref()
        .map(|c| string_to_u8(c, "comment"))
        .transpose()?;

    let df_csv_options = DFCsvOptions {
        has_header: Some(options.has_header),
        delimiter,
        quote,
        escape: Some(escape),
        terminator: Some(terminator),
        comment,
        truncated_rows: Some(options.truncated_rows),
        ..Default::default()
    };

    let csv_source = CsvSource::new(schema).with_csv_options(df_csv_options);
    Ok(Arc::new(csv_source))
}

fn string_to_u8(option: &str, option_name: &str) -> Result<u8> {
    match option.as_bytes().first() {
        Some(&ch) if ch.is_ascii() => Ok(ch),
        _ => Err(DataFusionError::Configuration(format!(
            "invalid {option_name} character '{option}': must be an ASCII character"
        ))),
    }
}
