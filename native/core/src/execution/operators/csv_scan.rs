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

use std::collections::HashMap;
use crate::execution::operators::ExecutionError;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::CsvSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use std::sync::Arc;
use datafusion_comet_proto::spark_operator::CsvOptions;
use crate::execution::spark_config::SparkConfig;

pub fn init_csv_datasource_exec(
    object_store_url: ObjectStoreUrl,
    file_groups: Vec<Vec<PartitionedFile>>,
    data_schema: SchemaRef,
    csv_options: &CsvOptions
) -> Result<Arc<DataSourceExec>, ExecutionError> {
    let csv_source = build_csv_source(csv_options);

    let file_groups = file_groups
        .iter()
        .map(|files| FileGroup::new(files.clone()))
        .collect();

    let file_scan_config: FileScanConfig =
        FileScanConfigBuilder::new(object_store_url, data_schema, csv_source)
            .with_file_groups(file_groups)
            .build();

    Ok(Arc::new(DataSourceExec::new(Arc::new(file_scan_config))))
}

fn build_csv_source(options: &CsvOptions) -> Arc<CsvSource> {
    let csv_source = CsvSource::new(options.has_header, options.delimiter as u8, options.quote as u8)
        .with_terminator(None);

    Arc::new(csv_source)
}
