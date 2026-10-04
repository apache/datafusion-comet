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

//! Operators

pub use crate::errors::ExecutionError;

pub use iceberg_scan::*;
pub use scan::*;

mod dynamic_filter;
pub(crate) use dynamic_filter::{DynamicFilterJoinExec, TopKReaderFilterExec};
pub(crate) mod iceberg_common;
pub use iceberg_common::clear_file_io_cache;
pub(crate) mod iceberg_location_scoped;
mod iceberg_partition_path;
mod iceberg_partition_value;
mod iceberg_scan;
mod iceberg_write;
pub use iceberg_write::IcebergWriteExec;
mod parquet_writer;
pub use parquet_writer::{ParquetCompression, ParquetWriterExec};
mod csv_scan;
pub mod projection;
mod scan;
mod shuffle_scan;
pub use csv_scan::init_csv_datasource_exec;
pub use shuffle_scan::ShuffleScanExec;
