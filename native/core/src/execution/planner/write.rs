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

//! Builders for native write operators.

use std::{collections::HashMap, sync::Arc};

use datafusion_comet_proto::spark_operator::{CompressionCodec as SparkCompressionCodec, Operator};
use jni::objects::{Global, JObject};

use super::{
    convert_spark_types_to_arrow_schema, operator_registry::OperatorBuilder, PhysicalPlanner,
    PlanCreationResult,
};
use crate::{
    execution::{
        operators::{ExecutionError, IcebergWriteExec, ParquetCompression, ParquetWriterExec},
        spark_plan::SparkPlan,
    },
    extract_op,
};

/// Builder for native Iceberg writes.
pub struct IcebergWriteBuilder;

impl OperatorBuilder for IcebergWriteBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let iceberg_write = extract_op!(spark_plan, IcebergWrite);
        let children = &spark_plan.children;

        assert_eq!(children.len(), 1);
        let (scans, shuffle_scans, child) =
            planner.create_plan(&children[0], inputs, partition_count)?;
        let exec = Arc::new(IcebergWriteExec::try_new(
            Arc::clone(&child.native_plan),
            iceberg_write.clone(),
        )?);

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                exec,
                vec![Arc::clone(&child)],
            )),
        ))
    }
}

/// Builder for native Parquet writes.
pub struct ParquetWriterBuilder;

impl OperatorBuilder for ParquetWriterBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let writer = extract_op!(spark_plan, ParquetWriter);
        let children = &spark_plan.children;

        assert_eq!(children.len(), 1);
        let (scans, shuffle_scans, child) =
            planner.create_plan(&children[0], inputs, partition_count)?;

        let codec = match writer.compression.try_into() {
            Ok(SparkCompressionCodec::None) => Ok(ParquetCompression::None),
            Ok(SparkCompressionCodec::Snappy) => Ok(ParquetCompression::Snappy),
            Ok(SparkCompressionCodec::Zstd) => Ok(ParquetCompression::Zstd(3)),
            Ok(SparkCompressionCodec::Lz4) => Ok(ParquetCompression::Lz4),
            Ok(SparkCompressionCodec::Gzip) => Ok(ParquetCompression::Gzip),
            _ => Err(ExecutionError::GeneralError(format!(
                "Unsupported parquet compression codec: {:?}",
                writer.compression
            ))),
        }?;

        let object_store_options: HashMap<String, String> = writer
            .object_store_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let parquet_writer = Arc::new(ParquetWriterExec::try_new(
            Arc::clone(&child.native_plan),
            writer.output_path.clone(),
            writer.work_dir.clone(),
            writer.job_id.clone(),
            writer.task_attempt_id,
            codec,
            planner.partition(),
            writer.column_names.clone(),
            (!writer.output_schema.is_empty())
                .then(|| convert_spark_types_to_arrow_schema(&writer.output_schema)),
            object_store_options,
        )?);

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                parquet_writer,
                vec![Arc::clone(&child)],
            )),
        ))
    }
}
