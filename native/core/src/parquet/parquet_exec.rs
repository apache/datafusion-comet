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
use crate::jvm_bridge::JVMClasses;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkSchemaAdapterFactory;
use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::common::extensions_options;
use datafusion::config::{EncryptionFactoryOptions, TableParquetOptions};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_comet_spark_expr::EvalMode;
use itertools::Itertools;
use jni::objects::{GlobalRef, JMethodID};
use object_store::path::Path;
use parquet::encryption::decrypt::{FileDecryptionProperties, KeyRetriever};
use parquet::encryption::encrypt::FileEncryptionProperties;
use std::collections::HashMap;
use std::sync::Arc;

/// Initializes a DataSourceExec plan with a ParquetSource. This may be used by either the
/// `native_datafusion` scan or the `native_iceberg_compat` scan.
///
///   `required_schema`: Schema to be projected by the scan.
///
///   `data_schema`: Schema of the underlying data. It is optional and, if provided, is used
/// instead of `required_schema` to initialize the file scan
///
///   `partition_schema` and `partition_fields` are optional. If `partition_schema` is specified,
/// then `partition_fields` must also be specified
///
///   `object_store_url`: Url to read data from
///
///   `file_groups`: A collection of groups of `PartitionedFiles` that are to be read by the scan
///
///   `projection_vector`: A vector of the indexes in the schema of the fields to be projected
///
///   `data_filters`: Any predicate that must be applied to the data returned by the scan. If
/// specified, then `data_schema` must also be specified.
#[allow(clippy::too_many_arguments)]
pub(crate) fn init_datasource_exec(
    required_schema: SchemaRef,
    data_schema: Option<SchemaRef>,
    partition_schema: Option<SchemaRef>,
    partition_fields: Option<Vec<Field>>,
    object_store_url: ObjectStoreUrl,
    file_groups: Vec<Vec<PartitionedFile>>,
    projection_vector: Option<Vec<usize>>,
    data_filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    default_values: Option<HashMap<usize, ScalarValue>>,
    session_timezone: &str,
    case_sensitive: bool,
    session_ctx: &Arc<SessionContext>,
    encryption_enabled: bool,
) -> Result<Arc<DataSourceExec>, ExecutionError> {
    let (table_parquet_options, spark_parquet_options) = get_options(
        session_timezone,
        case_sensitive,
        &object_store_url,
        encryption_enabled,
    );

    let mut parquet_source = ParquetSource::new(table_parquet_options);

    // Create a conjunctive form of the vector because ParquetExecBuilder takes
    // a single expression
    if let Some(data_filters) = data_filters {
        let cnf_data_filters = data_filters.clone().into_iter().reduce(|left, right| {
            Arc::new(BinaryExpr::new(
                left,
                datafusion::logical_expr::Operator::And,
                right,
            ))
        });

        if let Some(filter) = cnf_data_filters {
            parquet_source = parquet_source.with_predicate(filter);
        }
    }

    parquet_source = parquet_source.with_encryption_factory(
        session_ctx
            .runtime_env()
            .parquet_encryption_factory(ENCRYPTION_FACTORY_ID)?,
    );

    let file_source = parquet_source.with_schema_adapter_factory(Arc::new(
        SparkSchemaAdapterFactory::new(spark_parquet_options, default_values),
    ))?;

    let file_groups = file_groups
        .iter()
        .map(|files| FileGroup::new(files.clone()))
        .collect();

    let file_scan_config = match (data_schema, projection_vector, partition_fields) {
        (Some(data_schema), Some(projection_vector), Some(partition_fields)) => {
            get_file_config_builder(
                data_schema,
                partition_schema,
                file_groups,
                object_store_url,
                file_source,
            )
            .with_projection(Some(projection_vector))
            .with_table_partition_cols(partition_fields)
            .build()
        }
        _ => get_file_config_builder(
            required_schema,
            partition_schema,
            file_groups,
            object_store_url,
            file_source,
        )
        .build(),
    };

    Ok(Arc::new(DataSourceExec::new(Arc::new(file_scan_config))))
}

pub const ENCRYPTION_FACTORY_ID: &str = "comet.jni_kms_encryption";

// Options used to configure our example encryption factory
extensions_options! {
    struct CometEncryptionConfig {
        url_base: String, default = "file:///".into()
    }
}
#[derive(Debug)]
pub struct CometEncryptionFactory {
    pub(crate) key_unwrapper: GlobalRef,
}

/// `EncryptionFactory` is a DataFusion trait for types that generate
/// file encryption and decryption properties.
#[async_trait]
impl EncryptionFactory for CometEncryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        _options: &EncryptionFactoryOptions,
        _schema: &SchemaRef,
        _file_path: &Path,
    ) -> Result<Option<FileEncryptionProperties>, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "Comet does not support Parquet encryption yet."
                .parse()
                .unwrap(),
        ))
    }

    /// Generate file decryption properties to use when reading a Parquet file.
    /// Rather than provide the AES keys directly for decryption, we set a `KeyRetriever`
    /// that can determine the keys using the encryption metadata.
    async fn get_file_decryption_properties(
        &self,
        options: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> Result<Option<FileDecryptionProperties>, DataFusionError> {
        let config: CometEncryptionConfig = options.to_extension_options()?;

        let full_path: String = config.url_base + file_path.as_ref();
        let key_retriever = CometKeyRetriever::new(&full_path, self.key_unwrapper.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let decryption_properties =
            FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever)).build()?;
        Ok(Some(decryption_properties))
    }
}

struct CometKeyRetriever {
    file_path: String,
    key_unwrapper: GlobalRef,
    get_key_method_id: JMethodID,
}

impl CometKeyRetriever {
    fn new(file_path: &str, key_unwrapper: GlobalRef) -> Result<Self, ExecutionError> {
        // Get JNI environment
        let mut env = JVMClasses::get_env()?;

        Ok(CometKeyRetriever {
            file_path: file_path.to_string(),
            key_unwrapper,
            get_key_method_id: env
                .get_method_id(
                    "org/apache/comet/parquet/CometFileKeyUnwrapper",
                    "getKey",
                    "(Ljava/lang/String;[B)[B",
                )
                .unwrap(),
        })
    }
}

impl KeyRetriever for CometKeyRetriever {
    /// Get a data encryption key using the metadata stored in the Parquet file.
    fn retrieve_key(&self, key_metadata: &[u8]) -> datafusion::parquet::errors::Result<Vec<u8>> {
        use jni::{objects::JObject, signature::ReturnType};

        // Get JNI environment
        let mut env = JVMClasses::get_env()
            .map_err(|e| datafusion::parquet::errors::ParquetError::General(e.to_string()))?;

        // Get the key unwrapper instance from GlobalRef
        let unwrapper_instance = self.key_unwrapper.as_obj();

        let instance: JObject = unsafe { JObject::from_raw(unwrapper_instance.as_raw()) };

        // Convert file path to JString
        let file_path_jstring = env.new_string(&self.file_path).unwrap();

        // Convert key_metadata to JByteArray
        let key_metadata_array = env.byte_array_from_slice(key_metadata).unwrap();

        // Call instance method FileKeyUnwrapper.getKey(String, byte[]) -> byte[]
        let result = unsafe {
            env.call_method_unchecked(
                instance,
                self.get_key_method_id,
                ReturnType::Array,
                &[
                    jni::objects::JValue::from(&file_path_jstring).as_jni(),
                    jni::objects::JValue::from(&key_metadata_array).as_jni(),
                ],
            )
        };

        let result = result.unwrap();

        // Extract the byte array from the result
        let result_array = result.l().unwrap();

        // Convert JObject to JByteArray and then to Vec<u8>
        let byte_array: jni::objects::JByteArray = result_array.into();

        let result_vec = env.convert_byte_array(&byte_array).unwrap();
        Ok(result_vec)
    }
}

fn get_options(
    session_timezone: &str,
    case_sensitive: bool,
    object_store_url: &ObjectStoreUrl,
    encryption_enabled: bool,
) -> (TableParquetOptions, SparkParquetOptions) {
    let mut table_parquet_options = TableParquetOptions::new();
    table_parquet_options.global.pushdown_filters = true;
    table_parquet_options.global.reorder_filters = true;
    table_parquet_options.global.coerce_int96 = Some("us".to_string());
    let mut spark_parquet_options =
        SparkParquetOptions::new(EvalMode::Legacy, session_timezone, false);
    spark_parquet_options.allow_cast_unsigned_ints = true;
    spark_parquet_options.case_sensitive = case_sensitive;

    if encryption_enabled {
        table_parquet_options.crypto.configure_factory(
            ENCRYPTION_FACTORY_ID,
            &CometEncryptionConfig {
                url_base: object_store_url.to_string(),
            },
        );
    }

    (table_parquet_options, spark_parquet_options)
}

fn get_file_config_builder(
    schema: SchemaRef,
    partition_schema: Option<SchemaRef>,
    file_groups: Vec<FileGroup>,
    object_store_url: ObjectStoreUrl,
    file_source: Arc<dyn FileSource>,
) -> FileScanConfigBuilder {
    match partition_schema {
        Some(partition_schema) => {
            let partition_fields: Vec<Field> = partition_schema
                .fields()
                .iter()
                .map(|field| {
                    Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                })
                .collect_vec();
            FileScanConfigBuilder::new(object_store_url, Arc::clone(&schema), file_source)
                .with_file_groups(file_groups)
                .with_table_partition_cols(partition_fields)
        }
        _ => FileScanConfigBuilder::new(object_store_url, Arc::clone(&schema), file_source)
            .with_file_groups(file_groups),
    }
}
