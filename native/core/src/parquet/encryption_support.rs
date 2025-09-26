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
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::extensions_options;
use datafusion::config::EncryptionFactoryOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use jni::objects::{GlobalRef, JMethodID};
use object_store::path::Path;
use parquet::encryption::decrypt::{FileDecryptionProperties, KeyRetriever};
use parquet::encryption::encrypt::FileEncryptionProperties;
use std::sync::Arc;

pub const ENCRYPTION_FACTORY_ID: &str = "comet.jni_kms_encryption";

// Options used to configure our example encryption factory
extensions_options! {
    pub struct CometEncryptionConfig {
        pub url_base: String, default = "file:///".into()
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

pub struct CometKeyRetriever {
    file_path: String,
    key_unwrapper: GlobalRef,
    get_key_method_id: JMethodID,
}

impl CometKeyRetriever {
    pub fn new(file_path: &str, key_unwrapper: GlobalRef) -> Result<Self, ExecutionError> {
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
