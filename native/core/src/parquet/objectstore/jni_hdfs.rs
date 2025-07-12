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
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use futures::{stream, stream::BoxStream};
use jni::{
    objects::{JClass, JObject, JValue},
    JNIEnv, JavaVM,
};
use object_store::{
    path::Path, Attributes, Error as ObjectStoreError, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use once_cell::sync::OnceCell;

static JVM: OnceCell<JavaVM> = OnceCell::new();

pub fn init_jvm(env: &JNIEnv) {
    let _ = JVM.set(env.get_java_vm().expect("Failed to get JavaVM"));
}

fn get_jni_env<'a>() -> jni::AttachGuard<'a> {
    JVM.get()
        .expect("JVM not initialized")
        .attach_current_thread()
        .expect("Failed to attach thread")
}

mod jni_helpers {
    use super::*;

    pub fn create_jni_hashmap<'local>(
        env: &mut JNIEnv<'local>,
        configs: &HashMap<String, String>,
    ) -> Result<JObject<'local>, ObjectStoreError> {
        let map_class = env.find_class("java/util/HashMap").map_err(jni_error)?;
        let jmap = env.new_object(map_class, "()V", &[]).map_err(jni_error)?;

        for (k, v) in configs {
            let jkey = env.new_string(k).map_err(jni_error)?;
            let jval = env.new_string(v).map_err(jni_error)?;

            env.call_method(
                &jmap,
                "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&jkey), JValue::Object(&jval)],
            )
            .map_err(jni_error)?;
        }

        Ok(jmap)
    }

    pub fn jni_error(e: jni::errors::Error) -> ObjectStoreError {
        ObjectStoreError::Generic {
            store: "jni",
            source: Box::new(e),
        }
    }

    pub fn get_native_class<'local>(
        env: &mut JNIEnv<'local>,
    ) -> Result<JClass<'local>, ObjectStoreError> {
        env.find_class("org/apache/comet/parquet/JniHDFSBridge")
            .map_err(jni_error)
    }
}

/// Retrieves the length (in bytes) of a file through JNI interface.
///
/// This method makes a JNI call to Java to get the size of the file at the specified path,
/// using the provided configuration parameters for the storage backend.
///
/// # Arguments
/// * `path` - The filesystem path or URI of the target file
/// * `configs` - Configuration parameters for the storage backend as key-value pairs.
///
/// # Returns
/// Returns `Ok(usize)` with the file size in bytes on success, or an `ObjectStoreError`
/// if the operation fails.
pub fn get_length(
    path: &str,
    configs: &HashMap<String, String>,
) -> Result<usize, ObjectStoreError> {
    let mut env = get_jni_env();
    let jmap = jni_helpers::create_jni_hashmap(&mut env, configs)?;
    let class = jni_helpers::get_native_class(&mut env)?;
    let jpath = env.new_string(path).map_err(jni_helpers::jni_error)?;

    let result = env
        .call_static_method(
            class,
            "getLength",
            "(Ljava/lang/String;Ljava/util/Map;)J",
            &[JValue::Object(&jpath), JValue::Object(&jmap)],
        )
        .map_err(jni_helpers::jni_error)?
        .j()
        .unwrap_or(-1);

    if result < 0 {
        Err(ObjectStoreError::NotFound {
            path: path.to_string(),
            source: Box::new(Arc::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("File not found or error reading: {path}"),
            ))),
        })
    } else {
        Ok(result as usize)
    }
}

/// Reads a range of bytes from a file through JNI interface.
///
/// # Arguments
/// * `raw_path` - The filesystem path or URI of the file to read
/// * `configs` - Configuration parameters for the read operation as key-value pairs
/// * `offset` - The starting byte position to read from (0-based)
/// * `len` - The number of bytes to read
///
/// # Returns
/// Returns `Ok(Vec<u8>)` containing the requested bytes on success, or an
/// `ObjectStoreError` if the operation fails.
pub fn read(
    path: &str,
    configs: &HashMap<String, String>,
    offset: usize,
    len: usize,
) -> Result<Vec<u8>, ObjectStoreError> {
    let mut env = get_jni_env();
    let jmap = jni_helpers::create_jni_hashmap(&mut env, configs)?;
    let class = jni_helpers::get_native_class(&mut env)?;

    let jpath = env.new_string(path).map_err(jni_helpers::jni_error)?;

    let result = env
        .call_static_method(
            class,
            "read",
            "(Ljava/lang/String;Ljava/util/Map;JI)[B",
            &[
                JValue::Object(&jpath),
                JValue::Object(&jmap),
                JValue::Long(offset as i64),
                JValue::Int(len as i32),
            ],
        )
        .map_err(jni_helpers::jni_error)?;

    let byte_array = jni::objects::JByteArray::from(result.l().map_err(jni_helpers::jni_error)?);

    if byte_array.is_null() {
        return Err(ObjectStoreError::Generic {
            store: "jni",
            source: "Received null byte array from Java".into(),
        });
    }

    let output = env
        .convert_byte_array(byte_array)
        .map_err(jni_helpers::jni_error)?;
    Ok(output)
}

/// A JNI-backed implementation of [`ObjectStore`] for interacting with storage systems
/// through Java Native Interface.
#[derive(Debug, Clone)]
pub struct JniObjectStore {
    base_uri: String,
    configs: HashMap<String, String>,
}

// Mark as thread-safe
unsafe impl Send for JniObjectStore {}
unsafe impl Sync for JniObjectStore {}

impl JniObjectStore {
    /// Creates a new JniObjectStore with the given base URI and configurations.
    pub fn new(base_uri: String, configs: HashMap<String, String>) -> Self {
        Self {
            base_uri: base_uri.trim_end_matches('/').to_string(),
            configs,
        }
    }

    /// Converts a relative path to an absolute URI using the store's base URI.
    fn to_absolute_uri(&self, location: &Path) -> String {
        let path_str = location.to_string();

        // If already absolute-looking (s3a://, hdfs://, etc.), return as is
        if path_str.contains("://") {
            return path_str;
        }

        // Handle absolute-looking paths (start with /)
        let clean_path = path_str.trim_start_matches('/');
        format!("{}/{}", self.base_uri, clean_path)
    }
}

impl std::fmt::Display for JniObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JniObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for JniObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        let path_str = self.to_absolute_uri(location);
        let total_len = get_length(&path_str, &self.configs)? as u64;

        let range = match options.range {
            Some(GetRange::Bounded(range)) => range,
            Some(GetRange::Offset(offset)) => offset..total_len,
            Some(GetRange::Suffix(length)) => {
                let start = total_len.saturating_sub(length);
                start..total_len
            }
            None => 0..total_len,
        };

        if range.end > total_len {
            return Err(ObjectStoreError::NotFound {
                path: path_str.clone(),
                source: format!(
                    "Invalid range {}-{} for file of length {}",
                    range.start, range.end, total_len
                )
                .into(),
            });
        }

        let range_len = (range.end - range.start) as usize;
        let bytes = read(&path_str, &self.configs, range.start as usize, range_len)?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream::once(async move {
                Ok(Bytes::from(bytes))
            }))),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: Utc::now(),
                size: total_len,
                version: None,
                e_tag: None,
            },
            range,
            attributes: Attributes::default(),
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        todo!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload + 'static>, ObjectStoreError> {
        todo!()
    }

    async fn delete(&self, _location: &Path) -> Result<(), ObjectStoreError> {
        todo!()
    }

    fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        todo!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<(), ObjectStoreError> {
        todo!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<(), ObjectStoreError> {
        todo!()
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        let path = location.to_string();
        let len = get_length(&path, &self.configs)? as usize;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: len as u64,
            version: None,
            e_tag: None,
        })
    }
}
