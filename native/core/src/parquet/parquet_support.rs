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
use arrow::array::{ListArray, MapArray};
use arrow::buffer::NullBuffer;
use arrow::compute::can_cast_types;
use arrow::datatypes::{FieldRef, Fields};
use arrow::{
    array::{
        cast::AsArray, new_null_array, types::Int32Type, types::TimestampMicrosecondType, Array,
        ArrayRef, DictionaryArray, StructArray,
    },
    compute::{cast_with_options, take, CastOptions},
    datatypes::{DataType, TimeUnit},
    util::display::FormatOptions,
};
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::EvalMode;
use object_store::path::Path;
use object_store::{parse_url, ObjectStore};
use std::collections::HashMap;
use std::time::Duration;
use std::{fmt::Debug, hash::Hash, sync::Arc};
use url::Url;

use super::objectstore;

// This file originates from cast.rs. While developing native scan support and implementing
// SparkSchemaAdapter we observed that Spark's type conversion logic on Parquet reads does not
// always align to the CAST expression's logic, so it was duplicated here to adapt its behavior.

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");

static PARQUET_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

/// Spark Parquet type conversion options
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SparkParquetOptions {
    /// Spark evaluation mode
    pub eval_mode: EvalMode,
    /// When cast from/to timezone related types, we need timezone, which will be resolved with
    /// session local timezone by an analyzer in Spark.
    // TODO we should change timezone to Tz to avoid repeated parsing
    pub timezone: String,
    /// Allow casts that are supported but not guaranteed to be 100% compatible
    pub allow_incompat: bool,
    /// Support casting unsigned ints to signed ints (used by Parquet SchemaAdapter)
    pub allow_cast_unsigned_ints: bool,
    /// Whether to always represent decimals using 128 bits. If false, the native reader may represent decimals using 32 or 64 bits, depending on the precision.
    pub use_decimal_128: bool,
    /// Whether to read dates/timestamps that were written in the legacy hybrid Julian + Gregorian calendar as it is. If false, throw exceptions instead. If the spark type is TimestampNTZ, this should be true.
    pub use_legacy_date_timestamp_or_ntz: bool,
    // Whether schema field names are case sensitive
    pub case_sensitive: bool,
}

impl SparkParquetOptions {
    pub fn new(eval_mode: EvalMode, timezone: &str, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: timezone.to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
            use_decimal_128: false,
            use_legacy_date_timestamp_or_ntz: false,
            case_sensitive: false,
        }
    }

    pub fn new_without_timezone(eval_mode: EvalMode, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: "".to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
            use_decimal_128: false,
            use_legacy_date_timestamp_or_ntz: false,
            case_sensitive: false,
        }
    }
}

/// Spark-compatible cast implementation. Defers to DataFusion's cast where that is known
/// to be compatible, and returns an error when a not supported and not DF-compatible cast
/// is requested.
pub fn spark_parquet_convert(
    arg: ColumnarValue,
    data_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(parquet_convert_array(
            array,
            data_type,
            parquet_options,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
            // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
            // here.
            let array = scalar.to_array()?;
            let scalar = ScalarValue::try_from_array(
                &parquet_convert_array(array, data_type, parquet_options)?,
                0,
            )?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

fn parquet_convert_array(
    array: ArrayRef,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ArrayRef> {
    use DataType::*;
    let from_type = array.data_type().clone();

    let array = match &from_type {
        Dictionary(key_type, value_type)
            if key_type.as_ref() == &Int32
                && (value_type.as_ref() == &Utf8 || value_type.as_ref() == &LargeUtf8) =>
        {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected a dictionary array");

            let casted_dictionary = DictionaryArray::<Int32Type>::new(
                dict_array.keys().clone(),
                parquet_convert_array(Arc::clone(dict_array.values()), to_type, parquet_options)?,
            );

            let casted_result = match to_type {
                Dictionary(_, _) => Arc::new(casted_dictionary.clone()),
                _ => take(casted_dictionary.values().as_ref(), dict_array.keys(), None)?,
            };
            return Ok(casted_result);
        }
        _ => array,
    };
    let from_type = array.data_type();

    // Try Comet specific handlers first, then arrow-rs cast if supported,
    // return uncasted data otherwise
    match (from_type, to_type) {
        (Struct(_), Struct(_)) => Ok(parquet_convert_struct_to_struct(
            array.as_struct(),
            from_type,
            to_type,
            parquet_options,
        )?),
        (List(_), List(to_inner_type)) => {
            let list_arr: &ListArray = array.as_list();
            let cast_field = parquet_convert_array(
                Arc::clone(list_arr.values()),
                to_inner_type.data_type(),
                parquet_options,
            )?;

            Ok(Arc::new(ListArray::new(
                Arc::clone(to_inner_type),
                list_arr.offsets().clone(),
                cast_field,
                list_arr.nulls().cloned(),
            )))
        }
        (Timestamp(TimeUnit::Microsecond, None), Timestamp(TimeUnit::Microsecond, Some(tz))) => {
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .reinterpret_cast::<TimestampMicrosecondType>()
                    .with_timezone(Arc::clone(tz)),
            ))
        }
        (Map(_, ordered_from), Map(_, ordered_to)) if ordered_from == ordered_to =>
            parquet_convert_map_to_map(array.as_map(), to_type, parquet_options, *ordered_to)
            ,
        // If Arrow cast supports the cast, delegate the cast to Arrow
        _ if can_cast_types(from_type, to_type) => {
            Ok(cast_with_options(&array, to_type, &PARQUET_OPTIONS)?)
        }
        _ => Ok(array),
    }
}

/// Cast between struct types based on logic in
/// `org.apache.spark.sql.catalyst.expressions.Cast#castStruct`.
fn parquet_convert_struct_to_struct(
    array: &StructArray,
    from_type: &DataType,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            // if dest and target schemas has any column in common
            let mut field_overlap = false;
            // TODO some of this logic may be specific to converting Parquet to Spark
            let mut field_name_to_index_map = HashMap::new();
            for (i, field) in from_fields.iter().enumerate() {
                if parquet_options.case_sensitive {
                    field_name_to_index_map.insert(field.name().clone(), i);
                } else {
                    field_name_to_index_map.insert(field.name().to_lowercase(), i);
                }
            }
            assert_eq!(field_name_to_index_map.len(), from_fields.len());
            let mut cast_fields: Vec<ArrayRef> = Vec::with_capacity(to_fields.len());
            for i in 0..to_fields.len() {
                // Fields in the to_type schema may not exist in the from_type schema
                // i.e. the required schema may have fields that the file does not
                // have
                let key = if parquet_options.case_sensitive {
                    to_fields[i].name().clone()
                } else {
                    to_fields[i].name().to_lowercase()
                };
                if field_name_to_index_map.contains_key(&key) {
                    let from_index = field_name_to_index_map[&key];
                    let cast_field = parquet_convert_array(
                        Arc::clone(array.column(from_index)),
                        to_fields[i].data_type(),
                        parquet_options,
                    )?;
                    cast_fields.push(cast_field);
                    field_overlap = true;
                } else {
                    cast_fields.push(new_null_array(to_fields[i].data_type(), array.len()));
                }
            }

            // If target schema doesn't contain any of the existing fields
            // mark such a column in array as NULL
            let nulls = if field_overlap {
                array.nulls().cloned()
            } else {
                Some(NullBuffer::new_null(array.len()))
            };

            Ok(Arc::new(StructArray::new(
                to_fields.clone(),
                cast_fields,
                nulls,
            )))
        }
        _ => unreachable!(),
    }
}

/// Cast a map type to another map type. The same as arrow-cast except we recursively call our own
/// parquet_convert_array
fn parquet_convert_map_to_map(
    from: &MapArray,
    to_data_type: &DataType,
    parquet_options: &SparkParquetOptions,
    to_ordered: bool,
) -> Result<ArrayRef, DataFusionError> {
    match to_data_type {
        DataType::Map(entries_field, _) => {
            let key_field = key_field(entries_field).ok_or(DataFusionError::Internal(
                "map is missing key field".to_string(),
            ))?;
            let value_field = value_field(entries_field).ok_or(DataFusionError::Internal(
                "map is missing value field".to_string(),
            ))?;

            let key_array = parquet_convert_array(
                Arc::clone(from.keys()),
                key_field.data_type(),
                parquet_options,
            )?;
            let value_array = parquet_convert_array(
                Arc::clone(from.values()),
                value_field.data_type(),
                parquet_options,
            )?;

            Ok(Arc::new(MapArray::new(
                Arc::<arrow::datatypes::Field>::clone(entries_field),
                from.offsets().clone(),
                StructArray::new(
                    Fields::from(vec![key_field, value_field]),
                    vec![key_array, value_array],
                    from.entries().nulls().cloned(),
                ),
                from.nulls().cloned(),
                to_ordered,
            )))
        }
        dt => Err(DataFusionError::Internal(format!(
            "Expected MapType. Got: {dt}"
        ))),
    }
}

/// Gets the key field from the entries of a map.  For all other types returns None.
fn key_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.first().cloned()
    } else {
        None
    }
}

/// Gets the value field from the entries of a map.  For all other types returns None.
fn value_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.get(1).cloned()
    } else {
        None
    }
}

fn is_hdfs_scheme(url: &Url, object_store_configs: &HashMap<String, String>) -> bool {
    const COMET_LIBHDFS_SCHEMES_KEY: &str = "fs.comet.libhdfs.schemes";
    let scheme = url.scheme();
    if let Some(libhdfs_schemes) = object_store_configs.get(COMET_LIBHDFS_SCHEMES_KEY) {
        use itertools::Itertools;
        libhdfs_schemes.split(",").contains(scheme)
    } else {
        scheme == "hdfs"
    }
}

// Mirrors object_store::parse::parse_url for the hdfs object store
#[cfg(feature = "hdfs")]
fn parse_hdfs_url(url: &Url) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    match datafusion_comet_objectstore_hdfs::object_store::hdfs::HadoopFileSystem::new(url.as_ref())
    {
        Some(object_store) => {
            let path = object_store.get_path(url.as_str());
            Ok((Box::new(object_store), path))
        }
        _ => Err(object_store::Error::Generic {
            store: "HadoopFileSystem",
            source: "Could not create hdfs object store".into(),
        }),
    }
}

#[cfg(feature = "hdfs-opendal")]
fn parse_hdfs_url(url: &Url) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    let name_node = get_name_node_uri(url)?;
    let builder = opendal::services::Hdfs::default().name_node(&name_node);

    let op = opendal::Operator::new(builder)
        .map_err(|error| object_store::Error::Generic {
            store: "hdfs-opendal",
            source: error.into(),
        })?
        .finish();
    let store = object_store_opendal::OpendalStore::new(op);
    let path = Path::parse(url.path())?;
    Ok((Box::new(store), path))
}

#[cfg(feature = "hdfs-opendal")]
fn get_name_node_uri(url: &Url) -> Result<String, object_store::Error> {
    use std::fmt::Write;
    if let Some(host) = url.host() {
        let schema = url.scheme();
        let mut uri_builder = String::new();
        write!(&mut uri_builder, "{schema}://{host}").unwrap();

        if let Some(port) = url.port() {
            write!(&mut uri_builder, ":{port}").unwrap();
        }
        Ok(uri_builder)
    } else {
        Err(object_store::Error::InvalidPath {
            source: object_store::path::Error::InvalidPath {
                path: std::path::PathBuf::from(url.as_str()),
            },
        })
    }
}

#[cfg(all(not(feature = "hdfs"), not(feature = "hdfs-opendal")))]
fn parse_hdfs_url(_url: &Url) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    Err(object_store::Error::Generic {
        store: "HadoopFileSystem",
        source: "Hdfs support is not enabled in this build".into(),
    })
}

/// Parses the url, registers the object store with configurations, and returns a tuple of the object store url
/// and object store path
pub(crate) fn prepare_object_store_with_configs(
    runtime_env: Arc<RuntimeEnv>,
    url: String,
    object_store_configs: &HashMap<String, String>,
) -> Result<(ObjectStoreUrl, Path), ExecutionError> {
    let mut url = Url::parse(url.as_str())
        .map_err(|e| ExecutionError::GeneralError(format!("Error parsing URL {url}: {e}")))?;
    let mut scheme = url.scheme();
    if scheme == "s3a" {
        scheme = "s3";
        url.set_scheme("s3").map_err(|_| {
            ExecutionError::GeneralError("Could not convert scheme from s3a to s3".to_string())
        })?;
    }
    let url_key = format!(
        "{}://{}",
        scheme,
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    );

    let (object_store, object_store_path): (Box<dyn ObjectStore>, Path) =
        if is_hdfs_scheme(&url, object_store_configs) {
            parse_hdfs_url(&url)
        } else if scheme == "s3" {
            objectstore::s3::create_store(&url, object_store_configs, Duration::from_secs(300))
        } else {
            parse_url(&url)
        }
        .map_err(|e| ExecutionError::GeneralError(e.to_string()))?;

    let object_store_url = ObjectStoreUrl::parse(url_key.clone())?;
    runtime_env.register_object_store(&url, Arc::from(object_store));
    Ok((object_store_url, object_store_path))
}

#[cfg(test)]
mod tests {
    use crate::execution::operators::ExecutionError;
    use crate::parquet::parquet_support::prepare_object_store_with_configs;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use object_store::path::Path;
    use std::collections::HashMap;
    use std::sync::Arc;
    use url::Url;

    /// Parses the url, registers the object store, and returns a tuple of the object store url and object store path
    pub(crate) fn prepare_object_store(
        runtime_env: Arc<RuntimeEnv>,
        url: String,
    ) -> Result<(ObjectStoreUrl, Path), ExecutionError> {
        prepare_object_store_with_configs(runtime_env, url, &HashMap::new())
    }

    #[cfg(not(feature = "hdfs"))]
    #[test]
    fn test_prepare_object_store() {
        use crate::execution::operators::ExecutionError;

        let local_file_system_url = "file:///comet/spark-warehouse/part-00000.snappy.parquet";
        let hdfs_url = "hdfs://localhost:8020/comet/spark-warehouse/part-00000.snappy.parquet";

        let all_urls = [local_file_system_url, hdfs_url];
        let expected: Vec<Result<(ObjectStoreUrl, Path), ExecutionError>> = vec![
            Ok((
                ObjectStoreUrl::parse("file://").unwrap(),
                Path::from("/comet/spark-warehouse/part-00000.snappy.parquet"),
            )),
            Err(ExecutionError::GeneralError(
                "Generic HadoopFileSystem error: Hdfs support is not enabled in this build"
                    .parse()
                    .unwrap(),
            )),
        ];

        for (i, url_str) in all_urls.iter().enumerate() {
            let url = &Url::parse(url_str).unwrap();
            let res = prepare_object_store(Arc::new(RuntimeEnv::default()), url.to_string());

            let expected = expected.get(i).unwrap();
            match expected {
                Ok((o, p)) => {
                    let (r_o, r_p) = res.unwrap();
                    assert_eq!(r_o, *o);
                    assert_eq!(r_p, *p);
                }
                Err(e) => {
                    assert!(res.is_err());
                    let Err(res_e) = res else {
                        panic!("test failed")
                    };
                    assert_eq!(e.to_string(), res_e.to_string())
                }
            }
        }
    }

    #[test]
    #[cfg(feature = "hdfs")]
    fn test_prepare_object_store() {
        // we use a local file system url instead of an hdfs url because the latter requires
        // a running namenode
        let hdfs_url = "file:///comet/spark-warehouse/part-00000.snappy.parquet";
        let expected: (ObjectStoreUrl, Path) = (
            ObjectStoreUrl::parse("file://").unwrap(),
            Path::from("/comet/spark-warehouse/part-00000.snappy.parquet"),
        );

        let url = &Url::parse(hdfs_url).unwrap();
        let res = prepare_object_store(Arc::new(RuntimeEnv::default()), url.to_string());

        let res = res.unwrap();
        assert_eq!(res.0, expected.0);
        assert_eq!(res.1, expected.1);
    }
}
