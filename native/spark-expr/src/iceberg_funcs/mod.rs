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

//! Native implementations of Iceberg's Spark system functions: `bucket`, `truncate`, `years`,
//! `months`, `days`, and `hours`.
//!
//! Spark binds these as `StaticInvoke` calls on the classes under
//! `org.apache.iceberg.spark.functions`, and they show up wherever hidden partitioning does:
//! the hash distribution and local sort in front of a partitioned Iceberg write, and row-level
//! filters, projections, and sort orders that mention a partition transform. The kernels here
//! reproduce Iceberg's Java implementations exactly (see the partition transforms section of the
//! Iceberg table spec), so a row lands in the same bucket or day whether Comet or Iceberg computes
//! it.

mod bucket;
mod temporal;
mod truncate;

pub use bucket::SparkIcebergBucket;
pub use temporal::SparkIcebergTemporalTransform;
pub use truncate::SparkIcebergTruncate;

use arrow::array::{Array, ArrayRef};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

/// Unpacks a dictionary-encoded array to its value type so that the kernels only ever see plain
/// arrays. Any other array is returned unchanged.
fn unpack_dictionary(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => Ok(cast(&array, value_type)?),
        _ => Ok(array),
    }
}

/// The type a kernel sees for an input of type `data_type`, after dictionary unpacking.
fn unpacked_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => value_type.as_ref().clone(),
        other => other.clone(),
    }
}

/// Applies an array kernel to a `ColumnarValue`, round-tripping a scalar through a one-row array.
fn apply_unary(
    value: &ColumnarValue,
    kernel: impl Fn(&ArrayRef) -> Result<ArrayRef>,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            let array = unpack_dictionary(Arc::clone(array))?;
            Ok(ColumnarValue::Array(kernel(&array)?))
        }
        ColumnarValue::Scalar(scalar) => {
            let array = unpack_dictionary(scalar.to_array()?)?;
            let result = kernel(&array)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        }
    }
}

/// Reads the `numBuckets` / `width` parameter. The Comet serde only converts these functions when
/// the parameter is a positive integer literal, so anything else here is a wiring bug.
fn positive_int_param(fn_name: &str, param: &str, value: &ColumnarValue) -> Result<i32> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(n))) if *n > 0 => Ok(*n),
        other => Err(DataFusionError::Execution(format!(
            "{fn_name}: {param} must be a positive Int32 literal, got {other:?}"
        ))),
    }
}

fn unsupported_type(fn_name: &str, data_type: &DataType) -> DataFusionError {
    DataFusionError::Execution(format!(
        "{fn_name} does not support input type {data_type:?}"
    ))
}

#[cfg(test)]
mod test_util {
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::Result;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    /// Invokes `udf` on `args` and returns the resulting array (scalars are widened).
    pub(super) fn invoke(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>) -> Result<ArrayRef> {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, a)| Arc::new(Field::new(format!("arg{i}"), a.data_type(), true)))
            .collect::<Vec<_>>();
        let arg_types = arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<DataType>>();
        let return_type = udf.return_type(&arg_types)?;
        let number_rows = args
            .iter()
            .find_map(|a| match a {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new(udf.name(), return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        result.to_array(number_rows)
    }
}
