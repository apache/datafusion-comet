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

//! Optimized `contains` string function for Spark compatibility.
//!
//! Optimized for scalar pattern case by passing scalar directly to arrow_contains
//! instead of expanding to arrays like DataFusion's built-in contains.

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::compute::kernels::comparison::contains as arrow_contains;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-optimized contains function.
/// Returns true if the first string argument contains the second string argument.
/// Optimized for scalar pattern constants.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkContains {
    signature: Signature,
}

impl Default for SparkContains {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkContains {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkContains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("contains function requires exactly 2 arguments");
        }
        spark_contains(&args.args[0], &args.args[1])
    }
}

/// Execute contains function with optimized scalar pattern handling.
fn spark_contains(haystack: &ColumnarValue, needle: &ColumnarValue) -> Result<ColumnarValue> {
    match (haystack, needle) {
        // Both arrays - use arrow's contains directly
        (ColumnarValue::Array(haystack_array), ColumnarValue::Array(needle_array)) => {
            let result = arrow_contains(haystack_array, needle_array)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }

        // Array haystack, scalar needle - OPTIMIZED PATH
        (ColumnarValue::Array(haystack_array), ColumnarValue::Scalar(needle_scalar)) => {
            let result = contains_with_arrow_scalar(haystack_array, needle_scalar)?;
            Ok(ColumnarValue::Array(result))
        }

        // Scalar haystack, array needle - less common
        (ColumnarValue::Scalar(haystack_scalar), ColumnarValue::Array(needle_array)) => {
            let haystack_array = haystack_scalar.to_array_of_size(needle_array.len())?;
            let result = arrow_contains(&haystack_array, needle_array)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }

        // Both scalars - compute single result
        (ColumnarValue::Scalar(haystack_scalar), ColumnarValue::Scalar(needle_scalar)) => {
            let result = contains_scalar_scalar(haystack_scalar, needle_scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

/// Optimized contains for array haystack with scalar needle.
/// Uses Arrow's native scalar handling for better performance.
fn contains_with_arrow_scalar(
    haystack_array: &ArrayRef,
    needle_scalar: &ScalarValue,
) -> Result<ArrayRef> {
    // Handle null needle
    if needle_scalar.is_null() {
        return Ok(Arc::new(BooleanArray::new_null(haystack_array.len())));
    }

    // Extract the needle string
    let needle_str = match needle_scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.clone(),
        _ => {
            return exec_err!(
                "contains function requires string type for needle, got {:?}",
                needle_scalar.data_type()
            )
        }
    };

    // Create scalar array for needle - tells Arrow to use optimized paths
    let needle_scalar_array = StringArray::new_scalar(needle_str);

    // Use Arrow's contains which detects scalar case and uses optimized paths
    let result = arrow_contains(haystack_array, &needle_scalar_array)?;
    Ok(Arc::new(result))
}

/// Contains for two scalar values.
fn contains_scalar_scalar(
    haystack_scalar: &ScalarValue,
    needle_scalar: &ScalarValue,
) -> Result<ScalarValue> {
    // Handle nulls
    if haystack_scalar.is_null() || needle_scalar.is_null() {
        return Ok(ScalarValue::Boolean(None));
    }

    let haystack_str = match haystack_scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.as_str(),
        _ => {
            return exec_err!(
                "contains function requires string type for haystack, got {:?}",
                haystack_scalar.data_type()
            )
        }
    };

    let needle_str = match needle_scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.as_str(),
        _ => {
            return exec_err!(
                "contains function requires string type for needle, got {:?}",
                needle_scalar.data_type()
            )
        }
    };

    Ok(ScalarValue::Boolean(Some(
        haystack_str.contains(needle_str),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_contains_array_scalar() {
        let haystack = Arc::new(StringArray::from(vec![
            Some("hello world"),
            Some("foo bar"),
            Some("testing"),
            None,
        ])) as ArrayRef;
        let needle = ScalarValue::Utf8(Some("world".to_string()));

        let result = contains_with_arrow_scalar(&haystack, &needle).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(bool_array.value(0)); // "hello world" contains "world"
        assert!(!bool_array.value(1)); // "foo bar" does not contain "world"
        assert!(!bool_array.value(2)); // "testing" does not contain "world"
        assert!(bool_array.is_null(3)); // null input => null output
    }

    #[test]
    fn test_contains_scalar_scalar() {
        let haystack = ScalarValue::Utf8(Some("hello world".to_string()));
        let needle = ScalarValue::Utf8(Some("world".to_string()));

        let result = contains_scalar_scalar(&haystack, &needle).unwrap();
        assert_eq!(result, ScalarValue::Boolean(Some(true)));

        let needle_not_found = ScalarValue::Utf8(Some("xyz".to_string()));
        let result = contains_scalar_scalar(&haystack, &needle_not_found).unwrap();
        assert_eq!(result, ScalarValue::Boolean(Some(false)));
    }

    #[test]
    fn test_contains_null_needle() {
        let haystack = Arc::new(StringArray::from(vec![
            Some("hello world"),
            Some("foo bar"),
        ])) as ArrayRef;
        let needle = ScalarValue::Utf8(None);

        let result = contains_with_arrow_scalar(&haystack, &needle).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        // Null needle should produce null results
        assert!(bool_array.is_null(0));
        assert!(bool_array.is_null(1));
    }

    #[test]
    fn test_contains_empty_needle() {
        let haystack = Arc::new(StringArray::from(vec![Some("hello world"), Some("")])) as ArrayRef;
        let needle = ScalarValue::Utf8(Some("".to_string()));

        let result = contains_with_arrow_scalar(&haystack, &needle).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        // Empty string is contained in any string
        assert!(bool_array.value(0));
        assert!(bool_array.value(1));
    }
}
