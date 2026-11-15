// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Array, ArrayRef, BooleanArray, Scalar};
use arrow::compute::kernels::cast::cast;
use arrow::compute::kernels::comparison::contains as arrow_contains;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
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
            let result = contains_array_scalar(haystack_array, needle_scalar)?;
            Ok(ColumnarValue::Array(result))
        }

        // Scalar haystack, array needle - less common
        (ColumnarValue::Scalar(haystack_scalar), ColumnarValue::Array(needle_array)) => {
            let result = contains_scalar_array(haystack_scalar, needle_array)?;
            Ok(ColumnarValue::Array(result))
        }

        // Both scalars - compute single result
        (ColumnarValue::Scalar(haystack_scalar), ColumnarValue::Scalar(needle_scalar)) => {
            let result = contains_scalar_scalar(haystack_scalar, needle_scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

/// Helper to safely extract string reference from `ScalarValue`.
/// Unwraps dictionary scalars recursively.
#[inline]
fn get_string_scalar_value<'a>(scalar: &'a ScalarValue, arg_name: &str) -> Result<&'a str> {
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Ok(s.as_str()),
        ScalarValue::Dictionary(_, inner) => get_string_scalar_value(inner, arg_name),
        _ => exec_err!(
            "contains function requires string type for {}, got {:?}",
            arg_name,
            scalar.data_type()
        ),
    }
}

/// Materialize a scalar into a length-1 array whose type matches `target_type`,
/// so Arrow's CONTAINS kernel accepts the (scalar, array) pair.
/// Cost is O(1): the cast touches a single element.
fn scalar_to_aligned_array(
    scalar: &ScalarValue,
    target_type: &DataType,
    arg_name: &str,
) -> Result<ArrayRef> {
    let _ = get_string_scalar_value(scalar, arg_name)?;
    let array = scalar.to_array()?;
    if array.data_type() == target_type {
        Ok(array)
    } else {
        cast(&array, target_type).map_err(DataFusionError::from)
    }
}

/// Optimized contains for array haystack with scalar needle.
/// Uses Arrow's native scalar handling for better performance.
fn contains_array_scalar(
    haystack_array: &ArrayRef,
    needle_scalar: &ScalarValue,
) -> Result<ArrayRef> {
    // Handle null needle
    if needle_scalar.is_null() {
        return Ok(Arc::new(BooleanArray::new_null(haystack_array.len())));
    }
    let needle_array =
        scalar_to_aligned_array(needle_scalar, haystack_array.data_type(), "needle")?;
    let result = arrow_contains(haystack_array, &Scalar::new(needle_array))?;
    Ok(Arc::new(result))
}

/// Contains for scalar haystack with array needle - less common path.
fn contains_scalar_array(
    haystack_scalar: &ScalarValue,
    needle_array: &ArrayRef,
) -> Result<ArrayRef> {
    // Handle null haystack
    if haystack_scalar.is_null() {
        return Ok(Arc::new(BooleanArray::new_null(needle_array.len())));
    }
    let haystack_array =
        scalar_to_aligned_array(haystack_scalar, needle_array.data_type(), "haystack")?;
    let result = arrow_contains(&Scalar::new(haystack_array), needle_array)?;
    Ok(Arc::new(result))
}

/// Contains for two scalar values.
fn contains_scalar_scalar(
    haystack_scalar: &ScalarValue,
    needle_scalar: &ScalarValue,
) -> Result<ScalarValue> {
    if haystack_scalar.is_null() || needle_scalar.is_null() {
        return Ok(ScalarValue::Boolean(None));
    }

    let haystack_str = get_string_scalar_value(haystack_scalar, "haystack")?;
    let needle_str = get_string_scalar_value(needle_scalar, "needle")?;

    Ok(ScalarValue::Boolean(Some(
        haystack_str.contains(needle_str),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::Int32Type;

    #[test]
    fn test_contains_array_scalar() {
        let haystack = Arc::new(StringArray::from(vec![
            Some("hello world"),
            Some("foo bar"),
            Some("testing"),
            None,
        ])) as ArrayRef;
        let needle = ScalarValue::Utf8(Some("world".to_string()));

        let result = contains_array_scalar(&haystack, &needle).unwrap();
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

        let result = contains_array_scalar(&haystack, &needle).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        // Null needle should produce null results
        assert!(bool_array.is_null(0));
        assert!(bool_array.is_null(1));
    }

    #[test]
    fn test_contains_empty_needle() {
        let haystack = Arc::new(StringArray::from(vec![Some("hello world"), Some("")])) as ArrayRef;
        let needle = ScalarValue::Utf8(Some("".to_string()));

        let result = contains_array_scalar(&haystack, &needle).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        // Empty string is contained in any string
        assert!(bool_array.value(0));
        assert!(bool_array.value(1));
    }

    #[test]
    fn test_contains_scalar_array_null_haystack() {
        let haystack = ScalarValue::Utf8(None);
        let needle = Arc::new(StringArray::from(vec![
            Some("hello world"),
            Some("foo bar"),
        ])) as ArrayRef;

        let result = contains_scalar_array(&haystack, &needle).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        // Null haystack should produce null results for all array elements
        assert!(bool_array.is_null(0));
        assert!(bool_array.is_null(1));
    }

    #[test]
    fn test_spark_contains_dispatcher_scalar_array() {
        let haystack = ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string())));
        let needle =
            ColumnarValue::Array(
                Arc::new(StringArray::from(vec![Some("a"), Some("bc"), Some("d")])) as ArrayRef,
            );

        let result = spark_contains(&haystack, &needle).unwrap();
        let array = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected ColumnarValue::Array"),
        };
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(bool_array.value(0));
        assert!(bool_array.value(1));
        assert!(!bool_array.value(2));
    }

    #[test]
    fn test_contains_scalar_large_utf8() {
        let haystack = ScalarValue::LargeUtf8(Some("abc".to_string()));
        let needle = Arc::new(LargeStringArray::from(vec![
            Some("a"),
            Some("bc"),
            None,
            Some(""),
            Some("d"),
        ])) as ArrayRef;

        let res = contains_scalar_array(&haystack, &needle).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected =
            BooleanArray::from(vec![Some(true), Some(true), None, Some(true), Some(false)]);

        assert_eq!(res, &expected);
    }

    #[test]
    fn test_contains_scalar_utf8_view() {
        let haystack = ScalarValue::Utf8View(Some("abc".to_string()));
        let needle = Arc::new(StringViewArray::from(vec![
            Some("a"),
            Some("bc"),
            None,
            Some(""),
            Some("d"),
        ])) as ArrayRef;

        let res = contains_scalar_array(&haystack, &needle).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected =
            BooleanArray::from(vec![Some(true), Some(true), None, Some(true), Some(false)]);

        assert_eq!(res, &expected);
    }

    #[test]
    fn test_contains_scalar_dictionary() {
        // Regression: a non-null dictionary-string scalar previously worked before
        // the optimization, then started failing at `get_string_scalar_value`.
        let haystack = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("abc".to_string()))),
        );
        let needle = Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
            Some("a"),
            Some("bc"),
            None,
            Some(""),
            Some("d"),
        ])) as ArrayRef;

        let res = contains_scalar_array(&haystack, &needle).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected =
            BooleanArray::from(vec![Some(true), Some(true), None, Some(true), Some(false)]);

        assert_eq!(res, &expected);
    }

    #[test]
    fn test_contains_array_scalar_large_utf8_haystack() {
        // Symmetric case: scalar needle must be aligned to the array's type,
        // so a Utf8 needle works against a LargeUtf8 haystack.
        let haystack = Arc::new(LargeStringArray::from(vec![Some("abc"), Some("xyz")])) as ArrayRef;
        let needle = ScalarValue::Utf8(Some("bc".to_string()));

        let res = contains_array_scalar(&haystack, &needle).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(res, &BooleanArray::from(vec![Some(true), Some(false)]));
    }

    #[test]
    fn test_contains_scalar_array_all_cases() {
        let haystack = ScalarValue::Utf8(Some("hello world".to_string()));
        let needle = Arc::new(StringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("foo"),
            None,
        ])) as ArrayRef;

        let res = contains_scalar_array(&haystack, &needle).unwrap();
        let bool_arr = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(
            bool_arr,
            &BooleanArray::from(vec![Some(true), Some(true), Some(false), None])
        );
    }

    #[test]
    fn test_contains_scalar_array_empty_needle() {
        let haystack = ScalarValue::Utf8(Some("hello world".to_string()));
        let needle = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;

        let res = contains_scalar_array(&haystack, &needle).unwrap();
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn test_contains_scalar_array_invalid_type_error() {
        let haystack = ScalarValue::Int32(Some(123));
        let needle = Arc::new(StringArray::from(vec![Some("1")])) as ArrayRef;

        let err = contains_scalar_array(&haystack, &needle).unwrap_err();
        assert!(
            err.to_string()
                .contains("contains function requires string type for haystack"),
            "unexpected error: {err}"
        );
    }
}
