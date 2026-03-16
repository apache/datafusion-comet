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

use arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use std::sync::Arc;

#[derive(thiserror::Error, Debug, Clone)]
pub enum SparkError {
    // This list was generated from the Spark code. Many of the exceptions are not yet used by Comet
    #[error("[CAST_INVALID_INPUT] The value '{value}' of the type \"{from_type}\" cannot be cast to \"{to_type}\" \
        because it is malformed. Correct the value as per the syntax, or change its target type. \
        Use `try_cast` to tolerate malformed input and return NULL instead. If necessary \
        set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    CastInvalidValue {
        value: String,
        from_type: String,
        to_type: String,
    },

    #[error("[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] {value} cannot be represented as Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error, and return NULL instead.")]
    NumericValueOutOfRange {
        value: String,
        precision: u8,
        scale: i8,
    },

    #[error("[NUMERIC_OUT_OF_SUPPORTED_RANGE] The value {value} cannot be interpreted as a numeric since it has more than 38 digits.")]
    NumericOutOfRange { value: String },

    #[error("[CAST_OVERFLOW] The value {value} of the type \"{from_type}\" cannot be cast to \"{to_type}\" \
        due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. If necessary \
        set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    CastOverFlow {
        value: String,
        from_type: String,
        to_type: String,
    },

    #[error("[CANNOT_PARSE_DECIMAL] Cannot parse decimal.")]
    CannotParseDecimal,

    #[error("[ARITHMETIC_OVERFLOW] {from_type} overflow. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    ArithmeticOverflow { from_type: String },

    #[error("[ARITHMETIC_OVERFLOW] Overflow in integral divide. Use `try_divide` to tolerate overflow and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    IntegralDivideOverflow,

    #[error("[ARITHMETIC_OVERFLOW] Overflow in sum of decimals. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    DecimalSumOverflow,

    #[error("[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    DivideByZero,

    #[error("[REMAINDER_BY_ZERO] Division by zero. Use `try_remainder` to tolerate divisor being 0 and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    RemainderByZero,

    #[error("[INTERVAL_DIVIDED_BY_ZERO] Divide by zero in interval arithmetic.")]
    IntervalDividedByZero,

    #[error("[BINARY_ARITHMETIC_OVERFLOW] {value1} {symbol} {value2} caused overflow. Use `{function_name}` to tolerate overflow and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    BinaryArithmeticOverflow {
        value1: String,
        symbol: String,
        value2: String,
        function_name: String,
    },

    #[error("[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION] Interval arithmetic overflow. Use `{function_name}` to tolerate overflow and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    IntervalArithmeticOverflowWithSuggestion { function_name: String },

    #[error("[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Interval arithmetic overflow. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    IntervalArithmeticOverflowWithoutSuggestion,

    #[error("[DATETIME_OVERFLOW] Datetime arithmetic overflow.")]
    DatetimeOverflow,

    #[error("[INVALID_ARRAY_INDEX] The index {index_value} is out of bounds. The array has {array_size} elements. Use the SQL function get() to tolerate accessing element at invalid index and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    InvalidArrayIndex { index_value: i32, array_size: i32 },

    #[error("[INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index {index_value} is out of bounds. The array has {array_size} elements. Use try_element_at to tolerate accessing element at invalid index and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    InvalidElementAtIndex { index_value: i32, array_size: i32 },

    #[error("[INVALID_BITMAP_POSITION] The bit position {bit_position} is out of bounds. The bitmap has {bitmap_num_bytes} bytes ({bitmap_num_bits} bits).")]
    InvalidBitmapPosition {
        bit_position: i64,
        bitmap_num_bytes: i64,
        bitmap_num_bits: i64,
    },

    #[error("[INVALID_INDEX_OF_ZERO] The index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1).")]
    InvalidIndexOfZero,

    #[error("[DUPLICATED_MAP_KEY] Cannot create map with duplicate keys: {key}.")]
    DuplicatedMapKey { key: String },

    #[error("[NULL_MAP_KEY] Cannot use null as map key.")]
    NullMapKey,

    #[error("[MAP_KEY_VALUE_DIFF_SIZES] The key array and value array of a map must have the same length.")]
    MapKeyValueDiffSizes,

    #[error("[EXCEED_LIMIT_LENGTH] Cannot create a map with {size} elements which exceeds the limit {max_size}.")]
    ExceedMapSizeLimit { size: i32, max_size: i32 },

    #[error("[COLLECTION_SIZE_LIMIT_EXCEEDED] Cannot create array with {num_elements} elements which exceeds the limit {max_elements}.")]
    CollectionSizeLimitExceeded {
        num_elements: i64,
        max_elements: i64,
    },

    #[error("[NOT_NULL_ASSERT_VIOLATION] The field `{field_name}` cannot be null.")]
    NotNullAssertViolation { field_name: String },

    #[error("[VALUE_IS_NULL] The value of field `{field_name}` at row {row_index} is null.")]
    ValueIsNull { field_name: String, row_index: i32 },

    #[error("[CANNOT_PARSE_TIMESTAMP] Cannot parse timestamp: {message}. Try using `{suggested_func}` instead.")]
    CannotParseTimestamp {
        message: String,
        suggested_func: String,
    },

    #[error("[INVALID_FRACTION_OF_SECOND] The fraction of second {value} is invalid. Valid values are in the range [0, 60]. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.")]
    InvalidFractionOfSecond { value: f64 },

    #[error("[INVALID_UTF8_STRING] Invalid UTF-8 string: {hex_string}.")]
    InvalidUtf8String { hex_string: String },

    #[error("[UNEXPECTED_POSITIVE_VALUE] The {parameter_name} parameter must be less than or equal to 0. The actual value is {actual_value}.")]
    UnexpectedPositiveValue {
        parameter_name: String,
        actual_value: i32,
    },

    #[error("[UNEXPECTED_NEGATIVE_VALUE] The {parameter_name} parameter must be greater than or equal to 0. The actual value is {actual_value}.")]
    UnexpectedNegativeValue {
        parameter_name: String,
        actual_value: i32,
    },

    #[error("[INVALID_PARAMETER_VALUE] Invalid regex group index {group_index} in function `{function_name}`. Group count is {group_count}.")]
    InvalidRegexGroupIndex {
        function_name: String,
        group_count: i32,
        group_index: i32,
    },

    #[error("[DATATYPE_CANNOT_ORDER] Cannot order by type: {data_type}.")]
    DatatypeCannotOrder { data_type: String },

    #[error("[SCALAR_SUBQUERY_TOO_MANY_ROWS] Scalar subquery returned more than one row.")]
    ScalarSubqueryTooManyRows,

    #[error("ArrowError: {0}.")]
    Arrow(Arc<ArrowError>),

    #[error("InternalError: {0}.")]
    Internal(String),
}

impl SparkError {
    /// Serialize this error to JSON format for JNI transfer
    pub fn to_json(&self) -> String {
        let error_class = self.error_class().unwrap_or("");

        // Create a JSON structure with errorType, errorClass, and params
        match serde_json::to_string(&serde_json::json!({
            "errorType": self.error_type_name(),
            "errorClass": error_class,
            "params": self.params_as_json(),
        })) {
            Ok(json) => json,
            Err(e) => {
                // Fallback if serialization fails
                format!(
                    "{{\"errorType\":\"SerializationError\",\"message\":\"{}\"}}",
                    e
                )
            }
        }
    }

    /// Get the error type name for JSON serialization
    fn error_type_name(&self) -> &'static str {
        match self {
            SparkError::CastInvalidValue { .. } => "CastInvalidValue",
            SparkError::NumericValueOutOfRange { .. } => "NumericValueOutOfRange",
            SparkError::NumericOutOfRange { .. } => "NumericOutOfRange",
            SparkError::CastOverFlow { .. } => "CastOverFlow",
            SparkError::CannotParseDecimal => "CannotParseDecimal",
            SparkError::ArithmeticOverflow { .. } => "ArithmeticOverflow",
            SparkError::IntegralDivideOverflow => "IntegralDivideOverflow",
            SparkError::DecimalSumOverflow => "DecimalSumOverflow",
            SparkError::DivideByZero => "DivideByZero",
            SparkError::RemainderByZero => "RemainderByZero",
            SparkError::IntervalDividedByZero => "IntervalDividedByZero",
            SparkError::BinaryArithmeticOverflow { .. } => "BinaryArithmeticOverflow",
            SparkError::IntervalArithmeticOverflowWithSuggestion { .. } => {
                "IntervalArithmeticOverflowWithSuggestion"
            }
            SparkError::IntervalArithmeticOverflowWithoutSuggestion => {
                "IntervalArithmeticOverflowWithoutSuggestion"
            }
            SparkError::DatetimeOverflow => "DatetimeOverflow",
            SparkError::InvalidArrayIndex { .. } => "InvalidArrayIndex",
            SparkError::InvalidElementAtIndex { .. } => "InvalidElementAtIndex",
            SparkError::InvalidBitmapPosition { .. } => "InvalidBitmapPosition",
            SparkError::InvalidIndexOfZero => "InvalidIndexOfZero",
            SparkError::DuplicatedMapKey { .. } => "DuplicatedMapKey",
            SparkError::NullMapKey => "NullMapKey",
            SparkError::MapKeyValueDiffSizes => "MapKeyValueDiffSizes",
            SparkError::ExceedMapSizeLimit { .. } => "ExceedMapSizeLimit",
            SparkError::CollectionSizeLimitExceeded { .. } => "CollectionSizeLimitExceeded",
            SparkError::NotNullAssertViolation { .. } => "NotNullAssertViolation",
            SparkError::ValueIsNull { .. } => "ValueIsNull",
            SparkError::CannotParseTimestamp { .. } => "CannotParseTimestamp",
            SparkError::InvalidFractionOfSecond { .. } => "InvalidFractionOfSecond",
            SparkError::InvalidUtf8String { .. } => "InvalidUtf8String",
            SparkError::UnexpectedPositiveValue { .. } => "UnexpectedPositiveValue",
            SparkError::UnexpectedNegativeValue { .. } => "UnexpectedNegativeValue",
            SparkError::InvalidRegexGroupIndex { .. } => "InvalidRegexGroupIndex",
            SparkError::DatatypeCannotOrder { .. } => "DatatypeCannotOrder",
            SparkError::ScalarSubqueryTooManyRows => "ScalarSubqueryTooManyRows",
            SparkError::Arrow(_) => "Arrow",
            SparkError::Internal(_) => "Internal",
        }
    }

    /// Extract parameters as JSON value
    fn params_as_json(&self) -> serde_json::Value {
        match self {
            SparkError::CastInvalidValue {
                value,
                from_type,
                to_type,
            } => {
                serde_json::json!({
                    "value": value,
                    "fromType": from_type,
                    "toType": to_type,
                })
            }
            SparkError::NumericValueOutOfRange {
                value,
                precision,
                scale,
            } => {
                serde_json::json!({
                    "value": value,
                    "precision": precision,
                    "scale": scale,
                })
            }
            SparkError::NumericOutOfRange { value } => {
                serde_json::json!({
                    "value": value,
                })
            }
            SparkError::CastOverFlow {
                value,
                from_type,
                to_type,
            } => {
                serde_json::json!({
                    "value": value,
                    "fromType": from_type,
                    "toType": to_type,
                })
            }
            SparkError::ArithmeticOverflow { from_type } => {
                serde_json::json!({
                    "fromType": from_type,
                })
            }
            SparkError::BinaryArithmeticOverflow {
                value1,
                symbol,
                value2,
                function_name,
            } => {
                serde_json::json!({
                    "value1": value1,
                    "symbol": symbol,
                    "value2": value2,
                    "functionName": function_name,
                })
            }
            SparkError::IntervalArithmeticOverflowWithSuggestion { function_name } => {
                serde_json::json!({
                    "functionName": function_name,
                })
            }
            SparkError::InvalidArrayIndex {
                index_value,
                array_size,
            } => {
                serde_json::json!({
                    "indexValue": index_value,
                    "arraySize": array_size,
                })
            }
            SparkError::InvalidElementAtIndex {
                index_value,
                array_size,
            } => {
                serde_json::json!({
                    "indexValue": index_value,
                    "arraySize": array_size,
                })
            }
            SparkError::InvalidBitmapPosition {
                bit_position,
                bitmap_num_bytes,
                bitmap_num_bits,
            } => {
                serde_json::json!({
                    "bitPosition": bit_position,
                    "bitmapNumBytes": bitmap_num_bytes,
                    "bitmapNumBits": bitmap_num_bits,
                })
            }
            SparkError::DuplicatedMapKey { key } => {
                serde_json::json!({
                    "key": key,
                })
            }
            SparkError::ExceedMapSizeLimit { size, max_size } => {
                serde_json::json!({
                    "size": size,
                    "maxSize": max_size,
                })
            }
            SparkError::CollectionSizeLimitExceeded {
                num_elements,
                max_elements,
            } => {
                serde_json::json!({
                    "numElements": num_elements,
                    "maxElements": max_elements,
                })
            }
            SparkError::NotNullAssertViolation { field_name } => {
                serde_json::json!({
                    "fieldName": field_name,
                })
            }
            SparkError::ValueIsNull {
                field_name,
                row_index,
            } => {
                serde_json::json!({
                    "fieldName": field_name,
                    "rowIndex": row_index,
                })
            }
            SparkError::CannotParseTimestamp {
                message,
                suggested_func,
            } => {
                serde_json::json!({
                    "message": message,
                    "suggestedFunc": suggested_func,
                })
            }
            SparkError::InvalidFractionOfSecond { value } => {
                serde_json::json!({
                    "value": value,
                })
            }
            SparkError::InvalidUtf8String { hex_string } => {
                serde_json::json!({
                    "hexString": hex_string,
                })
            }
            SparkError::UnexpectedPositiveValue {
                parameter_name,
                actual_value,
            } => {
                serde_json::json!({
                    "parameterName": parameter_name,
                    "actualValue": actual_value,
                })
            }
            SparkError::UnexpectedNegativeValue {
                parameter_name,
                actual_value,
            } => {
                serde_json::json!({
                    "parameterName": parameter_name,
                    "actualValue": actual_value,
                })
            }
            SparkError::InvalidRegexGroupIndex {
                function_name,
                group_count,
                group_index,
            } => {
                serde_json::json!({
                    "functionName": function_name,
                    "groupCount": group_count,
                    "groupIndex": group_index,
                })
            }
            SparkError::DatatypeCannotOrder { data_type } => {
                serde_json::json!({
                    "dataType": data_type,
                })
            }
            SparkError::Arrow(e) => {
                serde_json::json!({
                    "message": e.to_string(),
                })
            }
            SparkError::Internal(msg) => {
                serde_json::json!({
                    "message": msg,
                })
            }
            // Simple errors with no parameters
            _ => serde_json::json!({}),
        }
    }

    /// Returns the appropriate Spark exception class for this error
    pub fn exception_class(&self) -> &'static str {
        match self {
            // ArithmeticException
            SparkError::DivideByZero
            | SparkError::RemainderByZero
            | SparkError::IntervalDividedByZero
            | SparkError::NumericValueOutOfRange { .. }
            | SparkError::NumericOutOfRange { .. } // Comet-specific extension
            | SparkError::ArithmeticOverflow { .. }
            | SparkError::IntegralDivideOverflow
            | SparkError::DecimalSumOverflow
            | SparkError::BinaryArithmeticOverflow { .. }
            | SparkError::IntervalArithmeticOverflowWithSuggestion { .. }
            | SparkError::IntervalArithmeticOverflowWithoutSuggestion
            | SparkError::DatetimeOverflow => "org/apache/spark/SparkArithmeticException",

            // CastOverflow gets special handling with CastOverflowException
            SparkError::CastOverFlow { .. } => "org/apache/spark/sql/comet/CastOverflowException",

            // NumberFormatException (for cast invalid input errors)
            SparkError::CastInvalidValue { .. } => "org/apache/spark/SparkNumberFormatException",

            // ArrayIndexOutOfBoundsException
            SparkError::InvalidArrayIndex { .. }
            | SparkError::InvalidElementAtIndex { .. }
            | SparkError::InvalidBitmapPosition { .. }
            | SparkError::InvalidIndexOfZero => "org/apache/spark/SparkArrayIndexOutOfBoundsException",

            // RuntimeException
            SparkError::CannotParseDecimal
            | SparkError::DuplicatedMapKey { .. }
            | SparkError::NullMapKey
            | SparkError::MapKeyValueDiffSizes
            | SparkError::ExceedMapSizeLimit { .. }
            | SparkError::CollectionSizeLimitExceeded { .. }
            | SparkError::NotNullAssertViolation { .. }
            | SparkError::ValueIsNull { .. } // Comet-specific extension
            | SparkError::UnexpectedPositiveValue { .. }
            | SparkError::UnexpectedNegativeValue { .. }
            | SparkError::InvalidRegexGroupIndex { .. }
            | SparkError::ScalarSubqueryTooManyRows => "org/apache/spark/SparkRuntimeException",

            // DateTimeException
            SparkError::CannotParseTimestamp { .. }
            | SparkError::InvalidFractionOfSecond { .. } => "org/apache/spark/SparkDateTimeException",

            // IllegalArgumentException
            SparkError::DatatypeCannotOrder { .. }
            | SparkError::InvalidUtf8String { .. } => "org/apache/spark/SparkIllegalArgumentException",

            // Generic errors
            SparkError::Arrow(_) | SparkError::Internal(_) => "org/apache/spark/SparkException",
        }
    }

    /// Returns the Spark error class code for this error
    pub fn error_class(&self) -> Option<&'static str> {
        match self {
            // Cast errors
            SparkError::CastInvalidValue { .. } => Some("CAST_INVALID_INPUT"),
            SparkError::CastOverFlow { .. } => Some("CAST_OVERFLOW"),
            SparkError::NumericValueOutOfRange { .. } => {
                Some("NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION")
            }
            SparkError::NumericOutOfRange { .. } => Some("NUMERIC_OUT_OF_SUPPORTED_RANGE"),
            SparkError::CannotParseDecimal => Some("CANNOT_PARSE_DECIMAL"),

            // Arithmetic errors
            SparkError::DivideByZero => Some("DIVIDE_BY_ZERO"),
            SparkError::RemainderByZero => Some("REMAINDER_BY_ZERO"),
            SparkError::IntervalDividedByZero => Some("INTERVAL_DIVIDED_BY_ZERO"),
            SparkError::ArithmeticOverflow { .. } => Some("ARITHMETIC_OVERFLOW"),
            SparkError::IntegralDivideOverflow => Some("ARITHMETIC_OVERFLOW"),
            SparkError::DecimalSumOverflow => Some("ARITHMETIC_OVERFLOW"),
            SparkError::BinaryArithmeticOverflow { .. } => Some("BINARY_ARITHMETIC_OVERFLOW"),
            SparkError::IntervalArithmeticOverflowWithSuggestion { .. } => {
                Some("INTERVAL_ARITHMETIC_OVERFLOW")
            }
            SparkError::IntervalArithmeticOverflowWithoutSuggestion => {
                Some("INTERVAL_ARITHMETIC_OVERFLOW")
            }
            SparkError::DatetimeOverflow => Some("DATETIME_OVERFLOW"),

            // Array index errors
            SparkError::InvalidArrayIndex { .. } => Some("INVALID_ARRAY_INDEX"),
            SparkError::InvalidElementAtIndex { .. } => Some("INVALID_ARRAY_INDEX_IN_ELEMENT_AT"),
            SparkError::InvalidBitmapPosition { .. } => Some("INVALID_BITMAP_POSITION"),
            SparkError::InvalidIndexOfZero => Some("INVALID_INDEX_OF_ZERO"),

            // Map/Collection errors
            SparkError::DuplicatedMapKey { .. } => Some("DUPLICATED_MAP_KEY"),
            SparkError::NullMapKey => Some("NULL_MAP_KEY"),
            SparkError::MapKeyValueDiffSizes => Some("MAP_KEY_VALUE_DIFF_SIZES"),
            SparkError::ExceedMapSizeLimit { .. } => Some("EXCEED_LIMIT_LENGTH"),
            SparkError::CollectionSizeLimitExceeded { .. } => {
                Some("COLLECTION_SIZE_LIMIT_EXCEEDED")
            }

            // Null validation errors
            SparkError::NotNullAssertViolation { .. } => Some("NOT_NULL_ASSERT_VIOLATION"),
            SparkError::ValueIsNull { .. } => Some("VALUE_IS_NULL"),

            // DateTime errors
            SparkError::CannotParseTimestamp { .. } => Some("CANNOT_PARSE_TIMESTAMP"),
            SparkError::InvalidFractionOfSecond { .. } => Some("INVALID_FRACTION_OF_SECOND"),

            // String/UTF8 errors
            SparkError::InvalidUtf8String { .. } => Some("INVALID_UTF8_STRING"),

            // Function parameter errors
            SparkError::UnexpectedPositiveValue { .. } => Some("UNEXPECTED_POSITIVE_VALUE"),
            SparkError::UnexpectedNegativeValue { .. } => Some("UNEXPECTED_NEGATIVE_VALUE"),

            // Regex errors
            SparkError::InvalidRegexGroupIndex { .. } => Some("INVALID_PARAMETER_VALUE"),

            // Unsupported operation errors
            SparkError::DatatypeCannotOrder { .. } => Some("DATATYPE_CANNOT_ORDER"),

            // Subquery errors
            SparkError::ScalarSubqueryTooManyRows => Some("SCALAR_SUBQUERY_TOO_MANY_ROWS"),

            // Generic errors (no error class)
            SparkError::Arrow(_) | SparkError::Internal(_) => None,
        }
    }
}

/// Convert decimal overflow to SparkError::NumericValueOutOfRange.
///
/// Creates the appropriate SparkError when a decimal value exceeds the precision limit for Decimal128 storage.
///
/// # Arguments
/// * `value` - The i128 decimal value that overflowed
/// * `precision` - The target precision
/// * `scale` - The scale of the decimal
///
/// # Returns
/// SparkError::NumericValueOutOfRange with the value, precision, and scale
pub fn decimal_overflow_error(value: i128, precision: u8, scale: i8) -> SparkError {
    SparkError::NumericValueOutOfRange {
        value: value.to_string(),
        precision,
        scale,
    }
}

pub type SparkResult<T> = Result<T, SparkError>;

/// Wrapper that adds QueryContext to SparkError
///
/// This allows attaching SQL context information (query text, line/position, object name) to errors
#[derive(Debug, Clone)]
pub struct SparkErrorWithContext {
    /// The underlying SparkError
    pub error: SparkError,
    /// Optional QueryContext for SQL location information
    pub context: Option<Arc<crate::QueryContext>>,
}

impl SparkErrorWithContext {
    /// Create a SparkErrorWithContext without context
    pub fn new(error: SparkError) -> Self {
        Self {
            error,
            context: None,
        }
    }

    /// Create a SparkErrorWithContext with QueryContext
    pub fn with_context(error: SparkError, context: Arc<crate::QueryContext>) -> Self {
        Self {
            error,
            context: Some(context),
        }
    }

    /// Serialize to JSON including optional context field
    ///
    /// JSON structure:
    /// ```json
    /// {
    ///   "errorType": "DivideByZero",
    ///   "errorClass": "DIVIDE_BY_ZERO",
    ///   "params": {},
    ///   "context": {
    ///     "sqlText": "SELECT a/b FROM t",
    ///     "startIndex": 7,
    ///     "stopIndex": 9,
    ///     "line": 1,
    ///     "startPosition": 7
    ///   },
    ///   "summary": "== SQL (line 1, position 8) ==\n..."
    /// }
    /// ```
    pub fn to_json(&self) -> String {
        let mut json_obj = serde_json::json!({
            "errorType": self.error.error_type_name(),
            "errorClass": self.error.error_class().unwrap_or(""),
            "params": self.error.params_as_json(),
        });

        if let Some(ctx) = &self.context {
            // Serialize context fields
            json_obj["context"] = serde_json::json!({
                "sqlText": ctx.sql_text.as_str(),
                "startIndex": ctx.start_index,
                "stopIndex": ctx.stop_index,
                "objectType": ctx.object_type,
                "objectName": ctx.object_name,
                "line": ctx.line,
                "startPosition": ctx.start_position,
            });

            // Add formatted summary
            json_obj["summary"] = serde_json::json!(ctx.format_summary());
        }

        serde_json::to_string(&json_obj).unwrap_or_else(|e| {
            format!(
                "{{\"errorType\":\"SerializationError\",\"message\":\"{}\"}}",
                e
            )
        })
    }
}

impl std::fmt::Display for SparkErrorWithContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)?;
        if let Some(ctx) = &self.context {
            write!(f, "\n{}", ctx.format_summary())?;
        }
        Ok(())
    }
}

impl std::error::Error for SparkErrorWithContext {}

impl From<SparkError> for SparkErrorWithContext {
    fn from(error: SparkError) -> Self {
        SparkErrorWithContext::new(error)
    }
}

impl From<SparkErrorWithContext> for DataFusionError {
    fn from(value: SparkErrorWithContext) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

impl From<ArrowError> for SparkError {
    fn from(value: ArrowError) -> Self {
        SparkError::Arrow(Arc::new(value))
    }
}

impl From<SparkError> for DataFusionError {
    fn from(value: SparkError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_divide_by_zero_json() {
        let error = SparkError::DivideByZero;
        let json = error.to_json();

        assert!(json.contains("\"errorType\":\"DivideByZero\""));
        assert!(json.contains("\"errorClass\":\"DIVIDE_BY_ZERO\""));

        // Verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["errorType"], "DivideByZero");
        assert_eq!(parsed["errorClass"], "DIVIDE_BY_ZERO");
    }

    #[test]
    fn test_remainder_by_zero_json() {
        let error = SparkError::RemainderByZero;
        let json = error.to_json();

        assert!(json.contains("\"errorType\":\"RemainderByZero\""));
        assert!(json.contains("\"errorClass\":\"REMAINDER_BY_ZERO\""));
    }

    #[test]
    fn test_binary_overflow_json() {
        let error = SparkError::BinaryArithmeticOverflow {
            value1: "32767".to_string(),
            symbol: "+".to_string(),
            value2: "1".to_string(),
            function_name: "try_add".to_string(),
        };
        let json = error.to_json();

        // Verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["errorType"], "BinaryArithmeticOverflow");
        assert_eq!(parsed["errorClass"], "BINARY_ARITHMETIC_OVERFLOW");
        assert_eq!(parsed["params"]["value1"], "32767");
        assert_eq!(parsed["params"]["symbol"], "+");
        assert_eq!(parsed["params"]["value2"], "1");
        assert_eq!(parsed["params"]["functionName"], "try_add");
    }

    #[test]
    fn test_invalid_array_index_json() {
        let error = SparkError::InvalidArrayIndex {
            index_value: 10,
            array_size: 3,
        };
        let json = error.to_json();

        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["errorType"], "InvalidArrayIndex");
        assert_eq!(parsed["errorClass"], "INVALID_ARRAY_INDEX");
        assert_eq!(parsed["params"]["indexValue"], 10);
        assert_eq!(parsed["params"]["arraySize"], 3);
    }

    #[test]
    fn test_numeric_value_out_of_range_json() {
        let error = SparkError::NumericValueOutOfRange {
            value: "999.99".to_string(),
            precision: 5,
            scale: 2,
        };
        let json = error.to_json();

        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["errorType"], "NumericValueOutOfRange");
        assert_eq!(
            parsed["errorClass"],
            "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"
        );
        assert_eq!(parsed["params"]["value"], "999.99");
        assert_eq!(parsed["params"]["precision"], 5);
        assert_eq!(parsed["params"]["scale"], 2);
    }

    #[test]
    fn test_cast_invalid_value_json() {
        let error = SparkError::CastInvalidValue {
            value: "abc".to_string(),
            from_type: "STRING".to_string(),
            to_type: "INT".to_string(),
        };
        let json = error.to_json();

        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["errorType"], "CastInvalidValue");
        assert_eq!(parsed["errorClass"], "CAST_INVALID_INPUT");
        assert_eq!(parsed["params"]["value"], "abc");
        assert_eq!(parsed["params"]["fromType"], "STRING");
        assert_eq!(parsed["params"]["toType"], "INT");
    }

    #[test]
    fn test_duplicated_map_key_json() {
        let error = SparkError::DuplicatedMapKey {
            key: "duplicate_key".to_string(),
        };
        let json = error.to_json();

        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["errorType"], "DuplicatedMapKey");
        assert_eq!(parsed["errorClass"], "DUPLICATED_MAP_KEY");
        assert_eq!(parsed["params"]["key"], "duplicate_key");
    }

    #[test]
    fn test_null_map_key_json() {
        let error = SparkError::NullMapKey;
        let json = error.to_json();

        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["errorType"], "NullMapKey");
        assert_eq!(parsed["errorClass"], "NULL_MAP_KEY");
        // Params should be an empty object
        assert_eq!(parsed["params"], serde_json::json!({}));
    }

    #[test]
    fn test_error_class_mapping() {
        // Test that error_class() returns the correct error class
        assert_eq!(
            SparkError::DivideByZero.error_class(),
            Some("DIVIDE_BY_ZERO")
        );
        assert_eq!(
            SparkError::RemainderByZero.error_class(),
            Some("REMAINDER_BY_ZERO")
        );
        assert_eq!(
            SparkError::InvalidArrayIndex {
                index_value: 0,
                array_size: 0
            }
            .error_class(),
            Some("INVALID_ARRAY_INDEX")
        );
        assert_eq!(SparkError::NullMapKey.error_class(), Some("NULL_MAP_KEY"));
    }

    #[test]
    fn test_exception_class_mapping() {
        // Test that exception_class() returns the correct Java exception class
        assert_eq!(
            SparkError::DivideByZero.exception_class(),
            "org/apache/spark/SparkArithmeticException"
        );
        assert_eq!(
            SparkError::InvalidArrayIndex {
                index_value: 0,
                array_size: 0
            }
            .exception_class(),
            "org/apache/spark/SparkArrayIndexOutOfBoundsException"
        );
        assert_eq!(
            SparkError::NullMapKey.exception_class(),
            "org/apache/spark/SparkRuntimeException"
        );
    }
}
