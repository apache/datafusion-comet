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

use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::common::{
    cast::as_generic_string_array, exec_err, Result as DataFusionResult, ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use serde_json::Value;
use std::sync::Arc;

/// Spark-compatible `get_json_object` function.
///
/// Extracts a JSON value from a JSON string using a JSONPath expression.
/// Returns the result as a string, or null if the path doesn't match or input is invalid.
///
/// Supported JSONPath syntax:
/// - `$` — root element
/// - `.name` or `['name']` — named child
/// - `[n]` — array index (0-based)
/// - `[*]` — array wildcard (iterates over array elements)
pub fn spark_get_json_object(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!(
            "get_json_object expects 2 arguments (json, path), got {}",
            args.len()
        );
    }

    match (&args[0], &args[1]) {
        // Column json, scalar path (most common case)
        (ColumnarValue::Array(json_array), ColumnarValue::Scalar(path_scalar)) => {
            let path_str = match path_scalar {
                ScalarValue::Utf8(Some(p)) | ScalarValue::LargeUtf8(Some(p)) => p.as_str(),
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    let null_array: ArrayRef = Arc::new(StringArray::new_null(json_array.len()));
                    return Ok(ColumnarValue::Array(null_array));
                }
                _ => return exec_err!("get_json_object path must be a string"),
            };

            let parsed_path = match parse_json_path(path_str) {
                Some(p) => p,
                None => {
                    let null_array: ArrayRef = Arc::new(StringArray::new_null(json_array.len()));
                    return Ok(ColumnarValue::Array(null_array));
                }
            };

            let json_strings = as_generic_string_array::<i32>(json_array)?;
            let mut builder = StringBuilder::new();

            for i in 0..json_strings.len() {
                if json_strings.is_null(i) {
                    builder.append_null();
                } else {
                    let json_str = json_strings.value(i);
                    match evaluate_path(json_str, &parsed_path) {
                        Some(result) => builder.append_value(&result),
                        None => builder.append_null(),
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        // Scalar json, scalar path
        (ColumnarValue::Scalar(json_scalar), ColumnarValue::Scalar(path_scalar)) => {
            let json_str = match json_scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_str(),
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                _ => return exec_err!("get_json_object json must be a string"),
            };
            let path_str = match path_scalar {
                ScalarValue::Utf8(Some(p)) | ScalarValue::LargeUtf8(Some(p)) => p.as_str(),
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                _ => return exec_err!("get_json_object path must be a string"),
            };

            let parsed_path = match parse_json_path(path_str) {
                Some(p) => p,
                None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };

            let result = evaluate_path(json_str, &parsed_path);
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
        }
        // Column json, column path
        (ColumnarValue::Array(json_array), ColumnarValue::Array(path_array)) => {
            let json_strings = as_generic_string_array::<i32>(json_array)?;
            let path_strings = as_generic_string_array::<i32>(path_array)?;
            let mut builder = StringBuilder::new();

            for i in 0..json_strings.len() {
                if json_strings.is_null(i) || path_strings.is_null(i) {
                    builder.append_null();
                } else {
                    let json_str = json_strings.value(i);
                    let path_str = path_strings.value(i);
                    match parse_json_path(path_str) {
                        Some(parsed_path) => match evaluate_path(json_str, &parsed_path) {
                            Some(result) => builder.append_value(&result),
                            None => builder.append_null(),
                        },
                        None => builder.append_null(),
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        _ => exec_err!("get_json_object: unsupported argument types"),
    }
}

/// A parsed JSONPath segment.
#[derive(Debug, Clone)]
enum PathSegment {
    /// Named field: `.name` or `['name']`
    Field(String),
    /// Array index: `[n]`
    Index(usize),
    /// Wildcard: `[*]` (iterates over array elements)
    Wildcard,
}

/// A parsed JSONPath expression with precomputed metadata.
struct ParsedPath {
    segments: Vec<PathSegment>,
    has_wildcard: bool,
}

/// Parse a Spark-compatible JSONPath expression.
/// Returns None for invalid paths.
fn parse_json_path(path: &str) -> Option<ParsedPath> {
    let mut chars = path.chars().peekable();

    // Must start with '$'
    if chars.next()? != '$' {
        return None;
    }

    let mut segments = Vec::new();
    let mut has_wildcard = false;

    while chars.peek().is_some() {
        match chars.peek()? {
            '.' => {
                chars.next();
                if chars.peek() == Some(&'.') {
                    // Recursive descent not supported
                    return None;
                }
                if chars.peek() == Some(&'*') {
                    chars.next();
                    segments.push(PathSegment::Wildcard);
                    has_wildcard = true;
                } else {
                    // Read field name
                    let mut name = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == '.' || c == '[' {
                            break;
                        }
                        name.push(c);
                        chars.next();
                    }
                    if name.is_empty() {
                        return None;
                    }
                    segments.push(PathSegment::Field(name));
                }
            }
            '[' => {
                chars.next();
                if chars.peek() == Some(&'\'') {
                    // Bracket notation with quotes: ['name'] or ['*']
                    chars.next();
                    let mut name = String::new();
                    loop {
                        match chars.next()? {
                            '\'' => break,
                            c => name.push(c),
                        }
                    }
                    if chars.next()? != ']' {
                        return None;
                    }
                    if name == "*" {
                        segments.push(PathSegment::Wildcard);
                        has_wildcard = true;
                    } else {
                        segments.push(PathSegment::Field(name));
                    }
                } else if chars.peek() == Some(&'*') {
                    // [*]
                    chars.next();
                    if chars.next()? != ']' {
                        return None;
                    }
                    segments.push(PathSegment::Wildcard);
                    has_wildcard = true;
                } else {
                    // [n] — numeric index
                    let mut num_str = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == ']' {
                            break;
                        }
                        num_str.push(c);
                        chars.next();
                    }
                    if chars.next()? != ']' {
                        return None;
                    }
                    let idx: usize = num_str.parse().ok()?;
                    segments.push(PathSegment::Index(idx));
                }
            }
            _ => {
                // Unexpected character
                return None;
            }
        }
    }

    Some(ParsedPath {
        segments,
        has_wildcard,
    })
}

/// Evaluate a parsed JSONPath against a JSON string.
/// Returns the result as a string, or None if no match.
fn evaluate_path(json_str: &str, path: &ParsedPath) -> Option<String> {
    let value: Value = serde_json::from_str(json_str).ok()?;

    if !path.has_wildcard {
        // Fast path: no wildcards, no Vec allocations
        let result = evaluate_no_wildcard(&value, &path.segments)?;
        return value_to_string(result);
    }

    // Wildcard path: may return multiple results
    let results = evaluate_with_wildcard(&value, &path.segments);

    match results.len() {
        0 => None,
        1 => {
            // Single wildcard match: Spark preserves JSON serialization format
            // (strings keep their quotes, numbers don't)
            let s = serde_json::to_string(results[0]).ok()?;
            if s == "null" {
                None
            } else {
                Some(s)
            }
        }
        _ => {
            // Multiple results: wrap in JSON array
            let arr = Value::Array(results.into_iter().cloned().collect());
            Some(arr.to_string())
        }
    }
}

/// Fast-path evaluation for paths without wildcards.
/// Returns a reference to the matched value, or None if no match.
fn evaluate_no_wildcard<'a>(value: &'a Value, segments: &[PathSegment]) -> Option<&'a Value> {
    if segments.is_empty() {
        return Some(value);
    }

    match &segments[0] {
        PathSegment::Field(name) => {
            let child = value.as_object()?.get(name)?;
            evaluate_no_wildcard(child, &segments[1..])
        }
        PathSegment::Index(idx) => {
            let child = value.as_array()?.get(*idx)?;
            evaluate_no_wildcard(child, &segments[1..])
        }
        PathSegment::Wildcard => unreachable!("wildcard in no-wildcard path"),
    }
}

/// Evaluation for paths containing wildcards.
/// Returns references to all matching values.
fn evaluate_with_wildcard<'a>(value: &'a Value, segments: &[PathSegment]) -> Vec<&'a Value> {
    if segments.is_empty() {
        return vec![value];
    }

    let rest = &segments[1..];

    match &segments[0] {
        PathSegment::Field(name) => match value {
            Value::Object(map) => match map.get(name) {
                Some(v) => evaluate_with_wildcard(v, rest),
                None => vec![],
            },
            _ => vec![],
        },
        PathSegment::Index(idx) => match value {
            Value::Array(arr) => match arr.get(*idx) {
                Some(v) => evaluate_with_wildcard(v, rest),
                None => vec![],
            },
            _ => vec![],
        },
        PathSegment::Wildcard => match value {
            Value::Array(arr) => arr
                .iter()
                .flat_map(|v| evaluate_with_wildcard(v, rest))
                .collect(),
            _ => vec![],
        },
    }
}

/// Convert a JSON value to its string representation matching Spark behavior.
/// - Strings are returned without quotes
/// - null returns None
/// - Numbers, booleans, objects, arrays are serialized as JSON
fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        _ => Some(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_path() {
        // Root only
        let path = parse_json_path("$").unwrap();
        assert!(path.segments.is_empty());
        assert!(!path.has_wildcard);

        // Simple field
        let path = parse_json_path("$.name").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Field(n) if n == "name"));
        assert!(!path.has_wildcard);

        // Array index
        let path = parse_json_path("$[0]").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Index(0)));

        // Bracket notation
        let path = parse_json_path("$['key with spaces']").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Field(n) if n == "key with spaces"));

        // Wildcard
        let path = parse_json_path("$[*]").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Wildcard));
        assert!(path.has_wildcard);

        let path = parse_json_path("$.*").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Wildcard));
        assert!(path.has_wildcard);

        // Recursive descent not supported
        assert!(parse_json_path("$..name").is_none());

        // Must start with $
        assert!(parse_json_path("name").is_none());
        assert!(parse_json_path("[0]").is_none());
    }

    #[test]
    fn test_evaluate_simple_field() {
        let path = parse_json_path("$.name").unwrap();
        assert_eq!(
            evaluate_path(r#"{"name":"John","age":30}"#, &path),
            Some("John".to_string())
        );
        let path = parse_json_path("$.age").unwrap();
        assert_eq!(
            evaluate_path(r#"{"name":"John","age":30}"#, &path),
            Some("30".to_string())
        );
    }

    #[test]
    fn test_evaluate_nested() {
        let json = r#"{"user":{"profile":{"name":"Alice"}}}"#;
        let path = parse_json_path("$.user.profile.name").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("Alice".to_string()));
    }

    #[test]
    fn test_evaluate_array_index() {
        let path = parse_json_path("$[0]").unwrap();
        assert_eq!(evaluate_path(r#"[1,2,3]"#, &path), Some("1".to_string()));
        let path = parse_json_path("$[3]").unwrap();
        assert_eq!(evaluate_path(r#"[1,2,3]"#, &path), None);
    }

    #[test]
    fn test_evaluate_root() {
        let path = parse_json_path("$").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":"b"}"#, &path),
            Some(r#"{"a":"b"}"#.to_string())
        );
    }

    #[test]
    fn test_evaluate_null_value() {
        let path = parse_json_path("$.a").unwrap();
        assert_eq!(evaluate_path(r#"{"a":null}"#, &path), None);
    }

    #[test]
    fn test_evaluate_missing_field() {
        let path = parse_json_path("$.c").unwrap();
        assert_eq!(evaluate_path(r#"{"a":"b"}"#, &path), None);
    }

    #[test]
    fn test_evaluate_invalid_json() {
        let path = parse_json_path("$.a").unwrap();
        assert_eq!(evaluate_path("not json", &path), None);
    }

    #[test]
    fn test_evaluate_wildcard() {
        let json = r#"[{"a":"b"},{"a":"c"}]"#;
        let path = parse_json_path("$[*].a").unwrap();
        assert_eq!(evaluate_path(json, &path), Some(r#"["b","c"]"#.to_string()));
    }

    #[test]
    fn test_evaluate_string_unquoted() {
        // Strings should be returned without quotes for non-wildcard paths
        let path = parse_json_path("$[1]").unwrap();
        assert_eq!(evaluate_path(r#"["a","b"]"#, &path), Some("b".to_string()));
    }

    #[test]
    fn test_evaluate_nested_array_field() {
        let json = r#"{"items":["apple","banana","cherry"]}"#;
        let path = parse_json_path("$.items[1]").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("banana".to_string()));
    }

    #[test]
    fn test_evaluate_bracket_notation_with_spaces() {
        let json = r#"{"key with spaces":"it works"}"#;
        let path = parse_json_path("$['key with spaces']").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("it works".to_string()));
    }

    #[test]
    fn test_evaluate_boolean_and_nested_object() {
        let json = r#"{"a":true,"b":{"c":1}}"#;
        let path_a = parse_json_path("$.a").unwrap();
        assert_eq!(evaluate_path(json, &path_a), Some("true".to_string()));
        let path_b = parse_json_path("$.b").unwrap();
        assert_eq!(evaluate_path(json, &path_b), Some(r#"{"c":1}"#.to_string()));
    }

    #[test]
    fn test_object_key_order_preserved() {
        // Depends on serde_json "preserve_order" feature (see Cargo.toml)
        let json = r#"{"z":1,"a":2}"#;
        let path = parse_json_path("$").unwrap();
        assert_eq!(
            evaluate_path(json, &path),
            Some(r#"{"z":1,"a":2}"#.to_string())
        );
    }

    #[test]
    fn test_wildcard_single_match() {
        // Single wildcard match on string: Spark preserves JSON quotes
        let json = r#"[{"a":"only"}]"#;
        let path = parse_json_path("$[*].a").unwrap();
        assert_eq!(evaluate_path(json, &path), Some(r#""only""#.to_string()));

        // Single wildcard match on number: no quotes
        let json = r#"[{"a":42}]"#;
        assert_eq!(evaluate_path(json, &path), Some("42".to_string()));
    }

    #[test]
    fn test_wildcard_missing_fields() {
        // Wildcard should skip elements where the field is missing
        let json = r#"[{"a":1},{"b":2},{"a":3}]"#;
        let path = parse_json_path("$[*].a").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("[1,3]".to_string()));
    }

    #[test]
    fn test_field_with_colon() {
        let json = r#"{"fb:testid":"123"}"#;
        let path = parse_json_path("$.fb:testid").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("123".to_string()));
    }

    #[test]
    fn test_dot_bracket_invalid() {
        // $.[0] is not valid path syntax in Spark
        assert!(parse_json_path("$.[0]").is_none());
    }

    #[test]
    fn test_object_wildcard() {
        // Spark returns null for $.* on objects (wildcard only works in array contexts)
        let json = r#"{"a":1,"b":2,"c":3}"#;
        let path = parse_json_path("$.*").unwrap();
        assert_eq!(evaluate_path(json, &path), None);
    }
}
