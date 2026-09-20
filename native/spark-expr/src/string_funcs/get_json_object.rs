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
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// Extracts a string from a ScalarValue, returning Ok(None) for null values.
fn scalar_to_str(scalar: &ScalarValue, arg_name: &str) -> DataFusionResult<Option<String>> {
    match scalar {
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => Ok(s.clone()),
        _ => exec_err!("get_json_object {arg_name} must be a string"),
    }
}

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
/// - `[*][*]` — double wildcard (flattens one array level, then applies the
///   rest of the path to the outer elements themselves, matching Spark)
/// - `.*` or `['*']` — child wildcard (accepted for parse compatibility; never
///   matches, matching Spark, whose field-wildcard arm is unreachable)
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
            let path_str = match scalar_to_str(path_scalar, "path")? {
                Some(p) => p,
                None => {
                    let null_array: ArrayRef = Arc::new(StringArray::new_null(json_array.len()));
                    return Ok(ColumnarValue::Array(null_array));
                }
            };

            let parsed_path = match parse_json_path(&path_str) {
                Some(p) => p,
                None => {
                    let null_array: ArrayRef = Arc::new(StringArray::new_null(json_array.len()));
                    return Ok(ColumnarValue::Array(null_array));
                }
            };

            let json_strings = as_generic_string_array::<i32>(json_array)?;
            let mut builder = StringBuilder::with_capacity(json_strings.len(), 0);

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
            let json_str = match scalar_to_str(json_scalar, "json")? {
                Some(s) => s,
                None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };
            let path_str = match scalar_to_str(path_scalar, "path")? {
                Some(p) => p,
                None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };

            let parsed_path = match parse_json_path(&path_str) {
                Some(p) => p,
                None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };

            let result = evaluate_path(&json_str, &parsed_path);
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
        }
        // Column json, column path
        (ColumnarValue::Array(json_array), ColumnarValue::Array(path_array)) => {
            let json_strings = as_generic_string_array::<i32>(json_array)?;
            let path_strings = as_generic_string_array::<i32>(path_array)?;
            let mut builder = StringBuilder::with_capacity(json_strings.len(), 0);

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
    /// Subscript wildcard: `[*]` (iterates over array elements)
    SubscriptWildcard,
    /// Double wildcard: `[*][*]`. Spark consumes both subscript wildcards as a
    /// single non-structure-preserving step: the remaining path is applied to
    /// the outer array's elements in flatten style, not to their children.
    DoubleWildcard,
    /// Child wildcard: `.*` or `['*']`. Spark's evaluator has no reachable arm
    /// for this form (its parser emits a bare wildcard that no dispatch case
    /// consumes), so it never matches.
    ChildWildcard,
}

/// The output style in effect at a point in the path, mirroring Spark's
/// `WriteStyle`. It decides how wildcard results are wrapped at each level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Style {
    /// No wildcard has been entered: a string leaf is emitted without quotes,
    /// and a subscript wildcard keeps its array wrapper only when more than one
    /// element matched.
    Raw,
    /// A subscript wildcard (or an index immediately preceding one) has been
    /// entered: values are JSON-quoted and wildcard wrappers are always kept.
    Quoted,
    /// A double wildcard has been entered: array leaves are spliced into the
    /// parent recursively instead of copied verbatim.
    Flatten,
}

/// A parsed JSONPath expression.
struct ParsedPath {
    segments: Vec<PathSegment>,
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
    // Spark's parser emits `Subscript :: Wildcard` pairs, and its evaluator
    // special-cases two consecutive subscript wildcards (`[*][*]`) as a single
    // flattening step. Wildcards written as `.*` or `['*']` do not combine
    // this way, so only the `[*]` form merges here.
    let mut prev_subscript_wildcard = false;

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
                    segments.push(PathSegment::ChildWildcard);
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
                prev_subscript_wildcard = false;
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
                        segments.push(PathSegment::ChildWildcard);
                    } else {
                        segments.push(PathSegment::Field(name));
                    }
                } else if chars.peek() == Some(&'*') {
                    // [*]
                    chars.next();
                    if chars.next()? != ']' {
                        return None;
                    }
                    if prev_subscript_wildcard {
                        segments.pop();
                        segments.push(PathSegment::DoubleWildcard);
                        prev_subscript_wildcard = false;
                    } else {
                        segments.push(PathSegment::SubscriptWildcard);
                        prev_subscript_wildcard = true;
                    }
                    continue;
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
                prev_subscript_wildcard = false;
            }
            _ => {
                // Unexpected character
                return None;
            }
        }
    }

    Some(ParsedPath { segments })
}

/// Jackson (and therefore Spark) rejects numbers whose digit count exceeds
/// 1000 wherever they appear in the document — including values this evaluation
/// skips — so `get_json_object` returns null. serde_json enforces no such limit
/// when skipping (see its `ignore_integer`/`ignore_decimal`), so mirror
/// Jackson's `StreamReadConstraints` counters with a byte scan before parsing:
/// the sign and the decimal point do not count, integers are limited by their
/// digit count, and floats by the sum of their integer-part (a lone leading
/// zero counts as zero digits), fraction and exponent digit counts.
/// Find the end of a string body starting at `i` (just past the opening
/// quote). Short bodies are scanned inline; long bodies use memchr2, the same
/// approach as serde_json's `ignore_str`. Returns the index just past the
/// closing quote, or None for an unterminated string (the parser rejects the
/// document anyway).
#[inline]
fn skip_string_body(bytes: &[u8], mut i: usize) -> Option<usize> {
    const SHORT_STRING: usize = 32;
    if bytes.len() - i <= SHORT_STRING {
        while i < bytes.len() {
            match bytes[i] {
                b'"' => return Some(i + 1),
                b'\\' => i += 2,
                _ => i += 1,
            }
        }
        return None;
    }
    loop {
        match memchr::memchr2(b'"', b'\\', &bytes[i..]) {
            Some(off) if bytes[i + off] == b'"' => return Some(i + off + 1),
            Some(off) => i += off + 2, // escaped byte
            None => return None,
        }
    }
}

fn has_oversized_number(json: &str) -> bool {
    const MAX_NUMBER_DIGITS: usize = 1000;
    let bytes = json.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            // Skip string bodies: Jackson applies no numeric constraint to
            // string content.
            b'"' => match skip_string_body(bytes, i + 1) {
                Some(end) => i = end,
                None => return false,
            },
            b'-' | b'0'..=b'9' => {
                let mut j = i + usize::from(bytes[i] == b'-');
                let int_start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                let int_len = j - int_start;
                let mut fract_len = 0;
                if j < bytes.len() && bytes[j] == b'.' {
                    j += 1;
                    let start = j;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    fract_len = j - start;
                }
                let mut exp_len = 0;
                if j < bytes.len() && (bytes[j] | 0x20) == b'e' {
                    j += 1;
                    if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
                        j += 1;
                    }
                    let start = j;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    exp_len = j - start;
                }
                let is_float = fract_len > 0 || exp_len > 0;
                let digit_count = if is_float {
                    // jackson-core counts a lone leading-zero integer part as
                    // zero digits — except when both a fraction and an
                    // exponent are present, where it counts as one (verified
                    // against jackson-core 2.21.2: `0.5e` followed by 999
                    // exponent digits is rejected with "Number value length
                    // (1001) exceeds the maximum allowed (1000)").
                    let int_digits = if int_len == 1
                        && bytes[int_start] == b'0'
                        && !(fract_len > 0 && exp_len > 0)
                    {
                        0
                    } else {
                        int_len
                    };
                    int_digits + fract_len + exp_len
                } else {
                    int_len
                };
                if digit_count > MAX_NUMBER_DIGITS {
                    return true;
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    false
}

/// Evaluate a parsed JSONPath against a JSON string.
/// Returns the result as a string, or None if no match.
fn evaluate_path(json_str: &str, path: &ParsedPath) -> Option<String> {
    if has_oversized_number(json_str) {
        return None;
    }

    let result = extract_path(json_str, &path.segments)?;
    if !result.matched {
        return None;
    }
    // The top level is not an array context. Jackson's generator separates
    // consecutive root-level writes with a single space, so join with one.
    Some(result.writes.join(" "))
}

/// Descends into the document while it is being parsed, so only the matched
/// subtrees are materialized as `Value`s; everything else is skipped by the
/// parser without allocating. The whole document is still consumed, so malformed
/// JSON anywhere in the input yields no match, as a full parse would.
fn extract_path(json_str: &str, segments: &[PathSegment]) -> Option<PathResult> {
    let mut de = serde_json::Deserializer::from_str(json_str);
    let found = PathSeed {
        segments,
        style: Style::Raw,
        reject_direct_null: false,
    }
    .deserialize(&mut de)
    .ok()?;
    de.end().ok()?;
    Some(found)
}

/// Deserializes the value at `segments`, discarding everything else.
struct PathSeed<'a> {
    segments: &'a [PathSegment],
    /// The output style in effect, mirroring the `style` parameter Spark
    /// threads through `evaluatePath`.
    style: Style,
    /// A JSON null directly below a named field is not a match in Spark. Nulls
    /// reached through array traversal are matches and serialize as `null`.
    reject_direct_null: bool,
}

/// The outcome of applying (part of) a path, modeled on Spark's generator
/// protocol: `writes` holds one rendered fragment per generator write and
/// `matched` is Spark's dirty flag.
///
/// The two can diverge: the wildcard arms that write directly to the generator
/// emit their array wrapper even when nothing inside matched, so an unmatched
/// result can still carry writes. Spark's generator keeps those bytes — a later
/// occurrence of a duplicated field can build on them — so they are preserved
/// here rather than discarded.
#[derive(Default)]
struct PathResult {
    writes: Vec<String>,
    matched: bool,
}

impl PathResult {
    /// A single verbatim write of a matched value, honoring the output style:
    /// a string in Raw style is written unquoted (Spark's scalar-unwrap arm),
    /// everything else keeps JSON serialization.
    fn write(value: Value, style: Style) -> Self {
        match value {
            Value::String(s) if style == Style::Raw => Self {
                writes: vec![s],
                matched: true,
            },
            value => Self {
                writes: vec![value.to_string()],
                matched: true,
            },
        }
    }

    /// Wrap the accumulated writes in an array wrapper, as Jackson's generator
    /// does after `writeStartArray`: one write whose content is the writes
    /// joined with commas.
    fn wrap(writes: Vec<String>, matched: bool) -> Self {
        Self {
            writes: vec![format!("[{}]", writes.join(","))],
            matched,
        }
    }
}

impl<'de> DeserializeSeed<'de> for PathSeed<'_> {
    type Value = PathResult;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        if self.segments.is_empty() {
            return Value::deserialize(deserializer).map(|value| {
                if self.reject_direct_null && value.is_null() {
                    return PathResult::default();
                }
                match value {
                    // Flatten style splices an array leaf into the parent
                    // recursively: each flattened leaf becomes its own write
                    // into the enclosing array context, and an array that
                    // flattens to nothing writes nothing at all (Spark's
                    // dirty flag).
                    Value::Array(arr) if self.style == Style::Flatten => {
                        let mut leaves = Vec::new();
                        for element in arr {
                            flatten_into(element, &mut leaves);
                        }
                        let writes: Vec<String> =
                            leaves.into_iter().map(|v| v.to_string()).collect();
                        PathResult {
                            matched: !writes.is_empty(),
                            writes,
                        }
                    }
                    value => PathResult::write(value, self.style),
                }
            });
        }
        deserializer.deserialize_any(SegmentVisitor {
            segments: self.segments,
            style: self.style,
        })
    }
}

/// Applies `segments[0]` to the value being visited. A value whose shape does
/// not match the segment (an index into an object, say) is skipped and reported
/// as no match rather than as an error, matching a lookup on a parsed document.
struct SegmentVisitor<'a> {
    segments: &'a [PathSegment],
    style: Style,
}

impl<'de> Visitor<'de> for SegmentVisitor<'_> {
    type Value = PathResult;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(PathResult::default())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(PathResult::default())
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(PathResult::default())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(PathResult::default())
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
        Ok(PathResult::default())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(PathResult::default())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let PathSegment::Field(name) = &self.segments[0] else {
            IgnoredAny.visit_map(map)?;
            return Ok(PathResult::default());
        };

        // First-wins with fall-through: a field occurrence is only locked in
        // once the remaining path produces a match. Writes made by earlier,
        // ultimately unmatched occurrences still went to Spark's shared
        // generator, so they are kept.
        let mut found = PathResult::default();
        while let Some(matched) = map.next_key_seed(KeySeed(name))? {
            if matched && !found.matched {
                let mut candidate = map.next_value_seed(PathSeed {
                    segments: &self.segments[1..],
                    style: self.style,
                    reject_direct_null: true,
                })?;
                found.matched |= candidate.matched;
                found.writes.append(&mut candidate.writes);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(found)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        match &self.segments[0] {
            PathSegment::Index(idx) => {
                // Spark switches to Quoted style for the "one or more results"
                // case: an index immediately followed by a subscript wildcard.
                let child_style = match self.segments.get(1) {
                    Some(PathSegment::SubscriptWildcard) | Some(PathSegment::DoubleWildcard) => {
                        Style::Quoted
                    }
                    _ => self.style,
                };
                for _ in 0..*idx {
                    if seq.next_element::<IgnoredAny>()?.is_none() {
                        return Ok(PathResult::default());
                    }
                }
                let found = seq
                    .next_element_seed(PathSeed {
                        segments: &self.segments[1..],
                        style: child_style,
                        reject_direct_null: false,
                    })?
                    .unwrap_or_default();
                // The remaining elements are still visited, so that a malformed element
                // after the match yields no match, as a full parse would.
                IgnoredAny.visit_seq(seq)?;
                Ok(found)
            }
            PathSegment::DoubleWildcard => {
                // Spark consumes both wildcards of `[*][*]` at once: the
                // remaining path applies to the outer elements in flatten
                // style, and the collected writes always form a single array,
                // even when there is only one element or none matched.
                let mut writes = Vec::new();
                let mut matched = false;
                while let Some(mut result) = seq.next_element_seed(PathSeed {
                    segments: &self.segments[1..],
                    style: Style::Flatten,
                    reject_direct_null: false,
                })? {
                    matched |= result.matched;
                    writes.append(&mut result.writes);
                }
                Ok(PathResult::wrap(writes, matched))
            }
            PathSegment::SubscriptWildcard => match self.style {
                // Quoted style: the array wrapper is always kept, even for a
                // single match.
                Style::Quoted => {
                    let mut writes = Vec::new();
                    let mut matched = false;
                    while let Some(mut result) = seq.next_element_seed(PathSeed {
                        segments: &self.segments[1..],
                        style: Style::Quoted,
                        reject_direct_null: false,
                    })? {
                        matched |= result.matched;
                        writes.append(&mut result.writes);
                    }
                    Ok(PathResult::wrap(writes, matched))
                }
                // Raw or Flatten style: Spark buffers the element writes into
                // a temporary array and only emits it when more than one
                // element wrote; a lone writer's brackets are stripped, and
                // nothing at all is written when no element matched.
                Style::Raw | Style::Flatten => {
                    let child_style = if self.style == Style::Raw {
                        Style::Quoted
                    } else {
                        Style::Flatten
                    };
                    let mut writers = 0;
                    let mut writes = Vec::new();
                    while let Some(mut result) = seq.next_element_seed(PathSeed {
                        segments: &self.segments[1..],
                        style: child_style,
                        reject_direct_null: false,
                    })? {
                        if result.matched {
                            writers += 1;
                        }
                        writes.append(&mut result.writes);
                    }
                    match writers {
                        0 => Ok(PathResult::default()),
                        // Strip the buffered array's outer brackets: the
                        // buffered content becomes a single write.
                        1 => Ok(PathResult {
                            writes: vec![writes.join(",")],
                            matched: true,
                        }),
                        _ => Ok(PathResult::wrap(writes, true)),
                    }
                }
            },
            // Spark's evaluator has no reachable arm for `.*`/`['*']` or for a
            // field lookup on an array: the value is skipped entirely.
            PathSegment::Field(_) | PathSegment::ChildWildcard => {
                IgnoredAny.visit_seq(seq)?;
                Ok(PathResult::default())
            }
        }
    }
}

/// Compares an object key against a field name without allocating it.
struct KeySeed<'a>(&'a str);

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = bool;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for KeySeed<'_> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an object key")
    }

    fn visit_str<E>(self, key: &str) -> Result<bool, E> {
        Ok(key == self.0)
    }
}

/// Recursively splices array elements into `out`, matching Spark's
/// `(START_ARRAY, Nil) if style == FlattenStyle` case, which re-applies itself
/// to each child.
fn flatten_into(value: Value, out: &mut Vec<Value>) {
    match value {
        Value::Array(arr) => {
            for element in arr {
                flatten_into(element, out);
            }
        }
        value => out.push(value),
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

        // Simple field
        let path = parse_json_path("$.name").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Field(n) if n == "name"));

        // Array index
        let path = parse_json_path("$[0]").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Index(0)));

        // Bracket notation
        let path = parse_json_path("$['key with spaces']").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::Field(n) if n == "key with spaces"));

        // Subscript wildcard
        let path = parse_json_path("$[*]").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::SubscriptWildcard));

        // Child wildcard forms (`.*` and `['*']`) are distinct from `[*]`
        let path = parse_json_path("$.*").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::ChildWildcard));
        let path = parse_json_path("$['*']").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::ChildWildcard));

        // Two consecutive subscript wildcards merge into a double wildcard
        let path = parse_json_path("$[*][*]").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::DoubleWildcard));
        assert_eq!(path.segments.len(), 1);

        let path = parse_json_path("$[*][*][*]").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::DoubleWildcard));
        assert!(matches!(&path.segments[1], PathSegment::SubscriptWildcard));
        assert_eq!(path.segments.len(), 2);

        // `.*` and `['*']` wildcards do not combine with a subscript wildcard
        let path = parse_json_path("$.*[*]").unwrap();
        assert!(matches!(&path.segments[0], PathSegment::ChildWildcard));
        assert!(matches!(&path.segments[1], PathSegment::SubscriptWildcard));
        assert_eq!(path.segments.len(), 2);

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
    fn test_null_midpath_is_null() {
        // A null encountered before the path is exhausted has no child to
        // recurse into, so the lookup yields no match (SegmentVisitor::visit_unit).
        let path = parse_json_path("$.a.b").unwrap();
        assert_eq!(evaluate_path(r#"{"a":null}"#, &path), None);
    }

    #[test]
    fn test_match_then_trailing_garbage_is_null() {
        // The match is found early, but trailing content makes the document
        // malformed; the full document must still be validated (de.end()).
        let path = parse_json_path("$.a").unwrap();
        assert_eq!(evaluate_path(r#"{"a":1} garbage"#, &path), None);
    }

    #[test]
    fn test_match_then_malformed_sibling_is_null() {
        // "a" matches early, but the "b" sibling is malformed; visit_map must
        // keep draining entries after a match so the parse still rejects this.
        let path = parse_json_path("$.a").unwrap();
        assert_eq!(evaluate_path(r#"{"a":1,"b":}"#, &path), None);
    }

    #[test]
    fn test_array_match_then_malformed_element_is_null() {
        // The element at index 0 matches, but a later element is malformed;
        // visit_seq must keep draining elements after a match.
        let path = parse_json_path("$[0]").unwrap();
        assert_eq!(evaluate_path(r#"[1,,]"#, &path), None);
    }

    #[test]
    fn test_duplicate_key_first_wins() {
        // The first occurrence of a duplicated key wins, matching Spark.
        let path = parse_json_path("$.a").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":1,"a":2}"#, &path),
            Some("1".to_string())
        );
    }

    #[test]
    fn test_duplicate_key_first_wins_nested() {
        // First-wins also applies when recursing into the matched value.
        let path = parse_json_path("$.a.b").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":{"b":1},"a":{"b":2}}"#, &path),
            Some("1".to_string())
        );
    }

    #[test]
    fn test_duplicate_key_first_successful_match_wins() {
        // A duplicate field is only locked in after the remaining path
        // produces a non-null result. Spark continues to later occurrences
        // when an earlier occurrence is null or misses the remaining path.
        let path = parse_json_path("$.a.b").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":{"x":1},"a":{"b":2}}"#, &path),
            Some("2".to_string())
        );
        assert_eq!(
            evaluate_path(r#"{"a":null,"a":{"b":2}}"#, &path),
            Some("2".to_string())
        );

        let path = parse_json_path("$.a").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":null,"a":2}"#, &path),
            Some("2".to_string())
        );

        let path = parse_json_path("$.a.b").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":{"b":null,"b":2}}"#, &path),
            Some("2".to_string())
        );
    }

    #[test]
    fn test_duplicate_key_first_successful_match_wins_with_wildcard() {
        let path = parse_json_path("$.a[*].b").unwrap();
        assert_eq!(
            evaluate_path(
                r#"{"a":[{"x":1}],"a":[{"b":2},{"b":3}],"a":[{"b":4}]}"#,
                &path
            ),
            Some("[2,3]".to_string())
        );

        let path = parse_json_path("$.a[*]").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":[1],"a":[2]}"#, &path),
            Some("1".to_string())
        );
        assert_eq!(
            evaluate_path(r#"{"a":[],"a":[2]}"#, &path),
            Some("2".to_string())
        );
    }

    #[test]
    fn test_duplicate_key_null_reached_through_array_locks_match() {
        let json = r#"{"a":[null],"a":[2]}"#;

        // Unlike a null directly under a named field, a null reached through
        // array traversal is emitted as JSON text and locks that field match.
        let path = parse_json_path("$.a[0]").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("null".to_string()));

        let path = parse_json_path("$.a[*]").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("null".to_string()));
    }

    #[test]
    fn test_null_reached_through_array_serializes_as_null_text() {
        // A null reached through array traversal is a match and serializes as
        // JSON text, matching Spark; a null directly under a named field is
        // not a match (see test_evaluate_null_value).
        let path = parse_json_path("$.a[0]").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":[null]}"#, &path),
            Some("null".to_string())
        );

        let path = parse_json_path("$.a[*]").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":[null]}"#, &path),
            Some("null".to_string())
        );

        let path = parse_json_path("$.a[*]").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":[null,1]}"#, &path),
            Some("[null,1]".to_string())
        );
    }

    #[test]
    fn test_double_wildcard_flattens_one_level() {
        // Mirrors Spark's own suite ($.store.basket[*][*]): the elements of
        // the outer array are spliced into the output.
        let json = r#"{"b":[[1,2,{"c":"y"}],[3,4],[5,6]]}"#;
        let path = parse_json_path("$.b[*][*]").unwrap();
        assert_eq!(
            evaluate_path(json, &path),
            Some(r#"[1,2,{"c":"y"},3,4,5,6]"#.to_string())
        );
    }

    #[test]
    fn test_double_wildcard_flattens_recursively() {
        let path = parse_json_path("$[*][*]").unwrap();
        assert_eq!(
            evaluate_path(r#"[[[1],[2]],[3]]"#, &path),
            Some("[1,2,3]".to_string())
        );

        // Scalars pass through; string leaves keep their JSON quotes.
        assert_eq!(
            evaluate_path(r#"[1,"a"]"#, &path),
            Some(r#"[1,"a"]"#.to_string())
        );
    }

    #[test]
    fn test_double_wildcard_applies_rest_to_outer_elements() {
        // The remaining path applies to the outer elements themselves, not
        // their children: the inner arrays have no field `c`, so nothing
        // matches (Spark's `$.store.basket[*][*].non_exist_key` is null).
        let json = r#"{"b":[[1,2,{"c":"y"}],[3,4],[5,6]]}"#;
        let path = parse_json_path("$.b[*][*].c").unwrap();
        assert_eq!(evaluate_path(json, &path), None);

        // Objects directly in the outer array do match the remaining path.
        let path = parse_json_path("$[*][*].b").unwrap();
        assert_eq!(
            evaluate_path(r#"[{"b":1},{"b":2}]"#, &path),
            Some("[1,2]".to_string())
        );
    }

    #[test]
    fn test_double_wildcard_single_match_stays_wrapped() {
        // A single wildcard unwraps a single match; a double wildcard always
        // wraps its matches in an array.
        let path = parse_json_path("$[*]").unwrap();
        assert_eq!(evaluate_path(r#"[5]"#, &path), Some("5".to_string()));

        let path = parse_json_path("$[*][*]").unwrap();
        assert_eq!(evaluate_path(r#"[5]"#, &path), Some("[5]".to_string()));
        assert_eq!(evaluate_path(r#"[[5]]"#, &path), Some("[5]".to_string()));

        // A null element reached through the double wildcard serializes as
        // JSON text, as it does through a single wildcard.
        let path = parse_json_path("$.a[*][*]").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":[null]}"#, &path),
            Some("[null]".to_string())
        );
    }

    #[test]
    fn test_double_wildcard_empty_flatten_is_no_match() {
        // An element that flattens to nothing writes no leaf nodes, so the
        // double wildcard misses.
        let path = parse_json_path("$[*][*]").unwrap();
        assert_eq!(evaluate_path(r#"[]"#, &path), None);
        assert_eq!(evaluate_path(r#"[[]]"#, &path), None);
    }

    #[test]
    fn test_duplicate_key_double_wildcard_match_decision() {
        // Regression test for the PR review: `[*][*]` is one flattening step,
        // so the remaining `.b` applies to the outer element `[{"b":1}]`
        // itself, which is an array and has no fields. The first `a` misses,
        // the second `a` is null, and Spark returns NULL.
        let path = parse_json_path("$.a[*][*].b").unwrap();
        assert_eq!(evaluate_path(r#"{"a":[[{"b":1}]],"a":null}"#, &path), None);
        assert_eq!(evaluate_path(r#"{"a":[[{"b":1}]]}"#, &path), None);

        // The miss still falls through to a later occurrence that matches.
        // The unmatched first occurrence already wrote its (empty) double
        // wildcard wrapper into Spark's shared generator, so those bytes are
        // part of the output; Jackson separates root-level writes with a
        // space.
        assert_eq!(
            evaluate_path(r#"{"a":[[{"b":1}]],"a":[{"b":2}]}"#, &path),
            Some("[] [2]".to_string())
        );
    }

    #[test]
    fn test_index_then_wildcard_keeps_wrapper() {
        // Spark switches to Quoted style when an index is immediately followed
        // by a subscript wildcard, so that wildcard keeps its array wrapper
        // even for a single match.
        let path = parse_json_path("$[0][*]").unwrap();
        assert_eq!(evaluate_path("[[5]]", &path), Some("[5]".to_string()));
        assert_eq!(evaluate_path("[[5,6]]", &path), Some("[5,6]".to_string()));
        assert_eq!(evaluate_path("[7]", &path), None);

        // The wrapper decision is per wildcard level, not made once at the
        // top (review regression: a nested wildcard lost an array dimension).
        let path = parse_json_path("$[0][*][0][*][*]").unwrap();
        assert_eq!(
            evaluate_path("[[[[[[[1]]]]]]]", &path),
            Some("[[1]]".to_string())
        );

        // Same shape through a field: `$.store.basket[0][*].b` in Spark's own
        // JSON suite returns a one-element array, not the bare string.
        let path = parse_json_path("$.store.basket[0][*].b").unwrap();
        assert_eq!(
            evaluate_path(r#"{"store":{"basket":[[{"b":"y"},1],[2]]}}"#, &path),
            Some(r#"["y"]"#.to_string())
        );
    }

    #[test]
    fn test_wildcard_below_wildcard_keeps_inner_wrapper() {
        // `$.a[*].b[*]`: each inner wildcard sits below an outer wildcard and
        // runs in Quoted style, so each inner match stays wrapped, giving a
        // matrix rather than a flat list.
        let path = parse_json_path("$.a[*].b[*]").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":[{"b":[1,2]},{"b":[3]}]}"#, &path),
            Some("[[1,2],[3]]".to_string())
        );
    }

    #[test]
    fn test_oversized_number_in_skipped_field() {
        // Jackson (and therefore Spark) rejects numbers whose digit count
        // exceeds 1000 anywhere in the document, including values the path
        // never selects. serde_json's IgnoredAny skip does not, so the length
        // is checked before parsing. The counters mirror jackson-core: the
        // sign and decimal point do not count, and floats are limited by the
        // sum of integer-part (a lone leading zero counts as zero), fraction
        // and exponent digit counts.
        let path = parse_json_path("$[*].a").unwrap();
        let ok = format!(r#"[{{"a":1,"b":{}}}]"#, "9".repeat(1000));
        assert_eq!(evaluate_path(&ok, &path), Some("1".to_string()));
        let oversized = format!(r#"[{{"a":1,"b":{}}}]"#, "9".repeat(1001));
        assert_eq!(evaluate_path(&oversized, &path), None);

        // Sign does not count towards the integer length.
        let neg_ok = format!(r#"[{{"a":1,"b":-{}}}]"#, "9".repeat(1000));
        assert_eq!(evaluate_path(&neg_ok, &path), Some("1".to_string()));
        let neg_over = format!(r#"[{{"a":1,"b":-{}}}]"#, "9".repeat(1001));
        assert_eq!(evaluate_path(&neg_over, &path), None);

        // Floats: digit counts of all parts are summed; the decimal point and
        // exponent sign do not count, and a lone leading zero contributes
        // nothing (verified against jackson-core 2.21.2).
        let fract_ok = format!(r#"[{{"a":1,"b":1.{}}}]"#, "1".repeat(999));
        assert_eq!(evaluate_path(&fract_ok, &path), Some("1".to_string()));
        let fract_over = format!(r#"[{{"a":1,"b":1.{}}}]"#, "1".repeat(1000));
        assert_eq!(evaluate_path(&fract_over, &path), None);
        let zero_int_ok = format!(r#"[{{"a":1,"b":0.{}}}]"#, "1".repeat(1000));
        assert_eq!(evaluate_path(&zero_int_ok, &path), Some("1".to_string()));
        let exp_ok = format!(r#"[{{"a":1,"b":1e{}}}]"#, "0".repeat(999));
        assert_eq!(evaluate_path(&exp_ok, &path), Some("1".to_string()));
        let exp_over = format!(r#"[{{"a":1,"b":1e{}}}]"#, "0".repeat(1000));
        assert_eq!(evaluate_path(&exp_over, &path), None);
        let neg_exp_ok = format!(r#"[{{"a":1,"b":1e-{}}}]"#, "0".repeat(999));
        assert_eq!(evaluate_path(&neg_exp_ok, &path), Some("1".to_string()));
        // jackson-core quirk: with both a fraction and an exponent, a lone
        // leading zero counts as one digit, so 0.5e<999 zeros> totals 1001.
        let fract_exp_ok = format!(r#"[{{"a":1,"b":0.5e{}}}]"#, "0".repeat(998));
        assert_eq!(evaluate_path(&fract_exp_ok, &path), Some("1".to_string()));
        let fract_exp_over = format!(r#"[{{"a":1,"b":0.5e{}}}]"#, "0".repeat(999));
        assert_eq!(evaluate_path(&fract_exp_over, &path), None);

        // The check also applies to non-wildcard paths, and digits inside
        // string literals are ignored.
        let path = parse_json_path("$.a").unwrap();
        assert_eq!(evaluate_path(&oversized, &path), None);
        let in_string = format!(r#"{{"a":1,"b":"{}"}}"#, "9".repeat(1001));
        assert_eq!(evaluate_path(&in_string, &path), Some("1".to_string()));
    }

    #[test]
    fn test_child_wildcard_never_matches() {
        // Spark's evaluator has no reachable arm for the `.*`/`['*']` wildcard
        // forms: its parser emits a bare wildcard instruction that no dispatch
        // case consumes, so these paths return null for every document.
        assert_eq!(
            evaluate_path("[1,2]", &parse_json_path("$.*").unwrap()),
            None
        );
        assert_eq!(
            evaluate_path("[1,2]", &parse_json_path("$['*']").unwrap()),
            None
        );
        assert_eq!(
            evaluate_path(r#"{"a":{"x":1,"y":2}}"#, &parse_json_path("$.a.*").unwrap()),
            None
        );
    }

    #[test]
    fn test_wildcard_unmatched_writes_are_kept() {
        // The Quoted-style wildcard arm writes its array wrapper even when
        // nothing inside matched; Spark's generator keeps those bytes, so an
        // unmatched duplicate-key occurrence followed by a matching one emits
        // both fragments.
        let path = parse_json_path("$.a[0][*].b").unwrap();
        assert_eq!(
            evaluate_path(r#"{"a":[[{}]],"a":[[{"b":1}]]}"#, &path),
            Some("[] [1]".to_string())
        );

        // With no later match the dirty flag still wins and the result is null.
        assert_eq!(evaluate_path(r#"{"a":[[{}]]}"#, &path), None);
    }

    #[test]
    fn test_triple_wildcard_flatten() {
        // `[*][*][*]`: the double wildcard's flatten style flows into the
        // remaining wildcard, whose single-writer strip keeps the flattened
        // elements comma-joined, as in Spark's buffered generator output.
        let path = parse_json_path("$[*][*][*]").unwrap();
        assert_eq!(
            evaluate_path("[[[1,2],[]]]", &path),
            Some("[1,2]".to_string())
        );
        assert_eq!(evaluate_path("[[[1,2]]]", &path), Some("[1,2]".to_string()));
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

    #[test]
    fn test_unicode_field_names() {
        let json = r#"{"名前":"太郎","年齢":25}"#;
        let path = parse_json_path("$.名前").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("太郎".to_string()));
    }

    #[test]
    fn test_unicode_values() {
        let json = r#"{"greeting":"こんにちは世界"}"#;
        let path = parse_json_path("$.greeting").unwrap();
        assert_eq!(
            evaluate_path(json, &path),
            Some("こんにちは世界".to_string())
        );
    }

    #[test]
    fn test_unicode_emoji() {
        let json = r#"{"emoji":"🎉🚀","nested":{"flag":"🇺🇸"}}"#;
        let path = parse_json_path("$.emoji").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("🎉🚀".to_string()));
        let path = parse_json_path("$.nested.flag").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("🇺🇸".to_string()));
    }

    #[test]
    fn test_unicode_bracket_notation() {
        let json = r#"{"键":"值"}"#;
        let path = parse_json_path("$['键']").unwrap();
        assert_eq!(evaluate_path(json, &path), Some("值".to_string()));
    }

    #[test]
    fn test_unicode_mixed_scripts() {
        let json = r#"{"data":"café résumé naïve"}"#;
        let path = parse_json_path("$.data").unwrap();
        assert_eq!(
            evaluate_path(json, &path),
            Some("café résumé naïve".to_string())
        );
    }

    #[test]
    fn test_unicode_wildcard() {
        let json = r#"[{"名":"Alice"},{"名":"太郎"}]"#;
        let path = parse_json_path("$[*].名").unwrap();
        assert_eq!(
            evaluate_path(json, &path),
            Some(r#"["Alice","太郎"]"#.to_string())
        );
    }
}
