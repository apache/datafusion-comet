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

// TODO upstream this to DataFusion as long as we have a way to specify all
// of the Spark-specific compatibility features that we need (including
// being able to specify Spark-compatible cast from all types to string)

use crate::SparkCastOptions;
use crate::{spark_cast, EvalMode};
use arrow::array::builder::StringBuilder;
use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter, Write};
use std::hash::Hash;
use std::sync::Arc;

/// to_json function
#[derive(Debug, Eq)]
pub struct ToJson {
    /// The input to convert to JSON
    expr: Arc<dyn PhysicalExpr>,
    /// Timezone to use when converting timestamps to JSON
    timezone: String,
    ignore_null_fields: bool,
}

impl Hash for ToJson {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.timezone.hash(state);
        self.ignore_null_fields.hash(state);
    }
}
impl PartialEq for ToJson {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.timezone.eq(&other.timezone)
            && self.ignore_null_fields.eq(&other.ignore_null_fields)
    }
}

impl ToJson {
    pub fn new(expr: Arc<dyn PhysicalExpr>, timezone: &str, ignore_null_fields: bool) -> Self {
        Self {
            expr,
            timezone: timezone.to_owned(),
            ignore_null_fields,
        }
    }
}

impl Display for ToJson {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "to_json({}, timezone={}, ignore_null_fields={})",
            self.expr, self.timezone, self.ignore_null_fields
        )
    }
}

impl PartialEq<dyn Any> for ToJson {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<ToJson>() {
            self == other
        } else {
            false
        }
    }
}

impl PhysicalExpr for ToJson {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(array_to_json_string(
            &input,
            &self.timezone,
            self.ignore_null_fields,
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 1);
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            &self.timezone,
            self.ignore_null_fields,
        )))
    }
}

/// Convert an array into a JSON value string representation
fn array_to_json_string(
    arr: &Arc<dyn Array>,
    timezone: &str,
    ignore_null_fields: bool,
) -> Result<ArrayRef> {
    if let Some(struct_array) = arr.as_any().downcast_ref::<StructArray>() {
        struct_to_json(struct_array, timezone, ignore_null_fields)
    } else {
        spark_cast(
            ColumnarValue::Array(Arc::clone(arr)),
            &DataType::Utf8,
            &SparkCastOptions::new(EvalMode::Legacy, timezone, false),
        )?
        .into_array(arr.len())
    }
}

/// Returns the JSON escape sequence for a byte that requires escaping, or `None`
/// otherwise. Every escaped character is ASCII, so scanning the input as bytes is
/// safe: a UTF-8 continuation or lead byte (>= 0x80) can never match here, so run
/// boundaries always fall on `char` boundaries.
#[inline]
fn escape_replacement(b: u8) -> Option<&'static str> {
    match b {
        b'"' => Some("\\\""),
        b'\\' => Some("\\\\"),
        b'\t' => Some("\\t"),
        b'\r' => Some("\\r"),
        b'\n' => Some("\\n"),
        0x0C => Some("\\f"),
        0x08 => Some("\\b"),
        _ => None,
    }
}

/// Escapes the characters that Spark's JSON writer escapes (`"`, `\`, and the
/// `\t`/`\r`/`\n`/`\f`/`\b` control characters). Returns the input unchanged
/// (without allocating) when nothing needs escaping, which is the common case,
/// and otherwise copies unescaped runs in bulk rather than character by character.
fn escape_string(input: &str) -> Cow<'_, str> {
    let bytes = input.as_bytes();
    let first = match bytes.iter().position(|&b| escape_replacement(b).is_some()) {
        None => return Cow::Borrowed(input),
        Some(pos) => pos,
    };

    let mut escaped = String::with_capacity(input.len() + 8);
    let mut run_start = 0;
    for i in first..bytes.len() {
        if let Some(replacement) = escape_replacement(bytes[i]) {
            escaped.push_str(&input[run_start..i]);
            escaped.push_str(replacement);
            run_start = i + 1;
        }
    }
    escaped.push_str(&input[run_start..]);
    Cow::Owned(escaped)
}

/// How the rendered value of a column has to be written into the JSON output.
enum FieldStyle {
    /// String column: always quoted.
    Quoted,
    /// The rendering may be `NaN` / `Infinity`, which Spark writes as a quoted string.
    MaybeQuoted,
    /// The rendering is always written verbatim.
    Raw,
}

/// Peels off a dictionary encoding to get at the type the values are rendered from.
fn value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, dt) => dt.as_ref(),
        dt => dt,
    }
}

/// True for types whose string rendering can never be `NaN` or `Infinity`, so the
/// per-value check for those spellings can be skipped entirely.
fn never_renders_special_float(data_type: &DataType) -> bool {
    let data_type = value_type(data_type);
    data_type.is_null()
        || data_type.is_integer()
        || data_type.is_decimal()
        || data_type.is_temporal()
        || matches!(data_type, DataType::Boolean)
}

fn struct_to_json(
    array: &StructArray,
    timezone: &str,
    ignore_null_fields: bool,
) -> Result<ArrayRef> {
    // create JSON string representation of each column
    let string_arrays: Vec<ArrayRef> = array
        .columns()
        .iter()
        .map(|arr| array_to_json_string(arr, timezone, ignore_null_fields))
        .collect::<Result<Vec<_>>>()?;

    // pre-render `"field_name":` so that each field costs a single copy rather than one
    // per punctuation character, and decide up front how its values have to be written
    let fields: Vec<(&StringArray, String, FieldStyle)> = array
        .fields()
        .iter()
        .zip(string_arrays.iter())
        .map(|(f, values)| {
            let values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array");
            let prefix = format!("\"{}\":", escape_string(f.name()));
            let style = match value_type(f.data_type()) {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => FieldStyle::Quoted,
                _ if never_renders_special_float(f.data_type()) => FieldStyle::Raw,
                _ => FieldStyle::MaybeQuoted,
            };
            (values, prefix, style)
        })
        .collect();

    // size the output buffer from the rendered columns so that it does not have to grow:
    // the braces of each row, plus every field's `,"name":` and its (possibly quoted) value.
    // The values length is taken from the offsets so a sliced input does not over-allocate.
    let num_rows = array.len();
    let data_capacity = 2 * num_rows
        + fields
            .iter()
            .map(|(values, prefix, _)| {
                let offsets = values.value_offsets();
                let bytes = if offsets.is_empty() {
                    0
                } else {
                    (offsets[offsets.len() - 1] - offsets[0]) as usize
                };
                bytes + num_rows * (prefix.len() + 3)
            })
            .sum::<usize>();

    // build the JSON string containing entries in the format `"field_name":field_value`,
    // writing straight into the builder rather than staging each row in a temporary
    let mut builder = StringBuilder::with_capacity(num_rows, data_capacity);
    for row_index in 0..num_rows {
        if array.is_null(row_index) {
            builder.append_null();
            continue;
        }
        let mut any_fields_written = false;
        append(&mut builder, "{");
        for (values, prefix, style) in &fields {
            let is_null = values.is_null(row_index);
            if is_null && ignore_null_fields {
                continue;
            }
            if any_fields_written {
                append(&mut builder, ",");
            }
            any_fields_written = true;
            append(&mut builder, prefix);
            if is_null {
                append(&mut builder, "null");
                continue;
            }
            let string_value = values.value(row_index);
            match style {
                FieldStyle::Quoted => {
                    append(&mut builder, "\"");
                    append(&mut builder, escape_string(string_value).as_ref());
                    append(&mut builder, "\"");
                }
                // the special float spellings contain nothing that needs escaping
                FieldStyle::MaybeQuoted if is_infinity(string_value) || is_nan(string_value) => {
                    append(&mut builder, "\"");
                    append(&mut builder, string_value);
                    append(&mut builder, "\"");
                }
                _ => append(&mut builder, string_value),
            }
        }
        append(&mut builder, "}");
        // finalizes the value accumulated by the `append` calls above
        builder.append_value("");
    }
    Ok(Arc::new(builder.finish()))
}

/// Appends to the value currently being built. The builder's `Write` impl is infallible.
#[inline]
fn append(builder: &mut StringBuilder, s: &str) {
    let _ = builder.write_str(s);
}

fn is_infinity(input: &str) -> bool {
    input == "Infinity" || input == "-Infinity"
}

fn is_nan(input: &str) -> bool {
    input == "NaN"
}

#[cfg(test)]
mod test {
    use crate::json_funcs::to_json::{struct_to_json, ToJson};
    use arrow::array::types::Int32Type;
    use arrow::array::{Array, PrimitiveArray, StringArray};
    use arrow::array::{ArrayRef, BooleanArray, Int32Array, StructArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::Result;
    use datafusion::physical_plan::expressions::Column;
    use std::any::Any;
    use std::sync::Arc;

    #[test]
    fn test_primitives() -> Result<()> {
        let bools: ArrayRef = create_bools();
        let ints: ArrayRef = create_ints();
        let strings: ArrayRef = create_strings();
        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Boolean, true)), bools),
            (Arc::new(Field::new("b", DataType::Int32, true)), ints),
            (Arc::new(Field::new("c", DataType::Utf8, true)), strings),
        ]);
        let json = struct_to_json(&struct_array, "UTC", true)?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(4, json.len());
        assert_eq!(r#"{"b":123}"#, json.value(0));
        assert_eq!(r#"{"a":true,"c":"foo"}"#, json.value(1));
        assert_eq!(r#"{"a":false,"b":2147483647,"c":"bar"}"#, json.value(2));
        assert_eq!(r#"{"a":false,"b":-2147483648,"c":""}"#, json.value(3));
        Ok(())
    }

    #[test]
    fn test_nested_struct() -> Result<()> {
        let bools: ArrayRef = create_bools();
        let ints: ArrayRef = create_ints();

        // create first struct array
        let struct_fields = vec![
            Arc::new(Field::new("a", DataType::Boolean, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ];
        let struct_values = vec![bools, ints];
        let struct_array = StructArray::from(
            struct_fields
                .clone()
                .into_iter()
                .zip(struct_values)
                .collect::<Vec<_>>(),
        );

        // create second struct array containing the first struct array
        let struct_fields2 = vec![Arc::new(Field::new(
            "a",
            DataType::Struct(struct_fields.into()),
            true,
        ))];
        let struct_values2: Vec<ArrayRef> = vec![Arc::new(struct_array.clone())];
        let struct_array2 = StructArray::from(
            struct_fields2
                .into_iter()
                .zip(struct_values2)
                .collect::<Vec<_>>(),
        );

        let json = struct_to_json(&struct_array2, "UTC", true)?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(4, json.len());
        assert_eq!(r#"{"a":{"b":123}}"#, json.value(0));
        assert_eq!(r#"{"a":{"a":true}}"#, json.value(1));
        assert_eq!(r#"{"a":{"a":false,"b":2147483647}}"#, json.value(2));
        assert_eq!(r#"{"a":{"a":false,"b":-2147483648}}"#, json.value(3));
        Ok(())
    }

    #[test]
    fn test_float_nan_and_infinity_quoted() -> Result<()> {
        use arrow::array::Float64Array;
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Float64, true)),
            floats,
        )]);
        let json = struct_to_json(&struct_array, "UTC", true)?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(r#"{"a":1.5}"#, json.value(0));
        assert_eq!(r#"{"a":"NaN"}"#, json.value(1));
        assert_eq!(r#"{"a":"Infinity"}"#, json.value(2));
        assert_eq!(r#"{"a":"-Infinity"}"#, json.value(3));
        Ok(())
    }

    #[test]
    fn test_string_value_is_escaped() -> Result<()> {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"a"b"#),
            Some("line1\nline2"),
            Some("tab\tsep"),
            Some("back\\slash"),
        ]));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("s", DataType::Utf8, true)),
            strings,
        )]);
        let json = struct_to_json(&struct_array, "UTC", true)?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(r#"{"s":"a\"b"}"#, json.value(0));
        assert_eq!(r#"{"s":"line1\nline2"}"#, json.value(1));
        assert_eq!(r#"{"s":"tab\tsep"}"#, json.value(2));
        assert_eq!(r#"{"s":"back\\slash"}"#, json.value(3));
        Ok(())
    }

    #[test]
    fn test_string_value_unicode_passthrough() -> Result<()> {
        // Non-ASCII characters that do not need escaping should pass through unchanged.
        let strings: ArrayRef =
            Arc::new(StringArray::from(vec![Some("日本語"), Some("émoji 🚀")]));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("s", DataType::Utf8, true)),
            strings,
        )]);
        let json = struct_to_json(&struct_array, "UTC", true)?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(r#"{"s":"日本語"}"#, json.value(0));
        assert_eq!("{\"s\":\"émoji 🚀\"}", json.value(1));
        Ok(())
    }

    #[test]
    fn test_ignore_null_fields_false_keeps_nulls() -> Result<()> {
        let ints: ArrayRef = create_ints();
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("b", DataType::Int32, true)),
            ints,
        )]);
        let json = struct_to_json(&struct_array, "UTC", false)?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(r#"{"b":123}"#, json.value(0));
        assert_eq!(r#"{"b":null}"#, json.value(1));
        assert_eq!(r#"{"b":2147483647}"#, json.value(2));
        assert_eq!(r#"{"b":-2147483648}"#, json.value(3));
        Ok(())
    }

    #[test]
    fn test_utf8_view_field_is_quoted() -> Result<()> {
        use arrow::array::StringViewArray;
        let strings: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("hello"),
            Some("wo\"rld"),
        ]));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("s", DataType::Utf8View, true)),
            strings,
        )]);
        let json = struct_to_json(&struct_array, "UTC", true)?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(r#"{"s":"hello"}"#, json.value(0));
        assert_eq!(r#"{"s":"wo\"rld"}"#, json.value(1));
        Ok(())
    }

    fn create_ints() -> Arc<PrimitiveArray<Int32Type>> {
        Arc::new(Int32Array::from(vec![
            Some(123),
            None,
            Some(i32::MAX),
            Some(i32::MIN),
        ]))
    }

    fn create_bools() -> Arc<BooleanArray> {
        Arc::new(BooleanArray::from(vec![
            None,
            Some(true),
            Some(false),
            Some(false),
        ]))
    }

    fn create_strings() -> Arc<StringArray> {
        Arc::new(StringArray::from(vec![
            None,
            Some("foo"),
            Some("bar"),
            Some(""),
        ]))
    }

    fn make_to_json(timezone: &str, ignore_null_fields: bool) -> ToJson {
        ToJson::new(Arc::new(Column::new("x", 0)), timezone, ignore_null_fields)
    }

    #[test]
    fn test_partial_eq_same() {
        let a = make_to_json("UTC", true);
        let b = make_to_json("UTC", true);
        assert_eq!(a, b);
        assert!(<ToJson as PartialEq<dyn Any>>::eq(&a, &b as &dyn Any));
    }

    #[test]
    fn test_partial_eq_dyn_any_differs_on_timezone() {
        let a = make_to_json("UTC", true);
        let b = make_to_json("America/New_York", true);
        assert_ne!(a, b);
        assert!(!<ToJson as PartialEq<dyn Any>>::eq(&a, &b as &dyn Any));
    }

    #[test]
    fn test_partial_eq_dyn_any_differs_on_ignore_null_fields() {
        let a = make_to_json("UTC", true);
        let b = make_to_json("UTC", false);
        assert_ne!(a, b);
        assert!(!<ToJson as PartialEq<dyn Any>>::eq(&a, &b as &dyn Any));
    }
}
