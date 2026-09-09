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
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BufferBuilder, GenericListArray, GenericStringArray,
    GenericStringBuilder, ListArray, NullBufferBuilder, OffsetSizeTrait, StringArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{
    cast::as_generic_string_array, exec_err, DataFusionError, Result as DataFusionResult,
    ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use regex::Regex;
use std::sync::Arc;

/// Spark-compatible split function
/// Splits a string around matches of a regex pattern with optional limit
///
/// Arguments:
/// - string: The string to split
/// - pattern: The regex pattern to split on
/// - limit (optional): Controls the number of splits
///   - limit > 0: At most limit-1 splits, array length <= limit
///   - limit = 0: treated as -1 (Spark remaps 0 to -1, so trailing empty strings are kept, unlike Java's String.split default)
///   - limit < 0: As many splits as possible, trailing empty strings kept
pub fn spark_split(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "split expects 2 or 3 arguments (string, pattern, [limit]), got {}",
            args.len()
        );
    }

    // Get limit parameter (default to -1 if not provided)
    let limit = if args.len() == 3 {
        match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(l))) => *l,
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => {
                // NULL limit, return NULL
                return Ok(ColumnarValue::Scalar(ScalarValue::Null));
            }
            _ => {
                return exec_err!("split limit argument must be an Int32 scalar");
            }
        }
    } else {
        -1
    };

    // Spark's UTF8String.split remaps limit == 0 to -1 before calling Java's
    // String.split, specifically to avoid Java's "drop trailing empty strings"
    // behavior. Normalize here so every helper below shares Spark semantics.
    let limit = if limit == 0 { -1 } else { limit };

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(string_array), ColumnarValue::Scalar(ScalarValue::Utf8(pattern)))
        | (
            ColumnarValue::Array(string_array),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(pattern)),
        ) => {
            if pattern.is_none() {
                // NULL pattern returns NULL
                let null_array = new_null_list_array(string_array.len());
                return Ok(ColumnarValue::Array(null_array));
            }

            let pattern_str = pattern.as_ref().unwrap();
            split_array(string_array.as_ref(), pattern_str, limit)
        }
        (ColumnarValue::Scalar(ScalarValue::Utf8(string)), ColumnarValue::Scalar(pattern_val))
        | (
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(string)),
            ColumnarValue::Scalar(pattern_val),
        ) => {
            if string.is_none() {
                return Ok(ColumnarValue::Scalar(new_null_list_scalar()));
            }

            let pattern_str = match pattern_val {
                ScalarValue::Utf8(Some(p)) | ScalarValue::LargeUtf8(Some(p)) => p,
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    return Ok(ColumnarValue::Scalar(new_null_list_scalar()));
                }
                _ => {
                    return exec_err!("split pattern must be a string");
                }
            };

            let s = string.as_ref().unwrap();

            let mut str_offsets = BufferBuilder::<i32>::new(8);
            let mut str_values = BufferBuilder::<u8>::new(s.len());
            str_offsets.append(0);

            let mut scratch = Vec::new();
            if is_regex_literal(pattern_str) {
                let mut chars = pattern_str.chars();
                if let (Some(ch), None) = (chars.next(), chars.next()) {
                    push_split_char(
                        s,
                        ch,
                        limit,
                        &mut str_offsets,
                        &mut str_values,
                        &mut scratch,
                    );
                } else {
                    push_split_literal(
                        s,
                        pattern_str,
                        limit,
                        &mut str_offsets,
                        &mut str_values,
                        &mut scratch,
                    );
                }
            } else {
                let regex = Regex::new(pattern_str).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Invalid regex pattern '{}': {}",
                        pattern_str, e
                    ))
                })?;
                push_split_parts(
                    s,
                    &regex,
                    limit,
                    &mut str_offsets,
                    &mut str_values,
                    &mut scratch,
                );
            }

            let item_offsets_buffer = OffsetBuffer::new(str_offsets.finish().into());
            let item_values_buffer = str_values.finish();

            // SAFETY: every byte appended to `values` comes from a `&str` of the input
            // (via `append_str`), so the value buffer is valid UTF-8. Offsets start at 0
            // and only ever advance by the byte length of the appended part, so they are
            // monotonically non-decreasing, which satisfies the invariants checked by
            // `OffsetBuffer::new` / `GenericStringArray::new_unchecked`. Nullability is
            // `None`: nullability of rows is carried by the wrapping list array.
            let string_array_values = unsafe {
                GenericStringArray::<i32>::new_unchecked(
                    item_offsets_buffer,
                    item_values_buffer,
                    None,
                )
            };

            let list_array = create_list_array(Arc::new(string_array_values));

            Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                list_array,
            ))))
        }
        _ => exec_err!("split expects (array, scalar) or (scalar, scalar) arguments"),
    }
}

/// Spark-compatible StringSplitSQL function.
/// Splits a string around literal delimiter matches and keeps trailing empty strings.
pub fn spark_split_sql(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!(
            "split_sql expects 2 arguments (string, delimiter), got {}",
            args.len()
        );
    }

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(string_array), ColumnarValue::Scalar(delimiter)) => {
            let delimiter = match delimiter {
                ScalarValue::Utf8(Some(d)) | ScalarValue::LargeUtf8(Some(d)) => d,
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    return Ok(ColumnarValue::Array(new_null_list_array(
                        string_array.len(),
                    )));
                }
                _ => return exec_err!("split_sql delimiter must be a string"),
            };
            split_sql_array_scalar(string_array.as_ref(), delimiter)
        }
        (ColumnarValue::Array(string_array), ColumnarValue::Array(delimiter_array)) => {
            split_sql_array_array(string_array.as_ref(), delimiter_array.as_ref())
        }
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(string)),
            ColumnarValue::Array(delimiter_array),
        ) => split_sql_scalar_array::<i32>(string.as_deref(), delimiter_array.as_ref()),
        (
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(string)),
            ColumnarValue::Array(delimiter_array),
        ) => split_sql_scalar_array::<i64>(string.as_deref(), delimiter_array.as_ref()),
        (ColumnarValue::Scalar(ScalarValue::Utf8(string)), ColumnarValue::Scalar(delimiter))
        | (
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(string)),
            ColumnarValue::Scalar(delimiter),
        ) => {
            if string.is_none() {
                return Ok(ColumnarValue::Scalar(new_null_list_scalar()));
            }

            let delimiter = match delimiter {
                ScalarValue::Utf8(Some(d)) | ScalarValue::LargeUtf8(Some(d)) => d,
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    return Ok(ColumnarValue::Scalar(new_null_list_scalar()));
                }
                _ => return exec_err!("split_sql delimiter must be a string"),
            };
            let string = string.clone().unwrap();

            let mut offsets_builder = BufferBuilder::<i32>::new(2);
            let mut values_builder = BufferBuilder::<u8>::new(string.len());

            offsets_builder.append(0);

            if delimiter.is_empty() {
                values_builder.append_slice(string.as_bytes());
                offsets_builder.append(string.len() as i32);
            } else {
                let mut offset = 0i32;
                for part in string.split(delimiter.as_str()) {
                    values_builder.append_slice(part.as_bytes());
                    offset += part.len() as i32;
                    offsets_builder.append(offset);
                }
            }

            let offsets_buffer = offsets_builder.finish();
            let values_buffer = values_builder.finish();

            let list_field = Arc::new(Field::new("item", DataType::Utf8, false));
            let values_array = Arc::new(StringArray::try_new(
                OffsetBuffer::new(offsets_buffer.into()),
                values_buffer,
                None,
            )?);

            let list_offsets =
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, values_array.len() as i32]));
            let list_array = ListArray::try_new(list_field, list_offsets, values_array, None)?;

            Ok(ColumnarValue::Array(Arc::new(list_array)))
        }
        _ => exec_err!("split_sql expects string arguments"),
    }
}

fn is_regex_literal(pattern: &str) -> bool {
    !pattern.chars().any(|c| {
        matches!(
            c,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        )
    })
}

#[inline]
fn push_split_literal<'a, O: OffsetSizeTrait>(
    string: &'a str,
    delimiter: &str,
    limit: i32,
    offsets: &mut BufferBuilder<O>,
    values: &mut BufferBuilder<u8>,
    scratch: &mut Vec<&'a str>,
) {
    if limit > 0 {
        let cap = (limit - 1) as usize;
        let mut last_end = 0;
        for (count, (start, _)) in string.match_indices(delimiter).enumerate() {
            if count >= cap {
                break;
            }
            append_str(&string[last_end..start], offsets, values);
            last_end = start + delimiter.len();
        }
        append_str(&string[last_end..], offsets, values);
    } else {
        for p in string.split(delimiter) {
            append_str(p, offsets, values);
        }
    }
}

#[inline]
fn push_split_char<'a, O: OffsetSizeTrait>(
    string: &'a str,
    delimiter: char,
    limit: i32,
    offsets: &mut BufferBuilder<O>,
    values: &mut BufferBuilder<u8>,
    scratch: &mut Vec<&'a str>,
) {
    if limit > 0 {
        let cap = (limit - 1) as usize;
        let mut last_end = 0;
        for (count, (start, _)) in string.match_indices(delimiter).enumerate() {
            if count >= cap {
                break;
            }
            append_str(&string[last_end..start], offsets, values);
            last_end = start + delimiter.len_utf8();
        }
        append_str(&string[last_end..], offsets, values);
    } else {
        // limit < 0
        for p in string.split(delimiter) {
            append_str(p, offsets, values);
        }
    }
}

fn split_generic_literal<O: OffsetSizeTrait>(
    string_array: &GenericStringArray<O>,
    pattern: &str,
    limit: i32,
) -> DataFusionResult<ColumnarValue> {
    let len = string_array.len();
    let mut list_offsets: Vec<O> = Vec::with_capacity(len + 1);

    let estimated_items = (len * 4).max(16);
    let bytes_capacity = string_array.value_data().len();

    let mut str_offsets = BufferBuilder::<O>::new(estimated_items + 1);
    let mut str_values = BufferBuilder::<u8>::new(bytes_capacity);
    str_offsets.append(O::usize_as(0));

    let mut scratch = Vec::new();
    list_offsets.push(O::usize_as(0));

    let mut chars = pattern.chars();
    let single_char = match (chars.next(), chars.next()) {
        (Some(ch), None) => Some(ch),
        _ => None,
    };

    if let Some(ch) = single_char {
        for i in 0..len {
            if !string_array.is_null(i) {
                let s = string_array.value(i);
                push_split_char(
                    s,
                    ch,
                    limit,
                    &mut str_offsets,
                    &mut str_values,
                    &mut scratch,
                );
            }
            list_offsets.push(O::usize_as(str_offsets.len() - 1));
        }
    } else {
        for i in 0..len {
            if !string_array.is_null(i) {
                let s = string_array.value(i);
                push_split_literal(
                    s,
                    pattern,
                    limit,
                    &mut str_offsets,
                    &mut str_values,
                    &mut scratch,
                );
            }
            list_offsets.push(O::usize_as(str_offsets.len() - 1));
        }
    }

    let item_offsets_buffer = OffsetBuffer::new(str_offsets.finish().into());
    let item_values_buffer = str_values.finish();

    // SAFETY: every byte appended to `values` comes from a `&str` of the input
    // (via `append_str`), so the value buffer is valid UTF-8. Offsets start at 0
    // and only ever advance by the byte length of the appended part, so they are
    // monotonically non-decreasing, which satisfies the invariants checked by
    // `OffsetBuffer::new` / `GenericStringArray::new_unchecked`. Nullability is
    // `None`: nullability of rows is carried by the wrapping list array.
    let string_array_values = unsafe {
        GenericStringArray::<O>::new_unchecked(item_offsets_buffer, item_values_buffer, None)
    };
    let values_array = Arc::new(string_array_values) as ArrayRef;

    let item_type = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };
    let field = Arc::new(Field::new("item", item_type, false));
    let list_array = GenericListArray::<O>::new(
        field,
        OffsetBuffer::new(list_offsets.into()),
        values_array,
        string_array.nulls().cloned(),
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

fn split_array(
    string_array: &dyn Array,
    pattern: &str,
    limit: i32,
) -> DataFusionResult<ColumnarValue> {
    let is_literal = is_regex_literal(pattern);
    match string_array.data_type() {
        DataType::Utf8 => {
            let string_array = as_generic_string_array::<i32>(string_array)?;
            if is_literal {
                split_generic_literal::<i32>(string_array, pattern, limit)
            } else {
                let regex = Regex::new(pattern).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Invalid regex pattern '{}': {}",
                        pattern, e
                    ))
                })?;
                split_generic::<i32>(string_array, &regex, limit)
            }
        }
        DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<i64>(string_array)?;
            if is_literal {
                split_generic_literal::<i64>(string_array, pattern, limit)
            } else {
                let regex = Regex::new(pattern).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Invalid regex pattern '{}': {}",
                        pattern, e
                    ))
                })?;
                split_generic::<i64>(string_array, &regex, limit)
            }
        }
        _ => exec_err!(
            "split expects Utf8 or LargeUtf8 string array, got {:?}",
            string_array.data_type()
        ),
    }
}

fn split_sql_array_scalar(
    string_array: &dyn arrow::array::Array,
    delimiter: &str,
) -> DataFusionResult<ColumnarValue> {
    match string_array.data_type() {
        DataType::Utf8 => split_sql_generic_scalar::<i32>(
            as_generic_string_array::<i32>(string_array)?,
            delimiter,
        ),
        DataType::LargeUtf8 => split_sql_generic_scalar::<i64>(
            as_generic_string_array::<i64>(string_array)?,
            delimiter,
        ),
        _ => exec_err!(
            "split_sql expects Utf8 or LargeUtf8 string array, got {:?}",
            string_array.data_type()
        ),
    }
}

fn split_sql_array_array(
    string_array: &dyn arrow::array::Array,
    delimiter_array: &dyn arrow::array::Array,
) -> DataFusionResult<ColumnarValue> {
    if string_array.len() != delimiter_array.len() {
        return exec_err!(
            "split_sql string and delimiter arrays must have the same length, got {} and {}",
            string_array.len(),
            delimiter_array.len()
        );
    }

    match (string_array.data_type(), delimiter_array.data_type()) {
        (DataType::Utf8, DataType::Utf8) => split_sql_generic_array::<i32, i32>(
            as_generic_string_array::<i32>(string_array)?,
            as_generic_string_array::<i32>(delimiter_array)?,
        ),
        (DataType::Utf8, DataType::LargeUtf8) => split_sql_generic_array::<i32, i64>(
            as_generic_string_array::<i32>(string_array)?,
            as_generic_string_array::<i64>(delimiter_array)?,
        ),
        (DataType::LargeUtf8, DataType::Utf8) => split_sql_generic_array::<i64, i32>(
            as_generic_string_array::<i64>(string_array)?,
            as_generic_string_array::<i32>(delimiter_array)?,
        ),
        (DataType::LargeUtf8, DataType::LargeUtf8) => split_sql_generic_array::<i64, i64>(
            as_generic_string_array::<i64>(string_array)?,
            as_generic_string_array::<i64>(delimiter_array)?,
        ),
        _ => exec_err!(
            "split_sql expects Utf8 or LargeUtf8 string arrays, got {:?} and {:?}",
            string_array.data_type(),
            delimiter_array.data_type()
        ),
    }
}

fn split_sql_scalar_array<O: OffsetSizeTrait>(
    string: Option<&str>,
    delimiter_array: &dyn arrow::array::Array,
) -> DataFusionResult<ColumnarValue> {
    let Some(string) = string else {
        return Ok(ColumnarValue::Array(new_null_list_array_with_offset::<O>(
            delimiter_array.len(),
        )));
    };

    match delimiter_array.data_type() {
        DataType::Utf8 => split_sql_generic_scalar_array::<O, i32>(
            string,
            as_generic_string_array::<i32>(delimiter_array)?,
        ),
        DataType::LargeUtf8 => split_sql_generic_scalar_array::<O, i64>(
            string,
            as_generic_string_array::<i64>(delimiter_array)?,
        ),
        _ => exec_err!(
            "split_sql expects Utf8 or LargeUtf8 delimiter array, got {:?}",
            delimiter_array.data_type()
        ),
    }
}

fn split_generic<O: OffsetSizeTrait>(
    string_array: &GenericStringArray<O>,
    regex: &Regex,
    limit: i32,
) -> DataFusionResult<ColumnarValue> {
    let len = string_array.len();
    let mut list_offsets: Vec<O> = Vec::with_capacity(len + 1);

    let estimated_items = (len * 4).max(16);
    let bytes_capacity = string_array.value_data().len();

    let mut str_offsets = BufferBuilder::<O>::new(estimated_items + 1);
    let mut str_values = BufferBuilder::<u8>::new(bytes_capacity);
    str_offsets.append(O::usize_as(0));

    let mut scratch = Vec::new();
    list_offsets.push(O::usize_as(0));

    for i in 0..len {
        if !string_array.is_null(i) {
            let s = string_array.value(i);
            push_split_parts(
                s,
                regex,
                limit,
                &mut str_offsets,
                &mut str_values,
                &mut scratch,
            );
        }
        list_offsets.push(O::usize_as(str_offsets.len() - 1));
    }

    let item_offsets_buffer = OffsetBuffer::new(str_offsets.finish().into());
    let item_values_buffer = str_values.finish();

    // SAFETY: every byte appended to `values` comes from a `&str` of the input
    // (via `append_str`), so the value buffer is valid UTF-8. Offsets start at 0
    // and only ever advance by the byte length of the appended part, so they are
    // monotonically non-decreasing, which satisfies the invariants checked by
    // `OffsetBuffer::new` / `GenericStringArray::new_unchecked`. Nullability is
    // `None`: nullability of rows is carried by the wrapping list array.
    let string_array_values = unsafe {
        GenericStringArray::<O>::new_unchecked(item_offsets_buffer, item_values_buffer, None)
    };
    let values_array = Arc::new(string_array_values) as ArrayRef;

    let item_type = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };
    let field = Arc::new(Field::new("item", item_type, false));
    let list_array = GenericListArray::<O>::new(
        field,
        OffsetBuffer::new(list_offsets.into()),
        values_array,
        string_array.nulls().cloned(),
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

fn split_sql_generic_scalar<O: OffsetSizeTrait>(
    string_array: &GenericStringArray<O>,
    delimiter: &str,
) -> DataFusionResult<ColumnarValue> {
    let len = string_array.len();
    let mut offsets: Vec<O> = Vec::with_capacity(len + 1);

    let estimated_items = (len * 4).max(16);
    let bytes_capacity = string_array.value_data().len();
    let mut values_builder =
        GenericStringBuilder::<O>::with_capacity(estimated_items, bytes_capacity);

    offsets.push(O::usize_as(0));

    for i in 0..len {
        if !string_array.is_null(i) {
            push_split_sql_parts(string_array.value(i), delimiter, &mut values_builder);
        }
        offsets.push(O::usize_as(values_builder.len()));
    }

    let values_array = Arc::new(values_builder.finish()) as ArrayRef;
    let item_type = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };
    let field = Arc::new(Field::new("item", item_type, false));
    let list_array = GenericListArray::<O>::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        string_array.nulls().cloned(),
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

fn split_sql_generic_scalar_array<O: OffsetSizeTrait, D: OffsetSizeTrait>(
    string: &str,
    delimiter_array: &GenericStringArray<D>,
) -> DataFusionResult<ColumnarValue> {
    let len = delimiter_array.len();
    let mut offsets: Vec<O> = Vec::with_capacity(len + 1);

    let estimated_items = (len * 4).max(16);
    let bytes_capacity = string.len() * len;
    let mut values_builder =
        GenericStringBuilder::<O>::with_capacity(estimated_items, bytes_capacity);

    let mut nulls = NullBufferBuilder::new(len);
    offsets.push(O::usize_as(0));

    for i in 0..len {
        if delimiter_array.is_null(i) {
            nulls.append_null();
        } else {
            push_split_sql_parts(string, delimiter_array.value(i), &mut values_builder);
            nulls.append_non_null();
        }
        offsets.push(O::usize_as(values_builder.len()));
    }

    let values_array = Arc::new(values_builder.finish()) as ArrayRef;
    let item_type = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };
    let field = Arc::new(Field::new("item", item_type, false));
    let list_array = GenericListArray::<O>::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        nulls.finish(),
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

fn split_sql_generic_array<O: OffsetSizeTrait, D: OffsetSizeTrait>(
    string_array: &GenericStringArray<O>,
    delimiter_array: &GenericStringArray<D>,
) -> DataFusionResult<ColumnarValue> {
    let len = string_array.len();
    let mut offsets: Vec<O> = Vec::with_capacity(len + 1);

    let estimated_items = (len * 4).max(16);
    let bytes_capacity = string_array.value_data().len();
    let mut values_builder =
        GenericStringBuilder::<O>::with_capacity(estimated_items, bytes_capacity);

    let mut nulls = NullBufferBuilder::new(len);
    offsets.push(O::usize_as(0));

    for i in 0..len {
        if string_array.is_null(i) || delimiter_array.is_null(i) {
            nulls.append_null();
        } else {
            push_split_sql_parts(
                string_array.value(i),
                delimiter_array.value(i),
                &mut values_builder,
            );
            nulls.append_non_null();
        }
        offsets.push(O::usize_as(values_builder.len()));
    }

    let values_array = Arc::new(values_builder.finish()) as ArrayRef;
    let item_type = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };
    let field = Arc::new(Field::new("item", item_type, false));
    let list_array = GenericListArray::<O>::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        nulls.finish(),
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

#[inline]
fn append_str<O: OffsetSizeTrait>(
    s: &str,
    offsets: &mut BufferBuilder<O>,
    values: &mut BufferBuilder<u8>,
) {
    values.append_slice(s.as_bytes());
    offsets.append(O::usize_as(values.len()));
}

#[inline]
fn push_split_parts<'a, O: OffsetSizeTrait>(
    string: &'a str,
    regex: &Regex,
    limit: i32,
    offsets: &mut BufferBuilder<O>,
    values: &mut BufferBuilder<u8>,
    scratch: &mut Vec<&'a str>,
) {
    if limit > 0 {
        let mut last_end = 0;
        let cap = (limit - 1) as usize;
        for (count, mat) in regex.find_iter(string).enumerate() {
            if count >= cap {
                break;
            }
            append_str(&string[last_end..mat.start()], offsets, values);
            last_end = mat.end();
        }
        append_str(&string[last_end..], offsets, values);
    } else {
        for p in regex.split(string) {
            append_str(p, offsets, values);
        }
    }
}

fn push_split_sql_parts<O: OffsetSizeTrait>(
    string: &str,
    delimiter: &str,
    builder: &mut GenericStringBuilder<O>,
) {
    if delimiter.is_empty() {
        builder.append_value(string);
    } else {
        for p in string.split(delimiter) {
            builder.append_value(p);
        }
    }
}

fn create_list_array(values: ArrayRef) -> ListArray {
    let field = Arc::new(Field::new("item", DataType::Utf8, false));
    let offsets = vec![0i32, values.len() as i32];
    ListArray::new(field, OffsetBuffer::new(offsets.into()), values, None)
}

fn new_null_list_array(len: usize) -> ArrayRef {
    Arc::new(new_null_list_array_value(len))
}

fn new_null_list_scalar() -> ScalarValue {
    ScalarValue::List(Arc::new(new_null_list_array_value(1)))
}

fn new_null_list_array_with_offset<O: OffsetSizeTrait>(len: usize) -> ArrayRef {
    let item_type = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };
    let field = Arc::new(Field::new("item", item_type, false));
    let values = Arc::new(GenericStringArray::<O>::from(Vec::<String>::new())) as ArrayRef;
    let offsets = vec![O::usize_as(0); len + 1];
    let nulls = arrow::buffer::NullBuffer::new_null(len);

    Arc::new(GenericListArray::<O>::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        Some(nulls),
    ))
}

fn new_null_list_array_value(len: usize) -> ListArray {
    let field = Arc::new(Field::new("item", DataType::Utf8, false));
    let values = Arc::new(GenericStringArray::<i32>::from(Vec::<String>::new())) as ArrayRef;
    let offsets = vec![0i32; len + 1];
    let nulls = arrow::buffer::NullBuffer::new_null(len);

    ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        Some(nulls),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_split_basic() {
        let string_array = Arc::new(StringArray::from(vec!["a,b,c", "x,y,z"])) as ArrayRef;
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let args = vec![ColumnarValue::Array(string_array), pattern];

        let result = spark_split(&args).unwrap();
        // Should produce [["a", "b", "c"], ["x", "y", "z"]]
        assert!(matches!(result, ColumnarValue::Array(_)));
    }

    #[test]
    fn test_split_with_limit() {
        let string_array = Arc::new(StringArray::from(vec!["a,b,c,d"])) as ArrayRef;
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let limit = ColumnarValue::Scalar(ScalarValue::Int32(Some(2)));
        let args = vec![ColumnarValue::Array(string_array), pattern, limit];

        let result = spark_split(&args).unwrap();
        // Should produce [["a", "b,c,d"]]
        assert!(matches!(result, ColumnarValue::Array(_)));
    }

    #[test]
    fn test_split_with_nulls() {
        // Test that NULL inputs produce NULL outputs (not empty arrays)
        let string_array = Arc::new(StringArray::from(vec![
            Some("a,b,c"),
            None,
            Some("x,y"),
            None,
        ])) as ArrayRef;
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let args = vec![ColumnarValue::Array(string_array), pattern];

        let result = spark_split(&args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_array.len(), 4);
                // First row: valid ["a", "b", "c"]
                assert!(!list_array.is_null(0));
                // Second row: NULL
                assert!(list_array.is_null(1));
                // Third row: valid ["x", "y"]
                assert!(!list_array.is_null(2));
                // Fourth row: NULL
                assert!(list_array.is_null(3));
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[test]
    fn test_split_sql_scalar_nulls_return_typed_null_list() {
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let result = spark_split_sql(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            delimiter.clone(),
        ])
        .unwrap();
        assert_null_list_scalar(result);

        let result = spark_split_sql(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a,b".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ])
        .unwrap();
        assert_null_list_scalar(result);
    }

    #[test]
    fn test_split_sql_scalar_string_array_delimiter() {
        let delimiter_array =
            Arc::new(StringArray::from(vec![Some("||"), Some("."), None])) as ArrayRef;
        let result = spark_split_sql(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a||b||".to_string()))),
            ColumnarValue::Array(delimiter_array),
        ])
        .unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_array.len(), 3);
                assert_list_value(list_array, 0, &["a", "b", ""]);
                assert_list_value(list_array, 1, &["a||b||"]);
                assert!(list_array.is_null(2));
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[test]
    fn test_split_sql_null_scalar_string_array_delimiter() {
        let delimiter_array = Arc::new(StringArray::from(vec![Some(","), Some(".")])) as ArrayRef;
        let result = spark_split_sql(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ColumnarValue::Array(delimiter_array),
        ])
        .unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_array.len(), 2);
                assert!(list_array.is_null(0));
                assert!(list_array.is_null(1));
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[test]
    fn test_split_sql_empty_delimiter_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello world".to_string())));
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string())));

        let result = spark_split_sql(&[input, delimiter])
            .unwrap()
            .into_array(1)
            .unwrap();
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 1);
        let values = list_array.value(0);
        let str_array = values.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(str_array.len(), 1);
        assert_eq!(str_array.value(0), "hello world");
    }

    #[test]
    fn test_split_sql_empty_string_and_empty_delimiter_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string())));
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string())));

        let result = spark_split_sql(&[input, delimiter])
            .unwrap()
            .into_array(1)
            .unwrap();
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 1);
        let values = list_array.value(0);
        let str_array = values.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(str_array.len(), 1);
        assert_eq!(str_array.value(0), "");
    }

    #[test]
    fn test_split_sql_scalar_empty_delimiter_keeps_whole_string() {
        // Spark semantics: an empty delimiter must NOT split into characters.
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string()))),
        ];
        let result = spark_split_sql(&args).unwrap();

        let list = match result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            ColumnarValue::Scalar(ScalarValue::List(list)) => (*list).clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        let first = list.value(0);
        let items = first
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("expected Utf8 items");

        assert_eq!(items.len(), 1, "empty delimiter must not split into chars");
        assert_eq!(items.value(0), "abc");
    }

    #[test]
    fn test_split_sql_scalar_empty_delimiter_empty_string() {
        // Empty input with an empty delimiter -> [""], matching the array path.
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string()))),
        ];
        let result = spark_split_sql(&args).unwrap();

        let list = match result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            ColumnarValue::Scalar(ScalarValue::List(list)) => (*list).clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        let first = list.value(0);
        let items = first
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("expected Utf8 items");

        assert_eq!(items.len(), 1);
        assert_eq!(items.value(0), "");
    }

    #[test]
    fn test_split_sql_empty_delimiter_scalar_array_parity() {
        // Scalar and array inputs must give identical results for the
        // empty-delimiter case: the whole string as a single element.
        let strings = vec!["abc", "", "hello world"];

        let array_args = vec![
            ColumnarValue::Array(Arc::new(StringArray::from(strings.clone()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string()))),
        ];
        let array_result = spark_split_sql(&array_args).unwrap();
        let array_list = match array_result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        for (row, s) in strings.iter().enumerate() {
            let item = array_list.value(row);
            let items = item
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .expect("expected Utf8 items");
            assert_eq!(items.len(), 1, "row {}: expected single element", row);
            assert_eq!(items.value(0), *s, "row {}", row);
        }

        let scalar_args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string()))),
        ];
        let scalar_result = spark_split_sql(&scalar_args).unwrap();
        let scalar_list = match scalar_result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            ColumnarValue::Scalar(ScalarValue::List(list)) => (*list).clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        let first = scalar_list.value(0);
        let items = first
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("expected Utf8 items");
        assert_eq!(items.len(), 1);
        assert_eq!(items.value(0), "abc");
    }

    #[test]
    fn test_split_sql_scalar_delimiter_still_splits_normally() {
        // Guard: the empty-delimiter handling must not affect normal delimiters.
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a,b,c".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
        ];
        let result = spark_split_sql(&args).unwrap();

        let list = match result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            ColumnarValue::Scalar(ScalarValue::List(list)) => (*list).clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        let first = list.value(0);
        let items = first
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("expected Utf8 items");

        assert_eq!(items.len(), 3);
        assert_eq!(items.value(0), "a");
        assert_eq!(items.value(1), "b");
        assert_eq!(items.value(2), "c");
    }

    #[test]
    fn test_split_sql_scalar_string_array_delimiter_with_empty_element() {
        // (Scalar string, Array delimiter): an empty delimiter element must
        // keep the whole string as one element, like the other branches.
        let delimiters = StringArray::from(vec![Some(""), Some(",")]);
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Array(Arc::new(delimiters)),
        ];
        let result = spark_split_sql(&args).unwrap();

        let list = match result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        assert_eq!(list.len(), 2);

        let first = list.value(0);
        let row0 = first
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("expected Utf8 items");
        assert_eq!(row0.len(), 1, "empty delimiter must not split into chars");
        assert_eq!(row0.value(0), "abc");

        let second = list.value(1);
        let row1 = second
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("expected Utf8 items");
        assert_eq!(row1.len(), 1);
        assert_eq!(row1.value(0), "abc");
    }

    #[test]
    fn test_split_sql_scalar_item_field_non_nullable() {
        // The scalar branch must produce the same List type as the array
        // branches: a non-nullable "item" field.
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
        ];
        let result = spark_split_sql(&args).unwrap();

        let list = match result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            ColumnarValue::Scalar(ScalarValue::List(list)) => (*list).clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        let item_field = match list.data_type() {
            DataType::List(field) | DataType::LargeList(field) => field.clone(),
            other => panic!("expected List type, got {:?}", other),
        };
        assert!(
            !item_field.is_nullable(),
            "scalar split_sql must keep the non-nullable item field"
        );

        // parity with the array path
        let array_args = vec![
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["abc"]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
        ];
        let array_result = spark_split_sql(&array_args).unwrap();
        let array_list = match array_result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .expect("expected ListArray")
                .clone(),
            other => panic!("unexpected result: {:?}", other.data_type()),
        };

        assert_eq!(
            list.data_type(),
            array_list.data_type(),
            "scalar and array split_sql must return the same List type"
        );
    }

    fn scalar_split_to_vec(s: &str, pattern: &str, limit: i32) -> Vec<String> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(limit))),
        ];
        match spark_split(&args).unwrap() {
            ColumnarValue::Scalar(ScalarValue::List(list)) => {
                let items = list.values();
                let items = items
                    .as_any()
                    .downcast_ref::<GenericStringArray<i32>>()
                    .expect("expected Utf8 items");
                (0..items.len())
                    .map(|i| items.value(i).to_string())
                    .collect()
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[test]
    fn test_split_regex_values() {
        assert_eq!(
            scalar_split_to_vec("foo123bar456baz", r"\d+", -1),
            vec!["foo", "bar", "baz"]
        );
    }

    #[test]
    fn test_split_limit_positive_values() {
        assert_eq!(
            scalar_split_to_vec("a,b,c,d,e", ",", 3),
            vec!["a", "b", "c,d,e"]
        );
    }

    #[test]
    fn test_split_limit_zero_keeps_trailing_empties_like_spark() {
        // Spark remaps limit == 0 to -1: trailing empty strings are kept.
        assert_eq!(
            scalar_split_to_vec("a,b,c,,", ",", 0),
            vec!["a", "b", "c", "", ""]
        );
    }

    #[test]
    fn test_split_limit_negative_values() {
        assert_eq!(
            scalar_split_to_vec("a,b,c,,", ",", -1),
            vec!["a", "b", "c", "", ""]
        );
    }

    #[test]
    fn test_split_empty_string_values() {
        assert_eq!(scalar_split_to_vec("", ",", -1), vec![""]);
    }

    #[test]
    fn test_split_multibyte_delimiter_values() {
        assert_eq!(scalar_split_to_vec("a→b→c", "→", -1), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_literal_and_regex_helpers_agree() {
        let regex = Regex::new(",").unwrap();
        for s in ["a,b,c,,", "", ",,,", "abc", "привет,мир"] {
            for limit in [-1i32, 0, 2] {
                let mut lit_off = BufferBuilder::<i32>::new(16);
                let mut lit_val = BufferBuilder::<u8>::new(64);
                let mut rx_off = BufferBuilder::<i32>::new(16);
                let mut rx_val = BufferBuilder::<u8>::new(64);
                let mut scratch: Vec<&str> = Vec::new();

                push_split_literal(s, ",", limit, &mut lit_off, &mut lit_val, &mut scratch);
                push_split_parts(s, &regex, limit, &mut rx_off, &mut rx_val, &mut scratch);

                // Same input must produce byte-identical Arrow buffers.
                assert_eq!(lit_off.finish().as_slice(), rx_off.finish().as_slice());
                assert_eq!(lit_val.finish().as_slice(), rx_val.finish().as_slice());
            }
        }
    }

    fn assert_list_value(list_array: &ListArray, row: usize, expected: &[&str]) {
        let value = list_array.value(row);
        let strings = value.as_any().downcast_ref::<StringArray>().unwrap();
        let actual = strings.iter().collect::<Vec<_>>();
        let expected = expected.iter().map(|s| Some(*s)).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    fn assert_null_list_scalar(result: ColumnarValue) {
        match result {
            ColumnarValue::Scalar(ScalarValue::List(array)) => {
                assert_eq!(array.len(), 1);
                assert!(array.is_null(0));
            }
            _ => panic!("Expected typed null list scalar, got {result:?}"),
        }
    }
}
