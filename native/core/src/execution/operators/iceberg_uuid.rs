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

//! Iceberg UUID strings use Java's `UUID.fromString` semantics, including its permissive
//! five-group fallback. Only Iceberg UUID fields use this conversion; `fixed(16)` stays binary.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeBinaryBuilder, ListArray, MapArray, StructArray,
};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{PrimitiveType, Schema, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Java parses UTF-16 code units, so supplementary-plane digits are deliberately excluded.
/// This table is pinned against Character.digit(char, 16) on JDK 17 and 21 over the entire BMP.
fn java_hex_digit(c: char) -> Option<u64> {
    match c {
        '0'..='9' => Some(c as u64 - '0' as u64),
        'a'..='f' => Some(c as u64 - 'a' as u64 + 10),
        'A'..='F' => Some(c as u64 - 'A' as u64 + 10),
        '\u{ff21}'..='\u{ff26}' => Some(c as u64 - 0xff21 + 10),
        '\u{ff41}'..='\u{ff46}' => Some(c as u64 - 0xff41 + 10),
        _ => {
            const ZEROS: &[u32] = &[
                0x660, 0x6f0, 0x7c0, 0x966, 0x9e6, 0xa66, 0xae6, 0xb66, 0xbe6, 0xc66, 0xce6, 0xd66,
                0xde6, 0xe50, 0xed0, 0xf20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80,
                0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0,
                0xaa50, 0xabf0, 0xff10,
            ];
            let value = c as u32;
            ZEROS.iter().find_map(|&zero| {
                (value >= zero && value < zero + 10).then(|| u64::from(value - zero))
            })
        }
    }
}

fn parse_java_uuid(value: &str) -> Option<[u8; 16]> {
    // Avoid the group parser and Unicode handling for canonical ASCII strings. parse_str alone
    // also accepts forms Java rejects, so require the canonical dash positions first.
    let bytes = value.as_bytes();
    if bytes.len() == 36 && [8, 13, 18, 23].iter().all(|&i| bytes[i] == b'-') {
        if let Ok(uuid) = uuid::Uuid::parse_str(value) {
            return Some(*uuid.as_bytes());
        }
    }
    if value.encode_utf16().take(37).count() > 36 {
        return None;
    }
    let mut groups = value.split('-');
    let mut parts = [0u64; 5];
    for part in &mut parts {
        let group = groups.next()?;
        let digits = group.strip_prefix('+').unwrap_or(group);
        if digits.is_empty() {
            return None;
        }
        let mut number = 0u64;
        for c in digits.chars() {
            number = number.checked_mul(16)?.checked_add(java_hex_digit(c)?)?;
            // Long.parseLong rejects signed overflow *before* UUID masks the group width.
            if number > i64::MAX as u64 {
                return None;
            }
        }
        *part = number;
    }
    if groups.next().is_some() {
        return None;
    }
    let most = ((parts[0] & 0xffff_ffff) << 32) | ((parts[1] & 0xffff) << 16) | (parts[2] & 0xffff);
    let least = ((parts[3] & 0xffff) << 48) | (parts[4] & 0xffff_ffff_ffff);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&most.to_be_bytes());
    out[8..].copy_from_slice(&least.to_be_bytes());
    Some(out)
}

/// Constructed once per task. Subtrees without UUID retain the ordinary Arrow cast path.
pub(super) struct UuidAlignment(Vec<Conversion>);

enum Conversion {
    Cast,
    Uuid,
    Struct(Vec<Conversion>),
    List(Box<Conversion>),
    Map(Box<Conversion>),
}

impl UuidAlignment {
    pub(super) fn new(schema: &Schema, target: &SchemaRef) -> Self {
        Self(
            target
                .fields()
                .iter()
                .map(|f| Conversion::new(schema, f))
                .collect(),
        )
    }

    pub(super) fn convert(&self, batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
        if batch.num_columns() != self.0.len() {
            return Err(DataFusionError::Execution(
                "Iceberg write column count mismatch".into(),
            ));
        }
        let columns = batch
            .columns()
            .iter()
            .zip(target.fields())
            .zip(&self.0)
            .map(|((array, field), conversion)| conversion.convert(array, field, None))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(Arc::clone(target), columns)?)
    }
}

impl Conversion {
    fn new(schema: &Schema, field: &Field) -> Self {
        if field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id| id.parse().ok())
            .and_then(|id| schema.field_by_id(id))
            .is_some_and(|f| *f.field_type == Type::Primitive(PrimitiveType::Uuid))
        {
            return Self::Uuid;
        }
        let result = match field.data_type() {
            DataType::Struct(fields) => {
                Self::Struct(fields.iter().map(|f| Self::new(schema, f)).collect())
            }
            DataType::List(element) => Self::List(Box::new(Self::new(schema, element))),
            DataType::Map(entries, _) => Self::Map(Box::new(Self::new(schema, entries))),
            _ => Self::Cast,
        };
        if result.has_uuid() {
            result
        } else {
            Self::Cast
        }
    }

    fn has_uuid(&self) -> bool {
        match self {
            Self::Cast => false,
            Self::Uuid => true,
            Self::Struct(fields) => fields.iter().any(Self::has_uuid),
            Self::List(element) | Self::Map(element) => element.has_uuid(),
        }
    }

    fn convert(
        &self,
        array: &ArrayRef,
        target: &Field,
        visible: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let cast = |ty| {
            arrow::compute::cast_with_options(
                array,
                ty,
                &arrow::compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
        };
        let type_error = || {
            DataFusionError::Execution(format!(
                "Iceberg field {} expected a container matching {}, got {}",
                target.name(),
                target.data_type(),
                array.data_type()
            ))
        };
        match self {
            Self::Cast => Ok(cast(target.data_type())?),
            Self::Uuid => {
                // FFI normally supplies Utf8 (possibly dictionary-encoded). Arrow also knows how
                // to normalize LargeUtf8 and Utf8View without changing string contents.
                let strings = cast(&DataType::Utf8)?;
                let strings = strings.as_string::<i32>();
                let mut out = FixedSizeBinaryBuilder::with_capacity(array.len(), 16);
                for row in 0..array.len() {
                    if visible.is_some_and(|v| !v[row]) {
                        // Invisible required children must remain valid Arrow values. They are
                        // never written, and must not cause errors by parsing arbitrary bytes.
                        out.append_value([0; 16])?;
                    } else if strings.is_null(row) {
                        out.append_null();
                    } else {
                        let bytes = parse_java_uuid(strings.value(row)).ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Invalid UUID string in Iceberg field {} at batch row {row}",
                                target.name()
                            ))
                        })?;
                        out.append_value(bytes)?;
                    }
                }
                Ok(Arc::new(out.finish()))
            }
            Self::Struct(conversions) => {
                let source = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(type_error)?;
                let DataType::Struct(fields) = target.data_type() else {
                    unreachable!()
                };
                if source.num_columns() != fields.len() {
                    return Err(DataFusionError::Execution(
                        "Iceberg struct field count mismatch".into(),
                    ));
                }
                let active: Vec<bool> = (0..array.len())
                    .map(|i| source.is_valid(i) && visible.is_none_or(|v| v[i]))
                    .collect();
                let columns = conversions
                    .iter()
                    .zip(source.columns())
                    .zip(fields)
                    .map(|((conversion, column), field)| {
                        conversion.convert(column, field, Some(&active))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(StructArray::try_new(
                    fields.clone(),
                    columns,
                    source.nulls().cloned(),
                )?))
            }
            Self::List(element) => {
                let source = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(type_error)?;
                let DataType::List(field) = target.data_type() else {
                    unreachable!()
                };
                let active = child_visibility(
                    array,
                    visible,
                    source.value_offsets(),
                    source.values().len(),
                );
                let values = element.convert(source.values(), field, Some(&active))?;
                Ok(Arc::new(ListArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )?))
            }
            Self::Map(entries) => {
                let source = array
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(type_error)?;
                let DataType::Map(field, ordered) = target.data_type() else {
                    unreachable!()
                };
                let active = child_visibility(
                    array,
                    visible,
                    source.value_offsets(),
                    source.entries().len(),
                );
                let values = entries.convert(
                    &(Arc::new(source.entries().clone()) as ArrayRef),
                    field,
                    Some(&active),
                )?;
                Ok(Arc::new(MapArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values.as_struct().clone(),
                    source.nulls().cloned(),
                    *ordered,
                )?))
            }
        }
    }
}

fn child_visibility(
    array: &ArrayRef,
    visible: Option<&[bool]>,
    offsets: &[i32],
    len: usize,
) -> Vec<bool> {
    let mut active = vec![false; len];
    for row in 0..array.len() {
        if array.is_valid(row) && visible.is_none_or(|v| v[row]) {
            active[offsets[row] as usize..offsets[row + 1] as usize].fill(true);
        }
    }
    active
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, DictionaryArray, Int32Array, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Int32Type, Schema as ArrowSchema};
    use iceberg::spec::{ListType, MapType, NestedField, StructType};

    const UUID: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

    fn uuid_type() -> Type {
        Type::Primitive(PrimitiveType::Uuid)
    }

    fn alignment(
        schema: Schema,
        columns: Vec<ArrayRef>,
    ) -> (UuidAlignment, RecordBatch, SchemaRef) {
        let target = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema).unwrap());
        let fields: Vec<_> = target
            .fields()
            .iter()
            .zip(&columns)
            .map(|(f, c)| Field::new(f.name(), c.data_type().clone(), f.is_nullable()))
            .collect();
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
        (UuidAlignment::new(&schema, &target), batch, target)
    }

    fn check_uuid(array: &ArrayRef, row: usize, value: &str) {
        let array = array.as_fixed_size_binary();
        assert_eq!(array.value(row), parse_java_uuid(value).unwrap());
    }

    #[test]
    fn uuid_flat_strings_nulls_empty_and_fixed16() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "u", uuid_type()).into(),
                NestedField::required(2, "fixed", Type::Primitive(PrimitiveType::Fixed(16))).into(),
            ])
            .build()
            .unwrap();
        let fixed: ArrayRef = Arc::new(BinaryArray::from_vec(vec![&[0xff; 16]; 3]));
        for strings in [
            Arc::new(StringArray::from(vec![Some("1-1-1-1-1"), Some(UUID), None])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some("1-1-1-1-1"),
                Some(UUID),
                None,
            ])) as ArrayRef,
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(0), Some(1), None]),
                    Arc::new(StringArray::from(vec!["1-1-1-1-1", UUID])),
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(StringViewArray::from(vec![
                Some("1-1-1-1-1"),
                Some(UUID),
                None,
            ])) as ArrayRef,
        ] {
            let (convert, batch, target) =
                alignment(schema.clone(), vec![strings, Arc::clone(&fixed)]);
            let result = convert.convert(batch.clone(), &target).unwrap();
            check_uuid(result.column(0), 0, "1-1-1-1-1");
            check_uuid(result.column(0), 1, UUID);
            assert!(result.column(0).is_null(2));
            assert_eq!(
                result.column(1).as_fixed_size_binary().value(0),
                &[0xff; 16]
            );
            assert!(matches!(convert.0[1], Conversion::Cast));
            assert_eq!(result.schema(), target);
            let empty = convert.convert(batch.slice(0, 0), &target).unwrap();
            assert_eq!(empty.num_rows(), 0);
        }
        let (convert, batch, target) = alignment(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>; 3])),
                Arc::clone(&fixed),
            ],
        );
        assert_eq!(
            convert
                .convert(batch, &target)
                .unwrap()
                .column(0)
                .null_count(),
            3
        );
        let (convert, batch, target) = alignment(
            schema,
            vec![Arc::new(StringArray::from(vec!["", UUID, UUID])), fixed],
        );
        assert!(convert
            .convert(batch, &target)
            .unwrap_err()
            .to_string()
            .contains("Invalid UUID"));
    }

    #[test]
    fn uuid_required_child_under_null_struct() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "s",
                Type::Struct(StructType::new(vec![NestedField::required(
                    2,
                    "u",
                    uuid_type(),
                )
                .into()])),
            )
            .into()])
            .build()
            .unwrap();
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["garbage", UUID]));
        let fields = vec![Field::new("u", DataType::Utf8, false)].into();
        let source: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![strings],
            Some(NullBuffer::from(vec![false, true])),
        ));
        let (convert, batch, target) = alignment(schema, vec![source]);
        let result = convert.convert(batch.clone(), &target).unwrap();
        assert!(result.column(0).is_null(0));
        check_uuid(result.column(0).as_struct().column(0), 1, UUID);
        let sliced = convert.convert(batch.slice(1, 1), &target).unwrap();
        check_uuid(sliced.column(0).as_struct().column(0), 0, UUID);
    }

    #[test]
    fn uuid_lists_ignore_null_ranges_and_slice_exterior() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "l",
                Type::List(ListType::new(
                    NestedField::optional(2, "element", uuid_type()).into(),
                )),
            )
            .into()])
            .build()
            .unwrap();
        let source: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("element", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 1, 2, 4, 5].into()),
            Arc::new(StringArray::from(vec![
                Some("bad"),
                Some("bad"),
                Some(UUID),
                None,
                Some("bad"),
            ])),
            Some(NullBuffer::from(vec![true, false, true, true])),
        ));
        let (convert, batch, target) = alignment(schema, vec![source]);
        assert!(convert.convert(batch.clone(), &target).is_err());
        let result = convert.convert(batch.slice(1, 2), &target).unwrap();
        let list = result.column(0).as_list::<i32>();
        assert!(list.is_null(0));
        assert_eq!(list.value_offsets(), &[1, 2, 4]);
        check_uuid(list.values(), 2, UUID);
        assert!(list.values().is_null(3));
    }

    #[test]
    fn uuid_map_keys_values_and_null_entries() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "m",
                Type::Map(MapType::new(
                    NestedField::required(2, "key", uuid_type()).into(),
                    NestedField::optional(3, "value", uuid_type()).into(),
                )),
            )
            .into()])
            .build()
            .unwrap();
        let fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into();
        let entries = StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec!["bad", "1-1-1-1-1", "bad"])),
                Arc::new(StringArray::from(vec![Some("bad"), Some(UUID), None])),
            ],
            None,
        );
        let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        let source: ArrayRef = Arc::new(MapArray::new(
            field,
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            entries,
            Some(NullBuffer::from(vec![false, true, true])),
            false,
        ));
        let (convert, batch, target) = alignment(schema, vec![source]);
        let result = convert.convert(batch.slice(0, 2), &target).unwrap();
        let map = result.column(0).as_map();
        check_uuid(map.keys(), 1, "1-1-1-1-1");
        check_uuid(map.values(), 1, UUID);
        assert!(map.is_null(0));
    }

    #[test]
    fn uuid_map_nonzero_offset_skips_null_ranges_and_slice_exterior() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "m",
                Type::Map(MapType::new(
                    NestedField::required(2, "key", uuid_type()).into(),
                    NestedField::optional(3, "value", uuid_type()).into(),
                )),
            )
            .into()])
            .build()
            .unwrap();
        let fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into();
        let entries = StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec![
                    "bad prefix",
                    "1-1-1-1-1",
                    UUID,
                    "bad null map",
                    "bad suffix",
                ])),
                Arc::new(StringArray::from(vec![
                    Some("bad prefix"),
                    Some(UUID),
                    None,
                    Some("bad null map"),
                    Some("bad suffix"),
                ])),
            ],
            None,
        );
        let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        let source: ArrayRef = Arc::new(MapArray::new(
            field,
            OffsetBuffer::new(vec![0, 1, 3, 4, 5].into()),
            entries,
            Some(NullBuffer::from(vec![true, true, false, true])),
            false,
        ));
        let (convert, batch, target) = alignment(schema, vec![source]);
        for len in [1, 2] {
            let window = batch.slice(1, len);
            let result = convert.convert(window.clone(), &target).unwrap();
            let map = result.column(0).as_map();
            assert_eq!(map.value_offsets(), &([1, 3, 4][..=len]));
            assert_eq!(map.nulls(), window.column(0).nulls());
            check_uuid(map.keys(), 1, "1-1-1-1-1");
            check_uuid(map.keys(), 2, UUID);
            check_uuid(map.values(), 1, UUID);
            assert!(map.values().is_null(2));
            assert_eq!(result.schema(), target);
        }
        // Making either exterior row visible must restore the UUID error.
        for row in [0, 3] {
            assert!(convert
                .convert(batch.slice(row, 1), &target)
                .unwrap_err()
                .to_string()
                .contains("Invalid UUID"));
        }
    }

    #[test]
    fn uuid_nested_container_visibility_reaches_map_keys_and_required_values() {
        // s.items: list<struct<m: map<uuid, struct<u: required uuid, n: required int>>>>
        let value_type = Type::Struct(StructType::new(vec![
            NestedField::required(7, "u", uuid_type()).into(),
            NestedField::required(8, "n", Type::Primitive(PrimitiveType::Int)).into(),
        ]));
        let map_type = Type::Map(MapType::new(
            NestedField::required(5, "key", uuid_type()).into(),
            NestedField::optional(6, "value", value_type).into(),
        ));
        let element_type = Type::Struct(StructType::new(vec![NestedField::optional(
            4, "m", map_type,
        )
        .into()]));
        let list_type = Type::List(ListType::new(
            NestedField::optional(3, "element", element_type).into(),
        ));
        let schema = Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "s",
                Type::Struct(StructType::new(vec![NestedField::optional(
                    2, "items", list_type,
                )
                .into()])),
            )
            .into()])
            .build()
            .unwrap();

        let source = |visible_key: &str, visible_value: &str| -> ArrayRef {
            // Each collection points at one child, including NULL collections. Rows 0 and 7
            // are outside the selected window. Rows 1..5 each mask a different nesting level.
            let values = StructArray::new(
                vec![
                    Field::new("u", DataType::Utf8, false),
                    Field::new("n", DataType::Int32, false),
                ]
                .into(),
                vec![
                    Arc::new(StringArray::from(
                        vec!["bad"; 6]
                            .into_iter()
                            .chain([visible_value, "bad"])
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from_iter_values(10..18)),
                ],
                Some(NullBuffer::from(vec![
                    true, true, true, true, true, false, true, true,
                ])),
            );
            let entries = StructArray::new(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", values.data_type().clone(), true),
                ]
                .into(),
                vec![
                    Arc::new(StringArray::from(vec![
                        "bad",
                        "bad",
                        "bad",
                        "bad",
                        "bad",
                        UUID,
                        visible_key,
                        "bad",
                    ])),
                    Arc::new(values),
                ],
                None,
            );
            let map = MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new((0..=8).collect::<Vec<i32>>().into()),
                entries,
                Some(NullBuffer::from(vec![
                    true, true, true, true, false, true, true, true,
                ])),
                false,
            );
            let elements = StructArray::new(
                vec![Field::new("m", map.data_type().clone(), true)].into(),
                vec![Arc::new(map)],
                Some(NullBuffer::from(vec![
                    true, true, true, false, true, true, true, true,
                ])),
            );
            let list = ListArray::new(
                Arc::new(Field::new("element", elements.data_type().clone(), true)),
                OffsetBuffer::new((0..=8).collect::<Vec<i32>>().into()),
                Arc::new(elements),
                Some(NullBuffer::from(vec![
                    true, true, false, true, true, true, true, true,
                ])),
            );
            Arc::new(StructArray::new(
                vec![Field::new("items", list.data_type().clone(), true)].into(),
                vec![Arc::new(list)],
                Some(NullBuffer::from(vec![
                    true, false, true, true, true, true, true, true,
                ])),
            ))
        };
        let upper = UUID.to_uppercase();
        let (convert, batch, target) = alignment(schema.clone(), vec![source("1-1-1-1-1", &upper)]);
        let result = convert.convert(batch.slice(1, 6), &target).unwrap();
        let outer = result.column(0).as_struct();
        let list = outer.column(0).as_list::<i32>();
        let elements = list.values().as_struct();
        let map = elements.column(0).as_map();
        let values = map.values().as_struct();
        assert!(outer.is_null(0));
        assert!(list.is_null(1));
        assert!(elements.is_null(3));
        assert!(map.is_null(4));
        assert!(values.is_null(5));
        assert_eq!(list.value_offsets(), &[1, 2, 3, 4, 5, 6, 7]);
        check_uuid(map.keys(), 5, UUID);
        check_uuid(map.keys(), 6, "1-1-1-1-1");
        check_uuid(values.column(0), 6, UUID);
        assert_eq!(values.column(0).null_count(), 0);
        assert_eq!(
            values
                .column(1)
                .as_primitive::<Int32Type>()
                .values()
                .as_ref(),
            &[10, 11, 12, 13, 14, 15, 16, 17]
        );
        assert_eq!(result.schema(), target);

        // Hidden garbage is ignored, but an invalid visible key or required value still fails.
        for (key, value) in [("bad visible key", UUID), (UUID, "bad visible value")] {
            let (convert, batch, target) = alignment(schema.clone(), vec![source(key, value)]);
            assert!(convert
                .convert(batch.slice(1, 6), &target)
                .unwrap_err()
                .to_string()
                .contains("Invalid UUID"));
        }
    }

    #[test]
    fn uuid_free_subtrees_keep_cast_path() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "s",
                Type::Struct(StructType::new(vec![NestedField::optional(
                    2,
                    "text",
                    Type::Primitive(PrimitiveType::String),
                )
                .into()])),
            )
            .into()])
            .build()
            .unwrap();
        let fields = vec![Field::new("text", DataType::Utf8, true)].into();
        let source: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(StringArray::from(vec!["bad uuid"]))],
            None,
        ));
        let (convert, batch, target) = alignment(schema, vec![source]);
        assert!(matches!(convert.0[0], Conversion::Cast));
        assert!(convert.convert(batch, &target).is_ok());
    }

    #[test]
    fn uuid_java_oracle() {
        let mut cases = 0;
        for line in include_str!("testdata/uuid-java.tsv")
            .lines()
            .filter(|s| !s.starts_with('#'))
        {
            let (input, expected) = line.split_once('\t').unwrap();
            let value = String::from_utf8(hex::decode(input).unwrap()).unwrap();
            let actual = parse_java_uuid(&value)
                .map(hex::encode)
                .unwrap_or_else(|| "error".into());
            assert_eq!(actual, expected, "{value:?}");
            cases += 1;
        }
        assert!(cases > 2000);
    }

    #[test]
    fn uuid_java_hex_digits_entire_bmp() {
        let expected = include_str!("testdata/uuid-java-digits.tsv")
            .lines()
            .filter(|line| !line.starts_with('#'))
            .map(|line| {
                let (ch, digit) = line.split_once('\t').unwrap();
                (
                    u32::from_str_radix(ch, 16).unwrap(),
                    digit.parse::<u64>().unwrap(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        for code in 0..=0xffff {
            if let Some(ch) = char::from_u32(code) {
                assert_eq!(
                    java_hex_digit(ch),
                    expected.get(&code).copied(),
                    "U+{code:04X}"
                );
            }
        }
    }
}
