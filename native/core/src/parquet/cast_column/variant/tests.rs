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
use super::*;
use arrow::array::{
    Decimal128Array, DictionaryArray, FixedSizeListArray, Int32Array, Int64Array,
    TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{Field, Fields, Int32Type};
use parquet::variant::{VariantDecimal16, VariantType};

fn unicode_object_keys() -> Vec<String> {
    let mut keys = (0..30).map(|i| format!("k{i:02}")).collect::<Vec<_>>();
    keys.push("\u{e000}".to_string());
    keys.push("😀".to_string());
    keys
}

fn assert_spark_unicode_variant(variant: Variant<'_, '_>) {
    let Variant::Object(object) = variant else {
        panic!("expected object")
    };
    let fields = object.iter().collect::<Vec<_>>();

    assert_eq!(fields.len(), 32);
    assert_eq!(fields[30].0, "😀");
    assert_eq!(fields[31].0, "\u{e000}");
    let emoji = fields
        .binary_search_by(|(name, _)| name.encode_utf16().cmp("😀".encode_utf16()))
        .unwrap();
    assert_eq!(fields[emoji].1.as_int64(), Some(531));
    let private_use = fields
        .binary_search_by(|(name, _)| name.encode_utf16().cmp("\u{e000}".encode_utf16()))
        .unwrap();
    assert_eq!(fields[private_use].1.as_int64(), Some(30));
}

fn assert_spark_unicode_object(output: &StructArray) {
    let value = output.column(0).as_binary::<i32>();
    let metadata = output.column(1).as_binary::<i32>();
    assert_spark_unicode_variant(Variant::new(metadata.value(0), value.value(0)));
}

fn normalize_typed_value(typed_value: ArrayRef, field_names: &[&str]) -> VariantArray {
    let (metadata_bytes, _) = VariantBuilder::new()
        .with_field_names(field_names.iter().copied())
        .finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), false),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    VariantArray::try_new(output.as_ref()).unwrap()
}

#[test]
fn test_normalize_shredded_variant_widens_unsigned_values() {
    let metadata_builder = VariantBuilder::new().with_field_names(["u8", "u16", "u32", "u64"]);
    let (metadata_bytes, _) = metadata_builder.finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));

    let fields = [
        ("u8", Arc::new(UInt8Array::from(vec![u8::MAX])) as ArrayRef),
        (
            "u16",
            Arc::new(UInt16Array::from(vec![u16::MAX])) as ArrayRef,
        ),
        (
            "u32",
            Arc::new(UInt32Array::from(vec![u32::MAX])) as ArrayRef,
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
        ),
    ];
    let mut object_fields = Vec::with_capacity(fields.len());
    let mut object_columns = Vec::with_capacity(fields.len());
    for (name, value) in fields {
        let shredded = StructArray::try_new(
            Fields::from(vec![Field::new(
                "typed_value",
                value.data_type().clone(),
                false,
            )]),
            vec![value],
            None,
        )
        .unwrap();
        object_fields.push(Field::new(name, shredded.data_type().clone(), false));
        object_columns.push(Arc::new(shredded) as ArrayRef);
    }
    let typed_value: ArrayRef =
        Arc::new(StructArray::try_new(object_fields.into(), object_columns, None).unwrap());
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), false),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    let variant = output.value(0);
    let Variant::Object(object) = variant else {
        panic!("expected object")
    };
    assert_eq!(object.get("u8"), Some(Variant::from(255_i16)));
    assert_eq!(object.get("u16"), Some(Variant::from(65_535_i32)));
    assert_eq!(object.get("u32"), Some(Variant::from(4_294_967_295_i64)));
    assert_eq!(
        object.get("u64"),
        Some(Variant::Decimal16(
            VariantDecimal16::try_new(18_446_744_073_709_551_615_i128, 0).unwrap()
        ))
    );
}

#[test]
fn test_normalize_variant_decodes_dictionary_storage() {
    let mut builder = VariantBuilder::new();
    builder.append_value(42_i64);
    let (metadata_bytes, value_bytes) = builder.finish();
    let values: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
    let value: ArrayRef =
        Arc::new(DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), values).unwrap());
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("value", value.data_type().clone(), false),
                Field::new("metadata", DataType::Binary, false),
            ]),
            vec![value, metadata],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );
    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    assert_eq!(output.value(0), Variant::Int64(42));

    let values: ArrayRef = Arc::new(Int64Array::from(vec![42]));
    let dictionary: ArrayRef =
        Arc::new(DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), values).unwrap());
    let child: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new(
                "typed_value",
                dictionary.data_type().clone(),
                false,
            )]),
            vec![dictionary],
            None,
        )
        .unwrap(),
    );
    let object: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("d", child.data_type().clone(), false)]),
            vec![child],
            None,
        )
        .unwrap(),
    );
    let output = normalize_typed_value(object, &["d"]);
    let Variant::Object(object) = output.value(0) else {
        panic!("expected object")
    };
    assert_eq!(object.get("d"), Some(Variant::Int8(42)));
}

#[test]
fn test_normalize_shredded_variant_widens_millisecond_timestamps() {
    let millis = 1_704_067_200_123_i64;
    let ltz: ArrayRef =
        Arc::new(TimestampMillisecondArray::from(vec![millis]).with_timezone("UTC"));
    let ntz: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![millis]));
    let shredded = |value: ArrayRef| -> ArrayRef {
        Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new(
                    "typed_value",
                    value.data_type().clone(),
                    false,
                )]),
                vec![value],
                None,
            )
            .unwrap(),
        )
    };
    let ltz = shredded(ltz);
    let ntz = shredded(ntz);
    let object: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("ltz", ltz.data_type().clone(), false),
                Field::new("ntz", ntz.data_type().clone(), false),
            ]),
            vec![ltz, ntz],
            None,
        )
        .unwrap(),
    );
    let output = normalize_typed_value(object, &["ltz", "ntz"]);
    let Variant::Object(object) = output.value(0) else {
        panic!("expected object")
    };

    let Some(Variant::TimestampMicros(ltz)) = object.get("ltz") else {
        panic!("expected timestamp")
    };
    assert_eq!(ltz.timestamp_micros(), millis * 1_000);

    let Some(Variant::TimestampNtzMicros(ntz)) = object.get("ntz") else {
        panic!("expected timestamp_ntz")
    };
    assert_eq!(ntz.and_utc().timestamp_micros(), millis * 1_000);
}

#[test]
fn test_normalize_shredded_variant_converts_fixed_size_list() {
    let elements: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![42, 43]))],
            None,
        )
        .unwrap(),
    );
    let typed_value: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", elements.data_type().clone(), false)),
            2,
            elements,
            None,
        )
        .unwrap(),
    );

    let output = normalize_typed_value(typed_value, &[]);
    let Variant::List(list) = output.value(0) else {
        panic!("expected list")
    };
    assert_eq!(
        list.iter()
            .map(|value| value.as_int64())
            .collect::<Vec<_>>(),
        vec![Some(42), Some(43)]
    );
}

#[test]
fn test_normalize_shredded_variant_compacts_spark_integer_widths() {
    let (metadata_bytes, _) = VariantBuilder::new().finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(metadata_bytes.as_slice()),
        Some(metadata_bytes.as_slice()),
        Some(metadata_bytes.as_slice()),
        Some(metadata_bytes.as_slice()),
    ]));
    let typed_value: ArrayRef = Arc::new(Int64Array::from(vec![
        1,
        i64::from(i8::MAX) + 1,
        i64::from(i16::MAX) + 1,
        i64::from(i32::MAX) + 1,
    ]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", DataType::Int64, false),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    assert_eq!(output.value(0), Variant::Int8(1));
    assert_eq!(output.value(1), Variant::Int16(128));
    assert_eq!(output.value(2), Variant::Int32(32_768));
    assert_eq!(output.value(3), Variant::Int64(2_147_483_648));
}

#[test]
fn test_compact_spark_typed_variant_canonicalizes_nan() {
    let Variant::Float(float) =
        compact_spark_typed_variant(Variant::Float(f32::from_bits(0x7fc0_0001)))
    else {
        panic!("expected float")
    };
    assert_eq!(float.to_bits(), 0x7fc0_0000);

    let Variant::Double(double) =
        compact_spark_typed_variant(Variant::Double(f64::from_bits(0x7ff8_0000_0000_0001)))
    else {
        panic!("expected double")
    };
    assert_eq!(double.to_bits(), 0x7ff8_0000_0000_0000);

    let (metadata, _) = VariantBuilder::new().finish();
    let metadata = VariantMetadata::new(&metadata);
    let residual = f32::from_bits(0x7fc0_0001);
    let bytes = spark_variant_bytes(&metadata, Variant::Float(residual)).unwrap();
    let Variant::Float(output) = Variant::new_with_metadata(metadata, &bytes) else {
        panic!("expected residual float")
    };
    assert_eq!(output.to_bits(), residual.to_bits());
}

#[test]
fn test_normalize_shredded_variant_rejects_missing_required_value() {
    let (metadata_bytes, _) = VariantBuilder::new().finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let typed_value: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", DataType::Int64, true),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    assert!(normalize_variant_array(&physical, &target_field).is_err());
}

#[test]
fn test_normalize_shredded_variant_uses_physical_metadata_order() {
    let (metadata_bytes, _) = VariantBuilder::new().with_field_names(["a", "b"]).finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (name, value) in [("b", 2_i64), ("a", 1_i64)] {
        let child = StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![value]))],
            None,
        )
        .unwrap();
        fields.push(Field::new(name, child.data_type().clone(), false));
        columns.push(Arc::new(child) as ArrayRef);
    }
    let typed_value: ArrayRef =
        Arc::new(StructArray::try_new(fields.into(), columns, None).unwrap());
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), false),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = output.as_struct();
    assert_eq!(
        output.column(1).as_binary::<i32>().value(0),
        &[0x01, 2, 0, 1, 2, b'b', b'a']
    );

    let mut expected = VariantBuilder::new().with_field_names(["b", "a"]);
    let mut object = expected.new_object();
    object.insert("b", 2_i8);
    object.insert("a", 1_i8);
    object.finish();
    let (_, expected_value) = expected.finish();
    assert_eq!(output.column(0).as_binary::<i32>().value(0), expected_value);
}

#[test]
fn test_normalize_shredded_variant_preserves_residual_scalar_width() {
    let mut builder = VariantBuilder::new().with_field_names(["known", "residual"]);
    let mut object = builder.new_object();
    object.insert("residual", 1_i64);
    object.finish();
    let (metadata_bytes, value_bytes) = builder.finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
    let known: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![2]))],
            None,
        )
        .unwrap(),
    );
    let typed_value: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("known", known.data_type().clone(), false)]),
            vec![known],
            None,
        )
        .unwrap(),
    );
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", typed_value.data_type().clone(), true),
            ]),
            vec![metadata, value, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    let Variant::Object(object) = output.value(0) else {
        panic!("expected object")
    };
    assert_eq!(object.get("known"), Some(Variant::Int8(2)));
    assert_eq!(object.get("residual"), Some(Variant::Int64(1)));
}

#[test]
fn test_normalize_shredded_variant_compacts_spark_decimal_width() {
    let (metadata_bytes, _) = VariantBuilder::new().finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let typed_value: ArrayRef = Arc::new(
        Decimal128Array::from(vec![123_i128])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), false),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    assert_eq!(
        output.value(0),
        Variant::Decimal4(VariantDecimal4::try_new(123, 2).unwrap())
    );
}

#[test]
fn test_normalize_shredded_variant_uses_spark_object_key_order() {
    let keys = unicode_object_keys();

    let metadata_builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    let (metadata_bytes, _) = metadata_builder.finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));

    let mut object_fields = Vec::with_capacity(keys.len());
    let mut object_columns = Vec::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        let value = if key == "😀" { 531 } else { index as i64 };
        let field = Field::new("typed_value", DataType::Int64, false);
        let column: ArrayRef = Arc::new(Int64Array::from(vec![value]));
        let shredded_field =
            StructArray::try_new(Fields::from(vec![field]), vec![column], None).unwrap();
        object_fields.push(Field::new(key, shredded_field.data_type().clone(), false));
        object_columns.push(Arc::new(shredded_field) as ArrayRef);
    }
    let typed_value: ArrayRef =
        Arc::new(StructArray::try_new(object_fields.into(), object_columns, None).unwrap());
    let physical_fields = Fields::from(vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("typed_value", typed_value.data_type().clone(), false),
    ]);
    let physical: ArrayRef =
        Arc::new(StructArray::try_new(physical_fields, vec![metadata, typed_value], None).unwrap());
    let target_fields = Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let target_field = Arc::new(
        Field::new("v", DataType::Struct(target_fields), false).with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = output.as_struct();
    assert_spark_unicode_object(output);
}

#[test]
fn test_normalize_nested_shredded_variant_uses_spark_object_key_order() {
    let keys = unicode_object_keys();
    let field_names = std::iter::once("nested")
        .chain(keys.iter().map(String::as_str))
        .collect::<Vec<_>>();
    let (metadata_bytes, _) = VariantBuilder::new().with_field_names(field_names).finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));

    let mut nested_fields = Vec::with_capacity(keys.len());
    let mut nested_columns = Vec::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        let value = if key == "😀" { 531 } else { index as i64 };
        let state = StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![value]))],
            None,
        )
        .unwrap();
        nested_fields.push(Field::new(key, state.data_type().clone(), false));
        nested_columns.push(Arc::new(state) as ArrayRef);
    }
    let nested_value: ArrayRef =
        Arc::new(StructArray::try_new(nested_fields.into(), nested_columns, None).unwrap());
    let nested_state: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new(
                "typed_value",
                nested_value.data_type().clone(),
                false,
            )]),
            vec![nested_value],
            None,
        )
        .unwrap(),
    );
    let typed_value: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new(
                "nested",
                nested_state.data_type().clone(),
                false,
            )]),
            vec![nested_state],
            None,
        )
        .unwrap(),
    );
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), false),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    let Variant::Object(object) = output.value(0) else {
        panic!("expected outer object")
    };
    assert_spark_unicode_variant(object.get("nested").expect("nested field"));
}

#[test]
fn test_normalize_partially_shredded_spark_object_key_order() {
    let keys = unicode_object_keys();
    let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    let mut object = builder.new_object();
    for (index, key) in keys.iter().enumerate().skip(1) {
        object.insert(key, if key == "😀" { 531 } else { index as i64 });
    }
    object.finish();
    let (metadata_bytes, value_bytes) = builder.finish();

    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let spark_value = reorder_variant_values(
        &value,
        &metadata,
        None,
        VariantObjectKeyOrder::SparkUtf16,
        false,
    )
    .unwrap();

    let shredded_k00: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![0]))],
            None,
        )
        .unwrap(),
    );
    let typed_value: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new(
                "k00",
                shredded_k00.data_type().clone(),
                false,
            )]),
            vec![shredded_k00],
            None,
        )
        .unwrap(),
    );
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", typed_value.data_type().clone(), true),
            ]),
            vec![metadata, spark_value, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    assert_spark_unicode_object(output.as_struct());
}

#[test]
fn test_normalize_unshredded_variant_uses_spark_object_key_order() {
    let keys = unicode_object_keys();
    let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    let mut object = builder.new_object();
    for (index, key) in keys.iter().enumerate() {
        object.insert(key, if key == "😀" { 531 } else { index as i64 });
    }
    object.finish();
    let (metadata_bytes, value_bytes) = builder.finish();

    let Variant::Object(canonical) = Variant::new(&metadata_bytes, &value_bytes) else {
        panic!("expected object")
    };
    let canonical_fields = canonical.iter().collect::<Vec<_>>();
    assert_eq!(canonical_fields[30].0, "\u{e000}");
    assert_eq!(canonical_fields[31].0, "😀");

    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let physical_fields = Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let physical: ArrayRef =
        Arc::new(StructArray::try_new(physical_fields, vec![value, metadata], None).unwrap());
    let target_fields = Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let target_field = Arc::new(
        Field::new("v", DataType::Struct(target_fields), false).with_extension_type(VariantType),
    );

    let first = normalize_variant_array(&physical, &target_field).unwrap();
    let first_value = first
        .as_struct()
        .column(0)
        .as_binary::<i32>()
        .value(0)
        .to_vec();
    assert_spark_unicode_object(first.as_struct());

    // Spark-produced already-unshredded input is UTF-16 ordered. Normalizing it again must
    // remain valid without Arrow's UTF-8-order full validation.
    let second = normalize_variant_array(&first, &target_field).unwrap();
    assert_spark_unicode_object(second.as_struct());
    assert_eq!(
        second.as_struct().column(0).as_binary::<i32>().value(0),
        first_value
    );
}

#[test]
fn test_normalize_spark_ordered_variant_preserves_value_bytes() {
    let mut builder = VariantBuilder::new();
    let mut object = builder.new_object();
    object.insert("b", 1_i64);
    object.insert("a", 2_i64);
    object.finish();
    let (metadata_bytes, value_bytes) = builder.finish();

    let Variant::Object(object) = Variant::new(&metadata_bytes, &value_bytes) else {
        panic!("expected object")
    };
    assert_eq!(
        object.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        vec!["a", "b"]
    );

    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let physical_fields = Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let physical: ArrayRef =
        Arc::new(StructArray::try_new(physical_fields, vec![value, metadata], None).unwrap());
    let target_fields = Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let target_field = Arc::new(
        Field::new("v", DataType::Struct(target_fields), false).with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    assert_eq!(
        output.as_struct().column(0).as_binary::<i32>().value(0),
        value_bytes
    );
}

#[test]
fn test_normalize_variant_preserves_empty_object_keys() {
    let mut builder = VariantBuilder::new();
    let mut object = builder.new_object();
    object.insert("", 1_i64);
    let mut nested = object.new_object("nested");
    nested.insert("", 2_i64);
    nested.finish();
    object.finish();
    let (mut metadata_bytes, value_bytes) = builder.finish();

    // Spark leaves the metadata dictionary unsorted. Equal offsets encode the empty key.
    metadata_bytes[0] &= !0x10;
    assert!(VariantMetadata::try_new(&metadata_bytes).is_err());

    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ]),
            vec![value, metadata],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = output.as_struct();
    assert_eq!(output.column(0).as_binary::<i32>().value(0), value_bytes);
    assert_eq!(output.column(1).as_binary::<i32>().value(0), metadata_bytes);

    let Variant::Object(object) = Variant::new(&metadata_bytes, &value_bytes) else {
        panic!("expected object")
    };
    assert_eq!(object.get(""), Some(Variant::from(1_i64)));
    let Variant::Object(nested) = object.get("nested").unwrap() else {
        panic!("expected nested object")
    };
    assert_eq!(nested.get(""), Some(Variant::from(2_i64)));
}

#[test]
fn test_compatibility_retry_rejects_non_spark_residual_order() {
    let mut value_builder = VariantBuilder::new().with_field_names(["a", "b", "c"]);
    let mut object = value_builder.new_object();
    object.insert("a", 1_i64);
    object.insert("b", 2_i64);
    object.insert("c", 3_i64);
    object.finish();
    let (_, value) = value_builder.finish();

    // Reassign the field IDs so the encoded slots read as b, a, c. That is neither canonical
    // UTF-8 order nor Spark's UTF-16 order and must not be repaired by the compatibility retry.
    let mut metadata_builder = WritableMetadataBuilder::from_iter(["b", "a", "c"]);
    metadata_builder.finish();
    let metadata = metadata_builder.into_inner();
    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value.as_slice())]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())]));

    let error = rewrite_residual_values(
        &value,
        metadata.as_binary::<i32>(),
        metadata.as_binary::<i32>(),
        &[Some(0)],
        false,
    )
    .unwrap_err();
    assert!(error.to_string().contains("neither UTF-8 nor Spark UTF-16"));
}

#[test]
fn test_empty_key_retry_rejects_non_spark_metadata() {
    let mut metadata_builder = WritableMetadataBuilder::from_iter(["", "b", "a"]);
    metadata_builder.finish();
    let mut metadata = metadata_builder.into_inner();
    metadata[0] |= 0x10;

    let mut value_builder = VariantBuilder::new();
    value_builder.append_value(1_i64);
    let (_, value) = value_builder.finish();
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ]),
            vec![
                Arc::new(BinaryArray::from(vec![Some(value.as_slice())])),
                Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())])),
            ],
            None,
        )
        .unwrap(),
    );
    let variant = VariantArray::try_new(physical.as_ref()).unwrap();
    assert!(canonicalize_spark_empty_key_metadata(&variant)
        .unwrap()
        .is_none());
}

#[test]
fn test_normalize_partially_shredded_nested_unicode_and_empty_keys() {
    let keys = unicode_object_keys();
    let mut builder = VariantBuilder::new().with_field_names(["nested", "known"]);
    let mut residual = builder.new_object();
    residual.insert("", -1_i64);
    for (index, key) in keys.iter().enumerate() {
        residual.insert(key, if key == "😀" { 531_i64 } else { index as i64 });
    }
    residual.finish();
    let (metadata_bytes, value_bytes) = builder.finish();
    assert!(VariantMetadata::try_new(&metadata_bytes).is_err());

    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
    let value = reorder_variant_values(
        &value,
        &metadata,
        None,
        VariantObjectKeyOrder::SparkUtf16,
        false,
    )
    .unwrap();
    let known: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![99]))],
            None,
        )
        .unwrap(),
    );
    let nested_typed_value: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("known", known.data_type().clone(), false)]),
            vec![known],
            None,
        )
        .unwrap(),
    );
    let nested: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", nested_typed_value.data_type().clone(), true),
            ]),
            vec![value, nested_typed_value],
            None,
        )
        .unwrap(),
    );
    let typed_value: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new(
                "nested",
                nested.data_type().clone(),
                false,
            )]),
            vec![nested],
            None,
        )
        .unwrap(),
    );
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), true),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    let Variant::Object(object) = output.value(0) else {
        panic!("expected object")
    };
    let Variant::Object(nested) = object.get("nested").unwrap() else {
        panic!("expected nested object")
    };
    let fields = nested.iter().collect::<Vec<_>>();
    assert_eq!(fields.len(), 34);
    assert_eq!(fields[0].0, "");
    assert_eq!(fields[32].0, "😀");
    assert_eq!(fields[33].0, "\u{e000}");
    assert_eq!(nested.get("known").unwrap().as_int64(), Some(99));
    let emoji = fields
        .binary_search_by(|(name, _)| name.encode_utf16().cmp("😀".encode_utf16()))
        .unwrap();
    assert_eq!(fields[emoji].1.as_int64(), Some(531));
}

#[test]
fn test_normalize_nested_list_residuals_tracks_root_metadata() {
    fn row(keys: &[&str]) -> DataFusionResult<(Vec<u8>, Vec<u8>)> {
        let mut builder = VariantBuilder::new().with_field_names(keys.iter().copied());
        let mut object = builder.new_object();
        for (index, key) in keys.iter().enumerate() {
            object.insert(key, index as i64);
        }
        object.finish();
        let (metadata, value) = builder.finish();
        let metadata_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())]));
        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value.as_slice())]));
        let value = reorder_variant_values(
            &value,
            &metadata_array,
            None,
            VariantObjectKeyOrder::SparkUtf16,
            false,
        )?;
        Ok((metadata, value.as_binary::<i32>().value(0).to_vec()))
    }

    let (metadata0, value0) = row(&["a", "\u{e000}", "😀"]).unwrap();
    let (metadata1, value1) = row(&["b", "zz", "\u{ffff}", "𐀀"]).unwrap();
    let states: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("value", DataType::Binary, true)]),
            vec![Arc::new(BinaryArray::from(vec![
                Some(value0.as_slice()),
                Some(value1.as_slice()),
            ]))],
            None,
        )
        .unwrap(),
    );
    let list: ArrayRef = Arc::new(
        arrow::array::ListArray::try_new(
            Arc::new(Field::new("element", states.data_type().clone(), false)),
            arrow::buffer::OffsetBuffer::new(vec![0, 1, 2].into()),
            states,
            None,
        )
        .unwrap(),
    );
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(metadata0.as_slice()),
        Some(metadata1.as_slice()),
    ]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", list.data_type().clone(), false),
            ]),
            vec![metadata, list],
            None,
        )
        .unwrap(),
    );

    let target_field = Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            false,
        )
        .with_extension_type(VariantType),
    );
    let output = normalize_variant_array(&physical, &target_field).unwrap();
    let output = VariantArray::try_new(output.as_ref()).unwrap();
    for (index, key) in ["😀", "𐀀"].into_iter().enumerate() {
        let Variant::List(list) = output.value(index) else {
            panic!("expected list")
        };
        let Variant::Object(object) = list.get(0).unwrap() else {
            panic!("expected object")
        };
        assert_eq!(object.get(key).unwrap().as_int64(), Some(index as i64 + 2));
    }
}

#[test]
fn test_normalize_variant_skips_empty_children_of_null_parent() {
    let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b""[..])]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b""[..])]));
    let physical_fields = Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            physical_fields,
            vec![value, metadata],
            Some(NullBuffer::from(vec![false])),
        )
        .unwrap(),
    );
    let target_fields = Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]);
    let target_field = Arc::new(
        Field::new("v", DataType::Struct(target_fields), true).with_extension_type(VariantType),
    );

    let output = normalize_variant_array(&physical, &target_field).unwrap();
    assert!(output.is_null(0));
    assert!(output.as_struct().column(0).is_null(0));
}
