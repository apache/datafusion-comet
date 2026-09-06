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
use arrow::{
    array::{Int64Array, ListArray},
    buffer::OffsetBuffer,
    datatypes::{Field, Fields},
};
use parquet::variant::{
    shred_variant, VariantArrayBuilder, VariantBuilder, VariantBuilderExt, VariantType,
};

fn target_field(nullable: bool) -> FieldRef {
    Arc::new(
        Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ])),
            nullable,
        )
        .with_extension_type(VariantType),
    )
}

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
    assert_eq!(object.get("😀").unwrap().as_int64(), Some(531));
}

fn assert_spark_unicode_output(output: &StructArray) {
    let value = output.column(0).as_binary::<i32>();
    let metadata = output.column(1).as_binary::<i32>();
    assert_spark_unicode_variant(Variant::new(metadata.value(0), value.value(0)));
}

#[test]
fn normalize_full_shredding_reorders_children_and_preserves_parent_nulls() {
    let mut builder = VariantArrayBuilder::new(3);
    builder.append_variant(Variant::from(1_i64));
    builder.append_null();
    builder.append_variant(Variant::from(3_i64));
    let base = builder.build();
    let metadata = Arc::clone(base.metadata_column());
    let typed_value: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("typed_value", DataType::Int64, true),
                Field::new("metadata", metadata.data_type().clone(), false),
            ]),
            vec![typed_value, metadata],
            base.inner().nulls().cloned(),
        )
        .unwrap(),
    );

    let output = normalize_variant_array(&physical, &target_field(true)).unwrap();
    let output = output.as_struct();
    assert_eq!(
        output
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["value", "metadata"]
    );
    assert!(output
        .columns()
        .iter()
        .all(|column| column.data_type() == &DataType::Binary));
    assert!(output.is_null(1));

    let variant = VariantArray::try_new(output).unwrap();
    assert_eq!(variant.value(0), Variant::from(10_i64));
    assert_eq!(variant.value(2), Variant::from(30_i64));
}

#[test]
fn normalize_fully_shredded_object_orders_for_spark() {
    let keys = unicode_object_keys();
    let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    builder.append_value(Variant::Null);
    let (metadata, _) = builder.finish();
    let mut fields = Vec::with_capacity(keys.len());
    let mut columns = Vec::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        let state = StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![if key == "😀" {
                531
            } else {
                index as i64
            }]))],
            None,
        )
        .unwrap();
        fields.push(Field::new(key, state.data_type().clone(), false));
        columns.push(Arc::new(state) as ArrayRef);
    }
    let typed_value: ArrayRef =
        Arc::new(StructArray::try_new(fields.into(), columns, None).unwrap());
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), false),
            ]),
            vec![
                Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())])),
                typed_value,
            ],
            None,
        )
        .unwrap(),
    );

    let output = normalize_variant_array(&physical, &target_field(false)).unwrap();
    assert_spark_unicode_output(output.as_struct());
}

#[test]
fn canonical_and_shredded_values_normalize_equally() {
    let mut builder = VariantArrayBuilder::new(6);
    builder.new_object().with_field("known", 1_i64).finish();
    builder
        .new_object()
        .with_field("known", 2_i64)
        .with_field("extra", 3_i64)
        .finish();
    builder
        .new_list()
        .with_value(4_i64)
        .with_value(Variant::Null)
        .finish();
    builder.append_variant(Variant::from(5_i64));
    builder.append_variant(Variant::Null);
    builder.append_null();
    let canonical = builder.build();
    let shredded = shred_variant(
        &canonical,
        &DataType::Struct(Fields::from(vec![Field::new(
            "known",
            DataType::Int64,
            true,
        )])),
    )
    .unwrap();

    let prepared = prepare_variant_for_unshredding(&shredded).unwrap();
    assert!(Arc::ptr_eq(
        shredded.value_column(),
        prepared.value_column()
    ));
    assert!(Arc::ptr_eq(
        shredded.typed_value_column().unwrap(),
        prepared.typed_value_column().unwrap(),
    ));

    let normalize = |array: &VariantArray| {
        let array: ArrayRef = Arc::new(array.inner().clone());
        let output = normalize_variant_array(&array, &target_field(true)).unwrap();
        VariantArray::try_new(output.as_ref()).unwrap()
    };
    let canonical = normalize(&canonical);
    let shredded = normalize(&shredded);

    for index in 0..canonical.len() {
        assert_eq!(canonical.is_null(index), shredded.is_null(index));
        if canonical.is_valid(index) {
            assert_eq!(canonical.value(index), shredded.value(index));
        }
    }
}

#[test]
fn normalize_unshredded_variant_orders_for_spark_and_is_idempotent() {
    let keys = unicode_object_keys();
    let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    let mut object = builder.new_object();
    for (index, key) in keys.iter().enumerate() {
        object.insert(key, if key == "😀" { 531 } else { index as i64 });
    }
    object.finish();
    let (metadata, value) = builder.finish();

    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, false),
            ]),
            vec![
                Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())])),
                Arc::new(BinaryArray::from(vec![Some(value.as_slice())])),
            ],
            None,
        )
        .unwrap(),
    );

    let first = normalize_variant_array(&physical, &target_field(false)).unwrap();
    assert_spark_unicode_output(first.as_struct());
    let first_value = first.as_struct().column(0).as_binary::<i32>().value(0);

    let second = normalize_variant_array(&first, &target_field(false)).unwrap();
    assert_spark_unicode_output(second.as_struct());
    assert_eq!(
        second.as_struct().column(0).as_binary::<i32>().value(0),
        first_value
    );
    assert_eq!(
        second
            .as_struct()
            .column(0)
            .as_binary::<i32>()
            .values()
            .as_ptr(),
        first
            .as_struct()
            .column(0)
            .as_binary::<i32>()
            .values()
            .as_ptr()
    );
}

#[test]
fn unchanged_values_reuse_buffers_and_still_validate() {
    let mut builder = VariantBuilder::new();
    builder.append_value(Variant::from("unchanged payload"));
    let (metadata, bytes) = builder.finish();
    let values: ArrayRef = Arc::new(BinaryArray::from(vec![
        None,
        Some(bytes.as_slice()),
        Some(bytes.as_slice()),
    ]));
    // Exercise a sliced array: prefix offsets need not start at zero.
    let values = values.slice(1, 2);
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![metadata.as_slice(); 2]));
    let output = reorder_variant_values(&values, &metadata, None).unwrap();
    assert!(Arc::ptr_eq(&values, &output));
    let target = target_field(false);
    let DataType::Struct(fields) = target.data_type() else {
        unreachable!()
    };
    let input: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::clone(&values), Arc::clone(&metadata)],
        None,
    ));
    let normalized = normalize_variant_array(&input, &target).unwrap();
    for (input, output) in input
        .as_struct()
        .columns()
        .iter()
        .zip(normalized.as_struct().columns())
    {
        assert_eq!(
            input.as_binary::<i32>().values().as_ptr(),
            output.as_binary::<i32>().values().as_ptr()
        );
    }
    let (output, changed) =
        rewrite_residual_values(&values, metadata.as_binary::<i32>(), &[Some(0), None]).unwrap();
    assert!(!changed);
    assert!(Arc::ptr_eq(&values, &output));

    let nulls = NullBuffer::from(vec![true, false]);
    let output = reorder_variant_values(&values, &metadata, Some(&nulls)).unwrap();
    assert_eq!(output.as_binary::<i32>().value(0), bytes);
    assert!(output.is_null(1));
    let again = reorder_variant_values(&output, &metadata, Some(&nulls)).unwrap();
    assert!(Arc::ptr_eq(&output, &again));

    let missing_metadata: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(metadata.as_binary::<i32>().value(0)),
        None,
    ]));
    assert!(reorder_variant_values(&values, &missing_metadata, None).is_err());
    assert!(rewrite_residual_values(
        &values,
        missing_metadata.as_binary::<i32>(),
        &[Some(0), Some(1)],
    )
    .is_err());
}

#[test]
fn lazy_rewrites_preserve_prefix_nulls_and_suffix() {
    let keys = unicode_object_keys();
    let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    let mut object = builder.new_object();
    for (index, key) in keys.iter().enumerate() {
        object.insert(key, index as i64);
    }
    object.finish();
    let (metadata, canonical) = builder.finish();
    let values: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(canonical.as_slice()),
        None,
        Some(canonical.as_slice()),
        None,
    ]));
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![metadata.as_slice(); 4]));
    let parents = NullBuffer::from(vec![true, false, true, false]);
    let legacy = reorder_variant_values(&values, &metadata, Some(&parents)).unwrap();
    let mixed: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(canonical.as_slice()),
        None,
        Some(legacy.as_binary::<i32>().value(2)),
        None,
    ]));
    let (output, changed) = rewrite_residual_values(
        &mixed,
        metadata.as_binary::<i32>(),
        &[Some(0), None, Some(2), None],
    )
    .unwrap();
    assert!(changed);
    let output = output.as_binary::<i32>();
    assert_eq!(output.value(0), canonical);
    assert!(output.is_null(1));
    assert!(output.is_null(3));
    assert_eq!(
        Variant::new(metadata.as_binary::<i32>().value(2), output.value(2)),
        Variant::new(metadata.as_binary::<i32>().value(2), &canonical),
    );
}

// Run explicitly with --ignored --nocapture; fixture construction is outside the timed loop.
#[test]
#[ignore]
fn benchmark_variant_buffer_reuse() {
    use std::{hint::black_box, time::Instant};
    let rows = 4096;
    let payload = "x".repeat(4096);
    let mut strings = VariantArrayBuilder::new(rows);
    let mut objects = VariantArrayBuilder::new(rows);
    for _ in 0..rows {
        strings.append_variant(Variant::from(payload.as_str()));
        objects
            .new_object()
            .with_field("known", 1_i64)
            .with_field("payload", payload.as_str())
            .finish();
    }
    let strings = strings.build();
    let objects = objects.build();
    let shredded = shred_variant(
        &objects,
        &DataType::Struct(Fields::from(vec![Field::new(
            "known",
            DataType::Int64,
            true,
        )])),
    )
    .unwrap();
    let target = target_field(false);
    let DataType::Struct(fields) = target.data_type() else {
        unreachable!()
    };
    let strings: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![
            cast(strings.value_column().as_ref(), &DataType::Binary).unwrap(),
            cast(strings.metadata_column().as_ref(), &DataType::Binary).unwrap(),
        ],
        None,
    ));
    let shredded: ArrayRef = Arc::new(shredded.into_inner());
    for (name, input) in [("canonical", strings), ("partially_shredded", shredded)] {
        for _ in 0..3 {
            black_box(normalize_variant_array(&input, &target).unwrap());
        }
        let start = Instant::now();
        for _ in 0..30 {
            black_box(normalize_variant_array(black_box(&input), &target).unwrap());
        }
        eprintln!(
            "{name}: {:.3} ms/batch, {rows} rows, 4096-byte payload",
            start.elapsed().as_secs_f64() * 1000.0 / 30.0
        );
    }
}

#[test]
fn normalize_partially_shredded_legacy_residual() {
    let keys = unicode_object_keys();
    let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    let mut object = builder.new_object();
    for (index, key) in keys.iter().enumerate().skip(1) {
        object.insert(key, if key == "😀" { 531 } else { index as i64 });
    }
    object.finish();
    let (metadata, value) = builder.finish();
    let metadata_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())]));
    let value_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value.as_slice())]));
    let legacy_value = reorder_variant_values(&value_array, &metadata_array, None).unwrap();

    let shredded: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![0]))],
            None,
        )
        .unwrap(),
    );
    let typed_value: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("k00", shredded.data_type().clone(), false)]),
            vec![shredded],
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
            vec![metadata_array, legacy_value, typed_value],
            None,
        )
        .unwrap(),
    );

    let output = normalize_variant_array(&physical, &target_field(false)).unwrap();
    assert_spark_unicode_output(output.as_struct());
}

#[test]
fn normalize_nested_list_residuals_use_their_root_metadata() {
    fn legacy_row(keys: &[&str]) -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new().with_field_names(keys.iter().copied());
        let mut object = builder.new_object();
        for (index, key) in keys.iter().enumerate() {
            object.insert(key, index as i64);
        }
        object.finish();
        let (metadata, value) = builder.finish();
        let metadata_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())]));
        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value.as_slice())]));
        let value = reorder_variant_values(&value, &metadata_array, None).unwrap();
        (metadata, value.as_binary::<i32>().value(0).to_vec())
    }

    let (metadata0, value0) = legacy_row(&["a", "\u{e000}", "😀"]);
    let (metadata1, value1) = legacy_row(&["b", "zz", "\u{ffff}", "𐀀"]);
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
        ListArray::try_new(
            Arc::new(Field::new("element", states.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
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

    let output = normalize_variant_array(&physical, &target_field(false)).unwrap();
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
fn normalize_null_parent_ignores_empty_children() {
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("value", DataType::Binary, false),
                Field::new("metadata", DataType::Binary, false),
            ]),
            vec![
                Arc::new(BinaryArray::from(vec![Some(&b""[..])])),
                Arc::new(BinaryArray::from(vec![Some(&b""[..])])),
            ],
            Some(NullBuffer::from(vec![false])),
        )
        .unwrap(),
    );

    let output = normalize_variant_array(&physical, &target_field(true)).unwrap();
    assert!(output.is_null(0));
    assert!(output.as_struct().column(0).is_null(0));
}
