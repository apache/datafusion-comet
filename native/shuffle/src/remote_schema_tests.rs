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

use crate::{decode_remote_shuffle_batch, CompressionCodec, ShuffleBlockWriter};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryDictionaryBuilder, DictionaryArray, FixedSizeListArray,
    Int16Array, Int32Array, Int64Array, LargeListArray, ListArray, MapArray, NullArray,
    PrimitiveDictionaryBuilder, RecordBatch, RecordBatchOptions, StringArray,
    StringDictionaryBuilder, StructArray, UInt16Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Field, Fields, Int16Type, Int32Type, Int64Type, Int8Type,
    Schema, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::ipc::writer::IpcWriteContext;
use datafusion::physical_plan::metrics::Time;
use std::io::Cursor;
use std::sync::Arc;

fn encoded_batch(batch: &RecordBatch, codec: CompressionCodec, rss: bool) -> Vec<u8> {
    let writer = if rss {
        ShuffleBlockWriter::try_new_rss(batch.schema(), codec)
    } else {
        ShuffleBlockWriter::try_new(batch.schema().as_ref(), codec)
    }
    .unwrap();
    let mut output = Cursor::new(Vec::new());
    let mut context = IpcWriteContext::default();
    if rss {
        writer
            .write_rss_batch(batch, &mut output, &mut context, &Time::default())
            .unwrap();
    } else {
        writer
            .write_batch(batch, &mut output, &mut context, &Time::default())
            .unwrap();
    }
    // The transport consumes the u64 block length and column count before the codec and IPC.
    output.into_inner()[16..].to_vec()
}

fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_from_iter(
        columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| (format!("column_{index}"), column)),
    )
    .unwrap()
}

fn assert_roundtrip(actual: Vec<ArrayRef>, expected: Vec<ArrayRef>) {
    let batch = batch(actual);
    let types: Vec<_> = expected
        .iter()
        .map(|array| array.data_type().clone())
        .collect();
    for codec in [
        CompressionCodec::None,
        CompressionCodec::Lz4Frame,
        CompressionCodec::Snappy,
        CompressionCodec::Zstd(1),
    ] {
        for rss in [false, true] {
            let bytes = encoded_batch(&batch, codec.clone(), rss);
            let decoded = decode_remote_shuffle_batch(&bytes, &types).unwrap();
            assert_eq!(decoded.num_rows(), batch.num_rows());
            for (actual, expected) in decoded.columns().iter().zip(&expected) {
                assert_eq!(actual.to_data(), expected.to_data());
            }
        }
    }
}

fn check_dictionary_key_type<K: ArrowDictionaryKeyType>() {
    let mut numbers = PrimitiveDictionaryBuilder::<K, Int32Type>::new();
    let mut strings = StringDictionaryBuilder::<K>::new();
    let mut binaries = BinaryDictionaryBuilder::<K>::new();
    for value in [Some(-7), None, Some(42), Some(-7)] {
        match value {
            Some(value) => {
                numbers.append(value).unwrap();
                strings
                    .append(if value < 0 { "first" } else { "second" })
                    .unwrap();
                binaries
                    .append(if value < 0 {
                        &b"\0\xff"[..]
                    } else {
                        &b"second"[..]
                    })
                    .unwrap();
            }
            None => {
                numbers.append_null();
                strings.append_null();
                binaries.append_null();
            }
        }
    }
    assert_roundtrip(
        vec![
            Arc::new(numbers.finish()),
            Arc::new(strings.finish()),
            Arc::new(binaries.finish()),
        ],
        vec![
            Arc::new(Int32Array::from(vec![Some(-7), None, Some(42), Some(-7)])),
            Arc::new(StringArray::from(vec![
                Some("first"),
                None,
                Some("second"),
                Some("first"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(&b"\0\xff"[..]),
                None,
                Some(&b"second"[..]),
                Some(&b"\0\xff"[..]),
            ])),
        ],
    );
}

#[test]
#[cfg_attr(miri, ignore)] // The codec matrix calls ZSTD_createCCtx.
fn integer_key_dictionary_values_survive_remote_shuffle() {
    check_dictionary_key_type::<Int8Type>();
    check_dictionary_key_type::<Int16Type>();
    check_dictionary_key_type::<Int32Type>();
    check_dictionary_key_type::<Int64Type>();
    check_dictionary_key_type::<UInt8Type>();
    check_dictionary_key_type::<UInt16Type>();
    check_dictionary_key_type::<UInt32Type>();
    check_dictionary_key_type::<UInt64Type>();
}

fn numbers(encoded: bool) -> ArrayRef {
    if encoded {
        Arc::new(
            DictionaryArray::<Int16Type>::try_new(
                Int16Array::from(vec![Some(1), None, Some(0), Some(1)]),
                Arc::new(Int32Array::from(vec![7, -42])),
            )
            .unwrap(),
        )
    } else {
        Arc::new(Int32Array::from(vec![Some(-42), None, Some(7), Some(-42)]))
    }
}

fn nested_columns(encoded: bool) -> Vec<ArrayRef> {
    let values = numbers(encoded);
    let item = Arc::new(Field::new(
        if encoded { "wire_item" } else { "element" },
        values.data_type().clone(),
        true,
    ));
    let validity = Some(NullBuffer::from(vec![true, false, true]));
    vec![
        Arc::new(ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 2, 2, 4].into()),
            Arc::clone(&values),
            validity.clone(),
        )),
        Arc::new(LargeListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0_i64, 2, 2, 4].into()),
            Arc::clone(&values),
            validity,
        )),
        Arc::new(FixedSizeListArray::new(
            item,
            2,
            Arc::clone(&values),
            Some(NullBuffer::from(vec![true, false])),
        )),
        Arc::new(StructArray::new(
            Fields::from(vec![Field::new(
                "payload",
                values.data_type().clone(),
                true,
            )]),
            vec![values],
            Some(NullBuffer::from(vec![true, true, false, true])),
        )),
    ]
}

fn dictionary_map(encoded: bool) -> ArrayRef {
    let keys: ArrayRef = if encoded {
        Arc::new(
            DictionaryArray::<UInt16Type>::try_new(
                UInt16Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(vec!["one", "two"])),
            )
            .unwrap(),
        )
    } else {
        Arc::new(StringArray::from(vec!["one", "two", "one"]))
    };
    let values: ArrayRef = if encoded {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1)]),
                Arc::new(Int32Array::from(vec![7, -42])),
            )
            .unwrap(),
        )
    } else {
        Arc::new(Int32Array::from(vec![Some(7), None, Some(-42)]))
    };
    let fields = Fields::from(vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", values.data_type().clone(), true),
    ]);
    Arc::new(MapArray::new(
        Arc::new(Field::new(
            if encoded { "wire_entries" } else { "entries" },
            DataType::Struct(fields.clone()),
            false,
        )),
        OffsetBuffer::new(vec![0, 2, 2, 3].into()),
        StructArray::new(fields, vec![keys, values], None),
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    ))
}

#[test]
#[cfg_attr(miri, ignore)]
fn dictionaries_inside_containers_survive_remote_shuffle() {
    for (actual, expected) in nested_columns(true).into_iter().zip(nested_columns(false)) {
        assert_roundtrip(vec![actual], vec![expected]);
    }
    assert_roundtrip(vec![dictionary_map(true)], vec![dictionary_map(false)]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn dictionaries_wrapping_lists_and_nested_dictionaries_survive_remote_shuffle() {
    let inner = Arc::new(
        DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![0, 1, 0]),
            Arc::new(Int32Array::from(vec![7, -42])),
        )
        .unwrap(),
    );
    let values = Arc::new(ListArray::new(
        Arc::new(Field::new("wire_item", inner.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 2, 3].into()),
        inner,
        None,
    ));
    let dictionary = Arc::new(
        DictionaryArray::<UInt16Type>::try_new(
            UInt16Array::from(vec![Some(1), None, Some(0), Some(1)]),
            values,
        )
        .unwrap(),
    );
    let expected = Arc::new(ListArray::new(
        Arc::new(Field::new("element", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1, 1, 3, 4].into()),
        Arc::new(Int32Array::from(vec![7, 7, -42, 7])),
        Some(NullBuffer::from(vec![true, false, true, true])),
    ));
    assert_roundtrip(vec![dictionary], vec![expected]);
}

#[test]
fn non_null_dictionary_keys_can_reference_null_values() {
    let dictionary = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(Int32Array::from(vec![Some(7), None])),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "wire",
            dictionary.data_type().clone(),
            false,
        )])),
        vec![dictionary],
    )
    .unwrap();
    let bytes = encoded_batch(&batch, CompressionCodec::None, true);
    let decoded = decode_remote_shuffle_batch(&bytes, &[DataType::Int32]).unwrap();
    assert_eq!(
        decoded.column(0).to_data(),
        Int32Array::from(vec![Some(7), None]).to_data()
    );
}

#[test]
fn unused_dictionary_values_do_not_prevent_nested_nullability_narrowing() {
    let values = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("value", DataType::Int32, true)]),
        vec![Arc::new(Int32Array::from(vec![Some(7), None]))],
        None,
    ));
    let dictionary = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 0]), values).unwrap(),
    );
    let expected = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("value", DataType::Int32, false)]),
        vec![Arc::new(Int32Array::from(vec![7, 7]))],
        None,
    ));
    let bytes = encoded_batch(&batch(vec![dictionary]), CompressionCodec::None, true);
    let decoded = decode_remote_shuffle_batch(&bytes, &[expected.data_type().clone()]).unwrap();
    assert_eq!(decoded.column(0).to_data(), expected.to_data());
}

fn struct_dictionary_values(encoded: bool, reference_null: bool) -> ArrayRef {
    if encoded {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, i32::from(reference_null)]),
                Arc::new(StructArray::new(
                    Fields::from(vec![Field::new("value", DataType::Int32, true)]),
                    vec![Arc::new(Int32Array::from(vec![Some(7), None]))],
                    None,
                )),
            )
            .unwrap(),
        )
    } else {
        Arc::new(StructArray::new(
            Fields::from(vec![Field::new("value", DataType::Int32, false)]),
            vec![Arc::new(Int32Array::from(vec![7, 7]))],
            None,
        ))
    }
}

fn nested_struct_dictionary_columns(encoded: bool, reference_null: bool) -> Vec<ArrayRef> {
    let values = struct_dictionary_values(encoded, reference_null);
    let item = Arc::new(Field::new("element", values.data_type().clone(), true));
    let nulls = Some(NullBuffer::from(vec![false, true]));
    let map_fields = Fields::from(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", values.data_type().clone(), true),
    ]);
    vec![
        Arc::new(ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::clone(&values),
            nulls.clone(),
        )),
        Arc::new(LargeListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0_i64, 1, 2].into()),
            Arc::clone(&values),
            nulls.clone(),
        )),
        Arc::new(FixedSizeListArray::new(
            item,
            1,
            Arc::clone(&values),
            nulls.clone(),
        )),
        Arc::new(StructArray::new(
            Fields::from(vec![Field::new(
                "payload",
                values.data_type().clone(),
                true,
            )]),
            vec![Arc::clone(&values)],
            nulls.clone(),
        )),
        Arc::new(MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(map_fields.clone()),
                false,
            )),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            StructArray::new(
                map_fields,
                vec![Arc::new(Int32Array::from(vec![1, 2])), values],
                None,
            ),
            nulls,
            false,
        )),
    ]
}

#[test]
#[cfg_attr(miri, ignore)]
fn nested_dictionary_values_are_selected_before_nullability_narrowing() {
    let list = |values: ArrayRef| -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("element", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            values,
            None,
        ))
    };
    // Regression: List<Dictionary<Int32, Struct<value: nullable Int32>>> must decode
    // to List<Struct<value: non-null Int32>> when neither key references the null value.
    assert_roundtrip(
        vec![list(struct_dictionary_values(true, false))],
        vec![list(struct_dictionary_values(false, false))],
    );
    for (actual, expected) in nested_struct_dictionary_columns(true, false)
        .into_iter()
        .zip(nested_struct_dictionary_columns(false, false))
    {
        // Keep a null parent and a non-null parent, then separately exercise a nonzero slice.
        // The unreferenced dictionary value contains a null, but both keys reference {value: 7}.
        assert_roundtrip(vec![actual.slice(1, 1)], vec![expected.slice(1, 1)]);
        assert_roundtrip(vec![actual], vec![expected]);
    }
}

#[test]
fn referenced_null_dictionary_values_cannot_satisfy_non_null_nested_fields() {
    for (actual, expected) in nested_struct_dictionary_columns(true, true)
        .into_iter()
        .zip(nested_struct_dictionary_columns(false, false))
    {
        for rss in [false, true] {
            let bytes = encoded_batch(
                &batch(vec![Arc::clone(&actual)]),
                CompressionCodec::None,
                rss,
            );
            let error = decode_remote_shuffle_batch(&bytes, &[expected.data_type().clone()])
                .unwrap_err()
                .to_string();
            assert!(error.contains("null"), "{error}");
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn dictionary_value_nulls_masked_by_parent_nulls_remain_valid() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(Int32Array::from(vec![Some(7), None])),
        )
        .unwrap(),
    );
    let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
    let nulls = Some(NullBuffer::from(vec![true, false]));
    let structure = |values: ArrayRef| -> ArrayRef {
        Arc::new(StructArray::new(
            Fields::from(vec![Field::new("value", values.data_type().clone(), false)]),
            vec![values],
            nulls.clone(),
        ))
    };
    let fixed_size_list = |values: ArrayRef| -> ArrayRef {
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("element", values.data_type().clone(), false)),
            1,
            values,
            nulls.clone(),
        ))
    };
    assert_roundtrip(
        vec![
            structure(Arc::clone(&dictionary)),
            fixed_size_list(dictionary),
        ],
        vec![structure(Arc::clone(&expected)), fixed_size_list(expected)],
    );
}

#[test]
fn dictionary_decoding_does_not_allow_logical_type_or_container_shape_changes() {
    let numeric = numbers(true);
    let expected_nested = nested_columns(false);
    let actual_nested = nested_columns(true);
    let mut cases = vec![
        (Arc::clone(&numeric), DataType::Int64),
        (Arc::clone(&numeric), DataType::UInt32),
        (numeric, DataType::Float32),
        (
            Arc::clone(&actual_nested[0]),
            DataType::List(Arc::new(Field::new("element", DataType::UInt32, true))),
        ),
        (
            Arc::clone(&actual_nested[0]),
            expected_nested[1].data_type().clone(),
        ),
        (
            Arc::clone(&actual_nested[2]),
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int32, true)), 3),
        ),
        (
            Arc::clone(&actual_nested[3]),
            DataType::Struct(Fields::from(vec![Field::new(
                "renamed",
                DataType::Int32,
                true,
            )])),
        ),
    ];
    let strings: ArrayRef = Arc::new(
        DictionaryArray::<UInt16Type>::try_new(
            UInt16Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["text"])),
        )
        .unwrap(),
    );
    cases.push((strings, DataType::Binary));
    let map = dictionary_map(true);
    let DataType::Map(entries, _) = dictionary_map(false).data_type().clone() else {
        unreachable!()
    };
    cases.push((map, DataType::Map(entries, true)));
    let structure: ArrayRef = Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("left", DataType::Int32, false),
            Field::new("right", DataType::Int32, false),
        ]),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![2])),
        ],
        None,
    ));
    cases.push((
        structure,
        DataType::Struct(Fields::from(vec![
            Field::new("right", DataType::Int32, true),
            Field::new("left", DataType::Int32, true),
        ])),
    ));
    for (column, expected) in cases {
        let bytes = encoded_batch(&batch(vec![column]), CompressionCodec::None, true);
        let error = decode_remote_shuffle_batch(&bytes, &[expected])
            .unwrap_err()
            .to_string();
        assert!(error.contains("type mismatch"), "{error}");
    }
}

#[test]
fn remote_shuffle_preserves_row_count_without_columns() {
    let options = RecordBatchOptions::new().with_row_count(Some(3));
    let batch =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options).unwrap();
    let bytes = encoded_batch(&batch, CompressionCodec::None, true);
    let decoded = decode_remote_shuffle_batch(&bytes, &[]).unwrap();
    assert_eq!(decoded.num_columns(), 0);
    assert_eq!(decoded.num_rows(), 3);
}

// A `NullType` column, and one nested under a list, a map value and a struct field, decode
// unchanged: a `NullArray` owns no buffers, so the encoding, the dictionary decoding and the
// nested-nullability reconciliation all have to pass it through by length alone.
#[test]
fn null_type_columns_and_children_survive_remote_shuffle() {
    let rows = 3;
    let null_field = |name: &str| Arc::new(Field::new(name, DataType::Null, true));
    let top_level: ArrayRef = Arc::new(NullArray::new(rows));
    let list: ArrayRef = Arc::new(ListArray::new(
        null_field("element"),
        OffsetBuffer::from_lengths([1, 0, 2]),
        Arc::new(NullArray::new(3)),
        None,
    ));
    let struct_fields = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("n", DataType::Null, true),
    ]);
    let structs: ArrayRef = Arc::new(StructArray::new(
        struct_fields,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(NullArray::new(rows)),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let entry_fields = Fields::from(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Null, true),
    ]);
    let entries = StructArray::new(
        entry_fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(NullArray::new(3)),
        ],
        None,
    );
    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
        OffsetBuffer::from_lengths([2, 0, 1]),
        entries,
        None,
        false,
    ));
    let columns = vec![top_level, list, structs, map];
    assert_roundtrip(columns.clone(), columns);
}
