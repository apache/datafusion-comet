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

use crate::create_hashes_internal;
use arrow::array::types::ArrowDictionaryKeyType;
use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, DictionaryArray, Int32Array};
use arrow::compute::take;
use arrow::datatypes::ArrowNativeType;
use datafusion::common::{internal_err, DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark compatible murmur3 hash (just `hash` in Spark) in vectorized execution fashion
pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u32> = vec![0_u32; num_rows];
            hashes.fill(*seed as u32);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => Arc::clone(array),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_murmur3_hashes(&arrays, &mut hashes)?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                    hashes[0] as i32,
                ))))
            } else {
                let hashes: Vec<i32> = hashes.into_iter().map(|x| x as i32).collect();
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function murmur3_hash must be an Int32 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}

/// Spark-compatible murmur3 hash function
#[inline]
pub fn spark_compatible_murmur3_hash<T: AsRef<[u8]>>(data: T, seed: u32) -> u32 {
    #[inline]
    fn mix_k1(mut k1: i32) -> i32 {
        k1 = k1.mul_wrapping(0xcc9e2d51u32 as i32);
        k1 = k1.rotate_left(15);
        k1 = k1.mul_wrapping(0x1b873593u32 as i32);
        k1
    }

    #[inline]
    fn mix_h1(mut h1: i32, k1: i32) -> i32 {
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.mul_wrapping(5).add_wrapping(0xe6546b64u32 as i32);
        h1
    }

    #[inline]
    fn fmix(mut h1: i32, len: i32) -> i32 {
        h1 ^= len;
        h1 ^= (h1 as u32 >> 16) as i32;
        h1 = h1.mul_wrapping(0x85ebca6bu32 as i32);
        h1 ^= (h1 as u32 >> 13) as i32;
        h1 = h1.mul_wrapping(0xc2b2ae35u32 as i32);
        h1 ^= (h1 as u32 >> 16) as i32;
        h1
    }

    #[inline]
    unsafe fn hash_bytes_by_int(data: &[u8], seed: u32) -> i32 {
        // safety: data length must be aligned to 4 bytes
        let mut h1 = seed as i32;
        for i in (0..data.len()).step_by(4) {
            let ints = data.as_ptr().add(i) as *const i32;
            let mut half_word = ints.read_unaligned();
            if cfg!(target_endian = "big") {
                half_word = half_word.reverse_bits();
            }
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        h1
    }
    let data = data.as_ref();
    let len = data.len();
    let len_aligned = len - len % 4;

    // safety:
    // avoid boundary checking in performance critical codes.
    // all operations are guaranteed to be safe
    // data is &[u8] so we do not need to check for proper alignment
    unsafe {
        let mut h1 = if len_aligned > 0 {
            hash_bytes_by_int(&data[0..len_aligned], seed)
        } else {
            seed as i32
        };

        for i in len_aligned..len {
            let half_word = *data.get_unchecked(i) as i8 as i32;
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        fmix(h1, len as i32) as u32
    }
}

/// Hash the values in a dictionary array
fn create_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes_buffer: &mut [u32],
    seeds_are_pristine: bool,
) -> datafusion::common::Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    if !seeds_are_pristine {
        // unpack the dictionary array as each row may have a different hash input
        let unpacked = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        create_murmur3_hashes(&[unpacked], hashes_buffer)?;
    } else {
        // Every row still carries the untouched seed, so each distinct dictionary value hashes to
        // the same result no matter which row it appears in. Hash each value once and reuse it per
        // key, which avoids redundant hashing of large dictionary elements (e.g. strings).
        let dict_values = Arc::clone(dict_array.values());
        // Seed from the buffer rather than assuming Spark's 42: `hash(col, seed)` lets the caller
        // choose, and the reuse is only sound if the per-value hashes start from the same seed the
        // rows carry. The caller guarantees the buffer is uniform, so any row's value will do.
        let seed = hashes_buffer.first().copied().unwrap_or(42);
        let mut dict_hashes = vec![seed; dict_values.len()];
        create_murmur3_hashes(&[dict_values], &mut dict_hashes)?;
        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.to_usize().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not convert key value {:?} to usize in dictionary of type {:?}",
                        key,
                        dict_array.data_type()
                    ))
                })?;
                *hash = dict_hashes[idx]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
pub fn create_murmur3_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut [u32],
) -> datafusion::common::Result<&'a mut [u32]> {
    create_hashes_internal!(
        arrays,
        hashes_buffer,
        spark_compatible_murmur3_hash,
        create_hashes_dictionary,
        create_murmur3_hashes
    );
    Ok(hashes_buffer)
}

#[cfg(test)]
mod tests {
    /// Produced by the per-element `hash_list_array!` implementation. An empty list and a null
    /// list both leave the seed untouched.
    const EXPECTED_LIST_OF_STRUCT: [u32; 6] =
        [262891156, 1206178823, 42, 42, 3798669693, 2032748937];
    use arrow::array::{Float32Array, Float64Array};
    use std::sync::Arc;

    use crate::murmur3::create_murmur3_hashes;
    use crate::test_hashes_with_nulls;
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array, StringArray};

    fn test_murmur3_hash<I: Clone, T: arrow::array::Array + From<Vec<Option<I>>> + 'static>(
        values: Vec<Option<I>>,
        expected: Vec<u32>,
    ) {
        test_hashes_with_nulls!(create_murmur3_hashes, T, values, expected, u32);
    }

    /// A dictionary array reached through a nested type arrives as the only column of its recursive
    /// call, so deciding the dictionary fast path from column position alone treated it as a first
    /// column and restarted from the seed, discarding the hash accumulated for earlier elements of
    /// the same row. The result differed from the identical decoded data.
    #[test]
    fn test_dictionary_element_in_list_matches_decoded() {
        use arrow::array::{DictionaryArray, Int32Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{Field, Int8Type};

        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let keys = arrow::array::Int8Array::from(vec![0i8, 1]);
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
        let decoded: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));

        // One row holding both elements, so the second element's hash chains onto the first.
        let as_list = |elems: ArrayRef| -> ArrayRef {
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", elems.data_type().clone(), true)),
                OffsetBuffer::new(vec![0i32, 2].into()),
                elems,
                None,
            ))
        };

        let mut from_dict = vec![42u32; 1];
        create_murmur3_hashes(&[as_list(dict)], &mut from_dict).unwrap();
        let mut from_decoded = vec![42u32; 1];
        create_murmur3_hashes(&[as_list(decoded)], &mut from_decoded).unwrap();

        assert_eq!(
            from_dict, from_decoded,
            "a dictionary-encoded list element must hash like the decoded value"
        );
    }

    /// The fast path must survive for a genuine first column, including one with a caller-supplied
    /// seed that is not Spark's 42, since the test for it is that every row is seeded alike.
    #[test]
    fn test_top_level_dictionary_matches_decoded() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int8Type;

        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let keys = arrow::array::Int8Array::from(vec![0i8, 1, 0]);
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
        let decoded: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 10]));

        for seed in [42u32, 7u32] {
            let mut a = vec![seed; 3];
            create_murmur3_hashes(&[Arc::clone(&dict)], &mut a).unwrap();
            let mut b = vec![seed; 3];
            create_murmur3_hashes(&[Arc::clone(&decoded)], &mut b).unwrap();
            assert_eq!(
                a, b,
                "top-level dictionary must match decoded for seed {seed}"
            );
        }
    }

    /// The uniformity check is what makes the fast path safe, and a single-row case cannot pin it:
    /// one row is trivially uniform. This hashes several rows whose incoming seeds all differ, so a
    /// dictionary first column has to take the unpacking fallback. It also covers a null key and a
    /// key pointing at a null dictionary value, since both skip the hash update.
    #[test]
    fn test_dictionary_with_nonuniform_seeds_matches_decoded() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int8Type;

        // values[2] is null, and one key is itself null
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), Some(20), None]));
        let keys = arrow::array::Int8Array::from(vec![Some(0), Some(1), Some(2), None, Some(0)]);
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
        // The same logical data with the dictionary resolved.
        let decoded: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(10),
            Some(20),
            None,
            None,
            Some(10),
        ]));

        let seeds: Vec<u32> = vec![7, 38, 69, 100, 131];
        let mut from_dict = seeds.clone();
        create_murmur3_hashes(&[dict], &mut from_dict).unwrap();
        let mut from_decoded = seeds;
        create_murmur3_hashes(&[decoded], &mut from_decoded).unwrap();

        assert_eq!(
            from_dict, from_decoded,
            "with per-row seeds a dictionary must hash like the decoded array"
        );
    }

    /// Arrow lets a `StructArray`'s children carry their own validity, so at a row where the struct
    /// itself is null the child buffer can still hold a value. Spark hashes a null struct as the
    /// seed, so those hidden child values must not reach the hash. This is the same null-mask
    /// propagation problem that #4432 fixed for `GetStructField`.
    #[test]
    fn test_null_struct_ignores_hidden_child_values() {
        use arrow::array::{Int32Array, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{DataType, Field, Fields};

        let fields: Fields = vec![Arc::new(Field::new("a", DataType::Int32, true))].into();
        // Row 1 is a null struct whose child still holds 999.
        let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(999)]));
        let nulls = NullBuffer::from(vec![true, false]);
        let with_hidden: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::clone(&child)],
            Some(nulls.clone()),
        ));
        // Same shape, but the hidden slot is null too.
        let child_null: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let without_hidden: ArrayRef =
            Arc::new(StructArray::new(fields, vec![child_null], Some(nulls)));

        let mut a = vec![42u32; 2];
        create_murmur3_hashes(&[with_hidden], &mut a).unwrap();
        let mut b = vec![42u32; 2];
        create_murmur3_hashes(&[without_hidden], &mut b).unwrap();

        assert_eq!(
            a, b,
            "a null struct must hash the same regardless of what its child buffer holds"
        );
        assert_eq!(a[1], 42, "a null struct must leave the seed untouched");
    }

    /// The struct branch is also reached once per element when hashing `array<struct<..>>`, which
    /// is the path #5567 made usable as a shuffle partitioning key. The test above hashes a struct
    /// directly, so it does not cover that route.
    ///
    /// Here the null is the list *element* itself, with valid elements either side so the chaining
    /// is exercised. The end-to-end test in `CometHashExpressionSuite` covers the other shape, a
    /// valid element wrapping a null struct, which is the one a query can produce.
    #[test]
    fn test_null_struct_element_of_list_ignores_hidden_child_values() {
        use arrow::array::{Int32Array, ListArray, StructArray};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, Fields};

        let fields: Fields = vec![Arc::new(Field::new("a", DataType::Int32, true))].into();
        // Element 1 is a null struct whose child still holds 999; elements 0 and 2 are valid.
        let element_nulls = NullBuffer::from(vec![true, false, true]);
        let with_hidden: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(999), Some(3)])) as ArrayRef],
            Some(element_nulls.clone()),
        ));
        // The same shape with the hidden slot null as well.
        let without_hidden: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef],
            Some(element_nulls),
        ));

        // One row holding all three elements, so element hashes chain in order.
        let as_list = |elements: ArrayRef| -> ArrayRef {
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", elements.data_type().clone(), true)),
                OffsetBuffer::new(vec![0i32, 3].into()),
                elements,
                None,
            ))
        };

        let mut from_hidden = vec![42u32; 1];
        create_murmur3_hashes(&[as_list(with_hidden)], &mut from_hidden).unwrap();
        let mut from_null = vec![42u32; 1];
        create_murmur3_hashes(&[as_list(without_hidden)], &mut from_null).unwrap();

        assert_eq!(
            from_hidden, from_null,
            "a null struct element must hash the same regardless of its child buffer"
        );
    }

    /// One `struct<a: Int32, b: Utf8>` element: `None` is a null struct, and the fields are
    /// independently nullable.
    type StructElem = Option<(Option<i32>, Option<&'static str>)>;
    /// One `array<struct<..>>` row: `None` is a null list.
    type ListRow = Option<Vec<StructElem>>;

    /// Builds `array<struct<a: Int32, b: Utf8>>` from `rows`.
    fn list_of_struct(rows: Vec<ListRow>) -> ArrayRef {
        use arrow::array::{Int32Builder, ListBuilder, StringBuilder, StructBuilder};
        use arrow::datatypes::{DataType, Field, Fields};

        let fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]
        .into();
        let struct_builder = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Int32Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        );
        let mut lb = ListBuilder::new(struct_builder);
        for row in rows {
            match row {
                None => lb.append(false),
                Some(elems) => {
                    for elem in elems {
                        let sb = lb.values();
                        match elem {
                            None => {
                                sb.field_builder::<Int32Builder>(0).unwrap().append_null();
                                sb.field_builder::<StringBuilder>(1).unwrap().append_null();
                                sb.append(false);
                            }
                            Some((a, b)) => {
                                match a {
                                    Some(v) => {
                                        sb.field_builder::<Int32Builder>(0).unwrap().append_value(v)
                                    }
                                    None => {
                                        sb.field_builder::<Int32Builder>(0).unwrap().append_null()
                                    }
                                }
                                match b {
                                    Some(v) => sb
                                        .field_builder::<StringBuilder>(1)
                                        .unwrap()
                                        .append_value(v),
                                    None => {
                                        sb.field_builder::<StringBuilder>(1).unwrap().append_null()
                                    }
                                }
                                sb.append(true);
                            }
                        }
                    }
                    lb.append(true);
                }
            }
        }
        Arc::new(lb.finish())
    }

    fn hash_of(array: ArrayRef, num_rows: usize) -> Vec<u32> {
        let mut hashes = vec![42u32; num_rows];
        create_murmur3_hashes(&[array], &mut hashes).unwrap();
        hashes
    }

    /// `array<struct<..>>` is the shape that goes through `hash_list_array!`, the per-element path.
    /// These values were produced by that implementation and pin it: the hash decides which
    /// partition a row lands in, so any rewrite of that path has to reproduce them exactly.
    #[test]
    fn test_list_of_struct_hashes_are_stable() {
        let rows = vec![
            Some(vec![Some((Some(1), Some("x"))), Some((Some(2), Some("y")))]),
            Some(vec![Some((Some(3), Some("z")))]),
            // empty list: contributes nothing, so the seed survives
            Some(vec![]),
            // null list
            None,
            // null struct element, and elements with null fields
            Some(vec![None, Some((None, Some("w"))), Some((Some(4), None))]),
            // repeated element values, to catch an implementation that dedupes or reorders
            Some(vec![Some((Some(5), Some("s"))), Some((Some(5), Some("s")))]),
        ];
        let n = rows.len();
        assert_eq!(hash_of(list_of_struct(rows), n), EXPECTED_LIST_OF_STRUCT);
    }

    /// Element order must matter: Spark chains the element hashes in sequence.
    #[test]
    fn test_list_of_struct_is_order_sensitive() {
        let forward = list_of_struct(vec![Some(vec![
            Some((Some(1), Some("a"))),
            Some((Some(2), Some("b"))),
        ])]);
        let reversed = list_of_struct(vec![Some(vec![
            Some((Some(2), Some("b"))),
            Some((Some(1), Some("a"))),
        ])]);
        assert_ne!(
            hash_of(forward, 1),
            hash_of(reversed, 1),
            "element order must change the hash"
        );
    }

    /// The batched implementation makes one pass per element position, so a single long list forces
    /// as many passes as its length while every other row is already finished. Check that a skewed
    /// batch still agrees with hashing each row on its own, which is what the per-element
    /// implementation effectively did.
    #[test]
    fn test_list_of_struct_skewed_lengths() {
        let mut rows: Vec<ListRow> = vec![Some(vec![Some((Some(1), Some("a")))]); 8];
        // One row far longer than the rest.
        rows.push(Some(
            (0..64)
                .map(|i| Some((Some(i), Some("long"))))
                .collect::<Vec<_>>(),
        ));
        rows.push(Some(vec![]));

        let batched = hash_of(list_of_struct(rows.clone()), rows.len());

        // Hash each row as its own batch of one; the result must match position by position.
        let per_row: Vec<u32> = rows
            .into_iter()
            .map(|row| hash_of(list_of_struct(vec![row]), 1)[0])
            .collect();
        assert_eq!(batched, per_row, "skewed batch must match per-row hashing");
    }

    /// `LargeList` reaches the same code path with 64-bit offsets. This pins that the two list
    /// widths agree on the same data. It does not exercise the 64-bit gather itself: narrowing the
    /// indices only goes wrong past `u32::MAX` elements, which is far larger than a test can build,
    /// so the index width is chosen from the offset type rather than guarded by a test here.
    #[test]
    fn test_large_list_of_struct_matches_list() {
        use arrow::array::{Int32Builder, LargeListBuilder, StringBuilder, StructBuilder};
        use arrow::datatypes::{DataType, Field, Fields};

        let fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]
        .into();
        let sb = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Int32Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        );
        let mut lb = LargeListBuilder::new(sb);
        for row in [vec![1, 2], vec![3], vec![]] {
            for v in row {
                let s = lb.values();
                s.field_builder::<Int32Builder>(0).unwrap().append_value(v);
                s.field_builder::<StringBuilder>(1)
                    .unwrap()
                    .append_value(format!("s{v}"));
                s.append(true);
            }
            lb.append(true);
        }
        let large: ArrayRef = Arc::new(lb.finish());

        let small = list_of_struct(vec![
            Some(vec![
                Some((Some(1), Some("s1"))),
                Some((Some(2), Some("s2"))),
            ]),
            Some(vec![Some((Some(3), Some("s3")))]),
            Some(vec![]),
        ]);

        assert_eq!(
            hash_of(large, 3),
            hash_of(small, 3),
            "LargeList and List must hash identically"
        );
    }

    /// `array<array<int>>`: the element is a list rather than a struct, so it takes the same
    /// non-primitive element path one level deeper.
    #[test]
    fn test_list_of_list_hashes_match_per_row() {
        use arrow::array::{Int32Builder, ListBuilder};

        let build = |rows: &[Vec<Vec<i32>>]| -> ArrayRef {
            let mut lb = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
            for row in rows {
                for inner in row {
                    for v in inner {
                        lb.values().values().append_value(*v);
                    }
                    lb.values().append(true);
                }
                lb.append(true);
            }
            Arc::new(lb.finish())
        };

        let rows = vec![
            vec![vec![1, 2], vec![3]],
            vec![vec![4]],
            vec![],
            vec![vec![5, 6, 7], vec![], vec![8]],
        ];
        let batched = hash_of(build(&rows), rows.len());
        let per_row: Vec<u32> = rows
            .iter()
            .map(|row| hash_of(build(std::slice::from_ref(row)), 1)[0])
            .collect();
        assert_eq!(
            batched, per_row,
            "array<array<int>> must match per-row hashing"
        );
    }

    /// The cursor drops a row once its elements run out, so rows finishing on different passes,
    /// including ones in the middle of the batch, exercise the survivor bookkeeping. Compared
    /// against hashing each row as its own batch.
    #[test]
    fn test_list_of_struct_rows_exhaust_on_different_passes() {
        let rows: Vec<ListRow> = vec![
            Some((0..5).map(|i| Some((Some(i), Some("a")))).collect()),
            Some(vec![Some((Some(9), Some("b")))]),
            Some((0..3).map(|i| Some((Some(i), Some("c")))).collect()),
            Some(vec![]),
            Some((0..7).map(|i| Some((Some(i), Some("d")))).collect()),
            None,
            Some(vec![Some((Some(1), Some("e"))), Some((Some(2), Some("f")))]),
        ];
        let batched = hash_of(list_of_struct(rows.clone()), rows.len());
        let per_row: Vec<u32> = rows
            .into_iter()
            .map(|row| hash_of(list_of_struct(vec![row]), 1)[0])
            .collect();
        assert_eq!(
            batched, per_row,
            "rows exhausting on different passes must agree"
        );
    }

    /// A sliced list has a non-zero first offset, and the cursor subtracts it when building gather
    /// indices, so an off-by-one there would only show up on a slice.
    #[test]
    fn test_sliced_list_of_struct_matches_unsliced() {
        let rows: Vec<ListRow> = vec![
            Some(vec![Some((Some(1), Some("x")))]),
            Some(vec![Some((Some(2), Some("y"))), Some((Some(3), Some("z")))]),
            Some((0..4).map(|i| Some((Some(i), Some("w")))).collect()),
            Some(vec![Some((Some(8), Some("v")))]),
        ];
        let full = list_of_struct(rows.clone());

        // Hash rows 1..3 through a slice, and the same rows built on their own.
        let sliced = full.slice(1, 2);
        let mut from_slice = vec![42u32; 2];
        create_murmur3_hashes(&[sliced], &mut from_slice).unwrap();

        let standalone = list_of_struct(rows[1..3].to_vec());
        let mut from_standalone = vec![42u32; 2];
        create_murmur3_hashes(&[standalone], &mut from_standalone).unwrap();

        assert_eq!(
            from_slice, from_standalone,
            "a sliced list must hash like the same rows built unsliced"
        );
    }

    /// A null list can still cover a non-empty range of elements. Those elements must not be
    /// hashed, and the row must not join the cursor.
    #[test]
    fn test_null_list_with_populated_range_is_skipped() {
        use arrow::array::{Int32Builder, ListArray, StringBuilder, StructBuilder};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, Fields};

        let fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]
        .into();
        let mut sb = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Int32Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        );
        for v in 0..3 {
            sb.field_builder::<Int32Builder>(0).unwrap().append_value(v);
            sb.field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value("hidden");
            sb.append(true);
        }
        let elements: ArrayRef = Arc::new(sb.finish());

        // Row 0 covers elements 0..1, row 1 is null but still covers 1..3.
        let list = ListArray::new(
            Arc::new(Field::new("item", elements.data_type().clone(), true)),
            OffsetBuffer::new(vec![0i32, 1, 3].into()),
            elements,
            Some(NullBuffer::from(vec![true, false])),
        );
        let mut hashes = vec![42u32; 2];
        create_murmur3_hashes(&[Arc::new(list) as ArrayRef], &mut hashes).unwrap();
        assert_eq!(hashes[1], 42, "a null list must leave the seed untouched");

        // And it must not disturb the visible row either.
        let only_visible = list_of_struct(vec![Some(vec![Some((Some(0), Some("hidden")))])]);
        let mut expected = vec![42u32; 1];
        create_murmur3_hashes(&[only_visible], &mut expected).unwrap();
        assert_eq!(hashes[0], expected[0]);
    }

    /// Lengths that step down rather than being either all equal or one long outlier, so the
    /// uniform-length gate is not taken and rows leave the cursor on consecutive passes.
    #[test]
    fn test_list_of_struct_descending_lengths() {
        let rows: Vec<ListRow> = (1..=12)
            .rev()
            .map(|n| Some((0..n).map(|i| Some((Some(i), Some("s")))).collect()))
            .collect();
        let batched = hash_of(list_of_struct(rows.clone()), rows.len());
        let per_row: Vec<u32> = rows
            .into_iter()
            .map(|row| hash_of(list_of_struct(vec![row]), 1)[0])
            .collect();
        assert_eq!(batched, per_row);
    }

    /// A lone-row list must not stop the caller hashing the remaining columns.
    #[test]
    fn test_single_row_list_then_another_column() {
        let l = list_of_struct(vec![Some(vec![
            Some((Some(1), Some("a"))),
            Some((Some(2), Some("b"))),
        ])]);
        let other: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![7]));

        // both columns together
        let mut both = vec![42u32; 1];
        create_murmur3_hashes(&[Arc::clone(&l), Arc::clone(&other)], &mut both).unwrap();

        // chaining them by hand must agree
        let mut step = vec![42u32; 1];
        create_murmur3_hashes(&[l], &mut step).unwrap();
        create_murmur3_hashes(&[other], &mut step).unwrap();

        assert_eq!(both, step, "the second column must still be hashed");
    }

    #[test]
    fn test_i8() {
        test_murmur3_hash::<i8, Int8Array>(
            vec![Some(1), Some(0), Some(-1), Some(i8::MAX), Some(i8::MIN)],
            vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x43b4d8ed, 0x422a1365],
        );
    }

    #[test]
    fn test_i32() {
        test_murmur3_hash::<i32, Int32Array>(
            vec![Some(1), Some(0), Some(-1), Some(i32::MAX), Some(i32::MIN)],
            vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x07fb67e7, 0x2b1f0fc6],
        );
    }

    #[test]
    fn test_i64() {
        test_murmur3_hash::<i64, Int64Array>(
            vec![Some(1), Some(0), Some(-1), Some(i64::MAX), Some(i64::MIN)],
            vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb],
        );
    }

    #[test]
    fn test_f32() {
        test_murmur3_hash::<f32, Float32Array>(
            vec![
                Some(1.0),
                Some(0.0),
                Some(-0.0),
                Some(-1.0),
                Some(99999999999.99999999999),
                Some(-99999999999.99999999999),
            ],
            vec![
                0xe434cc39, 0x379fae8f, 0x379fae8f, 0xdc0da8eb, 0xcbdc340f, 0xc0361c86,
            ],
        );
    }

    #[test]
    fn test_f64() {
        test_murmur3_hash::<f64, Float64Array>(
            vec![
                Some(1.0),
                Some(0.0),
                Some(-0.0),
                Some(-1.0),
                Some(99999999999.99999999999),
                Some(-99999999999.99999999999),
            ],
            vec![
                0xe4876492, 0x9c67b85d, 0x9c67b85d, 0x13d81357, 0xb87e1595, 0xa0eef9f9,
            ],
        );
    }

    #[test]
    fn test_str() {
        let input = [
            "hello", "bar", "", "😁", "天地", "a", "ab", "abc", "abcd", "abcde",
        ]
        .iter()
        .map(|s| Some(s.to_string()))
        .collect::<Vec<Option<String>>>();
        let expected: Vec<u32> = vec![
            3286402344, 2486176763, 142593372, 885025535, 2395000894, 1485273170, 0xfa37157b,
            1322437556, 0xe860e5cc, 814637928,
        ];

        test_murmur3_hash::<String, StringArray>(input.clone(), expected);
    }
    /// Both sides of the eligibility threshold must produce the same hashes, since the check only
    /// decides which path runs. A struct of flat leaves under the size limit is batched; the same
    /// data behind a child large enough to fail the limit is sliced. An off-by-one in either path's
    /// element index shows up as a mismatch.
    #[test]
    fn eligible_and_ineligible_shapes_hash_alike() {
        use crate::hash_funcs::utils::{gather_is_eligible, GATHER_ELIGIBLE_CHILD_BYTES};
        use arrow::array::builder::{Int32Builder, ListBuilder, StringBuilder, StructBuilder};
        use arrow::datatypes::{DataType, Field, Fields};

        fn build(payload_len: usize) -> ArrayRef {
            let fields: Fields = vec![
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Field::new("b", DataType::Utf8, true)),
            ]
            .into();
            let mut lb = ListBuilder::new(StructBuilder::new(
                fields,
                vec![
                    Box::new(Int32Builder::new()),
                    Box::new(StringBuilder::new()),
                ],
            ));
            // Uneven lengths with an empty row and a null row, so rows drop out on different passes.
            let payload = "x".repeat(payload_len);
            for (row, len) in [3usize, 0, 4, 1, 2].iter().enumerate() {
                for i in 0..*len {
                    let sb = lb.values();
                    sb.field_builder::<Int32Builder>(0)
                        .unwrap()
                        .append_value((row * 10 + i) as i32);
                    sb.field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_value(&payload);
                    sb.append(i % 3 != 2);
                }
                lb.append(row != 1);
            }
            Arc::new(lb.finish())
        }

        let small = build(4);
        let small_elements = small
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap()
            .values();
        assert!(
            gather_is_eligible(small_elements.as_ref()),
            "a small flat struct should be batched"
        );

        // One wide value pushes the retained child past the limit, so the same shape is sliced.
        let big = build(GATHER_ELIGIBLE_CHILD_BYTES);
        let big_elements = big
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap()
            .values();
        assert!(
            !gather_is_eligible(big_elements.as_ref()),
            "a child over the retained-size limit should not be batched"
        );

        // A following column, so leaving the pass loop must not skip it on either path.
        let following: ArrayRef = Arc::new(Int32Array::from(vec![5, 6, 7, 8, 9]));
        let seeds = [11u32, 22, 33, 44, 55];

        for (name, array) in [("batched", &small), ("sliced", &big)] {
            let mut got = seeds;
            create_murmur3_hashes(&[Arc::clone(array), Arc::clone(&following)], &mut got).unwrap();

            // Reference: hash each row alone, which cannot batch across rows at all.
            let mut want = [0u32; 5];
            for row in 0..5 {
                let mut one = [seeds[row]];
                create_murmur3_hashes(&[array.slice(row, 1), following.slice(row, 1)], &mut one)
                    .unwrap();
                want[row] = one[0];
            }
            assert_eq!(got, want, "{name}: must agree with hashing each row alone");
        }
    }

    /// A nested element type is not eligible however small it is, so the shape that used to
    /// accumulate a gather per nesting level keeps the per-element path.
    #[test]
    fn nested_and_dictionary_elements_are_not_eligible() {
        use crate::hash_funcs::utils::gather_is_eligible;
        use arrow::array::{DictionaryArray, Int32Array as I32, StringArray, StructArray};
        use arrow::datatypes::{DataType, Field, Fields, Int32Type};

        // struct<list<int>>: the child recurses, so each level would hold its own gather.
        let inner: ArrayRef = Arc::new(I32::from(vec![1, 2, 3, 4]));
        let offsets = arrow::buffer::OffsetBuffer::from_lengths([2usize, 2]);
        let list_child: ArrayRef = Arc::new(arrow::array::ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            inner,
            None,
        ));
        let nested_fields: Fields = vec![Arc::new(Field::new(
            "l",
            list_child.data_type().clone(),
            true,
        ))]
        .into();
        let nested: ArrayRef = Arc::new(StructArray::new(nested_fields, vec![list_child], None));
        assert!(
            !gather_is_eligible(nested.as_ref()),
            "a nested child must keep the per-element path"
        );

        // struct<dictionary<string>>: `take` shares the values, so batching is not modelled here.
        let dict: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                I32::from(vec![0, 0]),
                Arc::new(StringArray::from(vec!["x"])),
            )
            .unwrap(),
        );
        let dict_fields: Fields =
            vec![Arc::new(Field::new("d", dict.data_type().clone(), true))].into();
        let with_dict: ArrayRef = Arc::new(StructArray::new(dict_fields, vec![dict], None));
        assert!(
            !gather_is_eligible(with_dict.as_ref()),
            "a dictionary child must keep the per-element path"
        );
    }
}
