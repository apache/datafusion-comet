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

//! Row ranges that keep expanded dictionary columns within Arrow's `i32` offsets.
//!
//! The JVM columnar shuffle writes a string or binary column dictionary-encoded when its values
//! repeat, so a block stays small even when the expanded column would pass 2 GiB. Splitting the
//! block by these ranges before expanding it keeps every expanded column representable.

use arrow::array::{
    downcast_dictionary_array, Array, ArrayRef, AsArray, DictionaryArray, GenericByteArray,
    RecordBatch,
};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, ByteArrayType, DataType};
use std::ops::Range;

/// The most value bytes one expanded `Utf8` or `Binary` column can hold.
pub const MAX_EXPANDED_BYTES: usize = i32::MAX as usize;

/// Consecutive row ranges of `batch` such that expanding any top-level
/// `Dictionary(_, Utf8 | Binary)` column over one range produces at most `max_bytes` of values.
///
/// Never empty. A batch that needs no splitting, including an empty one, is one range over every
/// row, and otherwise every range holds at least one row. A row whose value alone exceeds
/// `max_bytes` gets its own range, which cannot happen at `i32::MAX`. Only top-level dictionaries
/// are measured, which is all the local read path expands.
pub fn dictionary_expansion_ranges(batch: &RecordBatch, max_bytes: usize) -> Vec<Range<usize>> {
    let num_rows = batch.num_rows();
    let row_bytes: Vec<Vec<usize>> = batch
        .columns()
        .iter()
        .filter_map(|column| expanded_row_bytes(column, max_bytes))
        .collect();
    if row_bytes.is_empty() {
        return std::iter::once(0..num_rows).collect();
    }

    let mut ranges = Vec::new();
    let mut start = 0;
    let mut totals = vec![0usize; row_bytes.len()];
    for row in 0..num_rows {
        let overflows = row_bytes
            .iter()
            .zip(&totals)
            .any(|(bytes, total)| total + bytes[row] > max_bytes);
        if overflows && row > start {
            ranges.push(start..row);
            start = row;
            totals.fill(0);
        }
        for (total, bytes) in totals.iter_mut().zip(&row_bytes) {
            *total += bytes[row];
        }
    }
    ranges.push(start..num_rows);
    ranges
}

/// The bytes each row contributes when `column` is expanded, or `None` when the column is not a
/// measured dictionary or provably fits because every key could select the longest value.
fn expanded_row_bytes(column: &ArrayRef, max_bytes: usize) -> Option<Vec<usize>> {
    let DataType::Dictionary(_, value_type) = column.data_type() else {
        return None;
    };
    downcast_dictionary_array!(
        column => match value_type.as_ref() {
            DataType::Utf8 => {
                row_bytes(column, column.values().as_string::<i32>(), max_bytes)
            }
            DataType::Binary => {
                row_bytes(column, column.values().as_binary::<i32>(), max_bytes)
            }
            _ => None,
        },
        _ => None
    )
}

fn row_bytes<K, T>(
    dictionary: &DictionaryArray<K>,
    values: &GenericByteArray<T>,
    max_bytes: usize,
) -> Option<Vec<usize>>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType<Offset = i32>,
{
    // A null value or an out-of-range key adds nothing, because `take` writes no bytes for it.
    let value_bytes = |key: usize| -> usize {
        if key < values.len() && values.is_valid(key) {
            values.value_length(key).as_usize()
        } else {
            0
        }
    };
    let longest = (0..values.len()).map(value_bytes).max().unwrap_or(0);
    if longest.saturating_mul(dictionary.len()) <= max_bytes {
        return None;
    }
    let keys = dictionary.keys();
    Some(
        (0..keys.len())
            .map(|row| {
                if keys.is_valid(row) {
                    value_bytes(keys.value(row).as_usize())
                } else {
                    0
                }
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, Int32Array, Int8Array, StringArray};
    use arrow::datatypes::{Field, Int32Type, Int8Type, Schema};
    use std::sync::Arc;

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn strings(keys: Vec<Option<i32>>, values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                Arc::new(StringArray::from(values)),
            )
            .unwrap(),
        )
    }

    /// One range over every row, since clippy rejects a one-element literal of ranges.
    fn whole(rows: usize) -> Vec<Range<usize>> {
        std::iter::once(0..rows).collect()
    }

    fn expanded_bytes(batch: &RecordBatch, range: &Range<usize>) -> Vec<usize> {
        batch
            .slice(range.start, range.len())
            .columns()
            .iter()
            .map(|c| {
                let expanded = arrow::compute::cast(c, &DataType::Utf8).unwrap();
                expanded.as_string::<i32>().values().len()
            })
            .collect()
    }

    #[test]
    fn keeps_a_block_that_fits_whole() {
        let b = batch(vec![strings(
            vec![Some(0), Some(1), Some(0)],
            vec![Some("aaaa"), Some("bb")],
        )]);
        assert_eq!(dictionary_expansion_ranges(&b, 10), whole(3));
    }

    #[test]
    fn splits_at_the_budget_and_keeps_every_row() {
        let b = batch(vec![strings(
            vec![Some(0); 10],
            vec![Some("aaaa"), Some("unused")],
        )]);
        // Four-byte rows under a ten-byte budget fit two per range.
        let ranges = dictionary_expansion_ranges(&b, 10);
        assert_eq!(ranges, [0..2, 2..4, 4..6, 6..8, 8..10]);
        for range in &ranges {
            assert!(expanded_bytes(&b, range).iter().all(|&n| n <= 10));
        }
    }

    #[test]
    fn counts_null_keys_and_null_values_as_empty() {
        // Null keys can sit on a large value's slot, and a valid key can select a null value.
        let big = "x".repeat(100);
        let b = batch(vec![strings(
            vec![None, Some(1), None, Some(1), Some(0)],
            vec![Some(big.as_str()), None],
        )]);
        assert_eq!(dictionary_expansion_ranges(&b, 100), whole(5));
        // An all-null column ships as a dictionary with no values at all.
        let b = batch(vec![strings(vec![None, None, None], vec![])]);
        assert_eq!(dictionary_expansion_ranges(&b, 0), whole(3));
    }

    #[test]
    fn cuts_where_any_column_would_overflow() {
        let a = strings(vec![Some(0); 4], vec![Some("aaaaaa")]);
        // Rows of `a` are six bytes and rows of `b` are one, then eight bytes, so `a` forces the
        // first cut and `b` the second.
        let b = strings(
            vec![Some(0), Some(1), Some(1), Some(1)],
            vec![Some("b"), Some("bbbbbbbb")],
        );
        let b = batch(vec![a, b]);
        let ranges = dictionary_expansion_ranges(&b, 12);
        assert_eq!(ranges, [0..2, 2..3, 3..4]);
        for range in &ranges {
            assert!(expanded_bytes(&b, range).iter().all(|&n| n <= 12));
        }
    }

    #[test]
    fn gives_an_oversized_row_its_own_range() {
        let b = batch(vec![strings(
            vec![Some(0), Some(1), Some(0)],
            vec![Some("a"), Some("bbbbbbbbbb")],
        )]);
        assert_eq!(dictionary_expansion_ranges(&b, 4), [0..1, 1..2, 2..3]);
    }

    #[test]
    fn measures_binary_values_and_narrow_keys() {
        let column: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0i8, 0, 0]),
                Arc::new(BinaryArray::from(vec![b"abcde".as_ref()])),
            )
            .unwrap(),
        );
        assert_eq!(
            dictionary_expansion_ranges(&batch(vec![column]), 9),
            [0..1, 1..2, 2..3]
        );
    }

    #[test]
    fn measures_a_sliced_batch() {
        let b = batch(vec![strings(
            vec![Some(1), Some(1), Some(0), Some(0), Some(0)],
            vec![Some("aaaa"), Some("bbbbbbbbbbbbbbbbbbbb")],
        )]);
        // The slice starts past the long values, so only the short ones count.
        assert_eq!(
            dictionary_expansion_ranges(&b.slice(2, 3), 8),
            vec![0..2, 2..3]
        );
    }

    #[test]
    fn ignores_plain_columns_and_returns_one_range_for_an_empty_batch() {
        let plain: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(50)]));
        assert_eq!(
            dictionary_expansion_ranges(&batch(vec![plain]), 1),
            whole(1)
        );
        let empty = batch(vec![strings(vec![], vec![Some("a")])]);
        assert_eq!(dictionary_expansion_ranges(&empty, 1), whole(0));
    }

    #[test]
    fn splits_the_issue_7496_block_under_the_real_budget() {
        // One 300 KiB value referenced by 8192 keys expands to about 2.4 GiB, which overflows a
        // single Utf8 array. Expanding it would allocate gigabytes, so this checks the plan only.
        let value = "x".repeat(300 * 1024);
        let b = batch(vec![strings(
            vec![Some(0); 8192],
            vec![Some(value.as_str())],
        )]);
        let column = b.column(0);
        assert!(arrow::compute::cast(column, &DataType::Utf8).is_err());

        let ranges = dictionary_expansion_ranges(&b, MAX_EXPANDED_BYTES);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges.iter().map(Range::len).sum::<usize>(), 8192);
        for range in &ranges {
            assert!(range.len() * value.len() <= MAX_EXPANDED_BYTES);
        }
    }
}
