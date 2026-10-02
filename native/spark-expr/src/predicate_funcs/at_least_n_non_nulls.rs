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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float32Type, Float64Type, Schema};
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

/// Spark's per-row count of non-null, non-NaN children, stopping once `n` is reached.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AtLeastNNonNulls {
    n: i32,
    children: Vec<Arc<dyn PhysicalExpr>>,
    small_batch_threshold: usize,
}

impl AtLeastNNonNulls {
    pub const DEFAULT_SMALL_BATCH_THRESHOLD: usize = 64;

    pub fn new(n: i32, children: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            n,
            children,
            small_batch_threshold: Self::DEFAULT_SMALL_BATCH_THRESHOLD,
        }
    }

    /// Sets the row-count/bitmap-count crossover without changing the bitmap word size.
    pub fn with_small_batch_threshold(mut self, threshold: usize) -> Result<Self> {
        if threshold == 0 {
            return plan_err!("AtLeastNNonNulls small batch threshold must be positive");
        }
        self.small_batch_threshold = threshold;
        Ok(self)
    }

    // Small batches may not amortize the bitmap counter's bookkeeping.
    fn evaluate_small_batch(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let mut counts = vec![0; batch.num_rows()];
        let mut remaining = batch.num_rows();
        for child in &self.children {
            if remaining == 0 {
                break;
            }
            // Column/literal reads cannot raise per-row errors or have side effects. Avoid
            // filtering the input batch for the common DataFrame.na.drop attribute inputs.
            let value =
                if remaining == batch.num_rows() || child.is::<Column>() || child.is::<Literal>() {
                    child.evaluate(batch)?
                } else {
                    let selection = BooleanArray::new(
                        BooleanBuffer::collect_bool(counts.len(), |i| counts[i] < self.n),
                        None,
                    );
                    child.evaluate_selection(batch, &selection)?
                };
            let scalar = matches!(value, ColumnarValue::Scalar(_));
            let array = value.into_array(if scalar { 1 } else { batch.num_rows() })?;
            let valid = valid_values(&array)?;
            if scalar {
                if valid.value(0) {
                    for count in &mut counts {
                        if *count < self.n {
                            *count += 1;
                            remaining -= usize::from(*count == self.n);
                        }
                    }
                }
            } else {
                for row in valid.set_indices() {
                    if counts[row] < self.n {
                        counts[row] += 1;
                        remaining -= usize::from(counts[row] == self.n);
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            BooleanBuffer::collect_bool(counts.len(), |i| counts[i] >= self.n),
            None,
        ))))
    }
}

impl Display for AtLeastNNonNulls {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "atleastnnonnulls({}, {:?})", self.n, self.children)
    }
}

impl PhysicalExpr for AtLeastNNonNulls {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.n <= 0 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))));
        }
        // The standard na.drop("all"/"any") cases reduce to bitmap OR/AND and do not
        // need a per-row counter. For AND, still evaluate every child: Spark does not
        // skip later expressions just because a row can no longer reach the threshold.
        if self.n == 1 || self.n as usize == self.children.len() {
            let all = self.n != 1;
            let mut result = if all {
                BooleanBuffer::new_set(batch.num_rows())
            } else {
                BooleanBuffer::new_unset(batch.num_rows())
            };
            for child in &self.children {
                if batch.num_rows() == 0 || (!all && result.count_set_bits() == batch.num_rows()) {
                    break;
                }
                let value = if all || child.is::<Column>() || child.is::<Literal>() {
                    child.evaluate(batch)?
                } else {
                    child.evaluate_selection(batch, &BooleanArray::new(!&result, None))?
                };
                let valid = valid_values(&value.into_array(batch.num_rows())?)?;
                result = if all {
                    &result & &valid
                } else {
                    &result | &valid
                };
            }
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
                result, None,
            ))));
        }
        if batch.num_rows() < self.small_batch_threshold {
            return self.evaluate_small_batch(batch);
        }
        // One bit plane per binary digit holds 64 row counts in each chunk.
        // Increment these counters with a half-adder, saturating at n by masking
        // out rows that have already reached it.
        let bits = (i32::BITS - self.n.leading_zeros()) as usize;
        let chunks = batch.num_rows().div_ceil(64);
        let mut counts = vec![0_u64; chunks * bits];
        let mut result = vec![0_u64; chunks];
        let mut remaining = batch.num_rows();
        for child in &self.children {
            if remaining == 0 {
                break;
            }
            let value =
                if remaining == batch.num_rows() || child.is::<Column>() || child.is::<Literal>() {
                    child.evaluate(batch)?
                } else {
                    let selection = BooleanArray::new(
                        BooleanBuffer::collect_bool(batch.num_rows(), |row| {
                            result[row / 64] & (1 << (row % 64)) == 0
                        }),
                        None,
                    );
                    child.evaluate_selection(batch, &selection)?
                };
            let scalar = matches!(value, ColumnarValue::Scalar(_));
            let array = value.into_array(if scalar { 1 } else { batch.num_rows() })?;
            let valid = valid_values(&array)?;
            let valid = if scalar {
                if !valid.value(0) {
                    continue;
                }
                BooleanBuffer::new_set(batch.num_rows())
            } else {
                valid
            };
            for ((counts, result), valid) in counts
                .chunks_exact_mut(bits)
                .zip(&mut result)
                .zip(valid.bit_chunks().iter_padded())
            {
                let mut carry = valid & !*result;
                if carry == 0 {
                    continue;
                }
                for count in counts.iter_mut() {
                    let next = *count & carry;
                    *count ^= carry;
                    carry = next;
                    if carry == 0 {
                        break;
                    }
                }
                // Counts never exceed n, so intersecting its set bit planes
                // identifies equality without comparing the zero bit planes.
                let mut threshold = (self.n as u32) ^ (1 << (bits - 1));
                let mut reached = counts[bits - 1];
                while threshold != 0 && reached != 0 {
                    reached &= counts[threshold.trailing_zeros() as usize];
                    threshold &= threshold - 1;
                }
                remaining -= (reached & !*result).count_ones() as usize;
                *result |= reached;
            }
        }
        // Arrow stores bitmap bytes in little-endian order on every platform.
        let result = result.into_iter().map(u64::to_le).collect::<Vec<_>>();
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            BooleanBuffer::new(Buffer::from_vec(result), 0, batch.num_rows()),
            None,
        ))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            n: self.n,
            children,
            small_batch_threshold: self.small_batch_threshold,
        }))
    }
}

fn valid_values(array: &ArrayRef) -> Result<BooleanBuffer> {
    // Dictionary validity also includes null dictionary values. Floating-point dictionaries
    // need decoding to inspect NaNs; other types need only their logical validity bitmap.
    let array = match array.data_type() {
        DataType::Dictionary(_, value_type) if value_type.is_floating() => cast(array, value_type)?,
        _ => Arc::clone(array),
    };
    let valid = array.logical_nulls().map_or_else(
        || BooleanBuffer::new_set(array.len()),
        |n| n.inner().clone(),
    );
    // Computing NaN bits independently of nulls lets Arrow scan the values without
    // a per-element validity branch. The final AND removes values beneath nulls.
    Ok(match array.data_type() {
        DataType::Float32 => {
            let not_nan =
                BooleanArray::from_unary(array.as_primitive::<Float32Type>(), |v| !v.is_nan());
            &valid & not_nan.values()
        }
        DataType::Float64 => {
            let not_nan =
                BooleanArray::from_unary(array.as_primitive::<Float64Type>(), |v| !v.is_nan());
            &valid & not_nan.values()
        }
        _ => valid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Float32Array, Float64Array, Int32Array, NullArray, StringArray,
    };
    use arrow::compute::CastOptions;
    use arrow::datatypes::{Field, Int32Type};
    use datafusion::physical_expr::expressions::CastExpr;

    fn batch(arrays: Vec<ArrayRef>) -> RecordBatch {
        let schema = Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn columns(batch: &RecordBatch) -> Vec<Arc<dyn PhysicalExpr>> {
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| Arc::new(Column::new(f.name(), i)) as _)
            .collect()
    }

    fn result(expr: &AtLeastNNonNulls, batch: &RecordBatch) -> Vec<bool> {
        let array = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        assert_eq!(array.null_count(), 0);
        array.as_boolean().values().iter().collect()
    }

    #[test]
    fn at_least_n_non_nulls_threshold_survives_rewrite() {
        let input = batch(vec![Arc::new(Int32Array::from(vec![Some(1), None]))]);
        let expr = AtLeastNNonNulls::new(2, vec![]);
        assert_eq!(expr.small_batch_threshold, 64);
        assert!(expr.with_small_batch_threshold(0).is_err());

        let expr = Arc::new(
            AtLeastNNonNulls::new(2, vec![])
                .with_small_batch_threshold(128)
                .unwrap(),
        );
        let rewritten = expr.with_new_children(columns(&input)).unwrap();
        let rewritten = rewritten.downcast_ref::<AtLeastNNonNulls>().unwrap();
        assert_eq!(rewritten.small_batch_threshold, 128);
        assert_eq!(result(rewritten, &input), [false, false]);
    }

    #[test]
    fn at_least_n_non_nulls_mixed_types_and_thresholds() {
        let batch = batch(vec![
            Arc::new(StringArray::from(vec![Some(""), None, Some("NaN"), None])),
            Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                Some(-0.0),
                None,
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
                None,
            ])),
            Arc::new(NullArray::new(4)),
        ]);
        for n in [-1, 0, 1, 2, 3, 4, 5, i32::MAX] {
            let expr = AtLeastNNonNulls::new(n, columns(&batch));
            assert_eq!(result(&expr, &batch), [2, 1, 2, 0].map(|count| count >= n));
            assert!(!expr.nullable(batch.schema_ref()).unwrap());
        }
        let sliced = batch.slice(1, 2);
        assert_eq!(
            result(&AtLeastNNonNulls::new(2, columns(&sliced)), &sliced),
            [false, true]
        );
        assert!(result(
            &AtLeastNNonNulls::new(1, columns(&batch)),
            &batch.slice(0, 0)
        )
        .is_empty());
    }

    #[test]
    fn at_least_n_non_nulls_sliced_bitmaps() {
        let batch = batch(
            (0..5)
                .map(|column| {
                    Arc::new(Float64Array::from_iter((0..133).map(|row| {
                        match (row + column) % 7 {
                            0 => None,
                            1 => Some(f64::NAN),
                            _ => Some(row as f64),
                        }
                    }))) as ArrayRef
                })
                .collect(),
        )
        .slice(3, 129);
        for n in 0..=6 {
            let expected = (3..132)
                .map(|row| (0..5).filter(|column| (row + column) % 7 > 1).count() >= n)
                .collect::<Vec<_>>();
            assert_eq!(
                result(&AtLeastNNonNulls::new(n as i32, columns(&batch)), &batch),
                expected
            );
        }
    }

    #[test]
    fn at_least_n_non_nulls_dictionary_values_and_scalars() {
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), Some(2), None]),
            Arc::new(Float64Array::from(vec![Some(3.0), Some(f64::NAN), None])),
        )
        .unwrap();
        let strings = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), Some(2), None]),
            Arc::new(StringArray::from(vec![Some(""), Some("NaN"), None])),
        )
        .unwrap();
        let batch = batch(vec![Arc::new(dict), Arc::new(strings)]);
        let mut children = columns(&batch);
        children.push(Arc::new(Literal::new(ScalarValue::Utf8(Some("".into())))));
        children.push(Arc::new(Literal::new(ScalarValue::Float32(Some(f32::NAN)))));
        for threshold in [1, 64, 128] {
            let expr = AtLeastNNonNulls::new(3, children.clone())
                .with_small_batch_threshold(threshold)
                .unwrap();
            assert_eq!(result(&expr, &batch), [true, false, false, false]);
        }
        assert_eq!(
            result(
                &AtLeastNNonNulls::new(1, vec![Arc::new(Column::new("c1", 1))]),
                &batch
            ),
            [true, true, false, false]
        );
        assert_eq!(
            result(&AtLeastNNonNulls::new(1, vec![]), &batch),
            [false; 4]
        );
    }

    #[test]
    fn at_least_n_non_nulls_short_circuits_per_row() {
        let batch = batch(vec![
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec!["bad", "7"])),
        ]);
        let children: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("c0", 0)),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("c1", 1)),
                DataType::Int32,
                Some(CastOptions {
                    safe: false,
                    ..Default::default()
                }),
            )),
        ];
        assert_eq!(
            result(&AtLeastNNonNulls::new(1, children.clone()), &batch),
            [true, true]
        );
        // Also exercise the general counter path, not only the n=1 bitmap path.
        let mut general = children.clone();
        general.insert(0, Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))));
        assert_eq!(
            result(&AtLeastNNonNulls::new(2, general), &batch),
            [true, true]
        );
        // Both non-positive thresholds and an empty batch must avoid evaluating the bad cast.
        assert_eq!(
            result(&AtLeastNNonNulls::new(0, children.clone()), &batch),
            [true, true]
        );
        assert!(result(
            &AtLeastNNonNulls::new(2, children.clone()),
            &batch.slice(0, 0)
        )
        .is_empty());
        // An impossible threshold is not permission to skip evaluations Spark would perform.
        let mut required = children;
        required[0] = Arc::new(Literal::new(ScalarValue::Int32(None)));
        for n in [2, 3] {
            assert!(AtLeastNNonNulls::new(n, required.clone())
                .evaluate(&batch)
                .is_err());
        }
    }

    #[test]
    fn at_least_n_non_nulls_counter_bit_boundaries() {
        // Every possible count up to 65 occurs on both sides of word boundaries.
        // Rotating the valid children also exercises different carry orders.
        let input = batch(
            (0..65)
                .map(|column| {
                    Arc::new(Float64Array::from_iter((0..200).map(|row| {
                        if (column + row) % 65 < row % 66 {
                            Some(row as f64)
                        } else if column % 2 == 0 {
                            Some(f64::NAN)
                        } else {
                            None
                        }
                    }))) as ArrayRef
                })
                .collect(),
        );
        for offset in [0, 1, 7, 63] {
            for len in [0, 1, 31, 32, 63, 64, 65, 127, 128, 129] {
                let sliced = input.slice(offset, len);
                for n in (0..=66).chain([i32::MAX as usize]) {
                    let expected = (offset..offset + len)
                        .map(|row| row % 66 >= n)
                        .collect::<Vec<_>>();
                    for threshold in [1, 32, 64, 65, 128, usize::MAX] {
                        let expr = AtLeastNNonNulls::new(n as i32, columns(&sliced))
                            .with_small_batch_threshold(threshold)
                            .unwrap();
                        assert_eq!(
                            result(&expr, &sliced),
                            expected,
                            "offset={offset}, len={len}, n={n}, threshold={threshold}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn at_least_n_non_nulls_bitmap_dictionary_and_scalars() {
        let keys =
            Int32Array::from_iter(
                (0..137).map(|row| if row % 4 == 3 { None } else { Some(row % 4) }),
            );
        let floats = DictionaryArray::<Int32Type>::try_new(
            keys.clone(),
            Arc::new(Float64Array::from(vec![Some(3.0), Some(f64::NAN), None])),
        )
        .unwrap();
        let strings = DictionaryArray::<Int32Type>::try_new(
            keys,
            Arc::new(StringArray::from(vec![Some(""), Some("NaN"), None])),
        )
        .unwrap();
        let input = batch(vec![Arc::new(floats), Arc::new(strings)]).slice(3, 129);
        let mut children = columns(&input);
        children.push(Arc::new(Literal::new(ScalarValue::Utf8(Some("".into())))));
        children.push(Arc::new(Literal::new(ScalarValue::Float32(Some(f32::NAN)))));
        for n in 1..=5 {
            let expected = (3..132)
                .map(|row| [3, 2, 1, 1][row % 4] >= n)
                .collect::<Vec<_>>();
            assert_eq!(
                result(&AtLeastNNonNulls::new(n, children.clone()), &input),
                expected
            );
        }
    }

    #[test]
    fn at_least_n_non_nulls_mixed_completion_before_cast() {
        let input = batch(vec![
            Arc::new(Int32Array::from_iter((0..137).map(|row| {
                if row % 3 == 0 {
                    None
                } else {
                    Some(1)
                }
            }))),
            Arc::new(StringArray::from_iter((0..137).map(|row| {
                if row % 3 == 0 {
                    Some("7")
                } else {
                    Some("bad")
                }
            }))),
        ]);
        let children: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("c0", 0)),
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("c1", 1)),
                DataType::Int32,
                Some(CastOptions {
                    safe: false,
                    ..Default::default()
                }),
            )),
            Arc::new(Literal::new(ScalarValue::Int32(None))),
        ];
        for len in [1, 63, 64, 65, 129] {
            let sliced = input.slice(7, len);
            for threshold in [1, 64, 128, usize::MAX] {
                let expr = |n| {
                    AtLeastNNonNulls::new(n, children.clone())
                        .with_small_batch_threshold(threshold)
                        .unwrap()
                };
                assert_eq!(result(&expr(2), &sliced), vec![true; len]);
                // These rows still need the cast when n=3, so its error is required.
                assert!(expr(3).evaluate(&sliced).is_err());
            }
        }
    }
}
