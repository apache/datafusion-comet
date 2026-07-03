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

use arrow::array::{Array, ArrayRef};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::RowConverter;
use arrow::row::SortField;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::format_state_name;
use std::mem::size_of_val;
use std::sync::Arc;

/// Spark-compatible `max_by(value, ordering)` / `min_by(value, ordering)` aggregate.
///
/// Returns the `value` associated with the maximum (`max_by`) or minimum (`min_by`)
/// non-null `ordering`. Rows with a null `ordering` are ignored. The returned value
/// may itself be null when it is the value paired with the extremum ordering. If every
/// `ordering` in the group is null, the result is null.
///
/// Spark's `MaxBy`/`MinBy` are `DeclarativeAggregate`s that keep a `(value, ordering)`
/// buffer and, on a tie in the ordering, the later row wins. Because ties across
/// partitions are processed in an unspecified order, Spark documents the function as
/// non-deterministic when several rows share the extremum ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxMinBy {
    name: String,
    signature: Signature,
    /// `true` for `max_by`, `false` for `min_by`.
    is_max: bool,
}

impl std::hash::Hash for MaxMinBy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.is_max.hash(state);
    }
}

impl MaxMinBy {
    /// Create a `max_by` aggregate.
    pub fn new_max_by() -> Self {
        Self {
            name: "max_by".to_string(),
            signature: Signature::any(2, Volatility::Immutable),
            is_max: true,
        }
    }

    /// Create a `min_by` aggregate.
    #[allow(dead_code)]
    pub fn new_min_by() -> Self {
        Self {
            name: "min_by".to_string(),
            signature: Signature::any(2, Volatility::Immutable),
            is_max: false,
        }
    }
}

impl AggregateUDFImpl for MaxMinBy {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // The result has the same type as the `value` argument.
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::try_new(
            value_type,
            ordering_type,
            self.is_max,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let value_type = args.input_fields[0].data_type().clone();
        let ordering_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(&self.name, "value"),
                value_type,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "ordering"),
                ordering_type,
                true,
            )),
        ])
    }
}

/// Accumulator that tracks the running `(value, ordering)` pair for the extremum ordering.
#[derive(Debug)]
struct MaxMinByAccumulator {
    /// The value paired with the current extremum ordering. May be null.
    value: ScalarValue,
    /// The current extremum ordering. Null means no non-null ordering has been seen yet.
    ordering: ScalarValue,
    /// `true` for `max_by`, `false` for `min_by`.
    is_max: bool,
}

impl MaxMinByAccumulator {
    fn try_new(value_type: DataType, ordering_type: DataType, is_max: bool) -> Result<Self> {
        Ok(Self {
            value: ScalarValue::try_from(&value_type)?,
            ordering: ScalarValue::try_from(&ordering_type)?,
            is_max,
        })
    }

    fn sort_options(&self) -> SortOptions {
        // Encode the ordering column into arrow's row format so that the extremum can be
        // found for any orderable type with a single comparison. For `max_by` we sort
        // ascending, so the largest ordering yields the largest row bytes. For `min_by`
        // we sort descending, so the smallest ordering yields the largest row bytes; the
        // same "argmax of the row bytes" scan then selects the minimum.
        SortOptions {
            descending: !self.is_max,
            nulls_first: true,
        }
    }

    /// Apply a batch of `(value, ordering)` columns, keeping the value paired with the
    /// extremum ordering. Rows with a null ordering are ignored.
    fn update_from(&mut self, value_arr: &ArrayRef, ordering_arr: &ArrayRef) -> Result<()> {
        if ordering_arr.is_empty() {
            return Ok(());
        }

        let converter = RowConverter::new(vec![SortField::new_with_options(
            ordering_arr.data_type().clone(),
            self.sort_options(),
        )])?;
        let rows = converter.convert_columns(&[Arc::clone(ordering_arr)])?;

        // Find the index of the extremum ordering in this batch (last one wins on a tie,
        // matching Spark's sequential row processing), ignoring null orderings.
        let mut best: Option<usize> = None;
        for i in 0..ordering_arr.len() {
            if ordering_arr.is_null(i) {
                continue;
            }
            best = match best {
                None => Some(i),
                Some(b) if rows.row(i) >= rows.row(b) => Some(i),
                Some(b) => Some(b),
            };
        }

        let Some(b) = best else {
            return Ok(());
        };

        let candidate_ordering = ScalarValue::try_from_array(ordering_arr, b)?;
        let take = if self.ordering.is_null() {
            true
        } else {
            // Compare the batch's extremum ordering against the running extremum using the
            // same row encoding. Build a two-row array [running, candidate] and compare.
            let pair = ScalarValue::iter_to_array(vec![
                self.ordering.clone(),
                candidate_ordering.clone(),
            ])?;
            let pair_rows = converter.convert_columns(&[pair])?;
            pair_rows.row(1) >= pair_rows.row(0)
        };

        if take {
            self.value = ScalarValue::try_from_array(value_arr, b)?;
            self.ordering = candidate_ordering;
        }

        Ok(())
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.update_from(&values[0], &values[1])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // State columns mirror the input columns: [value, ordering].
        self.update_from(&states[0], &states[1])
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.value.clone(), self.ordering.clone()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.value.size() + self.ordering.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};

    fn max_by_acc(value_type: DataType, ordering_type: DataType) -> MaxMinByAccumulator {
        MaxMinByAccumulator::try_new(value_type, ordering_type, true).unwrap()
    }

    fn min_by_acc(value_type: DataType, ordering_type: DataType) -> MaxMinByAccumulator {
        MaxMinByAccumulator::try_new(value_type, ordering_type, false).unwrap()
    }

    #[test]
    fn max_by_basic() {
        let mut acc = max_by_acc(DataType::Utf8, DataType::Int32);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let ordering: ArrayRef = Arc::new(Int32Array::from(vec![10, 50, 20]));
        acc.update_batch(&[values, ordering]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::from("b"));
    }

    #[test]
    fn min_by_basic() {
        let mut acc = min_by_acc(DataType::Utf8, DataType::Int32);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let ordering: ArrayRef = Arc::new(Int32Array::from(vec![10, 50, 20]));
        acc.update_batch(&[values, ordering]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::from("a"));
    }

    #[test]
    fn null_ordering_is_ignored() {
        let mut acc = max_by_acc(DataType::Utf8, DataType::Int32);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));
        let ordering: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(5)]));
        acc.update_batch(&[values, ordering]).unwrap();
        // The row with ordering=None (value "b") is ignored; max ordering is 10 -> "a".
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::from("a"));
    }

    #[test]
    fn all_null_ordering_yields_null() {
        let mut acc = max_by_acc(DataType::Utf8, DataType::Int32);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let ordering: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        acc.update_batch(&[values, ordering]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Utf8(None));
    }

    #[test]
    fn null_value_at_extremum_is_returned() {
        let mut acc = max_by_acc(DataType::Utf8, DataType::Int32);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let ordering: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), Some(50)]));
        acc.update_batch(&[values, ordering]).unwrap();
        // Max ordering 50 pairs with a null value.
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Utf8(None));
    }

    #[test]
    fn empty_group_yields_null() {
        let mut acc = max_by_acc(DataType::Utf8, DataType::Int32);
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Utf8(None));
    }

    #[test]
    fn max_by_nan_is_largest() {
        let mut acc = max_by_acc(DataType::Utf8, DataType::Float64);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let ordering: ArrayRef = Arc::new(Float64Array::from(vec![1.0, f64::NAN, 2.0]));
        acc.update_batch(&[values, ordering]).unwrap();
        // Spark treats NaN as the largest value, matching arrow's row ordering.
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::from("b"));
    }

    #[test]
    fn merge_matches_single_shot() {
        let single = {
            let mut acc = max_by_acc(DataType::Utf8, DataType::Int32);
            let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"]));
            let ordering: ArrayRef = Arc::new(Int32Array::from(vec![1, 6, 3, 2, 5, 4]));
            acc.update_batch(&[values, ordering]).unwrap();
            acc.evaluate().unwrap()
        };

        let mut left = max_by_acc(DataType::Utf8, DataType::Int32);
        left.update_batch(&[
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 6, 3])) as ArrayRef,
        ])
        .unwrap();
        let mut right = max_by_acc(DataType::Utf8, DataType::Int32);
        right
            .update_batch(&[
                Arc::new(StringArray::from(vec!["d", "e", "f"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2, 5, 4])) as ArrayRef,
            ])
            .unwrap();

        let mut merged = max_by_acc(DataType::Utf8, DataType::Int32);
        for acc in [&mut left, &mut right] {
            let state = acc.state().unwrap();
            let value_arr = ScalarValue::iter_to_array(vec![state[0].clone()]).unwrap();
            let ordering_arr = ScalarValue::iter_to_array(vec![state[1].clone()]).unwrap();
            merged.merge_batch(&[value_arr, ordering_arr]).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), single);
    }
}
