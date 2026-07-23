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

//! Spark-compatible `listagg` / `string_agg` aggregate function.
//!
//! Implements the simple form of Spark 4.0's `LISTAGG(expr, delimiter)` (no
//! `WITHIN GROUP (ORDER BY ...)`, no DISTINCT — DISTINCT is rewritten into a
//! multi-stage plan by Spark before it reaches Comet). Differences from
//! DataFusion's `string_agg`:
//!
//! * Returns `Utf8` to match Spark's `StringType` result type; DataFusion's
//!   `string_agg` returns `LargeUtf8`. Returning `Utf8` (not `LargeUtf8`) at
//!   the JVM FFI boundary is why this is a Comet-owned UDAF rather than a direct
//!   reuse of `string_agg`.
//! * A `NULL` delimiter is treated as the empty string (Spark treats `NULL` as
//!   the default empty delimiter; the JVM serde forwards the literal as-is).
//! * The delimiter is read once from the accumulator args (a literal is
//!   enforced by Spark's analyzer).
//!
//! The intermediate state is exposed as `Binary` to match the `BinaryType`
//! buffer of Spark's `TypedImperativeAggregate` (see `state_fields`), which is
//! the schema of the shuffle Exchange between the partial and final aggregate.
//!
//! A `GroupsAccumulator` fast path is provided for grouped aggregation, which
//! is the common shape for `listagg`.

use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::cast::{as_binary_array, as_string_array};
use datafusion::common::{internal_datafusion_err, not_impl_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkListAgg {
    signature: Signature,
}

impl Default for SparkListAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkListAgg {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Null]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for SparkListAgg {
    fn name(&self) -> &str {
        "listagg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // The partial state is the group's concatenated string so far, exposed
        // as `Binary`. Spark's `ListAgg` is a `TypedImperativeAggregate`, whose
        // `aggBufferAttributes` is a single `BinaryType` column. Comet's partial
        // `HashAggregate` output schema comes from those buffer attributes, so
        // the shuffle Exchange between the partial and final aggregate carries
        // `Binary`. The native state must match, even though partial and final
        // run in the same engine (`CometListAgg.supportsMixedPartialFinal` is
        // false): the Exchange schema is Spark's, not DataFusion's.
        Ok(vec![Field::new(
            format_state_name(args.name, "listagg"),
            DataType::Binary,
            true,
        )
        .into()])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ListAggAccumulator::new(Self::extract_delimiter(
            &acc_args,
        )?)))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(ListAggGroupsAccumulator::new(
            Self::extract_delimiter(&args)?,
        )))
    }
}

impl SparkListAgg {
    /// Read the delimiter string from the second argument. Spark's analyzer
    /// guarantees a literal; a `NULL` literal is Spark's empty default.
    fn extract_delimiter(args: &AccumulatorArgs) -> Result<String> {
        let Some(lit) = (*args.exprs[1]).downcast_ref::<Literal>() else {
            return not_impl_err!(
                "listagg delimiter must be a literal; got {:?}",
                args.exprs[1]
            );
        };
        if lit.value().is_null() {
            Ok(String::new())
        } else if let Some(s) = lit.value().try_as_str() {
            Ok(s.unwrap_or("").to_string())
        } else {
            not_impl_err!(
                "listagg delimiter literal must be Utf8; got {:?}",
                lit.value()
            )
        }
    }
}

#[derive(Debug)]
struct ListAggAccumulator {
    delimiter: String,
    accumulated: String,
    has_value: bool,
}

impl ListAggAccumulator {
    fn new(delimiter: String) -> Self {
        Self {
            delimiter,
            accumulated: String::new(),
            has_value: false,
        }
    }

    #[inline]
    fn append_values(&mut self, array: &StringArray) {
        for value in array.iter().flatten() {
            if self.has_value {
                self.accumulated.push_str(&self.delimiter);
            }
            self.accumulated.push_str(value);
            self.has_value = true;
        }
    }
}

impl Accumulator for ListAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values.first().ok_or_else(|| {
            internal_datafusion_err!("listagg update_batch expected the values array")
        })?;
        self.append_values(as_string_array(array)?);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states.first().ok_or_else(|| {
            internal_datafusion_err!("listagg merge_batch expected the state array")
        })?;
        // Partial state is a `Binary` array (see `state_fields`); each non-null
        // entry is the UTF-8 bytes of another partition's already-joined group
        // value.
        let bin = as_binary_array(array)?;
        for value in bin.iter().flatten() {
            let s = std::str::from_utf8(value).map_err(|e| {
                internal_datafusion_err!("listagg merge_batch got non-UTF-8 partial state: {e}")
            })?;
            if self.has_value {
                self.accumulated.push_str(&self.delimiter);
            }
            self.accumulated.push_str(s);
            self.has_value = true;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let value = if self.has_value {
            ScalarValue::Binary(Some(std::mem::take(&mut self.accumulated).into_bytes()))
        } else {
            ScalarValue::Binary(None)
        };
        self.has_value = false;
        Ok(vec![value])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.has_value {
            Ok(ScalarValue::Utf8(Some(self.accumulated.clone())))
        } else {
            Ok(ScalarValue::Utf8(None))
        }
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.delimiter.capacity() + self.accumulated.capacity()
    }
}

/// Grouped fast path. `values[group]` is the group's concatenated string so
/// far, or `None` if the group has seen no non-null input (emits `NULL`).
/// Values are appended in arrival order, matching Spark's non-`WITHIN GROUP`
/// concatenation order.
#[derive(Debug)]
struct ListAggGroupsAccumulator {
    delimiter: String,
    values: Vec<Option<String>>,
    /// Running total of accumulated string bytes, for `size()`.
    total_data_bytes: usize,
}

impl ListAggGroupsAccumulator {
    fn new(delimiter: String) -> Self {
        Self {
            delimiter,
            values: Vec::new(),
            total_data_bytes: 0,
        }
    }

    fn append(&mut self, group_idx: usize, value: &str) {
        match &mut self.values[group_idx] {
            Some(existing) => {
                existing.push_str(&self.delimiter);
                existing.push_str(value);
                self.total_data_bytes += self.delimiter.len() + value.len();
            }
            slot @ None => {
                *slot = Some(value.to_string());
                self.total_data_bytes += value.len();
            }
        }
    }

    /// Drain the requested groups, updating the byte accounting. Both `evaluate`
    /// (`Utf8` result) and `state` (`Binary` partial buffer) start here; they
    /// differ only in the array type they build from the drained strings.
    fn take_emitted(&mut self, emit_to: EmitTo) -> Vec<Option<String>> {
        let emitted = emit_to.take_needed(&mut self.values);
        let emitted_bytes: usize = emitted.iter().flatten().map(|s| s.len()).sum();
        self.total_data_bytes -= emitted_bytes;
        emitted
    }
}

impl GroupsAccumulator for ListAggGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.values.resize(total_num_groups, None);
        let array = as_string_array(&values[0])?;
        for (i, &group_idx) in group_indices.iter().enumerate() {
            if let Some(f) = opt_filter {
                if !f.is_valid(i) || !f.value(i) {
                    continue;
                }
            }
            if array.is_valid(i) {
                self.append(group_idx, array.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        // Final result is `Utf8` (Spark `StringType`).
        Ok(Arc::new(StringArray::from(self.take_emitted(emit_to))))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // Partial buffer is `Binary` (see `SparkListAgg::state_fields`).
        let array: BinaryArray = self
            .take_emitted(emit_to)
            .into_iter()
            .map(|opt| opt.map(String::into_bytes))
            .collect();
        Ok(vec![Arc::new(array)])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        debug_assert!(
            opt_filter.is_none(),
            "opt_filter is not supported in merge_batch"
        );
        self.values.resize(total_num_groups, None);
        // State is `Binary`: each non-null entry is another partition's joined
        // group value as UTF-8 bytes.
        let array = as_binary_array(&values[0])?;
        for (i, &group_idx) in group_indices.iter().enumerate() {
            if array.is_valid(i) {
                let s = std::str::from_utf8(array.value(i)).map_err(|e| {
                    internal_datafusion_err!("listagg merge_batch got non-UTF-8 partial state: {e}")
                })?;
                self.append(group_idx, s);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.total_data_bytes
            + self.values.capacity() * std::mem::size_of::<Option<String>>()
            + self.delimiter.capacity()
            + size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, StringArray};

    fn utf8(items: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(items.to_vec()))
    }

    fn some(items: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(items.to_vec()))
    }

    #[test]
    fn joins_non_null_values_with_delimiter() -> Result<()> {
        let mut acc = ListAggAccumulator::new(",".to_string());
        acc.update_batch(&[some(&["a", "b", "c"])])?;
        let ScalarValue::Utf8(Some(s)) = acc.evaluate()? else {
            panic!("expected Utf8");
        };
        assert_eq!(s, "a,b,c");
        Ok(())
    }

    #[test]
    fn empty_delimiter_concatenates() -> Result<()> {
        let mut acc = ListAggAccumulator::new(String::new());
        acc.update_batch(&[some(&["a", "b", "c"])])?;
        let ScalarValue::Utf8(Some(s)) = acc.evaluate()? else {
            panic!("expected Utf8");
        };
        assert_eq!(s, "abc");
        Ok(())
    }

    #[test]
    fn skips_null_inputs() -> Result<()> {
        let mut acc = ListAggAccumulator::new(",".to_string());
        acc.update_batch(&[utf8(&[Some("a"), None, Some("b"), None])])?;
        let ScalarValue::Utf8(Some(s)) = acc.evaluate()? else {
            panic!("expected Utf8");
        };
        assert_eq!(s, "a,b");
        Ok(())
    }

    #[test]
    fn returns_null_on_all_null_or_empty_input() -> Result<()> {
        let mut acc = ListAggAccumulator::new(",".to_string());
        acc.update_batch(&[utf8(&[None, None])])?;
        assert!(matches!(acc.evaluate()?, ScalarValue::Utf8(None)));

        let mut empty = ListAggAccumulator::new(",".to_string());
        assert!(matches!(empty.evaluate()?, ScalarValue::Utf8(None)));
        Ok(())
    }

    #[test]
    fn merge_state_across_partitions() -> Result<()> {
        let mut a = ListAggAccumulator::new(",".to_string());
        a.update_batch(&[some(&["a", "b"])])?;
        let state_bytes = match a.state()?.remove(0) {
            ScalarValue::Binary(Some(b)) => b,
            other => panic!("unexpected state {other:?}"),
        };

        let mut b = ListAggAccumulator::new(",".to_string());
        b.update_batch(&[some(&["c", "d"])])?;
        let partial_state: ArrayRef =
            Arc::new(BinaryArray::from(vec![Some(state_bytes.as_slice())]));
        b.merge_batch(&[partial_state])?;

        let ScalarValue::Utf8(Some(s)) = b.evaluate()? else {
            panic!("expected Utf8");
        };
        // partition A's already-joined "a,b" is appended as one value.
        assert_eq!(s, "c,d,a,b");
        Ok(())
    }

    fn groups_result(acc: &mut ListAggGroupsAccumulator) -> Vec<Option<String>> {
        let array = acc.evaluate(EmitTo::All).unwrap();
        let strings = as_string_array(&array).unwrap();
        strings
            .iter()
            .map(|opt| opt.map(|s| s.to_string()))
            .collect()
    }

    #[test]
    fn groups_accumulator_joins_per_group() -> Result<()> {
        let mut acc = ListAggGroupsAccumulator::new(",".to_string());
        // group 0: "a", "c"; group 1: "b"; group 2 (index 2) never seen -> NULL.
        acc.update_batch(
            &[utf8(&[Some("a"), Some("b"), None, Some("c")])],
            &[0, 1, 1, 0],
            None,
            3,
        )?;
        assert_eq!(
            groups_result(&mut acc),
            vec![Some("a,c".to_string()), Some("b".to_string()), None]
        );
        Ok(())
    }

    #[test]
    fn groups_accumulator_merges_partial_state() -> Result<()> {
        // Two partitions each produce partial state, then a final accumulator
        // merges them in arrival order.
        let mut p0 = ListAggGroupsAccumulator::new(",".to_string());
        p0.update_batch(&[some(&["a", "b"])], &[0, 0], None, 1)?;
        let s0 = p0.state(EmitTo::All)?.remove(0);

        let mut p1 = ListAggGroupsAccumulator::new(",".to_string());
        p1.update_batch(&[some(&["c"])], &[0], None, 1)?;
        let s1 = p1.state(EmitTo::All)?.remove(0);

        let mut fin = ListAggGroupsAccumulator::new(",".to_string());
        fin.merge_batch(&[s0], &[0], None, 1)?;
        fin.merge_batch(&[s1], &[0], None, 1)?;
        assert_eq!(groups_result(&mut fin), vec![Some("a,b,c".to_string())]);
        Ok(())
    }
}
