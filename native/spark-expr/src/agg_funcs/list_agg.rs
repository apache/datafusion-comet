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
//!   `string_agg` returns `LargeUtf8`.
//! * A `NULL` delimiter is treated as the empty string (Spark treats `NULL` as
//!   the default empty delimiter; the JVM serde forwards the literal as-is).
//! * The delimiter is read once from the accumulator args (a literal is
//!   enforced by Spark's analyzer).
//!
//! The intermediate state is exposed as `Binary` because Spark's `ListAgg` is
//! a `TypedImperativeAggregate` whose Catalyst buffer schema is `BinaryType`.
//! Emitting `Utf8` here would force a Comet shuffle-side cast (`Utf8` →
//! `Binary`) that the merge side then can no longer read.

use std::hash::Hash;
use std::mem::size_of_val;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::cast::{as_binary_array, as_string_array};
use datafusion::common::{internal_datafusion_err, not_impl_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
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
        // Spark's ListAgg is a TypedImperativeAggregate — Catalyst declares its
        // intermediate buffer as `BinaryType`. Match that so Comet's shuffle
        // layer doesn't have to insert a Utf8 -> Binary cast that the merge
        // side then can't read back.
        Ok(vec![Field::new(
            format_state_name(args.name, "listagg"),
            DataType::Binary,
            true,
        )
        .into()])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let Some(lit) = (*acc_args.exprs[1]).downcast_ref::<Literal>() else {
            return not_impl_err!(
                "listagg delimiter must be a literal; got {:?}",
                acc_args.exprs[1]
            );
        };
        let delimiter = if lit.value().is_null() {
            String::new()
        } else if let Some(s) = lit.value().try_as_str() {
            s.unwrap_or("").to_string()
        } else {
            return not_impl_err!(
                "listagg delimiter literal must be Utf8; got {:?}",
                lit.value()
            );
        };
        Ok(Box::new(ListAggAccumulator::new(delimiter)))
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
        // Partial state is emitted as `Binary` (see `state_fields`); each
        // entry is UTF-8 bytes originally produced by another partition's
        // accumulator.
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, StringArray};
    use std::sync::Arc;

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
}
