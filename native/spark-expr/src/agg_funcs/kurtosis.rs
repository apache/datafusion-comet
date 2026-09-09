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

//! Spark-compatible excess-kurtosis aggregate.
//!
//! Spark's `Kurtosis` is a `CentralMomentAgg` (`DeclarativeAggregate`) whose
//! intermediate buffer is `[n, avg, m2, m3, m4]` of Float64. This accumulator
//! mirrors that buffer exactly, using the same higher-order online update /
//! merge recurrences (Meng 2015) that `CentralMomentAgg` compiles into
//! catalyst expressions. Matching the wire format lets Spark's Partial and
//! Comet's Final (or vice versa) share intermediate state without a cast.
//!
//! Result formula (excess kurtosis, Fisher definition):
//!
//! * `n == 0`          -> NULL
//! * `m2 == 0`         -> NULL when `null_on_divide_by_zero`, else NaN
//! * otherwise         -> `n * m4 / (m2 * m2) - 3.0`

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{downcast_value, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion::physical_expr::expressions::format_state_name;

use crate::agg_funcs::welford::{moments4_merge, moments4_update};
use crate::divide_by_zero_error;

#[derive(Debug, PartialEq, Eq)]
pub struct Kurtosis {
    name: String,
    signature: Signature,
    null_on_divide_by_zero: bool,
    ansi_enabled: bool,
}

impl std::hash::Hash for Kurtosis {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.null_on_divide_by_zero.hash(state);
        self.ansi_enabled.hash(state);
    }
}

impl Kurtosis {
    pub fn new(name: impl Into<String>, null_on_divide_by_zero: bool, ansi_enabled: bool) -> Self {
        Self {
            name: name.into(),
            signature: Signature::numeric(1, Immutable),
            null_on_divide_by_zero,
            ansi_enabled,
        }
    }
}

impl AggregateUDFImpl for Kurtosis {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(KurtosisAccumulator::new(
            self.null_on_divide_by_zero,
            self.ansi_enabled,
        )))
    }

    // No `GroupsAccumulator`: grouped `kurtosis` deliberately runs through DataFusion's generic
    // `GroupsAccumulatorAdapter`, which costs one boxed `Accumulator` and a `ScalarValue` round
    // trip per group per batch. This is a gap relative to the neighbouring central-moment
    // aggregates - `VarianceGroupsAccumulator` keeps flat `Vec<f64>` state and
    // `StddevGroupsAccumulator` reuses it - and it is recorded here as a decision rather than an
    // oversight. The vectorized version wants to land with `skewness`, since both are an
    // `evaluate` over the same `[n, avg, m2, m3, m4]` state that `moments4_update` already
    // maintains, and one flat-state accumulator should then serve all three.

    // Fields ordered to match Spark's `[n, avg, m2, m3, m4]` buffer so that a
    // Spark-produced Partial state can be merged into a Comet-produced Final
    // (and vice versa) without a schema conversion.
    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(&self.name, "n"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "avg"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "m2"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "m3"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "m4"),
                DataType::Float64,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
pub struct KurtosisAccumulator {
    n: f64,
    avg: f64,
    m2: f64,
    m3: f64,
    m4: f64,
    null_on_divide_by_zero: bool,
    ansi_enabled: bool,
}

impl KurtosisAccumulator {
    pub fn new(null_on_divide_by_zero: bool, ansi_enabled: bool) -> Self {
        Self {
            n: 0.0,
            avg: 0.0,
            m2: 0.0,
            m3: 0.0,
            m4: 0.0,
            null_on_divide_by_zero,
            ansi_enabled,
        }
    }
}

impl Accumulator for KurtosisAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.n),
            ScalarValue::from(self.avg),
            ScalarValue::from(self.m2),
            ScalarValue::from(self.m3),
            ScalarValue::from(self.m4),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(&values[0], Float64Array).iter().flatten();
        for value in arr {
            let (n, avg, m2, m3, m4) =
                moments4_update(self.n, self.avg, self.m2, self.m3, self.m4, value);
            self.n = n;
            self.avg = avg;
            self.m2 = m2;
            self.m3 = m3;
            self.m4 = m4;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let ns = downcast_value!(states[0], Float64Array);
        let avgs = downcast_value!(states[1], Float64Array);
        let m2s = downcast_value!(states[2], Float64Array);
        let m3s = downcast_value!(states[3], Float64Array);
        let m4s = downcast_value!(states[4], Float64Array);

        for i in 0..ns.len() {
            let n2 = ns.value(i);
            if n2 == 0.0 {
                // Empty partial state contributes nothing and would produce
                // divide-by-zero garbage in `delta_n`; skip it.
                continue;
            }
            let (n, avg, m2, m3, m4) = moments4_merge(
                self.n,
                self.avg,
                self.m2,
                self.m3,
                self.m4,
                n2,
                avgs.value(i),
                m2s.value(i),
                m3s.value(i),
                m4s.value(i),
            );
            self.n = n;
            self.avg = avg;
            self.m2 = m2;
            self.m3 = m3;
            self.m4 = m4;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.n == 0.0 {
            return Ok(ScalarValue::Float64(None));
        }
        if self.m2 == 0.0 {
            return Ok(ScalarValue::Float64(if self.null_on_divide_by_zero {
                None
            } else {
                Some(f64::NAN)
            }));
        }
        // Spark's guard above is on `m2`, but the division is by `m2 * m2`, and that product can
        // underflow to zero while `m2` itself is finite and non-zero (`1e-100` and `2e-100` give
        // an `m2` of 5e-201, whose square is 0). Spark's `Divide` then sees a zero divisor and
        // applies its own rule, which is the session's ANSI setting rather than
        // `null_on_divide_by_zero`. Plain IEEE division here would return NaN instead.
        let divisor = self.m2 * self.m2;
        if divisor == 0.0 {
            return if self.ansi_enabled {
                Err(divide_by_zero_error().into())
            } else {
                Ok(ScalarValue::Float64(None))
            };
        }
        Ok(ScalarValue::Float64(Some(self.n * self.m4 / divisor - 3.0)))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(values: &[f64], null_on_divide_by_zero: bool) -> Option<f64> {
        let mut acc = KurtosisAccumulator::new(null_on_divide_by_zero, false);
        let arr: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        acc.update_batch(&[arr]).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(v) => v,
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn empty_group_returns_null() {
        assert_eq!(eval(&[], true), None);
    }

    #[test]
    fn single_value_returns_divide_by_zero_result() {
        // m2 == 0 with a single value => NULL when null_on_divide_by_zero, else NaN.
        assert_eq!(eval(&[42.0], true), None);
        let nan = eval(&[42.0], false).unwrap();
        assert!(nan.is_nan(), "expected NaN, got {nan}");
    }

    #[test]
    fn matches_spark_example() {
        // Spark's own example from ExpressionDescription:
        //   SELECT kurtosis(col) FROM VALUES (-10), (-20), (100), (1000) AS tab(col);
        //   => -0.7014368047529627
        let got = eval(&[-10.0, -20.0, 100.0, 1000.0], true).unwrap();
        assert!((got - -0.7014368047529627_f64).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn matches_spark_second_example() {
        //   SELECT kurtosis(col) FROM VALUES (1), (10), (100), (10), (1) as tab(col);
        //   => 0.19432323191699075
        let got = eval(&[1.0, 10.0, 100.0, 10.0, 1.0], true).unwrap();
        assert!((got - 0.19432323191699075_f64).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn merge_produces_same_result_as_single_batch() {
        // Merging two partitions must reproduce the single-batch result.
        let values = [-10.0_f64, -20.0, 100.0, 1000.0];
        let full = eval(&values, true).unwrap();

        let arr_a: ArrayRef = Arc::new(Float64Array::from(values[..2].to_vec()));
        let arr_b: ArrayRef = Arc::new(Float64Array::from(values[2..].to_vec()));

        let mut a = KurtosisAccumulator::new(true, false);
        a.update_batch(&[arr_a]).unwrap();
        let state_a = a.state().unwrap();

        let mut b = KurtosisAccumulator::new(true, false);
        b.update_batch(&[arr_b]).unwrap();

        // Represent partition-A state as five single-row Float64 arrays and merge.
        let state_arrays: Vec<ArrayRef> = state_a
            .into_iter()
            .map(|sv| match sv {
                ScalarValue::Float64(v) => {
                    Arc::new(Float64Array::from(vec![v.unwrap()])) as ArrayRef
                }
                other => panic!("unexpected state scalar {other:?}"),
            })
            .collect();
        b.merge_batch(&state_arrays).unwrap();

        let merged = match b.evaluate().unwrap() {
            ScalarValue::Float64(Some(v)) => v,
            other => panic!("expected Float64(Some(_)), got {other:?}"),
        };
        assert!((merged - full).abs() < 1e-9, "merged={merged}, full={full}");
    }

    /// `m2` is finite and non-zero here, so Spark's `m2 === 0` guard does not fire, but
    /// `m2 * m2` underflows to zero and Spark's `Divide` takes over. That means the session's
    /// ANSI setting decides, not `null_on_divide_by_zero`. Plain IEEE division returned NaN.
    #[test]
    fn divisor_underflow_follows_spark_division_semantics() {
        let values = [1e-100, 2e-100];

        // Confirm the premise rather than assuming it: m2 != 0 but m2 * m2 == 0.
        let mut probe = KurtosisAccumulator::new(true, false);
        probe
            .update_batch(&[Arc::new(Float64Array::from(values.to_vec())) as ArrayRef])
            .unwrap();
        assert_ne!(probe.m2, 0.0, "m2 must be non-zero for this case to bite");
        assert_eq!(probe.m2 * probe.m2, 0.0, "m2 * m2 must underflow to zero");

        // ANSI off: NULL, for either value of null_on_divide_by_zero, because this path is
        // governed by the Divide and not by the m2 == 0 branch.
        for null_on_divide_by_zero in [true, false] {
            assert_eq!(eval(&values, null_on_divide_by_zero), None);
        }

        // ANSI on: DIVIDE_BY_ZERO.
        let mut ansi = KurtosisAccumulator::new(true, true);
        ansi.update_batch(&[Arc::new(Float64Array::from(values.to_vec())) as ArrayRef])
            .unwrap();
        let err = ansi.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("DIVIDE_BY_ZERO"),
            "expected a DIVIDE_BY_ZERO error, got {err}"
        );
    }
}
