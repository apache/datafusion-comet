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

#[derive(Debug, PartialEq, Eq)]
pub struct Kurtosis {
    name: String,
    signature: Signature,
    null_on_divide_by_zero: bool,
}

impl std::hash::Hash for Kurtosis {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.null_on_divide_by_zero.hash(state);
    }
}

impl Kurtosis {
    pub fn new(name: impl Into<String>, null_on_divide_by_zero: bool) -> Self {
        Self {
            name: name.into(),
            signature: Signature::numeric(1, Immutable),
            null_on_divide_by_zero,
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
        )))
    }

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

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }
}

/// Online update for the first four central moments. Direct port of Spark's
/// `CentralMomentAgg.updateExpressionsDef` for `momentOrder = 4`.
#[inline]
fn kurtosis_update(
    n: f64,
    avg: f64,
    m2: f64,
    m3: f64,
    m4: f64,
    value: f64,
) -> (f64, f64, f64, f64, f64) {
    let new_n = n + 1.0;
    let delta = value - avg;
    let delta_n = delta / new_n;
    let new_avg = avg + delta_n;
    let new_m2 = m2 + delta * (delta - delta_n);
    let delta2 = delta * delta;
    let delta_n2 = delta_n * delta_n;
    let new_m3 = m3 - 3.0 * delta_n * new_m2 + delta * (delta2 - delta_n2);
    let new_m4 = m4 - 4.0 * delta_n * new_m3 - 6.0 * delta_n2 * new_m2
        + delta * (delta * delta2 - delta_n * delta_n2);
    (new_n, new_avg, new_m2, new_m3, new_m4)
}

/// Merge two partial states. Direct port of Spark's
/// `CentralMomentAgg.mergeExpressions` for `momentOrder = 4`.
#[inline]
#[allow(clippy::too_many_arguments)]
fn kurtosis_merge(
    n1: f64,
    avg1: f64,
    m2_1: f64,
    m3_1: f64,
    m4_1: f64,
    n2: f64,
    avg2: f64,
    m2_2: f64,
    m3_2: f64,
    m4_2: f64,
) -> (f64, f64, f64, f64, f64) {
    let new_n = n1 + n2;
    let delta = avg2 - avg1;
    let delta_n = if new_n == 0.0 { 0.0 } else { delta / new_n };
    let new_avg = avg1 + delta_n * n2;
    let new_m2 = m2_1 + m2_2 + delta * delta_n * n1 * n2;
    let new_m3 = m3_1
        + m3_2
        + delta_n * delta_n * delta * n1 * n2 * (n1 - n2)
        + 3.0 * delta_n * (n1 * m2_2 - n2 * m2_1);
    let new_m4 = m4_1
        + m4_2
        + delta_n * delta_n * delta_n * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2)
        + 6.0 * delta_n * delta_n * (n1 * n1 * m2_2 + n2 * n2 * m2_1)
        + 4.0 * delta_n * (n1 * m3_2 - n2 * m3_1);
    (new_n, new_avg, new_m2, new_m3, new_m4)
}

#[derive(Debug)]
pub struct KurtosisAccumulator {
    n: f64,
    avg: f64,
    m2: f64,
    m3: f64,
    m4: f64,
    null_on_divide_by_zero: bool,
}

impl KurtosisAccumulator {
    pub fn new(null_on_divide_by_zero: bool) -> Self {
        Self {
            n: 0.0,
            avg: 0.0,
            m2: 0.0,
            m3: 0.0,
            m4: 0.0,
            null_on_divide_by_zero,
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
                kurtosis_update(self.n, self.avg, self.m2, self.m3, self.m4, value);
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
            let (n, avg, m2, m3, m4) = kurtosis_merge(
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
        Ok(ScalarValue::Float64(if self.n == 0.0 {
            None
        } else if self.m2 == 0.0 {
            if self.null_on_divide_by_zero {
                None
            } else {
                Some(f64::NAN)
            }
        } else {
            Some(self.n * self.m4 / (self.m2 * self.m2) - 3.0)
        }))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(values: &[f64], null_on_divide_by_zero: bool) -> Option<f64> {
        let mut acc = KurtosisAccumulator::new(null_on_divide_by_zero);
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

        let mut a = KurtosisAccumulator::new(true);
        a.update_batch(&[arr_a]).unwrap();
        let state_a = a.state().unwrap();

        let mut b = KurtosisAccumulator::new(true);
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
}
