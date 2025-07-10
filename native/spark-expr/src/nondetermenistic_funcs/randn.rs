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

use crate::nondetermenistic_funcs::rand::XorShiftRandom;

use crate::internal::{evaluate_batch_for_rand, StatefulSeedValueGenerator};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Stateful extension of the Marsaglia polar method (https://en.wikipedia.org/wiki/Marsaglia_polar_method)
/// to convert uniform distribution to the standard normal one used by Apache Spark.
/// For correct processing of batches having odd number of elements, we need to keep not used yet generated value as a part of the state.
/// Note about Comet <-> Spark equivalence:
/// Under the hood, the spark algorithm refers to java.util.Random relying on a module StrictMath. The latter uses
/// native implementations of floating-point operations (ln, exp, sin, cos) and ensures
/// they are stable across different platforms.
/// See: https://github.com/openjdk/jdk/blob/07c9f7138affdf0d42ecdc30adcb854515569985/src/java.base/share/classes/java/util/Random.java#L745
/// Yet, for the Rust standard library this stability is not guaranteed (https://doc.rust-lang.org/std/primitive.f64.html#method.ln)
/// Moreover, potential usage of external library like rug (https://docs.rs/rug/latest/rug/) doesn't help because still there is no
/// guarantee it matches the StrictMath jvm implementation.
/// So, we can ensure only equivalence with some error tolerance between rust and spark(jvm).

#[derive(Debug, Clone)]
struct XorShiftRandomForGaussian {
    base_generator: XorShiftRandom,
    next_gaussian: Option<f64>,
}

impl XorShiftRandomForGaussian {
    pub fn next_gaussian(&mut self) -> f64 {
        if let Some(stored_value) = self.next_gaussian {
            self.next_gaussian = None;
            return stored_value;
        }
        let mut v1: f64;
        let mut v2: f64;
        let mut s: f64;
        loop {
            v1 = 2f64 * self.base_generator.next_f64() - 1f64;
            v2 = 2f64 * self.base_generator.next_f64() - 1f64;
            s = v1 * v1 + v2 * v2;
            if s < 1f64 && s != 0f64 {
                break;
            }
        }
        let multiplier = (-2f64 * s.ln() / s).sqrt();
        self.next_gaussian = Some(v2 * multiplier);
        v1 * multiplier
    }
}

type RandomGaussianState = (i64, Option<f64>);

impl StatefulSeedValueGenerator<RandomGaussianState, f64> for XorShiftRandomForGaussian {
    fn from_init_seed(init_value: i64) -> Self {
        XorShiftRandomForGaussian {
            base_generator: XorShiftRandom::from_init_seed(init_value),
            next_gaussian: None,
        }
    }

    fn from_stored_state(stored_state: RandomGaussianState) -> Self {
        XorShiftRandomForGaussian {
            base_generator: XorShiftRandom::from_stored_state(stored_state.0),
            next_gaussian: stored_state.1,
        }
    }

    fn next_value(&mut self) -> f64 {
        self.next_gaussian()
    }

    fn get_current_state(&self) -> RandomGaussianState {
        (self.base_generator.seed, self.next_gaussian)
    }
}

#[derive(Debug, Clone)]
pub struct RandnExpr {
    seed: Arc<dyn PhysicalExpr>,
    init_seed_shift: i32,
    state_holder: Arc<Mutex<Option<RandomGaussianState>>>,
}

impl RandnExpr {
    pub fn new(seed: Arc<dyn PhysicalExpr>, init_seed_shift: i32) -> Self {
        Self {
            seed,
            init_seed_shift,
            state_holder: Arc::new(Mutex::new(None)),
        }
    }
}

impl Display for RandnExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RANDN({})", self.seed)
    }
}

impl PartialEq for RandnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.seed.eq(&other.seed) && self.init_seed_shift == other.init_seed_shift
    }
}

impl Eq for RandnExpr {}

impl Hash for RandnExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.children().hash(state);
    }
}

impl PhysicalExpr for RandnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> datafusion::common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::common::Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        match self.seed.evaluate(batch)? {
            ColumnarValue::Scalar(seed) => {
                evaluate_batch_for_rand::<XorShiftRandomForGaussian, RandomGaussianState>(
                    &self.state_holder,
                    seed,
                    self.init_seed_shift as i64,
                    batch.num_rows(),
                )
            }
            ColumnarValue::Array(_arr) => Err(DataFusionError::NotImplemented(format!(
                "Only literal seeds are supported for {self}"
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.seed]
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(RandnExpr::new(
            Arc::clone(&children[0]),
            self.init_seed_shift,
        )))
    }
}

pub fn randn(
    seed: Arc<dyn PhysicalExpr>,
    init_seed_shift: i32,
) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(RandnExpr::new(seed, init_seed_shift)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array};
    use arrow::{array::StringArray, compute::concat, datatypes::*};
    use datafusion::common::cast::as_float64_array;
    use datafusion::physical_expr::expressions::lit;

    const SPARK_SEED_42_FIRST_5_GAUSSIAN: [f64; 5] = [
        2.384479054241165,
        0.1920934041293524,
        0.7337336533286575,
        -0.5224480195716871,
        2.060084179317831,
    ];

    #[test]
    fn test_rand_single_batch() -> datafusion::common::Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let data = StringArray::from(vec![Some("foo"), None, None, Some("bar"), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)])?;
        let randn_expr = randn(lit(42), 0)?;
        let result = randn_expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_float64_array(&result)?;
        let expected = &Float64Array::from(Vec::from(SPARK_SEED_42_FIRST_5_GAUSSIAN));
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_rand_multi_batch() -> datafusion::common::Result<()> {
        let first_batch_schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let first_batch_data = Int64Array::from(vec![Some(24), None, None]);
        let second_batch_schema = first_batch_schema.clone();
        let second_batch_data = Int64Array::from(vec![None, Some(22)]);
        let randn_expr = randn(lit(42), 0)?;
        let first_batch = RecordBatch::try_new(
            Arc::new(first_batch_schema),
            vec![Arc::new(first_batch_data)],
        )?;
        let first_batch_result = randn_expr
            .evaluate(&first_batch)?
            .into_array(first_batch.num_rows())?;
        let second_batch = RecordBatch::try_new(
            Arc::new(second_batch_schema),
            vec![Arc::new(second_batch_data)],
        )?;
        let second_batch_result = randn_expr
            .evaluate(&second_batch)?
            .into_array(second_batch.num_rows())?;
        let result_arrays: Vec<&dyn Array> = vec![
            as_float64_array(&first_batch_result)?,
            as_float64_array(&second_batch_result)?,
        ];
        let result_arrays = &concat(&result_arrays)?;
        let final_result = as_float64_array(result_arrays)?;
        let expected = &Float64Array::from(Vec::from(SPARK_SEED_42_FIRST_5_GAUSSIAN));
        assert_eq!(final_result, expected);
        Ok(())
    }
}
