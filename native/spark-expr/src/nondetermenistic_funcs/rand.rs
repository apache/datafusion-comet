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

use crate::hash_funcs::murmur3::spark_compatible_murmur3_hash;

use crate::internal::{evaluate_batch_for_rand, StatefulSeedValueGenerator};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Adoption of the XOR-shift algorithm used in Apache Spark.
/// See: https://github.com/apache/spark/blob/91f3fdd25852b43095dd5273358fc394ffd11b66/core/src/main/scala/org/apache/spark/util/random/XORShiftRandom.scala
/// Normalization multiplier used in mapping from a random i64 value to the f64 interval [0.0, 1.0).
/// Corresponds to the java implementation: https://github.com/openjdk/jdk/blob/07c9f7138affdf0d42ecdc30adcb854515569985/src/java.base/share/classes/java/util/Random.java#L302
/// Due to the lack of hexadecimal float literals support in rust, the scientific notation is used instead.
const DOUBLE_UNIT: f64 = 1.1102230246251565e-16;

/// Spark-compatible initial seed which is actually a part of the scala standard library murmurhash3 implementation.
/// The references:
/// https://github.com/apache/spark/blob/91f3fdd25852b43095dd5273358fc394ffd11b66/core/src/main/scala/org/apache/spark/util/random/XORShiftRandom.scala#L63
/// https://github.com/scala/scala/blob/360d5da544d84b821c40e4662ad08703b51a44e1/src/library/scala/util/hashing/MurmurHash3.scala#L331
const SPARK_MURMUR_ARRAY_SEED: u32 = 0x3c074a61;

#[derive(Debug, Clone)]
pub(crate) struct XorShiftRandom {
    pub(crate) seed: i64,
}

impl XorShiftRandom {
    fn next(&mut self, bits: u8) -> i32 {
        let mut next_seed = self.seed ^ (self.seed << 21);
        next_seed ^= ((next_seed as u64) >> 35) as i64;
        next_seed ^= next_seed << 4;
        self.seed = next_seed;
        (next_seed & ((1i64 << bits) - 1)) as i32
    }

    pub fn next_f64(&mut self) -> f64 {
        let a = self.next(26) as i64;
        let b = self.next(27) as i64;
        ((a << 27) + b) as f64 * DOUBLE_UNIT
    }
}

impl StatefulSeedValueGenerator<i64, f64> for XorShiftRandom {
    fn from_init_seed(init_seed: i64) -> Self {
        let bytes_repr = init_seed.to_be_bytes();
        let low_bits = spark_compatible_murmur3_hash(bytes_repr, SPARK_MURMUR_ARRAY_SEED);
        let high_bits = spark_compatible_murmur3_hash(bytes_repr, low_bits);
        let init_seed = ((high_bits as i64) << 32) | (low_bits as i64 & 0xFFFFFFFFi64);
        XorShiftRandom { seed: init_seed }
    }

    fn from_stored_state(stored_state: i64) -> Self {
        XorShiftRandom { seed: stored_state }
    }

    fn next_value(&mut self) -> f64 {
        self.next_f64()
    }

    fn get_current_state(&self) -> i64 {
        self.seed
    }
}

#[derive(Debug)]
pub struct RandExpr {
    seed: i64,
    state_holder: Arc<Mutex<Option<i64>>>,
}

impl RandExpr {
    pub fn new(seed: i64) -> Self {
        Self {
            seed,
            state_holder: Arc::new(Mutex::new(None::<i64>)),
        }
    }
}

impl Display for RandExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RAND({})", self.seed)
    }
}

impl PartialEq for RandExpr {
    fn eq(&self, other: &Self) -> bool {
        self.seed.eq(&other.seed)
    }
}

impl Eq for RandExpr {}

impl Hash for RandExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.children().hash(state);
    }
}

impl PhysicalExpr for RandExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        evaluate_batch_for_rand::<XorShiftRandom, i64>(
            &self.state_holder,
            self.seed,
            batch.num_rows(),
        )
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(RandExpr::new(self.seed)))
    }
}

pub fn rand(seed: i64) -> Arc<dyn PhysicalExpr> {
    Arc::new(RandExpr::new(seed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array};
    use arrow::{array::StringArray, compute::concat, datatypes::*};
    use datafusion::common::cast::as_float64_array;

    const SPARK_SEED_42_FIRST_5: [f64; 5] = [
        0.619189370225301,
        0.5096018842446481,
        0.8325259388871524,
        0.26322809041172357,
        0.6702867696264135,
    ];

    #[test]
    fn test_rand_single_batch() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let data = StringArray::from(vec![Some("foo"), None, None, Some("bar"), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)])?;
        let rand_expr = rand(42);
        let result = rand_expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_float64_array(&result)?;
        let expected = &Float64Array::from(Vec::from(SPARK_SEED_42_FIRST_5));
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_rand_multi_batch() -> Result<()> {
        let first_batch_schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let first_batch_data = Int64Array::from(vec![Some(42), None]);
        let second_batch_schema = first_batch_schema.clone();
        let second_batch_data = Int64Array::from(vec![None, Some(-42), None]);
        let rand_expr = rand(42);
        let first_batch = RecordBatch::try_new(
            Arc::new(first_batch_schema),
            vec![Arc::new(first_batch_data)],
        )?;
        let first_batch_result = rand_expr
            .evaluate(&first_batch)?
            .into_array(first_batch.num_rows())?;
        let second_batch = RecordBatch::try_new(
            Arc::new(second_batch_schema),
            vec![Arc::new(second_batch_data)],
        )?;
        let second_batch_result = rand_expr
            .evaluate(&second_batch)?
            .into_array(second_batch.num_rows())?;
        let result_arrays: Vec<&dyn Array> = vec![
            as_float64_array(&first_batch_result)?,
            as_float64_array(&second_batch_result)?,
        ];
        let result_arrays = &concat(&result_arrays)?;
        let final_result = as_float64_array(result_arrays)?;
        let expected = &Float64Array::from(Vec::from(SPARK_SEED_42_FIRST_5));
        assert_eq!(final_result, expected);
        Ok(())
    }
}
