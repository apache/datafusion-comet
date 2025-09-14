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

use std::{any::Any, sync::Arc};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, TypeSignature, Volatility,
};

/// CountNotNull aggregate function
/// Counts the number of non-null values in the input expression
#[derive(Debug)]
pub struct CountNotNull {
    signature: Signature,
}

impl Default for CountNotNull {
    fn default() -> Self {
        Self::new()
    }
}

impl CountNotNull {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountNotNull {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "count_not_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(CountNotNullAccumulator::new()))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CountNotNullGroupsAccumulator::new()))
    }
}

#[derive(Debug)]
struct CountNotNullAccumulator {
    count: i64,
}

impl CountNotNullAccumulator {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountNotNullAccumulator {
    fn update_batch(&mut self, values: &[Arc<dyn Array>]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];
        let non_null_count = array.len() - array.null_count();
        self.count += non_null_count as i64;
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[Arc<dyn Array>]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }

        let counts = states[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("Expected Int64Array".to_string()))?;

        for i in 0..counts.len() {
            if let Some(count) = counts.value(i).into() {
                self.count += count;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CountNotNullGroupsAccumulator {
    counts: Vec<i64>,
}

impl CountNotNullGroupsAccumulator {
    fn new() -> Self {
        Self { counts: vec![] }
    }
}

impl GroupsAccumulator for CountNotNullGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[Arc<dyn Array>],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        // Resize counts if needed
        if self.counts.len() < total_num_groups {
            self.counts.resize(total_num_groups, 0);
        }

        let array = &values[0];

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            // Check filter if present
            if let Some(filter) = opt_filter {
                if !filter.value(row_idx) {
                    continue;
                }
            }

            // Check if value is not null
            if !array.is_null(row_idx) {
                self.counts[group_idx] += 1;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<Arc<dyn Array>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let result = Int64Array::from_iter_values(counts.iter().copied());
        Ok(Arc::new(result))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<Arc<dyn Array>>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let result = Int64Array::from_iter_values(counts.iter().copied());
        Ok(vec![Arc::new(result)])
    }

    fn merge_batch(
        &mut self,
        values: &[Arc<dyn Array>],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        // Resize counts if needed
        if self.counts.len() < total_num_groups {
            self.counts.resize(total_num_groups, 0);
        }

        let counts_array = values[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("Expected Int64Array".to_string()))?;

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            // Check filter if present
            if let Some(filter) = opt_filter {
                if !filter.value(row_idx) {
                    continue;
                }
            }

            if let Some(count) = counts_array.value(row_idx).into() {
                self.counts[group_idx] += count;
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.counts.capacity() * std::mem::size_of::<i64>()
    }
}
