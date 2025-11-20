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

use std::any::Any;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::aggregates::avg_return_type;
use datafusion::logical_expr::Volatility::Immutable;
use crate::{AvgDecimal, EvalMode};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AvgInt {
    signature: Signature,
    eval_mode: EvalMode,
}

impl AvgInt {
    pub fn try_new(data_type: DataType, eval_mode: EvalMode) -> DFResult<Self> {
        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(Self {
                signature: Signature::user_defined(Immutable),
                eval_mode
                })
            },
            _ => {Err(DataFusionError::Internal("inalid data type for AvgInt".to_string()))}
        }
    }
}

impl AggregateUDFImpl for AvgInt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn signature(&self) -> &Signature  {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        avg_return_type(self.name(), &arg_types[0])
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> datafusion::common::Result<Box<dyn Accumulator>> {
        todo!()
    }

    fn state_fields(&self, args: StateFieldsArgs) -> datafusion::common::Result<Vec<FieldRef>> {
        todo!()
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(&self, _args: AccumulatorArgs) -> datafusion::common::Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(AvgIntGroupsAccumulator::new(self.eval_mode)))
    }

    fn default_value(&self, data_type: &DataType) -> datafusion::common::Result<ScalarValue> {
        todo!()
    }
}

struct AvgIntegerAccumulator{
    sum: Option<i64>,
    count: u64,
    eval_mode: EvalMode,
}

impl AvgIntegerAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        Self{
            sum : Some(0),
            count: 0,
            eval_mode
        }
    }
}

impl Accumulator for AvgIntegerAccumulator {
    
}

struct AvgIntGroupsAccumulator {

}

impl AvgIntGroupsAccumulator {

}


impl GroupsAccumulator for AvgIntGroupsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> datafusion::common::Result<()> {
        todo!()
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> datafusion::common::Result<ArrayRef> {
        todo!()
    }

    fn state(&mut self, emit_to: EmitTo) -> datafusion::common::Result<Vec<ArrayRef>> {
        todo!()
    }

    fn merge_batch(&mut self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> datafusion::common::Result<()> {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }
}