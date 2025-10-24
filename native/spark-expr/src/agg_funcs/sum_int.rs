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

use crate::{arithmetic_overflow_error, EvalMode};
use arrow::array::{
    cast::AsArray, Array, ArrayBuilder, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType,
    BooleanArray, Int64Array, PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
};
use std::{any::Any, sync::Arc};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SumInteger {
    /// Aggregate function signature
    signature: Signature,
    /// eval mode : ANSI, Legacy, Try
    eval_mode: EvalMode,
}

impl SumInteger {
    pub fn try_new(data_type: DataType, eval_mode: EvalMode) -> DFResult<Self> {
        // The `data_type` is the SUM result type passed from Spark side which should i64
        println!("data type: {:?} eval_mode {:?}", data_type, eval_mode);

        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(Self {
                signature: Signature::user_defined(Immutable),
                eval_mode,
            }),
            _ => Err(DataFusionError::Internal(
                "Invalid data type for SumInteger".into(),
            )),
        }
    }
}

impl AggregateUDFImpl for SumInteger {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SumIntegerAccumulator::new(self.eval_mode)))
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SumIntGroupsAccumulator::new(self.eval_mode)))
    }
}

#[derive(Debug)]
struct SumIntegerAccumulator {
    sum: i64,
    eval_mode: EvalMode,
    input_data_type: DataType,
}

impl SumIntegerAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        Self {
            sum: 0,
            eval_mode,
            input_data_type: DataType::Int64,
        }
    }
}

impl Accumulator for SumIntegerAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        fn update_sum_internal<T>(
            int_array: &PrimitiveArray<T>,
            eval_mode: EvalMode,
            mut sum: i64,
        ) -> Result<i64, DataFusionError>
        where
            T: ArrowPrimitiveType,
        {
            let len = int_array.len();
            for i in 0..int_array.len() {
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    println!("sum : {:?}, v : {:?}", sum, v);
                    match eval_mode {
                        EvalMode::Legacy => {
                            sum = v.add_wrapping(sum);
                        }
                        EvalMode::Ansi | EvalMode::Try => {
                            match v.add_checked(sum) {
                                Ok(v) => sum = v,
                                Err(e) => {
                                    if (eval_mode == EvalMode::Ansi){
                                        return Err(DataFusionError::from(arithmetic_overflow_error("integer")))
                                    }
                                    else {
                                        sum = None.unwrap();
                                    }
                                }
                            };
                        }
                    }
                }
            }
            println!("match internal (AFTER) function data type: {:?}", sum);

            Ok(sum)
        }

        let values = &values[0];
        println!("accumulator data type: {:?}", self.input_data_type);

        println!(
            "DEBUG: values[0] actual Rust type: {:?}, Arrow dtype: {:?}, len={}",
            values.as_any().type_id(),
            values.data_type(),
            values.len()
        );

        if values.len() == values.null_count() {
            println!(
                "ALL NULL in values accumulator data type: {:?}",
                self.input_data_type
            );
            Ok(())
        } else {
            self.sum = match values.data_type() {
                DataType::Int64 => update_sum_internal(
                    values
                        .as_any()
                        .downcast_ref::<PrimitiveArray<Int64Type>>()
                        .unwrap(),
                    self.eval_mode,
                    self.sum,
                )?,
                DataType::Int32 => update_sum_internal(
                    values
                        .as_any()
                        .downcast_ref::<PrimitiveArray<Int32Type>>()
                        .unwrap(),
                    self.eval_mode,
                    self.sum,
                )?,
                DataType::Int16 => update_sum_internal(
                    values
                        .as_any()
                        .downcast_ref::<PrimitiveArray<Int16Type>>()
                        .unwrap(),
                    self.eval_mode,
                    self.sum,
                )?,
                DataType::Int8 => update_sum_internal(
                    values
                        .as_any()
                        .downcast_ref::<PrimitiveArray<Int8Type>>()
                        .unwrap(),
                    self.eval_mode,
                    self.sum,
                )?,
                _ => {
                    panic!("Unsupported data type")
                }
            };
            println!(
                "sum updated accumulator data type: {:?}",
                self.input_data_type
            );

            Ok(())
        }
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        println!(
            "evaluate :: accumulator data type: {:?}",
            self.input_data_type
        );
        Ok(ScalarValue::Int64(Some(self.sum)))
    }

    fn size(&self) -> usize {
        println!("size :: accumulator data type: {:?}", self.input_data_type);
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        println!("state :: accumulator data type: {:?}", self.input_data_type);
        Ok(vec![ScalarValue::Int64(Some(self.sum))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        println!(
            "merge batch :: accumulator data type: {:?}",
            self.input_data_type
        );
        let that_sum = states[0].as_primitive::<Int64Type>();
        match self.eval_mode {
            EvalMode::Legacy => {
                self.sum.add_wrapping(that_sum.value(0));
            }
            EvalMode::Ansi | EvalMode::Try => match self.sum.add_checked(that_sum.value(0)) {
                Ok(v) => self.sum = v,
                Err(e) =>
                if (self.eval_mode == EvalMode::Ansi){
                    return Err(DataFusionError::from(arithmetic_overflow_error("integer"))),
                }
                else{
                    self.sum = None.unwrap();
                }
            },
        }
        Ok(())
    }
}

struct SumIntGroupsAccumulator {
    sums: Vec<i64>,
    eval_mode: EvalMode,
}

impl SumIntGroupsAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        Self {
            sums: Vec::new(),
            eval_mode,
        }
    }
}

impl GroupsAccumulator for SumIntGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        let values = values[0].as_primitive::<Int64Type>();
        let data = values.values();
        self.sums.resize(total_num_groups, 0);

        let iter = group_indices.iter().zip(data.iter());

        for (&group_index, &value) in iter {
            match self.eval_mode {
                EvalMode::Legacy => {
                    self.sums[group_index] = self.sums[group_index].add_wrapping(value);
                }
                EvalMode::Ansi | EvalMode::Try => {
                    match self.sums[group_index].add_checked(value) {
                        Ok(v) => self.sums[group_index] = v,
                        Err(e) => {
                            if (self.eval_mode == EvalMode::Ansi){
                                return Err(DataFusionError::from(arithmetic_overflow_error("integer")))
                            }
                            else{
                                self.sums[group_index] = None.unwrap();
                            }
                        }
                    };
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        match emit_to {
            // When emitting all groups, return all calculated sums and reset the internal state.
            EmitTo::All => {
                // Create an Arrow array from the accumulated sums.
                let result = Arc::new(Int64Array::from(self.sums.clone())) as ArrayRef;
                // Reset the accumulator state for the next use.
                self.sums.clear();
                Ok(result)
            }
            // When emitting the first `n` groups, return the first `n` sums
            // and retain the state for the remaining groups.
            EmitTo::First(n) => {
                // Take the first `n` sums.
                let emitted_sums: Vec<i64> = self.sums.drain(..n).collect();
                let result = Arc::new(Int64Array::from(emitted_sums)) as ArrayRef;
                Ok(result)
            }
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let state_array = Arc::new(Int64Array::from(self.sums.clone()));
        Ok(vec![state_array])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        println!("merge batch : {:?}", values[0]);
        let values = values[0].as_primitive::<Int64Type>();
        let data = values.values();
        self.sums.resize(total_num_groups, 0);

        let iter = group_indices.iter().zip(data.iter());

        for (&group_index, &value) in iter {
            match self.eval_mode {
                EvalMode::Legacy | EvalMode::Try => {
                    self.sums[group_index] = self.sums[group_index].add_wrapping(value);
                }
                EvalMode::Ansi | EvalMode::Try => {
                    match self.sums[group_index].add_checked(value) {
                        Ok(v) => self.sums[group_index] = v,
                        Err(e) => {
                            if (self.eval_mode == EvalMode::Ansi){
                                return Err(DataFusionError::from(arithmetic_overflow_error("integer")))
                            }
                            else{
                                self.sums[group_index] = None.unwrap();
                            }
                        }
                    };
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::builder::StringBuilder;
    use arrow::array::{Int64Builder, RecordBatch};
    use arrow::datatypes::DataType::Int64;
    use arrow::datatypes::*;
    use datafusion::common::Result;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;

    #[test]
    fn invalid_data_type() {
        assert!(SumInteger::try_new(DataType::Date32, EvalMode::Legacy).is_err());
    }

    #[tokio::test]
    async fn sum_no_overflow() -> Result<()> {
        let num_rows = 8192;
        let batch = create_record_batch(num_rows);
        let mut batches = Vec::new();
        for _ in 0..10 {
            batches.push(batch.clone());
        }
        let partitions = &[batches];
        let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
        let c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));

        let data_type = Int64;
        let schema = Arc::clone(&partitions[0][0].schema());
        let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None)?,
        )));

        let aggregate_udf = Arc::new(AggregateUDF::new_from_impl(SumInteger::try_new(
            data_type.clone(),
            EvalMode::Legacy,
        )?));

        let aggr_expr = AggregateExprBuilder::new(aggregate_udf, vec![c1])
            .schema(Arc::clone(&schema))
            .alias("sum")
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()?;

        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(c0, "c0".to_string())]),
            vec![aggr_expr.into()],
            vec![None], // no filter expressions
            scan,
            Arc::clone(&schema),
        )?);

        let mut stream = aggregate
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        while let Some(batch) = stream.next().await {
            let _batch = batch?;
        }

        Ok(())
    }

    fn create_record_batch(num_rows: usize) -> RecordBatch {
        let mut int_builder = Int64Builder::with_capacity(num_rows);
        let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for i in 0..num_rows {
            int_builder.append_value(i as i64);
            string_builder.append_value(format!("this is string #{}", i % 1024));
        }
        let int_array = Arc::new(int_builder.finish());
        let string_array = Arc::new(string_builder.finish());

        let mut fields = vec![];
        let mut columns: Vec<ArrayRef> = vec![];

        // string column
        fields.push(Field::new("c0", DataType::Utf8, false));
        columns.push(string_array);

        // decimal column
        fields.push(Field::new("c1", DataType::Int64, false));
        columns.push(int_array);

        let schema = Schema::new(fields);
        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }
}
