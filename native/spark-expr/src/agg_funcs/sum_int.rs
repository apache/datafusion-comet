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
    cast::AsArray, Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BooleanArray,
    Int64Array, PrimitiveArray,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
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

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SumIntegerAccumulator::new(self.eval_mode)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        if self.eval_mode == EvalMode::Try {
            Ok(vec![
                Arc::new(Field::new("sum", DataType::Int64, true)),
                Arc::new(Field::new("is_null", DataType::Boolean, false)),
            ])
        } else {
            Ok(vec![Arc::new(Field::new("sum", DataType::Int64, true))])
        }
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
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
    is_null: bool,
}

impl SumIntegerAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        Self {
            sum: 0,
            eval_mode,
            is_null: false,
        }
    }
}

impl Accumulator for SumIntegerAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        // accumulator internal to add sum and return is_null: true if there is an overflow in Try Eval mode
        fn update_sum_internal<T>(
            int_array: &PrimitiveArray<T>,
            eval_mode: EvalMode,
            mut sum: i64,
            is_null: bool,
        ) -> Result<(i64, bool), DataFusionError>
        where
            T: ArrowPrimitiveType,
        {
            for i in 0..int_array.len() {
                if !is_null && !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    match eval_mode {
                        EvalMode::Legacy => {
                            sum = v.add_wrapping(sum);
                        }
                        EvalMode::Ansi | EvalMode::Try => {
                            match v.add_checked(sum) {
                                Ok(v) => sum = v,
                                Err(_e) => {
                                    return if eval_mode == EvalMode::Ansi {
                                        Err(DataFusionError::from(arithmetic_overflow_error(
                                            "integer",
                                        )))
                                    } else {
                                        return Ok((sum, true));
                                    };
                                }
                            };
                        }
                    }
                }
            }
            Ok((sum, false))
        }

        if self.is_null {
            Ok(())
        } else {
            let values = &values[0];
            if values.len() == values.null_count() {
                Ok(())
            } else {
                let (sum, is_overflow) = match values.data_type() {
                    DataType::Int64 => update_sum_internal(
                        values
                            .as_any()
                            .downcast_ref::<PrimitiveArray<Int64Type>>()
                            .unwrap(),
                        self.eval_mode,
                        self.sum,
                        self.is_null,
                    )?,
                    DataType::Int32 => update_sum_internal(
                        values
                            .as_any()
                            .downcast_ref::<PrimitiveArray<Int32Type>>()
                            .unwrap(),
                        self.eval_mode,
                        self.sum,
                        self.is_null,
                    )?,
                    DataType::Int16 => update_sum_internal(
                        values
                            .as_any()
                            .downcast_ref::<PrimitiveArray<Int16Type>>()
                            .unwrap(),
                        self.eval_mode,
                        self.sum,
                        self.is_null,
                    )?,
                    DataType::Int8 => update_sum_internal(
                        values
                            .as_any()
                            .downcast_ref::<PrimitiveArray<Int8Type>>()
                            .unwrap(),
                        self.eval_mode,
                        self.sum,
                        self.is_null,
                    )?,
                    _ => {
                        panic!("Unsupported data type")
                    }
                };

                self.sum = sum;
                self.is_null = is_overflow;
                Ok(())
            }
        }
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.is_null {
            Ok(ScalarValue::Int64(None))
        } else {
            Ok(ScalarValue::Int64(Some(self.sum)))
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        if self.eval_mode == EvalMode::Try {
            Ok(vec![
                ScalarValue::Int64(Some(self.sum)),
                ScalarValue::Boolean(Some(self.is_null)),
            ])
        } else {
            Ok(vec![ScalarValue::Int64(Some(self.sum))])
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if self.is_null {
            return Ok(());
        }
        let that_sum = states[0].as_primitive::<Int64Type>();

        if self.eval_mode == EvalMode::Try && states[1].as_boolean().value(0) {
            return Ok(());
        }

        match self.eval_mode {
            EvalMode::Legacy => {
                self.sum = self.sum.add_wrapping(that_sum.value(0));
            }
            EvalMode::Ansi | EvalMode::Try => match self.sum.add_checked(that_sum.value(0)) {
                Ok(v) => self.sum = v,
                Err(_e) => {
                    if self.eval_mode == EvalMode::Ansi {
                        return Err(DataFusionError::from(arithmetic_overflow_error("integer")));
                    } else {
                        self.is_null = true;
                    }
                }
            },
        }
        Ok(())
    }
}

struct SumIntGroupsAccumulator {
    sums: Vec<i64>,
    has_nulls: Vec<bool>,
    eval_mode: EvalMode,
}

impl SumIntGroupsAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        Self {
            sums: Vec::new(),
            eval_mode,
            has_nulls: Vec::new(),
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
        fn update_groups_sum_internal<T>(
            int_array: &PrimitiveArray<T>,
            group_indices: &[usize],
            sums: &mut [i64],
            has_nulls: &mut [bool],
            eval_mode: EvalMode,
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
                if !has_nulls[group_index] && !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    match eval_mode {
                        EvalMode::Legacy => {
                            sums[group_index] = sums[group_index].add_wrapping(v);
                        }
                        EvalMode::Ansi | EvalMode::Try => {
                            match sums[group_index].add_checked(v) {
                                Ok(new_sum) => sums[group_index] = new_sum,
                                Err(_) => {
                                    if eval_mode == EvalMode::Ansi {
                                        return Err(DataFusionError::from(
                                            arithmetic_overflow_error("integer"),
                                        ));
                                    } else {
                                        has_nulls[group_index] = true
                                    }
                                }
                            };
                        }
                    }
                }
            }
            Ok(())
        }

        assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        let values = &values[0];
        self.sums.resize(total_num_groups, 0);
        self.has_nulls.resize(total_num_groups, false);

        match values.data_type() {
            DataType::Int64 => update_groups_sum_internal(
                values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap(),
                group_indices,
                &mut self.sums,
                &mut self.has_nulls,
                self.eval_mode,
            )?,
            DataType::Int32 => update_groups_sum_internal(
                values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int32Type>>()
                    .unwrap(),
                group_indices,
                &mut self.sums,
                &mut self.has_nulls,
                self.eval_mode,
            )?,
            DataType::Int16 => update_groups_sum_internal(
                values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int16Type>>()
                    .unwrap(),
                group_indices,
                &mut self.sums,
                &mut self.has_nulls,
                self.eval_mode,
            )?,
            DataType::Int8 => update_groups_sum_internal(
                values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int8Type>>()
                    .unwrap(),
                group_indices,
                &mut self.sums,
                &mut self.has_nulls,
                self.eval_mode,
            )?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type for SumIntGroupsAccumulator: {:?}",
                    values.data_type()
                )))
            }
        };
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        match emit_to {
            EmitTo::All => {
                // Create an Int64Array with nullability from has_nulls
                let result = Arc::new(Int64Array::from_iter(
                    self.sums
                        .iter()
                        .zip(self.has_nulls.iter())
                        .map(|(&sum, &is_null)| if is_null { None } else { Some(sum) }),
                )) as ArrayRef;

                self.sums.clear();
                self.has_nulls.clear();
                Ok(result)
            }
            EmitTo::First(n) => {
                let result = Arc::new(Int64Array::from_iter(
                    self.sums
                        .drain(..n)
                        .zip(self.has_nulls.drain(..n))
                        .map(|(sum, is_null)| if is_null { None } else { Some(sum) }),
                )) as ArrayRef;
                Ok(result)
            }
        }
    }

    fn state(&mut self, _emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        if self.eval_mode == EvalMode::Try {
            Ok(vec![
                Arc::new(Int64Array::from(self.sums.clone())),
                Arc::new(BooleanArray::from(self.has_nulls.clone())),
            ])
        } else {
            Ok(vec![Arc::new(Int64Array::from(self.sums.clone()))])
        }
    }

    fn merge_batch(
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
        self.has_nulls.resize(total_num_groups, false);

        let iter = group_indices.iter().zip(data.iter());

        for (&group_index, &value) in iter {
            if !self.has_nulls[group_index] {
                match self.eval_mode {
                    EvalMode::Legacy => {
                        self.sums[group_index] = self.sums[group_index].add_wrapping(value);
                    }
                    EvalMode::Ansi | EvalMode::Try => {
                        match self.sums[group_index].add_checked(value) {
                            Ok(v) => self.sums[group_index] = v,
                            Err(_e) => {
                                if self.eval_mode == EvalMode::Ansi {
                                    return Err(DataFusionError::from(arithmetic_overflow_error(
                                        "integer",
                                    )));
                                } else {
                                    self.has_nulls[group_index] = true
                                }
                            }
                        };
                    }
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
