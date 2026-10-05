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

//! MergeAsPartial wrapper for implementing Spark's PartialMerge aggregate mode.
//!
//! Spark's PartialMerge mode merges intermediate state buffers and outputs intermediate
//! state (not final values). DataFusion has no equivalent mode — `Partial` calls
//! `update_batch` and outputs state, while `Final` calls `merge_batch` and outputs
//! evaluated results.
//!
//! This wrapper bridges the gap: it operates under DataFusion's `Partial` mode (which
//! outputs state) but redirects `update_batch` calls to `merge_batch`, giving merge
//! semantics with state output.

use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::function::StateFieldsArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature, Volatility,
};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::GroupsAccumulatorAdapter;
use datafusion::scalar::ScalarValue;

use crate::execution::spark_aggregate_state::PartialMergeStateDecoder;

/// An AggregateUDF wrapper that gives merge semantics in Partial mode.
///
/// When DataFusion runs an AggregateExec in Partial mode, it calls `update_batch`
/// on each accumulator and outputs `state()`. This wrapper intercepts `update_batch`
/// and redirects it to `merge_batch` on the inner accumulator, effectively
/// implementing PartialMerge: merge inputs, output state.
///
/// Retain the original expression as the accumulator factory so its input types,
/// argument count and other options are not replaced by intermediate-state metadata.
/// Its expressions are never evaluated; updates receive the already-bound state inputs.
#[derive(Debug)]
pub struct MergeAsPartialUDF {
    inner_expr: AggregateFunctionExpr,
    /// Pre-computed state fields from the original expression.
    cached_state_fields: Vec<FieldRef>,
    /// Cached signature that accepts state field types.
    signature: Signature,
    /// Decoder for Spark JVM state, or pass-through for native-compatible state.
    state_decoder: PartialMergeStateDecoder,
    /// Name for this wrapper.
    name: String,
}

impl PartialEq for MergeAsPartialUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for MergeAsPartialUDF {}

impl Hash for MergeAsPartialUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl MergeAsPartialUDF {
    pub fn new(inner_expr: &AggregateFunctionExpr) -> Result<Self> {
        let name = format!("merge_as_partial_{}", inner_expr.name());
        let cached_state_fields = inner_expr.state_fields()?;
        let state_decoder = PartialMergeStateDecoder::try_new(inner_expr, &cached_state_fields)?;

        // Use a permissive signature since we accept state field types which
        // vary per aggregate function.
        let signature = Signature::variadic_any(Volatility::Immutable);

        Ok(Self {
            inner_expr: inner_expr.clone(),
            cached_state_fields,
            signature,
            state_decoder,
            name,
        })
    }
}

impl AggregateUDFImpl for MergeAsPartialUDF {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // In Partial mode, return_type isn't used for output schema (state_fields is).
        // Return the inner function's return type for consistency.
        Ok(self.inner_expr.field().data_type().clone())
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Cached at construction: state schema depends on the inner aggregate's
        // return type, not on StateFieldsArgs.
        Ok(self.cached_state_fields.clone())
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let inner_acc = self.inner_expr.create_accumulator()?;
        Ok(Box::new(MergeAsPartialAccumulator {
            inner: inner_acc,
            state_decoder: self.state_decoder.clone(),
        }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        // Scalar-only functions still have a direct state-input passthrough.
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let inner_acc = if self.inner_expr.groups_accumulator_supported() {
            self.inner_expr.create_groups_accumulator()?
        } else {
            let factory = self.inner_expr.clone();
            Box::new(GroupsAccumulatorAdapter::new(move || {
                factory.create_accumulator()
            }))
        };
        Ok(Box::new(MergeAsPartialGroupsAccumulator {
            inner: inner_acc,
            state_decoder: self.state_decoder.clone(),
        }))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::NotSupported
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }

    fn is_descending(&self) -> Option<bool> {
        None
    }
}

/// Accumulator wrapper that redirects update_batch to merge_batch.
struct MergeAsPartialAccumulator {
    inner: Box<dyn Accumulator>,
    state_decoder: PartialMergeStateDecoder,
}

impl Debug for MergeAsPartialAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeAsPartialAccumulator").finish()
    }
}

impl Accumulator for MergeAsPartialAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Redirect update to merge — this is the key trick.
        let decoded = self.state_decoder.decode(values)?;
        self.inner.merge_batch(decoded.as_ref())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let decoded = self.state_decoder.decode(states)?;
        self.inner.merge_batch(decoded.as_ref())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

/// GroupsAccumulator wrapper that redirects update_batch to merge_batch.
struct MergeAsPartialGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    state_decoder: PartialMergeStateDecoder,
}

impl Debug for MergeAsPartialGroupsAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeAsPartialGroupsAccumulator").finish()
    }
}

impl GroupsAccumulator for MergeAsPartialGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if opt_filter.is_some() {
            return exec_err!("PartialMerge cannot apply a filter to already aggregated states");
        }
        // Redirect update to merge — this is the key trick.
        let decoded = self.state_decoder.decode(values)?;
        self.inner
            .merge_batch(decoded.as_ref(), group_indices, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        let decoded = self.state_decoder.decode(values)?;
        self.inner
            .merge_batch(decoded.as_ref(), group_indices, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        if opt_filter.is_some() {
            return exec_err!("PartialMerge cannot apply a filter to already aggregated states");
        }
        // Keep each state's weight when local grouping is bypassed, decoding
        // serialized Spark buffers into the native intermediate-state schema.
        Ok(self.state_decoder.decode(values)?.into_owned())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.inner.evaluate(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::{Array, BinaryArray, Int64Array, ListArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion_comet_spark_expr::{CometCollectList, CometCollectSet};

    fn merge_expression(original: &AggregateFunctionExpr) -> AggregateFunctionExpr {
        let fields = original.state_fields().unwrap();
        let schema = Arc::new(Schema::new(fields));
        let args = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>
            })
            .collect();
        AggregateExprBuilder::new(
            Arc::new(AggregateUDF::new_from_impl(
                MergeAsPartialUDF::new(original).unwrap(),
            )),
            args,
        )
        .schema(schema)
        .alias(original.name())
        .build()
        .unwrap()
    }

    #[test]
    fn collect_set_merge_preserves_original_element_type_for_both_factories() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let original = AggregateExprBuilder::new(
            Arc::new(AggregateUDF::new_from_impl(CometCollectSet::new())),
            vec![Arc::new(Column::new("v", 0))],
        )
        .schema(schema)
        .alias("values")
        .build()
        .unwrap();
        let merge = merge_expression(&original);
        assert_eq!(
            merge.state_fields().unwrap(),
            original.state_fields().unwrap()
        );
        assert!(merge.groups_accumulator_supported());

        let mut partial = original.create_groups_accumulator().unwrap();
        partial
            .update_batch(
                &[Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(2),
                    None,
                ]))],
                &[0, 0, 1, 1],
                None,
                2,
            )
            .unwrap();
        let states = partial.state(EmitTo::All).unwrap();
        let expected = vec![1, 2];

        let mut grouped = merge.create_groups_accumulator().unwrap();
        grouped.update_batch(&states, &[0, 0], None, 1).unwrap();
        let result = grouped.evaluate(EmitTo::All).unwrap();
        assert_eq!(sorted_values(&result), expected);

        let mut scalar = merge.create_accumulator().unwrap();
        scalar.update_batch(&states).unwrap();
        let result = scalar.evaluate().unwrap().to_array().unwrap();
        assert_eq!(sorted_values(&result), expected);
    }

    #[test]
    fn spark_collect_state_decodes_before_merge_and_bypass() {
        // Spark UnsafeRow containing UnsafeArrayData for [1L, 2L]. The outer row
        // has one variable-width field at offset 16, and the array has two values.
        let mut row = vec![0_u8; 48];
        row[8..16].copy_from_slice(&((16_i64 << 32) | 32).to_le_bytes());
        row[16..24].copy_from_slice(&2_i64.to_le_bytes());
        row[32..40].copy_from_slice(&1_i64.to_le_bytes());
        row[40..48].copy_from_slice(&2_i64.to_le_bytes());
        let states = vec![Arc::new(BinaryArray::from_iter_values([row])) as ArrayRef];

        for function in [
            AggregateUDF::new_from_impl(CometCollectList::new()),
            AggregateUDF::new_from_impl(CometCollectSet::new()),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
            let original =
                AggregateExprBuilder::new(Arc::new(function), vec![Arc::new(Column::new("v", 0))])
                    .schema(schema)
                    .alias("values")
                    .build()
                    .unwrap();
            let merge = merge_expression(&original);
            let mut grouped = merge.create_groups_accumulator().unwrap();
            let converted = grouped.convert_to_state(&states, None).unwrap();
            assert_eq!(sorted_values(&converted[0]), vec![1, 2]);
            grouped.update_batch(&states, &[0], None, 1).unwrap();
            assert_eq!(
                sorted_values(&grouped.evaluate(EmitTo::All).unwrap()),
                vec![1, 2]
            );

            let mut scalar = merge.create_accumulator().unwrap();
            scalar.update_batch(&states).unwrap();
            assert_eq!(
                sorted_values(&scalar.evaluate().unwrap().to_array().unwrap()),
                vec![1, 2]
            );
        }
    }

    fn sorted_values(result: &ArrayRef) -> Vec<i64> {
        let values = result
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let mut values = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        values.sort_unstable();
        values
    }

    #[test]
    fn multiargument_count_merge_preserves_scalar_factory_and_state_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new("w", DataType::Int64, true),
        ]));
        let original = AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(Column::new("v", 0)), Arc::new(Column::new("w", 1))],
        )
        .schema(schema)
        .alias("both")
        .build()
        .unwrap();
        let merge = merge_expression(&original);
        assert!(!original.groups_accumulator_supported());
        assert!(merge.groups_accumulator_supported());
        assert_eq!(
            merge.state_fields().unwrap(),
            original.state_fields().unwrap()
        );

        let mut partial = original.create_accumulator().unwrap();
        partial
            .update_batch(&[
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
            ])
            .unwrap();
        let states = partial
            .state()
            .unwrap()
            .into_iter()
            .map(|state| state.to_array().unwrap())
            .collect::<Vec<_>>();
        let mut accumulator = merge.create_accumulator().unwrap();
        accumulator.update_batch(&states).unwrap();
        accumulator.update_batch(&states).unwrap();
        assert_eq!(
            accumulator.state().unwrap(),
            vec![ScalarValue::Int64(Some(2))]
        );
        assert_eq!(accumulator.evaluate().unwrap(), ScalarValue::Int64(Some(2)));

        let state_schema = Arc::new(Schema::new(merge.state_fields().unwrap()));
        let wrapped = crate::execution::partial_aggregation::wrap_aggregate_expr(
            Arc::new(merge),
            state_schema,
        )
        .unwrap();
        let mut grouped = wrapped.create_groups_accumulator().unwrap();
        grouped.update_batch(&states, &[0], None, 1).unwrap();
        grouped.update_batch(&states, &[0], None, 1).unwrap();
        let converted = grouped.convert_to_state(&states, None).unwrap();
        assert!(Arc::ptr_eq(&converted[0], &states[0]));
        let filter = BooleanArray::from(vec![false]);
        assert!(grouped.convert_to_state(&states, Some(&filter)).is_err());
        assert!(grouped
            .update_batch(&states, &[0], Some(&filter), 1)
            .is_err());
        let output = grouped.state(EmitTo::All).unwrap();
        assert_eq!(
            output[0].as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![2])
        );
    }
}
