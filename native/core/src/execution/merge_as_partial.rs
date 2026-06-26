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
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::function::StateFieldsArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF,
    Signature, Volatility,
};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::scalar::ScalarValue;

/// An AggregateUDF wrapper that gives merge semantics in Partial mode.
///
/// When DataFusion runs an AggregateExec in Partial mode, it calls `update_batch`
/// on each accumulator and outputs `state()`. This wrapper intercepts `update_batch`
/// and redirects it to `merge_batch` on the inner accumulator, effectively
/// implementing PartialMerge: merge inputs, output state.
///
/// We store the inner AggregateUDF (not the AggregateFunctionExpr) to avoid keeping
/// references to UnboundColumn expressions that would panic if evaluated.
#[derive(Debug)]
pub struct MergeAsPartialUDF {
    /// The inner aggregate UDF, cloned from the original expression.
    inner_udf: AggregateUDF,
    /// Pre-computed return type from the original expression.
    return_type: DataType,
    /// Pre-computed state fields from the original expression.
    cached_state_fields: Vec<FieldRef>,
    /// Cached signature that accepts state field types.
    signature: Signature,
    /// Override argument fields used only when constructing the inner accumulator.
    accumulator_expr_fields: Option<Vec<FieldRef>>,
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
        let return_type = inner_expr.field().data_type().clone();
        let cached_state_fields = inner_expr.state_fields()?;
        let accumulator_expr_fields =
            Self::accumulator_expr_fields(inner_expr, &cached_state_fields);

        // Use a permissive signature since we accept state field types which
        // vary per aggregate function.
        let signature = Signature::variadic_any(Volatility::Immutable);

        Ok(Self {
            inner_udf: inner_expr.fun().clone(),
            return_type,
            cached_state_fields,
            signature,
            accumulator_expr_fields,
            name,
        })
    }

    fn accumulator_expr_fields(
        inner_expr: &AggregateFunctionExpr,
        state_fields: &[FieldRef],
    ) -> Option<Vec<FieldRef>> {
        // Spark collect_list/collect_set use a single list state field but their accumulator
        // constructors expect the original scalar input type. MergeAsPartial receives state-typed
        // input columns, so passing those fields through would make the collect accumulator think
        // it is aggregating arrays and create nested list state (List(List(T))).
        match (inner_expr.fun().name(), state_fields) {
            ("collect_list" | "collect_set", [state_field]) => match state_field.data_type() {
                DataType::List(item_field) => Some(vec![Field::new(
                    item_field.name(),
                    item_field.data_type().clone(),
                    item_field.is_nullable(),
                )
                .into()]),
                _ => None,
            },
            _ => None,
        }
    }

    fn inner_accumulator_args<'a>(&'a self, args: AccumulatorArgs<'a>) -> AccumulatorArgs<'a> {
        let expr_fields = self
            .accumulator_expr_fields
            .as_deref()
            .unwrap_or(args.expr_fields);
        AccumulatorArgs {
            expr_fields,
            ..args
        }
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
        Ok(self.return_type.clone())
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Cached at construction: state schema depends on the inner aggregate's
        // return type, not on StateFieldsArgs.
        Ok(self.cached_state_fields.clone())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let inner_acc = self
            .inner_udf
            .accumulator(self.inner_accumulator_args(args))?;
        Ok(Box::new(MergeAsPartialAccumulator { inner: inner_acc }))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner_udf
            .groups_accumulator_supported(self.inner_accumulator_args(args))
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let inner_acc = self
            .inner_udf
            .create_groups_accumulator(self.inner_accumulator_args(args))?;
        Ok(Box::new(MergeAsPartialGroupsAccumulator {
            inner: inner_acc,
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
}

impl Debug for MergeAsPartialAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeAsPartialAccumulator").finish()
    }
}

impl Accumulator for MergeAsPartialAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Redirect update to merge — this is the key trick.
        self.inner.merge_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
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
        // Redirect update to merge — this is the key trick.
        self.inner
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
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
