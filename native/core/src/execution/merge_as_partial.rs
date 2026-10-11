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
//! evaluated results. `PartialReduce` merges state and outputs state, but from DataFusion 56
//! it emits a group more than once when it runs short of memory, as `Partial` does.
//!
//! This wrapper bridges the gap: it redirects `update_batch` calls to `merge_batch`, and it
//! evaluates to its state, packed into a struct. Under DataFusion's `Partial` mode, which
//! outputs state, that gives merge semantics with state output. Under `Single` mode, which
//! spills rather than emit a group twice, the planner unpacks the struct into the state columns.

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use datafusion::common::{internal_err, Result};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::function::StateFieldsArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF,
    Signature, Volatility,
};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{
    expressions::Column, GroupsAccumulatorAdapter, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::{projection::ProjectionExec, ExecutionPlan};
use datafusion::scalar::ScalarValue;
use datafusion_comet_spark_expr::GetStructField;

use crate::execution::spark_aggregate_state::PartialMergeStateDecoder;

/// An AggregateUDF wrapper that merges state and outputs state in Partial or Single mode.
///
/// When DataFusion runs an AggregateExec in Partial mode, it calls `update_batch`
/// on each accumulator and outputs `state()`. This wrapper intercepts `update_batch`
/// and redirects it to `merge_batch` on the inner accumulator, effectively
/// implementing PartialMerge: merge inputs, output state. In Single mode, DataFusion
/// outputs `evaluate()` instead, which returns the inner accumulator's state as a struct
/// of the state fields.
///
/// We store the inner AggregateUDF (not the AggregateFunctionExpr) to avoid keeping
/// references to UnboundColumn expressions that would panic if evaluated.
#[derive(Debug)]
pub struct MergeAsPartialUDF {
    /// The inner aggregate UDF, cloned from the original expression.
    inner_udf: AggregateUDF,
    /// The original expression's return field, which the inner accumulator is built with, since
    /// the wrapper returns its state instead.
    inner_return_field: FieldRef,
    /// Pre-computed state fields from the original expression, which are also the fields of
    /// the struct the wrapper evaluates to.
    cached_state_fields: Fields,
    /// Cached signature that accepts state field types.
    signature: Signature,
    /// Override argument fields used only when constructing the inner accumulator.
    accumulator_expr_fields: Option<Vec<FieldRef>>,
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
        let cached_state_fields = Fields::from(inner_expr.state_fields()?);
        let accumulator_expr_fields =
            Self::accumulator_expr_fields(inner_expr, &cached_state_fields);
        let state_decoder = PartialMergeStateDecoder::try_new(inner_expr, &cached_state_fields)?;

        // Use a permissive signature since we accept state field types which
        // vary per aggregate function.
        let signature = Signature::variadic_any(Volatility::Immutable);

        Ok(Self {
            inner_udf: inner_expr.fun().clone(),
            inner_return_field: inner_expr.field(),
            cached_state_fields,
            signature,
            accumulator_expr_fields,
            state_decoder,
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
            return_field: Arc::clone(&self.inner_return_field),
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
        // In Single mode, the wrapper evaluates to its state.
        Ok(DataType::Struct(self.cached_state_fields.clone()))
    }

    fn is_nullable(&self) -> bool {
        // The state struct itself is never null, so each state field unpacked from it keeps its
        // own nullability.
        false
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Cached at construction: state schema depends on the inner aggregate's
        // return type, not on StateFieldsArgs.
        Ok(self.cached_state_fields.to_vec())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let inner_acc = self
            .inner_udf
            .accumulator(self.inner_accumulator_args(args))?;
        Ok(Box::new(MergeAsPartialAccumulator {
            inner: inner_acc,
            state_decoder: self.state_decoder.clone(),
            state_fields: self.cached_state_fields.clone(),
        }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        // An inner aggregate without a GroupsAccumulator gets an adapter below, so that Single
        // mode emits its states a column at a time rather than evaluating each group to a struct.
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let inner_args = self.inner_accumulator_args(args);
        let inner_acc: Box<dyn GroupsAccumulator> = if self
            .inner_udf
            .groups_accumulator_supported(inner_args.clone())
        {
            self.inner_udf.create_groups_accumulator(inner_args)?
        } else {
            let factory = InnerAccumulatorFactory::new(&self.inner_udf, &inner_args);
            Box::new(GroupsAccumulatorAdapter::new(move || factory.create()))
        };
        Ok(Box::new(MergeAsPartialGroupsAccumulator {
            inner: inner_acc,
            state_decoder: self.state_decoder.clone(),
            state_fields: self.cached_state_fields.clone(),
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

/// Owned copies of the arguments that build an inner row accumulator, which a
/// `GroupsAccumulatorAdapter` does for each new group, after `AccumulatorArgs` has gone.
struct InnerAccumulatorFactory {
    udf: AggregateUDF,
    return_field: FieldRef,
    schema: Schema,
    ignore_nulls: bool,
    order_bys: Vec<PhysicalSortExpr>,
    is_reversed: bool,
    name: String,
    is_distinct: bool,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    expr_fields: Vec<FieldRef>,
}

impl InnerAccumulatorFactory {
    fn new(udf: &AggregateUDF, args: &AccumulatorArgs) -> Self {
        Self {
            udf: udf.clone(),
            return_field: Arc::clone(&args.return_field),
            schema: args.schema.clone(),
            ignore_nulls: args.ignore_nulls,
            order_bys: args.order_bys.to_vec(),
            is_reversed: args.is_reversed,
            name: args.name.to_string(),
            is_distinct: args.is_distinct,
            exprs: args.exprs.to_vec(),
            expr_fields: args.expr_fields.to_vec(),
        }
    }

    fn create(&self) -> Result<Box<dyn Accumulator>> {
        self.udf.accumulator(AccumulatorArgs {
            return_field: Arc::clone(&self.return_field),
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            order_bys: &self.order_bys,
            is_reversed: self.is_reversed,
            name: &self.name,
            is_distinct: self.is_distinct,
            exprs: &self.exprs,
            expr_fields: &self.expr_fields,
        })
    }
}

/// Projects the output of a Single-mode `aggregate` whose aggregates are all [`MergeAsPartialUDF`]s
/// into the output of a Partial-mode one: its `num_grouping` grouping columns, then one column per
/// state field, unpacked from the struct each aggregate evaluates to.
pub(crate) fn unpack_state_structs(
    aggregate: Arc<dyn ExecutionPlan>,
    num_grouping: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = aggregate.schema();
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
        if idx < num_grouping {
            exprs.push((column, field.name().clone()));
            continue;
        }
        let DataType::Struct(state_fields) = field.data_type() else {
            return internal_err!(
                "Expected a struct of aggregate state in column {idx}, got {}",
                field.data_type()
            );
        };
        for (ordinal, state_field) in state_fields.iter().enumerate() {
            exprs.push((
                Arc::new(GetStructField::new(Arc::clone(&column), ordinal)),
                state_field.name().clone(),
            ));
        }
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, aggregate)?))
}

/// Accumulator wrapper that redirects update_batch to merge_batch and evaluates to its state.
struct MergeAsPartialAccumulator {
    inner: Box<dyn Accumulator>,
    state_decoder: PartialMergeStateDecoder,
    state_fields: Fields,
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
        let state = self
            .inner
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;
        Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
            self.state_fields.clone(),
            state,
            None,
        )?)))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

/// GroupsAccumulator wrapper that redirects update_batch to merge_batch and evaluates to its
/// state.
struct MergeAsPartialGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    state_decoder: PartialMergeStateDecoder,
    state_fields: Fields,
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
        // Redirect update to merge — this is the key trick. Spark's PartialMerge mode
        // never applies a filter (filters apply once, at the Partial stage), so there's
        // nothing to forward into merge_batch, which no longer accepts one.
        debug_assert!(opt_filter.is_none());
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
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        datafusion::common::not_impl_err!("Input batch conversion to state not implemented")
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let state = self.inner.state(emit_to)?;
        Ok(Arc::new(StructArray::try_new(
            self.state_fields.clone(),
            state,
            None,
        )?))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}
