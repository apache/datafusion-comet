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

//! Builder for the native hash aggregate, and the conversion of Spark aggregate expressions into
//! DataFusion aggregate functions.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion::{
    arrow::compute::SortOptions,
    common::ScalarValue,
    functions_aggregate::{
        bit_and_or_xor::{bit_and_udaf, bit_or_udaf, bit_xor_udaf},
        count::count_udaf,
        first_last::{FirstValue, LastValue},
        min_max::{max_udaf, min_udaf},
        sum::sum_udaf,
    },
    logical_expr::AggregateUDF,
    physical_expr::{
        aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
        expressions::{CastExpr, Column, Literal, StatsType},
        LexOrdering, PhysicalExpr, PhysicalSortExpr,
    },
    physical_plan::{
        aggregates::{AggregateMode as DFAggregateMode, PhysicalGroupBy},
        projection::ProjectionExec,
        sorts::sort::SortExec,
        ExecutionPlan, ExecutionPlanProperties,
    },
};
use datafusion_comet_proto::{
    spark_expression::{
        self, agg_expr::ExprStruct as AggExprStruct, expr::ExprStruct, literal::Value, AggExpr,
    },
    spark_operator::{AggregateMode as ProtoAggregateMode, Operator},
};
use datafusion_comet_spark_expr::{
    ApproxPercentile, Avg, AvgDecimal, BloomFilterAgg, CometCollectList, CometCollectSet,
    Correlation, Covariance, HllPlusPlus, HllSketchAgg, HllUnionAgg, MaxMinBy, Mode, Regr,
    RegrType, SparkBloomFilterVersion, SparkListAgg, SparkMinMax, SparkPercentile, SparkPivotFirst,
    Stddev, SumDecimal, SumInteger, Variance,
};
use jni::objects::{Global, JObject};

use super::{
    from_protobuf_eval_mode, operator_registry::OperatorBuilder, PhysicalPlanner,
    PlanCreationResult,
};
use crate::{
    execution::{operators::ExecutionError, serde::to_arrow_datatype, spark_plan::SparkPlan},
    extract_op,
};

use ExecutionError::GeneralError;

// For clippy error on type_complexity.
type PhyAggResult = Result<Vec<AggregateFunctionExpr>, ExecutionError>;
type PhyExprResult = Result<Vec<(Arc<dyn PhysicalExpr>, String)>, ExecutionError>;

/// Builder for `HashAgg`, which plans both Spark's hash and sort aggregates.
pub struct HashAggBuilder;

impl OperatorBuilder for HashAggBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let agg = extract_op!(spark_plan, HashAgg);
        let children = &spark_plan.children;

        assert_eq!(children.len(), 1);
        let (scans, shuffle_scans, child) =
            planner.create_plan(&children[0], inputs, partition_count)?;

        // For `df.agg(sum($"a")).count()`, Spark plans a global aggregate with neither
        // keys nor functions that emits one row with no columns per partition. DataFusion
        // can't run that (apache/datafusion#25786), so plan a throwaway `count(NULL)` and
        // project it away. The count still drains the input, so errors below it surface.
        // Both Spark stages take this path, since no count passes between them.
        if agg.grouping_exprs.is_empty() && agg.agg_exprs.is_empty() {
            let schema = child.schema();
            let count = PhysicalPlanner::create_aggr_func_expr(
                "count",
                Arc::clone(&schema),
                vec![Arc::new(Literal::new(ScalarValue::Null))],
                Arc::unwrap_or_clone(count_udaf()),
            )?;
            let aggregate: Arc<dyn ExecutionPlan> = Arc::new(
                datafusion::physical_plan::aggregates::AggregateExec::try_new(
                    DFAggregateMode::Single,
                    PhysicalGroupBy::default(),
                    vec![Arc::new(count)],
                    vec![None],
                    Arc::clone(&child.native_plan),
                    schema,
                )?,
            );
            let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                Vec::<(Arc<dyn PhysicalExpr>, String)>::new(),
                Arc::clone(&aggregate),
            )?);
            return Ok((
                scans,
                shuffle_scans,
                Arc::new(SparkPlan::new_with_additional(
                    spark_plan.plan_id,
                    projection,
                    vec![child],
                    vec![aggregate],
                )),
            ));
        }

        let group_exprs: PhyExprResult = agg
            .grouping_exprs
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                // A native sort normalizes float keys, so normalize float grouping keys
                // the same way for DataFusion to see that the sort below a sort aggregate
                // orders them. Spark already normalized these values, so this changes
                // none of them.
                if agg.ordered_by_grouping_keys {
                    planner.create_normalized_key_expr(expr, child.schema())
                } else {
                    planner.create_expr(expr, child.schema())
                }
                .map(|r| (r, format!("col_{idx}")))
            })
            .collect();
        let group_by = PhysicalGroupBy::new_single(group_exprs?);
        let schema = child.schema();

        let proto_mode = ProtoAggregateMode::try_from(agg.mode).map_err(|_| {
            ExecutionError::GeneralError(format!("Unsupported aggregate mode: {}", agg.mode))
        })?;
        let mode = match proto_mode {
            ProtoAggregateMode::Partial => DFAggregateMode::Partial,
            ProtoAggregateMode::Final => DFAggregateMode::Final,
            // Spark's distinct-aggregate plan counts the rows of a PartialMerge, so it must
            // emit each key once. DataFusion's Partial emits what it holds when it runs
            // short of memory and starts again, while its Single spills. So run the
            // PartialMerge as Single, whose MergeAsPartial aggregates merge their input and
            // evaluate to their states, and unpack those states below.
            ProtoAggregateMode::PartialMerge => DFAggregateMode::Single,
        };

        // Check if any expression uses PartialMerge mode. When present,
        // those expressions are wrapped with MergeAsPartial to get merge
        // semantics with state output.
        let partial_merge_value = ProtoAggregateMode::PartialMerge as i32;
        let has_partial_merge = proto_mode == ProtoAggregateMode::PartialMerge
            || agg.expr_modes.contains(&partial_merge_value);

        let agg_exprs: PhyAggResult = agg
            .agg_exprs
            .iter()
            .map(|expr| planner.create_agg_expr(expr, Arc::clone(&schema)))
            .collect();

        let aggr_expr: Vec<Arc<AggregateFunctionExpr>> = if has_partial_merge {
            // Wrap PartialMerge expressions with MergeAsPartial.
            // State fields in the child's output start at initial_input_buffer_offset.
            let mut state_offset = agg.initial_input_buffer_offset as usize;
            let per_expr_modes: Vec<i32> = if !agg.expr_modes.is_empty() {
                agg.expr_modes.clone()
            } else {
                vec![agg.mode; agg.agg_exprs.len()]
            };

            agg_exprs?
                .into_iter()
                .enumerate()
                .map(|(idx, expr)| {
                    if per_expr_modes[idx] == partial_merge_value {
                        // PartialMerge: wrap with MergeAsPartial
                        let state_fields = expr
                            .state_fields()
                            .map_err(|e| ExecutionError::GeneralError(e.to_string()))?;
                        let num_state_fields = state_fields.len();

                        let state_cols: Vec<Arc<dyn PhysicalExpr>> = (0..num_state_fields)
                            .map(|i| {
                                let col_idx = state_offset + i;
                                let field = schema.field(col_idx);
                                Arc::new(Column::new(field.name(), col_idx))
                                    as Arc<dyn PhysicalExpr>
                            })
                            .collect();
                        state_offset += num_state_fields;

                        let merge_udf =
                            crate::execution::merge_as_partial::MergeAsPartialUDF::new(&expr)
                                .map_err(|e| ExecutionError::DataFusionError(e.to_string()))?;
                        let merge_udf_arc = Arc::new(
                            datafusion::logical_expr::AggregateUDF::new_from_impl(merge_udf),
                        );

                        let merge_expr = AggregateExprBuilder::new(merge_udf_arc, state_cols)
                            .schema(Arc::clone(&schema))
                            .alias(format!("col_{idx}"))
                            .with_ignore_nulls(expr.ignore_nulls())
                            .with_distinct(expr.is_distinct())
                            .build()
                            .map_err(|e| ExecutionError::DataFusionError(e.to_string()))?;

                        Ok(Arc::new(merge_expr))
                    } else {
                        Ok(Arc::new(expr))
                    }
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?
        } else {
            agg_exprs?.into_iter().map(Arc::new).collect()
        };

        // Build per-aggregate filter expressions from the FILTER (WHERE ...) clause.
        // Filters are only present in Partial mode; Final/PartialMerge always get None.
        let filter_exprs: Result<Vec<Option<Arc<dyn PhysicalExpr>>>, ExecutionError> = agg
            .agg_exprs
            .iter()
            .map(|expr| {
                if let Some(f) = expr.filter.as_ref() {
                    planner.create_expr(f, Arc::clone(&schema)).map(Some)
                } else {
                    Ok(None)
                }
            })
            .collect();

        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::aggregates::AggregateExec::try_new(
                mode,
                group_by,
                aggr_expr,
                filter_exprs?,
                Arc::clone(&child.native_plan),
                Arc::clone(&schema),
            )?,
        );

        let mut output = aggregate;
        let mut additional_native_plans = vec![];
        if proto_mode == ProtoAggregateMode::PartialMerge {
            additional_native_plans.push(Arc::clone(&output));
            output = crate::execution::merge_as_partial::unpack_state_structs(
                output,
                agg.grouping_exprs.len(),
            )?;
        }

        // Spark's SortAggregateExec reports its output as ordered by the grouping keys,
        // and Spark may have removed a sort above it on that basis. DataFusion emits
        // groups in input order only when it sees the input sorted on them. When that
        // ordering comes from outside this native plan, for example from a cached sorted
        // relation that reaches native code as an unordered ScanExec, the hash table can
        // emit groups out of order, so sort the aggregate output instead.
        if agg.ordered_by_grouping_keys && !agg.grouping_exprs.is_empty() {
            let output_schema = output.schema();
            let ordering: Vec<PhysicalSortExpr> = (0..agg.grouping_exprs.len())
                .map(|idx| {
                    PhysicalSortExpr::new(
                        Arc::new(Column::new(output_schema.field(idx).name(), idx)),
                        SortOptions {
                            descending: false,
                            nulls_first: true,
                        },
                    )
                })
                .collect();
            if !output
                .equivalence_properties()
                .ordering_satisfy(ordering.clone())?
            {
                additional_native_plans.push(Arc::clone(&output));
                output = Arc::new(SortExec::new(LexOrdering::new(ordering).unwrap(), output));
            }
        }

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new_with_additional(
                spark_plan.plan_id,
                output,
                vec![child],
                additional_native_plans,
            )),
        ))
    }
}

impl PhysicalPlanner {
    /// Create a DataFusion physical aggregate expression from Spark physical aggregate expression
    fn create_agg_expr(
        &self,
        spark_expr: &AggExpr,
        schema: SchemaRef,
    ) -> Result<AggregateFunctionExpr, ExecutionError> {
        self.register_query_context(spark_expr.expr_id, spark_expr.query_context.as_ref());

        match spark_expr.expr_struct.as_ref().unwrap() {
            AggExprStruct::Count(expr) => {
                assert!(!expr.children.is_empty());
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                AggregateExprBuilder::new(count_udaf(), children)
                    .schema(schema)
                    .alias("count")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Min(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let func = min_max_udaf(&datatype, false);
                let child = Arc::new(CastExpr::new(child, datatype.clone(), None));

                AggregateExprBuilder::new(func, vec![child])
                    .schema(schema)
                    .alias("min")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Max(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let func = min_max_udaf(&datatype, true);
                let child = Arc::new(CastExpr::new(child, datatype.clone(), None));

                AggregateExprBuilder::new(func, vec![child])
                    .schema(schema)
                    .alias("max")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Sum(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());

                let builder = match datatype {
                    DataType::Decimal128(_, _) => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = AggregateUDF::new_from_impl(SumDecimal::try_new(
                            datatype,
                            eval_mode,
                            spark_expr.expr_id,
                            Arc::clone(&self.query_context_registry),
                        )?);
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func =
                            AggregateUDF::new_from_impl(SumInteger::try_new(datatype, eval_mode)?);
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    _ => {
                        // cast to the result data type of SUM if necessary, we should not expect
                        // a cast failure since it should have already been checked at Spark side
                        let child =
                            Arc::new(CastExpr::new(Arc::clone(&child), datatype.clone(), None));
                        AggregateExprBuilder::new(sum_udaf(), vec![child])
                    }
                };
                builder
                    .schema(schema)
                    .alias("sum")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Avg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let input_datatype = to_arrow_datatype(expr.sum_datatype.as_ref().unwrap());

                let builder = match datatype {
                    DataType::Decimal128(_, _) => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = AggregateUDF::new_from_impl(AvgDecimal::new(
                            datatype,
                            input_datatype,
                            eval_mode,
                            spark_expr.expr_id,
                            Arc::clone(&self.query_context_registry),
                        ));
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    _ => {
                        // For all other numeric types (Int8/16/32/64, Float32/64):
                        // Cast to Float64 for accumulation
                        let child: Arc<dyn PhysicalExpr> =
                            Arc::new(CastExpr::new(Arc::clone(&child), DataType::Float64, None));
                        let func = AggregateUDF::new_from_impl(Avg::new("avg", DataType::Float64));
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                };
                builder
                    .schema(schema)
                    .alias("avg")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::First(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(FirstValue::new());

                AggregateExprBuilder::new(Arc::new(func), vec![child])
                    .schema(schema)
                    .alias("first")
                    .with_ignore_nulls(expr.ignore_nulls)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Last(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(LastValue::new());

                AggregateExprBuilder::new(Arc::new(func), vec![child])
                    .schema(schema)
                    .alias("last")
                    .with_ignore_nulls(expr.ignore_nulls)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitAndAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_and_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_and")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitOrAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_or_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_or")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitXorAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_xor_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_xor")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Covariance(expr) => {
                let child1 =
                    self.create_expr(expr.child1.as_ref().unwrap(), Arc::clone(&schema))?;
                let child2 =
                    self.create_expr(expr.child2.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Covariance::new(
                            "covariance",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr(
                            "covariance",
                            schema,
                            vec![child1, child2],
                            func,
                        )
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Covariance::new(
                            "covariance_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr(
                            "covariance_pop",
                            schema,
                            vec![child1, child2],
                            func,
                        )
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for Variance"
                    ))),
                }
            }
            AggExprStruct::Variance(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Variance::new(
                            "variance",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("variance", schema, vec![child], func)
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Variance::new(
                            "variance_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("variance_pop", schema, vec![child], func)
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for Variance"
                    ))),
                }
            }
            AggExprStruct::Stddev(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Stddev::new(
                            "stddev",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("stddev", schema, vec![child], func)
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Stddev::new(
                            "stddev_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("stddev_pop", schema, vec![child], func)
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for stddev"
                    ))),
                }
            }
            AggExprStruct::Correlation(expr) => {
                let child1 =
                    self.create_expr(expr.child1.as_ref().unwrap(), Arc::clone(&schema))?;
                let child2 =
                    self.create_expr(expr.child2.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let func = AggregateUDF::new_from_impl(Correlation::new(
                    "correlation",
                    datatype,
                    expr.null_on_divide_by_zero,
                    from_protobuf_eval_mode(expr.eval_mode)?,
                    spark_expr.expr_id,
                    Arc::clone(&self.query_context_registry),
                ));
                Self::create_aggr_func_expr("correlation", schema, vec![child1, child2], func)
            }
            AggExprStruct::Regr(expr) => {
                let child1 =
                    self.create_expr(expr.child1.as_ref().unwrap(), Arc::clone(&schema))?;
                let child2 =
                    self.create_expr(expr.child2.as_ref().unwrap(), Arc::clone(&schema))?;
                let (regr_type, name) = match expr.regr_type() {
                    spark_expression::regr::RegrType::Slope => (RegrType::Slope, "regr_slope"),
                    spark_expression::regr::RegrType::Intercept => {
                        (RegrType::Intercept, "regr_intercept")
                    }
                    spark_expression::regr::RegrType::R2 => (RegrType::R2, "regr_r2"),
                    spark_expression::regr::RegrType::Sxx => (RegrType::SXX, "regr_sxx"),
                    spark_expression::regr::RegrType::Syy => (RegrType::SYY, "regr_syy"),
                    spark_expression::regr::RegrType::Sxy => (RegrType::SXY, "regr_sxy"),
                };
                let func = AggregateUDF::new_from_impl(Regr::new(
                    regr_type,
                    name,
                    expr.filter_var_by_pair_nulls,
                    expr.r2_constant_dependent_is_perfect_fit,
                    from_protobuf_eval_mode(expr.eval_mode)?,
                    spark_expr.expr_id,
                    Arc::clone(&self.query_context_registry),
                ));
                Self::create_aggr_func_expr(name, schema, vec![child1, child2], func)
            }
            AggExprStruct::Percentile(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let percentile =
                    self.create_expr(expr.percentage.as_ref().unwrap(), Arc::clone(&schema))?;
                // Spark's exact Percentile uses full-precision linear interpolation. Comet uses
                // its own UDAF rather than DataFusion's percentile_cont because DataFusion
                // quantizes the interpolation weight.
                let percentile_value = percentile_value(expr.percentage.as_ref().unwrap())?;
                let func = AggregateUDF::new_from_impl(SparkPercentile::try_new(percentile_value)?);
                AggregateExprBuilder::new(func.into(), vec![child, percentile])
                    .schema(schema)
                    .alias("percentile")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::ApproxPercentile(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let input_type = to_arrow_datatype(expr.input_type.as_ref().unwrap());
                let func = AggregateUDF::new_from_impl(ApproxPercentile::new(
                    expr.percentiles.clone(),
                    expr.accuracy,
                    input_type,
                    expr.return_array,
                ));
                Self::create_aggr_func_expr("approx_percentile", schema, vec![child], func)
            }
            AggExprStruct::BloomFilterAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let num_items =
                    self.create_expr(expr.num_items.as_ref().unwrap(), Arc::clone(&schema))?;
                let num_bits =
                    self.create_expr(expr.num_bits.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let version = match expr.version() {
                    spark_expression::BloomFilterVersion::V2 => SparkBloomFilterVersion::V2,
                    // Default (Unspecified or V1) preserves the pre-Spark-4.1 format that
                    // Comet has always emitted, keeping older Spark versions byte-equivalent.
                    _ => SparkBloomFilterVersion::V1,
                };
                let func = AggregateUDF::new_from_impl(BloomFilterAgg::new(
                    Arc::clone(&num_items),
                    Arc::clone(&num_bits),
                    datatype,
                    version,
                ));
                Self::create_aggr_func_expr("bloom_filter_agg", schema, vec![child], func)
            }
            AggExprStruct::HllSketchAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(HllSketchAgg::new(expr.lg_config_k));
                Self::create_aggr_func_expr("hll_sketch_agg", schema, vec![child], func)
            }
            AggExprStruct::CollectSet(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let child = Self::coerce_collect_child_nullability(child, &schema)?;
                let func = AggregateUDF::new_from_impl(CometCollectSet::new());
                Self::create_aggr_func_expr("collect_set", schema, vec![child], func)
            }
            AggExprStruct::CollectList(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let child = Self::coerce_collect_child_nullability(child, &schema)?;
                let func = AggregateUDF::new_from_impl(CometCollectList::new());
                Self::create_aggr_func_expr("collect_list", schema, vec![child], func)
            }
            AggExprStruct::Hllpp(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(HllPlusPlus::new(expr.precision));
                Self::create_aggr_func_expr("approx_count_distinct", schema, vec![child], func)
            }
            AggExprStruct::HllUnionAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func =
                    AggregateUDF::new_from_impl(HllUnionAgg::new(expr.allow_different_lg_config_k));
                Self::create_aggr_func_expr("hll_union_agg", schema, vec![child], func)
            }
            AggExprStruct::ListAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let delimiter =
                    self.create_expr(expr.delimiter.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(SparkListAgg::new());
                Self::create_aggr_func_expr("listagg", schema, vec![child, delimiter], func)
            }
            AggExprStruct::MaxBy(expr) => {
                let value = self.create_expr(expr.value.as_ref().unwrap(), Arc::clone(&schema))?;
                let ordering =
                    self.create_expr(expr.ordering.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(MaxMinBy::new_max_by());
                Self::create_aggr_func_expr("max_by", schema, vec![value, ordering], func)
            }
            AggExprStruct::MinBy(expr) => {
                let value = self.create_expr(expr.value.as_ref().unwrap(), Arc::clone(&schema))?;
                let ordering =
                    self.create_expr(expr.ordering.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(MaxMinBy::new_min_by());
                Self::create_aggr_func_expr("min_by", schema, vec![value, ordering], func)
            }
            AggExprStruct::Mode(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let func =
                    AggregateUDF::new_from_impl(Mode::new(datatype, expr.normalize_neg_zero));
                Self::create_aggr_func_expr("mode", schema, vec![child], func)
            }
            AggExprStruct::PivotFirst(expr) => {
                let pivot_col =
                    self.create_expr(expr.pivot_column.as_ref().unwrap(), Arc::clone(&schema))?;
                let value_col =
                    self.create_expr(expr.value_column.as_ref().unwrap(), Arc::clone(&schema))?;
                let value_type = to_arrow_datatype(expr.value_datatype.as_ref().unwrap());
                // Reconstruct pivot values as ScalarValues from the serialized Literal exprs.
                // The Scala serde builds them as Literal(v, pivot_column.dataType), so they carry
                // the pivot column's type, which `SparkPivotFirst` builds its lookup index from.
                let mut pivot_values = Vec::with_capacity(expr.pivot_values.len());
                for lit_expr in &expr.pivot_values {
                    let physical = self.create_expr(lit_expr, Arc::clone(&schema))?;
                    let literal = physical.downcast_ref::<Literal>().ok_or_else(|| {
                        GeneralError(
                            "PivotFirst pivot_values must be Literal expressions".to_string(),
                        )
                    })?;
                    pivot_values.push(literal.value().clone());
                }
                let func = AggregateUDF::new_from_impl(SparkPivotFirst::try_new(
                    value_type,
                    pivot_values,
                )?);
                Self::create_aggr_func_expr("pivot_first", schema, vec![pivot_col, value_col], func)
            }
        }
    }

    /// Normalizes a `collect_list` / `collect_set` argument to the all-nullable variant of its
    /// declared type.
    ///
    /// `SparkCollectList::return_type` / `state_fields` (and `SparkCollectSet`) derive the list
    /// element type from the argument's *declared* type, while the accumulator builds its list
    /// from the argument arrays it actually received: `ScalarValue::new_list` honors its
    /// `data_type` parameter only when the value set is empty, and otherwise just concatenates
    /// the collected scalars. Nothing forces those two to agree, so if an expression's
    /// `evaluate()` output type differs from its `data_type()` in nested field nullability, the
    /// grouped `AggregateExec` fails validating its own output batch ("column types must match
    /// schema types") — `RecordBatch::try_new` compares with `DataType::equals_datatype`, which
    /// does compare nested nullability.
    ///
    /// The grouped path normalizes its own inputs, so this only still matters for the ungrouped
    /// `Accumulator` (a global aggregate, or a window frame).
    ///
    /// Casting unconditionally (rather than only when the declared type has a non-nullable
    /// nested field) makes this a normalization barrier: the accumulator is guaranteed to see
    /// arrays of exactly the type the plan declared, whichever direction the drift goes. The
    /// cast is metadata-only — `cast_struct_to_struct` and `cast_list_values` stamp the target
    /// fields and clone the child buffers — so it does not copy data.
    pub(super) fn coerce_collect_child_nullability(
        child: Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let child_type = child.data_type(schema.as_ref())?;
        if !child_type.is_nested() {
            // Only nested types carry field-level nullability that can drift.
            return Ok(child);
        }
        let nullable_type = make_all_fields_nullable(&child_type);
        Ok(Arc::new(CastExpr::new(child, nullable_type, None)))
    }

    pub(super) fn create_aggr_func_expr(
        name: &str,
        schema: SchemaRef,
        children: Vec<Arc<dyn PhysicalExpr>>,
        func: AggregateUDF,
    ) -> Result<AggregateFunctionExpr, ExecutionError> {
        AggregateExprBuilder::new(Arc::new(func), children)
            .schema(schema)
            .alias(name)
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()
            .map_err(|e| e.into())
    }
}

/// Return a copy of `data_type` with every nested field marked nullable. Map key fields are left
/// non-nullable to preserve Arrow's map invariant. Primitive types are returned unchanged.
fn make_all_fields_nullable(data_type: &DataType) -> DataType {
    fn nullable_field(field: &Field, nullable: bool) -> FieldRef {
        Arc::new(
            Field::new(
                field.name(),
                make_all_fields_nullable(field.data_type()),
                nullable,
            )
            .with_metadata(field.metadata().clone()),
        )
    }
    match data_type {
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|f| nullable_field(f, true)).collect())
        }
        DataType::List(field) => DataType::List(nullable_field(field, true)),
        DataType::LargeList(field) => DataType::LargeList(nullable_field(field, true)),
        DataType::FixedSizeList(field, len) => {
            DataType::FixedSizeList(nullable_field(field, true), *len)
        }
        DataType::Map(entries, sorted) => {
            // Map entries are a non-nullable struct of {key (non-null), value}. Recurse into the
            // key/value types but keep the key field non-nullable per Arrow's map invariant.
            match entries.data_type() {
                DataType::Struct(kv) => {
                    let new_kv = kv
                        .iter()
                        .enumerate()
                        .map(|(i, f)| nullable_field(f, i != 0))
                        .collect();
                    let new_entries = Field::new(
                        entries.name(),
                        DataType::Struct(new_kv),
                        entries.is_nullable(),
                    )
                    .with_metadata(entries.metadata().clone());
                    DataType::Map(Arc::new(new_entries), *sorted)
                }
                _ => data_type.clone(),
            }
        }
        other => other.clone(),
    }
}

/// `min` or `max` over `data_type`: Spark's version for floats, which DataFusion orders
/// differently, and DataFusion's for every other type.
pub(super) fn min_max_udaf(data_type: &DataType, is_max: bool) -> Arc<AggregateUDF> {
    // Spark has only 32- and 64-bit floating-point types. Keep Float16 on DataFusion's path so
    // an internal plan using it does not reach SparkMinMax, whose accumulators reject that type.
    if matches!(data_type, DataType::Float32 | DataType::Float64) {
        Arc::new(AggregateUDF::new_from_impl(SparkMinMax::new(is_max)))
    } else if is_max {
        max_udaf()
    } else {
        min_udaf()
    }
}

fn percentile_value(expr: &spark_expression::Expr) -> Result<f64, ExecutionError> {
    match &expr.expr_struct {
        Some(ExprStruct::Literal(literal)) if !literal.is_null => match &literal.value {
            Some(Value::DoubleVal(value)) => Ok(*value),
            Some(Value::FloatVal(value)) => Ok(*value as f64),
            _ => Err(GeneralError(
                "Percentile value must be a floating-point literal".to_string(),
            )),
        },
        _ => Err(GeneralError(
            "Percentile value must be a non-null literal".to_string(),
        )),
    }
}
