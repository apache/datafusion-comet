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

use crate::execution::partial_aggregation::PartialAggregationConfig;
use datafusion_comet_proto::spark_expression::{
    agg_expr::ExprStruct, data_type::DataTypeId, AggExpr, EvalMode,
};
use datafusion_comet_proto::spark_operator::{AggregateMode, HashAggregate};

/// State convertibility, SQL semantics and profitability are separate contracts.
/// Conversion is generic; this policy qualifies semantics; DataFusion probes profitability.
pub(super) fn eligibility(
    agg: &HashAggregate,
    config: Option<&PartialAggregationConfig>,
) -> (bool, &'static str) {
    let Some(config) = config else {
        return (false, "no-native-shuffle-policy");
    };
    if !config.native_shuffle {
        return (false, "non-native-shuffle");
    }
    // The Spark producer certifies that this operator need not return unique groups.
    // In particular, a DISTINCT deduplicator feeding a raw Partial must not bypass.
    if !agg.allow_partial_bypass {
        return (false, "disabled-or-required-deduplication");
    }
    if agg.grouping_exprs.is_empty() {
        return (false, "global-aggregate");
    }
    let partial =
        |mode| mode == AggregateMode::Partial as i32 || mode == AggregateMode::PartialMerge as i32;
    if !partial(agg.mode) || !agg.expr_modes.iter().copied().all(partial) {
        return (false, "non-partial-mode");
    }
    for (index, expr) in agg.agg_exprs.iter().enumerate() {
        let mode = agg.expr_modes.get(index).copied().unwrap_or(agg.mode);
        if mode == AggregateMode::PartialMerge as i32 && expr.filter.is_some() {
            return (false, "filtered-state-merge");
        }
        if let Some(reason) = restriction(expr, config.allow_numerical_differences) {
            return (false, reason);
        }
    }
    (true, "mergeable-states")
}

fn restriction(expr: &AggExpr, allow_numerical_differences: bool) -> Option<&'static str> {
    let numeric = || (!allow_numerical_differences).then_some("numerical-differences-disabled");
    let Some(expr) = expr.expr_struct.as_ref() else {
        return Some("missing-aggregate-expression");
    };
    match expr {
        ExprStruct::Count(_)
        | ExprStruct::Min(_)
        | ExprStruct::Max(_)
        | ExprStruct::BitAndAgg(_)
        | ExprStruct::BitOrAgg(_)
        | ExprStruct::BitXorAgg(_)
        | ExprStruct::Percentile(_)
        | ExprStruct::CollectSet(_) => None,
        ExprStruct::Sum(sum) => {
            match sum
                .datatype
                .as_ref()
                .and_then(|t| DataTypeId::try_from(t.type_id).ok())
            {
                Some(
                    DataTypeId::Int8 | DataTypeId::Int16 | DataTypeId::Int32 | DataTypeId::Int64,
                ) if sum.eval_mode == EvalMode::Legacy as i32 => None,
                Some(
                    DataTypeId::Int8
                    | DataTypeId::Int16
                    | DataTypeId::Int32
                    | DataTypeId::Int64
                    | DataTypeId::Float
                    | DataTypeId::Double
                    | DataTypeId::Decimal,
                ) => numeric(),
                _ => Some("unsupported-sum-type"),
            }
        }
        ExprStruct::Avg(_)
        | ExprStruct::Covariance(_)
        | ExprStruct::Variance(_)
        | ExprStruct::Stddev(_)
        | ExprStruct::Correlation(_) => numeric(),
        ExprStruct::First(_)
        | ExprStruct::Last(_)
        | ExprStruct::CollectList(_)
        | ExprStruct::MaxBy(_)
        | ExprStruct::MinBy(_)
        | ExprStruct::Mode(_)
        | ExprStruct::ListAgg(_) => Some("order-or-tie-sensitive-state"),
        ExprStruct::BloomFilterAgg(_)
        | ExprStruct::Hllpp(_)
        | ExprStruct::HllSketchAgg(_)
        | ExprStruct::HllUnionAgg(_) => Some("large-singleton-state"),
        ExprStruct::Regr(_) => Some("unsupported-aggregate"),
        ExprStruct::ApproxPercentile(_) => Some("approximation-sensitive-state"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_comet_proto::spark_expression::{Avg, Count, DataType, Expr, Sum};

    fn config() -> PartialAggregationConfig {
        PartialAggregationConfig {
            probe_ratio_threshold: 0.8,
            native_shuffle: true,
            allow_numerical_differences: false,
        }
    }

    fn partial() -> HashAggregate {
        HashAggregate {
            grouping_exprs: vec![Expr::default()],
            agg_exprs: vec![AggExpr {
                expr_struct: Some(ExprStruct::Count(Count {
                    children: vec![Expr::default(), Expr::default()],
                })),
                ..Default::default()
            }],
            mode: AggregateMode::Partial as i32,
            allow_partial_bypass: true,
            ..Default::default()
        }
    }

    #[test]
    fn unqualified_aggregate_families_remain_disabled_with_numerical_opt_in() {
        for allow_numerical_differences in [false, true] {
            let policy = PartialAggregationConfig {
                allow_numerical_differences,
                ..config()
            };
            for expr in [
                None,
                Some(ExprStruct::Regr(Default::default())),
                Some(ExprStruct::ListAgg(Default::default())),
                Some(ExprStruct::HllSketchAgg(Default::default())),
                Some(ExprStruct::HllUnionAgg(Default::default())),
            ] {
                let mut agg = partial();
                agg.agg_exprs[0].expr_struct = expr;
                assert!(!eligibility(&agg, Some(&policy)).0);
            }
        }
    }

    #[test]
    fn eligibility_requires_both_boundary_and_operator_contracts() {
        let policy = config();
        assert!(eligibility(&partial(), Some(&policy)).0);
        assert!(!eligibility(&partial(), None).0);
        assert!(
            !eligibility(
                &partial(),
                Some(&PartialAggregationConfig {
                    native_shuffle: false,
                    ..config()
                }),
            )
            .0
        );
        for agg in [
            HashAggregate {
                allow_partial_bypass: false,
                ..partial()
            },
            HashAggregate {
                grouping_exprs: vec![],
                ..partial()
            },
            HashAggregate {
                mode: AggregateMode::Final as i32,
                ..partial()
            },
            HashAggregate {
                expr_modes: vec![AggregateMode::Final as i32],
                ..partial()
            },
        ] {
            assert!(!eligibility(&agg, Some(&policy)).0, "{agg:?}");
        }
    }

    #[test]
    fn grouping_only_and_qualified_state_merges_can_bypass() {
        let policy = config();
        for agg in [
            HashAggregate {
                agg_exprs: vec![],
                ..partial()
            },
            HashAggregate {
                mode: AggregateMode::PartialMerge as i32,
                ..partial()
            },
            HashAggregate {
                agg_exprs: vec![partial().agg_exprs[0].clone(); 2],
                expr_modes: vec![
                    AggregateMode::Partial as i32,
                    AggregateMode::PartialMerge as i32,
                ],
                ..partial()
            },
        ] {
            assert!(eligibility(&agg, Some(&policy)).0, "{agg:?}");
        }
        let mut filtered_merge = partial();
        filtered_merge.mode = AggregateMode::PartialMerge as i32;
        filtered_merge.agg_exprs[0].filter = Some(Expr::default());
        assert_eq!(
            eligibility(&filtered_merge, Some(&policy)).1,
            "filtered-state-merge"
        );
    }

    #[test]
    fn numerical_differences_require_an_explicit_independent_policy() {
        for (datatype, eval_mode, strict_eligible) in [
            (DataTypeId::Int64, EvalMode::Legacy, true),
            (DataTypeId::Int64, EvalMode::Ansi, false),
            (DataTypeId::Int64, EvalMode::Try, false),
            (DataTypeId::Decimal, EvalMode::Legacy, false),
            (DataTypeId::Decimal, EvalMode::Ansi, false),
            (DataTypeId::Double, EvalMode::Legacy, false),
        ] {
            let agg = HashAggregate {
                agg_exprs: vec![AggExpr {
                    expr_struct: Some(ExprStruct::Sum(Sum {
                        datatype: Some(DataType {
                            type_id: datatype as i32,
                            ..Default::default()
                        }),
                        eval_mode: eval_mode as i32,
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..partial()
            };
            assert_eq!(eligibility(&agg, Some(&config())).0, strict_eligible);
            assert!(
                eligibility(
                    &agg,
                    Some(&PartialAggregationConfig {
                        allow_numerical_differences: true,
                        ..config()
                    })
                )
                .0
            );
        }
        let mixed = HashAggregate {
            agg_exprs: vec![
                partial().agg_exprs[0].clone(),
                AggExpr {
                    expr_struct: Some(ExprStruct::Avg(Avg::default())),
                    ..Default::default()
                },
            ],
            ..partial()
        };
        assert!(!eligibility(&mixed, Some(&config())).0);
        assert!(
            eligibility(
                &mixed,
                Some(&PartialAggregationConfig {
                    allow_numerical_differences: true,
                    ..config()
                })
            )
            .0
        );
        // A rejected operator does not alter any policy used by its siblings or children.
        assert!(eligibility(&partial(), Some(&config())).0);
    }
}
