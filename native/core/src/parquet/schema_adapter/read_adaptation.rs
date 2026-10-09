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

//! Prove when skipping a file's read adaptation cannot hide a conversion error.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::nested_struct::validate_data_type_compatibility;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::Cast;

/// Whether an already rewritten read-column expression cannot fail on a valid decoded batch.
///
/// Call this only after the file's ordinary schema-adapter rewrite. That rewrite owns Spark's
/// name/field-ID resolution, missing defaults, and conversion rules. In particular, a disallowed
/// INT32-to-BIGINT promotion becomes `RejectOnNonEmpty`, not a Spark `Cast`.
/// The reader must still bind column indices to its projected batch schema before evaluation.
///
/// This proof covers errors introduced by adaptation, not arbitrary Parquet decoding failures.
/// Unknown expressions remain unsafe. A supported conversion is not necessarily infallible:
/// timestamp scaling can overflow, and even equal Variant storage types require normalization.
pub(crate) fn is_infallible_read_adaptation(
    expr: &Arc<dyn PhysicalExpr>,
    physical_schema: &Schema,
) -> bool {
    if expr.is::<Literal>() || resolved_column_field(expr, physical_schema).is_some() {
        return true;
    }

    if let Some(cast) = expr.downcast_ref::<Cast>() {
        return cast.cast_options.is_adapting_schema
            && cast.data_type == DataType::Int64
            && resolved_column_field(&cast.child, physical_schema).is_some_and(|field| {
                field.extension_type_name().is_none() && field.data_type() == &DataType::Int32
            });
    }

    let Some(cast) = expr.downcast_ref::<CastExpr>() else {
        return false;
    };
    let Some(source) = resolved_column_field(cast.expr(), physical_schema) else {
        return false;
    };
    let target = cast.target_field();
    // Only retained DataFusion structural casts have a proof here. Keep their expression shape
    // unchanged so Parquet can still clip unrequested leaves. Do not generalize this to opaque
    // Comet casts, which can decode additional fields and perform fallible value conversions.
    matches!(source.data_type(), DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_))
        && source.extension_type_name().is_none()
        && target.extension_type_name().is_none()
        && source.metadata() == target.metadata()
        && is_structural_subset(source.data_type(), target.data_type())
        // DataFusion validates nested nullability and field overlap at evaluation time. A
        // failed check is an unsafe verdict, never an eager error from this optimization.
        && validate_data_type_compatibility("", source.data_type(), target.data_type()).is_ok()
}

fn resolved_column_field<'a>(
    expr: &Arc<dyn PhysicalExpr>,
    physical_schema: &'a Schema,
) -> Option<&'a Field> {
    let column = expr.downcast_ref::<Column>()?;
    // Input indices may belong to an earlier projection. The adapter restores physical names,
    // and DataFusion reassigns indices when it builds the file's projected batch schema.
    physical_schema.field_with_name(column.name()).ok()
}

fn is_structural_subset(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            if target_fields.is_empty() {
                return false;
            }
            let source_names: HashSet<_> = source_fields.iter().map(|f| f.name()).collect();
            let target_names: HashSet<_> = target_fields.iter().map(|f| f.name()).collect();
            source_names.len() == source_fields.len()
                && target_names.len() == target_fields.len()
                && target_fields.iter().all(|target| {
                    source_fields
                        .iter()
                        .find(|source| source.name() == target.name())
                        .is_some_and(|source| is_structural_subset_field(source, target))
                })
        }
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target)) => {
            is_structural_subset_field(source, target)
        }
        // Containers outside this proof must not slip through an equality shortcut.
        (DataType::Dictionary(_, _), _) | (_, DataType::Dictionary(_, _)) => false,
        _ => !source.is_nested() && source == target,
    }
}

fn is_structural_subset_field(source: &Field, target: &Field) -> bool {
    (!source.is_nullable() || target.is_nullable())
        && source.extension_type_name().is_none()
        && target.extension_type_name().is_none()
        && source.metadata() == target.metadata()
        && is_structural_subset(source.data_type(), target.data_type())
}

#[cfg(test)]
mod tests;
