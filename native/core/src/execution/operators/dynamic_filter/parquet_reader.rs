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

//! Attach a live predicate to a native Parquet reader while preserving input boundaries.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, IsNotNullExpr,
};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;

use super::super::CometFilterExec;

mod schema_adapter;

use schema_adapter::RuntimeFilterSchemaAdapterFactory;

/// Recognize only direct-column null checks joined by AND, without evaluating
/// or changing the predicate. Every accepted leaf is deterministic, infallible,
/// and only discards rows, so reader pruning cannot suppress expression errors
/// or alter stateful evaluation. All other expressions remain a boundary.
fn is_direct_column_null_checks(predicate: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(binary) = predicate.downcast_ref::<BinaryExpr>() {
        return binary.op() == &Operator::And
            && is_direct_column_null_checks(binary.left())
            && is_direct_column_null_checks(binary.right());
    }
    predicate
        .downcast_ref::<IsNotNullExpr>()
        .is_some_and(|is_not_null| is_not_null.arg().is::<Column>())
}

pub(super) fn try_attach_parquet_reader_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Filtering before a fetch can change which rows are selected by its limit.
    if input.fetch().is_some() {
        log::debug!("Join dynamic filter reader pushdown skipped: probe has a fetch limit");
        return Ok(None);
    }
    // Spark inserts IS NOT NULL residuals above equijoin inputs, including AND
    // chains of inferred null checks. A reader predicate can cross those direct
    // checks because both operations only discard rows. Keep every other filter
    // as a boundary: reader pruning would change which rows reach stateful
    // expressions and can suppress expression errors.
    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        if filter.has_projection() {
            log::debug!(
                "Join dynamic filter reader pushdown skipped: probe FilterExec has a projection"
            );
            return Ok(None);
        }
        if !is_direct_column_null_checks(filter.predicate()) {
            log::debug!(
                "Join dynamic filter reader pushdown skipped: probe filter is not direct column IS NOT NULL checks"
            );
            return Ok(None);
        }
        let Some(reader) =
            try_attach_parquet_reader_filter(filter.input(), Arc::clone(&predicate), config)?
        else {
            return Ok(None);
        };
        return match filter.with_execution_input(reader) {
            Ok(updated) => Ok(Some(updated)),
            Err(error) => {
                log::debug!(
                    "Join dynamic filter reader pushdown skipped: probe filter rebuild failed: {error}"
                );
                Ok(None)
            }
        };
    }
    let Some(scan) = input.downcast_ref::<DataSourceExec>() else {
        log::debug!(
            "Join dynamic filter reader pushdown skipped: probe root is {}",
            input.name()
        );
        return Ok(None);
    };
    let Some((file_config, source)) = scan.downcast_to_file_source::<ParquetSource>() else {
        log::debug!("Join dynamic filter reader pushdown skipped: probe is not Parquet");
        return Ok(None);
    };
    // File statistics can discard a file before its schema adapter is created.
    // Retain the decoded-batch filter when per-file checks cannot guard pruning.
    if file_config
        .file_groups
        .iter()
        .flat_map(|group| group.files())
        .any(|file| file.statistics.is_some())
    {
        log::debug!("Join dynamic filter reader pushdown skipped: supplied file statistics");
        return Ok(None);
    }
    let Some(adapter_factory) = &file_config.expr_adapter_factory else {
        log::debug!("Join dynamic filter reader pushdown skipped: missing schema adapter factory");
        return Ok(None);
    };
    let table_schema = source.table_schema();
    let file_column_count = table_schema.file_schema().fields().len();
    let projection = source
        .projection()
        .map(|projection| projection.column_indices())
        .unwrap_or_else(|| (0..file_column_count).collect());
    let mut read_columns = Vec::new();
    for index in projection {
        let Some(field) = table_schema.table_schema().fields().get(index) else {
            log::debug!(
                "Join dynamic filter reader pushdown skipped: projected column index {index} is outside the table schema"
            );
            return Ok(None);
        };
        // Partition columns become literals before the per-file adapter is created.
        if index < file_column_count {
            read_columns.push(Column::new(field.name(), index));
        }
    }
    if let Some(filter) = source.filter() {
        // Adapters resolve predicate columns by name and can repair stale indices.
        // Keep that identity, including when excluding partition-column literals.
        read_columns.extend(collect_columns(&filter).into_iter().filter(|column| {
            !table_schema
                .table_partition_cols()
                .iter()
                .any(|field| field.name() == column.name())
        }));
    }
    read_columns.sort_unstable_by(|a, b| {
        a.index()
            .cmp(&b.index())
            .then_with(|| a.name().cmp(b.name()))
    });
    read_columns.dedup();
    let adapter_factory = Arc::new(RuntimeFilterSchemaAdapterFactory::new(
        Arc::clone(adapter_factory),
        read_columns,
    ));

    let predicate: Arc<dyn PhysicalExpr> = predicate;
    let propagation = match scan
        .data_source()
        .try_pushdown_filters(vec![predicate], config)
    {
        Ok(propagation) => propagation,
        Err(error) => {
            log::debug!(
                "Join dynamic filter reader pushdown skipped: predicate remapping failed: {error}"
            );
            return Ok(None);
        }
    };
    let Some(data_source) = propagation.updated_node else {
        log::debug!("Join dynamic filter reader pushdown skipped: Parquet declined the predicate");
        return Ok(None);
    };
    let filtered = scan.clone().with_data_source(data_source);
    let Some((file_config, _)) = filtered.downcast_to_file_source::<ParquetSource>() else {
        log::debug!(
            "Join dynamic filter reader pushdown skipped: pushed-down scan is no longer Parquet"
        );
        return Ok(None);
    };
    let mut file_config = file_config.clone();
    file_config.expr_adapter_factory = Some(adapter_factory);
    Ok(Some(Arc::new(
        filtered.with_data_source(Arc::new(file_config)),
    )))
}

#[cfg(test)]
mod tests;
