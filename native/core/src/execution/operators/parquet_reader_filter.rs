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

//! Attach a live dynamic predicate to a native Parquet reader without crossing unsafe operators.

use std::sync::Arc;

use arrow::datatypes::DataType;
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

use super::CometFilterExec;

mod adapter;
mod reader;

use adapter::ReaderFilterAdapterFactory;
use reader::ReaderFilterMetadataFactory;

fn contains_timestamp(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, _) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_timestamp(field.data_type())),
        DataType::List(field) | DataType::Map(field, _) => contains_timestamp(field.data_type()),
        _ => false,
    }
}

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
        log::debug!("Dynamic filter reader pushdown skipped: input has a fetch limit");
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
                "Dynamic filter reader pushdown skipped: input FilterExec has a projection"
            );
            return Ok(None);
        }
        if !is_direct_column_null_checks(filter.predicate()) {
            log::debug!(
                "Dynamic filter reader pushdown skipped: input filter is not direct column IS NOT NULL checks"
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
                    "Dynamic filter reader pushdown skipped: input filter rebuild failed: {error}"
                );
                Ok(None)
            }
        };
    }
    let Some(scan) = input.downcast_ref::<DataSourceExec>() else {
        log::debug!(
            "Dynamic filter reader pushdown skipped: input root is {}",
            input.name()
        );
        return Ok(None);
    };
    let Some((file_config, source)) = scan.downcast_to_file_source::<ParquetSource>() else {
        log::debug!("Dynamic filter reader pushdown skipped: input is not Parquet");
        return Ok(None);
    };

    // Unfiltered Comet scans check TIMESTAMP_MILLIS conversions for overflow,
    // including inside structs, arrays, and maps. Pruning a later row group
    // could suppress that error. The physical Parquet units are unknown here,
    // so leave scans with any projected timestamp unfiltered. Check the scan's
    // output schema: timestamps that are not projected do not need decoding.
    if scan
        .schema()
        .fields()
        .iter()
        .any(|field| contains_timestamp(field.data_type()))
    {
        log::debug!("Dynamic filter reader pushdown skipped: projected timestamp conversion");
        return Ok(None);
    }

    let static_filter = source.filter();
    // Removing incomplete statistics must not undo Spark's own range/equality
    // pruning and expose errors in rows Spark skips. Direct IS NOT NULL checks
    // use only null counts: unknown and assumed-zero both keep a nonempty group,
    // and complete statistics (including all-null groups) remain unchanged.
    if static_filter
        .as_ref()
        .is_some_and(|filter| !is_direct_column_null_checks(filter))
    {
        log::debug!("Dynamic filter reader pushdown skipped: existing static predicate");
        return Ok(None);
    }

    // DataFusion may prune a file using supplied statistics before loading its
    // schema, or retain that predicate for early stopping. Our per-file adapter
    // cannot guard that path. Native Comet files normally have no such statistics.
    if file_config
        .file_groups
        .iter()
        .flat_map(|group| group.files())
        .any(|file| file.statistics.is_some())
    {
        log::debug!(
            "Dynamic filter reader pushdown skipped: file statistics precede schema checks"
        );
        return Ok(None);
    }
    let (Some(expr_adapter), Some(reader_factory)) = (
        &file_config.expr_adapter_factory,
        source.parquet_file_reader_factory(),
    ) else {
        log::debug!("Dynamic filter reader pushdown skipped: reader adaptation unavailable");
        return Ok(None);
    };
    let file_column_count = source.table_schema().file_schema().fields().len();
    let mut read_columns = source
        .projection()
        .map(|projection| projection.column_indices())
        .unwrap_or_else(|| (0..file_column_count).collect());
    if let Some(filter) = &static_filter {
        read_columns.extend(collect_columns(filter).iter().map(|column| column.index()));
    }
    // Partition values are literals substituted before schema adaptation.
    read_columns.retain(|&index| index < file_column_count);
    read_columns.sort_unstable();
    read_columns.dedup();
    let expr_adapter = Arc::new(ReaderFilterAdapterFactory::new(
        Arc::clone(expr_adapter),
        read_columns,
    ));
    let reader_factory = Arc::new(ReaderFilterMetadataFactory::new(Arc::clone(reader_factory)));

    let predicate: Arc<dyn PhysicalExpr> = predicate;
    let propagation = match scan
        .data_source()
        .try_pushdown_filters(vec![predicate], config)
    {
        Ok(propagation) => propagation,
        Err(error) => {
            log::debug!(
                "Dynamic filter reader pushdown skipped: predicate remapping failed: {error}"
            );
            return Ok(None);
        }
    };
    let Some(data_source) = propagation.updated_node else {
        log::debug!("Dynamic filter reader pushdown skipped: Parquet declined the predicate");
        return Ok(None);
    };
    let filtered = scan.clone().with_data_source(data_source);
    let Some((file_config, source)) = filtered.downcast_to_file_source::<ParquetSource>() else {
        return Ok(None);
    };
    let mut file_config = file_config.clone();
    file_config.expr_adapter_factory = Some(expr_adapter);
    file_config.file_source = Arc::new(
        source
            .clone()
            .with_parquet_file_reader_factory(reader_factory),
    );
    Ok(Some(Arc::new(
        filtered.with_data_source(Arc::new(file_config)),
    )))
}

#[cfg(test)]
mod tests;
