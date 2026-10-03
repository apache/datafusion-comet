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

//! Per-file read safety using the Spark adapter's resolved fields.

use super::{
    is_infallible_read_adaptation, SparkPhysicalExprAdapter, SparkPhysicalExprAdapterFactory,
};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use parquet::variant::VariantType;
use std::collections::HashMap;
use std::sync::Arc;

impl SparkPhysicalExprAdapterFactory {
    /// Recover a typed factory only when every file carries this scan's active factory.
    /// File extensions retain the type erased by DataFusion's adapter interface. Checking
    /// pointer identity keeps a replaced or custom adapter on the ordinary rewrite path.
    pub(crate) fn from_file_scan(scan: &FileScanConfig) -> Option<Arc<Self>> {
        let active = scan.expr_adapter_factory.as_ref()?;
        let mut files = scan.file_groups.iter().flat_map(|group| group.files());
        let factory = files.next()?.extensions.get_arc::<Self>()?;
        let erased: Arc<dyn PhysicalExprAdapterFactory> = Arc::<Self>::clone(&factory);
        if !Arc::ptr_eq(active, &erased) {
            return None;
        }
        files
            .all(|file| {
                file.extensions
                    .get_arc::<Self>()
                    .is_some_and(|other| Arc::ptr_eq(&factory, &other))
            })
            .then_some(factory)
    }

    /// Create the normal per-file adapter and determine whether pruning can skip its reads.
    pub(crate) fn create_with_read_safety(
        &self,
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
        read_columns: &[Column],
    ) -> Result<(Arc<dyn PhysicalExprAdapter>, bool)> {
        let adapter = self.create_adapter(logical_schema, Arc::clone(&physical_schema))?;
        let allow_runtime_filter =
            adapter.read_columns_are_infallible(read_columns, &physical_schema);
        Ok((Arc::new(adapter), allow_runtime_filter))
    }
}

impl SparkPhysicalExprAdapter {
    fn read_columns_are_infallible(&self, columns: &[Column], physical_schema: &Schema) -> bool {
        self.read_columns_are_direct(columns)
            || columns.iter().all(|column| {
                self.rewrite(Arc::new(column.clone()))
                    .is_ok_and(|expr| is_infallible_read_adaptation(&expr, physical_schema))
            })
    }

    fn read_columns_are_direct(&self, columns: &[Column]) -> bool {
        // These paths have additional validation or value replacement in rewrite().
        // Preserve those checks, including defaults for columns outside this projection.
        if self.original_physical_dup_check.is_some()
            || self
                .default_values
                .as_ref()
                .is_some_and(|values| !values.is_empty())
        {
            return false;
        }

        // Resolve names once per file, including stale predicate indices. A per-column
        // Schema::index_of lookup would still make wide-file eligibility quadratic.
        // Keep the first entry to match Arrow's field_with_name behavior.
        let mut logical_fields = HashMap::new();
        for field in self.logical_file_schema.fields() {
            logical_fields.entry(field.name().as_str()).or_insert(field);
        }
        let mut physical_fields = HashMap::new();
        for field in self.physical_file_schema.fields() {
            physical_fields
                .entry(field.name().as_str())
                .or_insert(field);
        }
        columns.iter().all(|column| {
            let Some(logical) = logical_fields.get(column.name()) else {
                return false;
            };
            let Some(physical) = physical_fields.get(column.name()) else {
                return false;
            };
            // Equality includes nullability and metadata, so the default adapter returns
            // a bare column. Spark still normalizes Variant even for equal fields.
            // Case/field-ID remapping was already resolved when this adapter was created.
            logical == physical && !logical.has_valid_extension_type::<VariantType>()
        })
    }
}

#[cfg(test)]
mod tests;
