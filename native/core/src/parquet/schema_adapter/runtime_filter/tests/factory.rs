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

use super::*;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;

fn scan_with_factories(
    active: Arc<dyn PhysicalExprAdapterFactory>,
    file_factories: &[Option<Arc<SparkPhysicalExprAdapterFactory>>],
) -> FileScanConfig {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
    let files = file_factories
        .iter()
        .enumerate()
        .map(|(index, factory)| {
            let mut file = PartitionedFile::new(format!("part-{index}.parquet"), 1);
            // Unrelated extensions coexist with the concrete adapter factory.
            file.extensions.insert(19_u64);
            if let Some(factory) = factory {
                file.extensions.insert_arc(Arc::clone(factory));
            }
            file
        })
        .collect();
    FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetSource::new(schema)),
    )
    .with_file_group(FileGroup::new(files))
    .with_expr_adapter(Some(active))
    .build()
}

#[test]
fn typed_factory_requires_every_file_to_identify_the_active_adapter() {
    let factory = Arc::new(factory());
    let active: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::<SparkPhysicalExprAdapterFactory>::clone(&factory);
    let mut scan = scan_with_factories(
        Arc::clone(&active),
        &[Some(Arc::clone(&factory)), Some(Arc::clone(&factory))],
    );
    let recovered = SparkPhysicalExprAdapterFactory::from_file_scan(&scan).unwrap();
    assert!(Arc::ptr_eq(&factory, &recovered));
    assert_eq!(scan.file_groups[0].files()[0].extension::<u64>(), Some(&19));

    scan.expr_adapter_factory = Some(Arc::new(DefaultPhysicalExprAdapterFactory));
    assert!(SparkPhysicalExprAdapterFactory::from_file_scan(&scan).is_none());

    let unrelated = Arc::new(SparkPhysicalExprAdapterFactory::new(
        factory.parquet_options.clone(),
        None,
    ));
    for factories in [
        vec![Some(Arc::clone(&factory)), None],
        vec![Some(Arc::clone(&factory)), Some(Arc::clone(&unrelated))],
        vec![Some(unrelated)],
        vec![],
    ] {
        let scan = scan_with_factories(Arc::clone(&active), &factories);
        assert!(SparkPhysicalExprAdapterFactory::from_file_scan(&scan).is_none());
    }
}
