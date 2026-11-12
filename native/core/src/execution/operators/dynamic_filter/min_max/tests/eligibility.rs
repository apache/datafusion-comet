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

#[test]
fn computed_scan_projections_are_rejected() {
    let session = session(100);
    let (_file, scan) = parquet_input(vec![Some(1), Some(2)], &DataType::Int32, &session, 1);
    let (file_config, source) = parquet_file_source(&scan).unwrap();
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let computed: Arc<dyn PhysicalExpr> =
        Arc::new(CastExpr::new(Arc::clone(&column), DataType::Int64, None));
    for projection in [
        // The aggregate still sees a direct Column, but the scan computes it.
        vec![ProjectionExpr::new(Arc::clone(&computed), "key")],
        // Even an unused computed output must not be skipped by pruning.
        vec![
            ProjectionExpr::new(Arc::clone(&column), "key"),
            ProjectionExpr::new(computed, "computed"),
        ],
    ] {
        let mut config = file_config.clone();
        config.file_source = source
            .try_pushdown_projection(&ProjectionExprs::new(projection))
            .unwrap()
            .unwrap();
        let scan = Arc::new(scan.as_ref().clone().with_data_source(Arc::new(config)));
        assert!(MinMaxReaderFilterExec::try_new(
            &aggregate(scan, false),
            &ConfigOptions::default(),
        )
        .unwrap()
        .is_none());
    }
}

#[test]
fn unsupported_aggregate_shapes_and_reader_boundaries_are_rejected() {
    let session = session(100);
    let (_file, scan) = parquet_input(vec![Some(1), Some(2)], &DataType::Int32, &session, 1);
    let plain = aggregate(Arc::clone(&scan) as _, false);
    let config = ConfigOptions::default();
    let rejected = |aggregate: AggregateExec| {
        assert!(MinMaxReaderFilterExec::try_new(&aggregate, &config)
            .unwrap()
            .is_none());
    };
    for mode in [AggregateMode::Final, AggregateMode::Single] {
        rejected(
            AggregateExec::try_new(
                mode,
                PhysicalGroupBy::default(),
                plain.aggr_expr().to_vec(),
                vec![None],
                Arc::clone(&scan) as _,
                scan.schema(),
            )
            .unwrap(),
        );
    }
    rejected(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(Arc::new(Column::new("key", 0)), "key".to_owned())]),
            plain.aggr_expr().to_vec(),
            vec![None],
            Arc::clone(&scan) as _,
            scan.schema(),
        )
        .unwrap(),
    );
    rejected(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            plain.aggr_expr().to_vec(),
            vec![Some(lit(true))],
            Arc::clone(&scan) as _,
            scan.schema(),
        )
        .unwrap(),
    );
    let sum = AggregateExprBuilder::new(
        sum_udaf(),
        vec![Arc::new(CastExpr::new(
            Arc::new(Column::new("key", 0)),
            DataType::Int64,
            None,
        ))],
    )
    .schema(scan.schema())
    .alias("sum")
    .build()
    .unwrap();
    rejected(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![Arc::clone(&plain.aggr_expr()[0]), Arc::new(sum)],
            vec![None, None],
            Arc::clone(&scan) as _,
            scan.schema(),
        )
        .unwrap(),
    );
    for distinct in [false, true] {
        let argument: Arc<dyn PhysicalExpr> = if distinct {
            Arc::new(Column::new("key", 0))
        } else {
            Arc::new(CastExpr::new(
                Arc::new(Column::new("key", 0)),
                DataType::Int64,
                None,
            ))
        };
        let expr = AggregateExprBuilder::new(min_udaf(), vec![argument])
            .schema(scan.schema())
            .alias("min")
            .with_distinct(distinct)
            .build()
            .unwrap();
        rejected(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                vec![Arc::new(expr)],
                vec![None],
                Arc::clone(&scan) as _,
                scan.schema(),
            )
            .unwrap(),
        );
    }
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let projection = Arc::new(
        ProjectionExec::try_new(
            vec![(Arc::clone(&column), "key".into())],
            Arc::clone(&scan) as _,
        )
        .unwrap(),
    );
    rejected(aggregate(projection, false));
    let filter = Arc::new(
        FilterExec::try_new(Arc::new(IsNotNullExpr::new(column)), Arc::clone(&scan) as _).unwrap(),
    );
    rejected(aggregate(filter, false));
    rejected(aggregate(scan.with_fetch(Some(1)).unwrap(), false));
    for key_type in [DataType::UInt32, DataType::Float64] {
        let (_file, other) = parquet_input(vec![Some(1), Some(2)], &key_type, &session, 1);
        rejected(aggregate(other, false));
    }
    for aggregate_enabled in [false, true] {
        let mut disabled = config.clone();
        disabled.optimizer.enable_dynamic_filter_pushdown = aggregate_enabled;
        disabled.optimizer.enable_aggregate_dynamic_filter_pushdown = !aggregate_enabled;
        assert!(MinMaxReaderFilterExec::try_new(&plain, &disabled)
            .unwrap()
            .is_none());
    }
    let input =
        RecordBatch::try_new(scan.schema(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let memory = MemorySourceConfig::try_new_exec(&[vec![input]], scan.schema(), None).unwrap();
    rejected(aggregate(memory, false));
    let (file_config, _, _) = concrete_parquet_source(&scan).unwrap();
    let mut partitioned = file_config.clone();
    partitioned
        .file_groups
        .push(partitioned.file_groups[0].clone());
    rejected(aggregate(
        Arc::new(
            scan.as_ref()
                .clone()
                .with_data_source(Arc::new(partitioned)),
        ),
        false,
    ));
}
