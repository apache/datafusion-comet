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

//! Conflict-free field-ID matching must not repeatedly select each child per batch.
//! Compare optimized builds with Criterion's --save-baseline / --baseline options.

use std::{collections::HashMap, hint::black_box, sync::Arc};

use arrow::array::{Array, ArrayRef, Int32Array, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use comet::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::EvalMode;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

const ROWS: usize = 32_768;
const CHILDREN: usize = 64;

fn field(name: String, id: usize) -> Arc<Field> {
    Arc::new(
        Field::new(name, DataType::Int32, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )])),
    )
}

fn benchmark(c: &mut Criterion) {
    let fields = (0..CHILDREN)
        .map(|id| field(format!("file_{id}"), id))
        .collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = (0..CHILDREN)
        .map(|id| Arc::new(Int32Array::from(vec![id as i32; ROWS])) as ArrayRef)
        .collect();
    let input: ArrayRef = Arc::new(StructArray::new(fields.into(), columns.clone(), None));
    let target_fields = (0..CHILDREN)
        .map(|id| field(format!("requested_{id}"), id))
        .collect::<Vec<_>>();
    let target = DataType::Struct(target_fields.clone().into());
    let expected: ArrayRef = Arc::new(StructArray::new(target_fields.into(), columns, None));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            input.data_type().clone(),
            true,
        )])),
        vec![Arc::clone(&input)],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = Bytes::from(writer.into_inner().unwrap());
    let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC");
    options.use_field_id = true;

    let mut group = c.benchmark_group("parquet_field_resolution");
    for batch_size in [64, 8192] {
        let decoded = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .unwrap()
            .with_batch_size(batch_size)
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let input = decoded.column(0);
        let converted =
            spark_parquet_convert(ColumnarValue::Array(Arc::clone(input)), &target, &options)
                .unwrap();
        let ColumnarValue::Array(converted) = converted else {
            unreachable!()
        };
        assert_eq!(converted.as_ref(), expected.slice(0, batch_size).as_ref());

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("convert", batch_size),
            input,
            |b, input| {
                b.iter(|| {
                    spark_parquet_convert(
                        ColumnarValue::Array(Arc::clone(black_box(input))),
                        black_box(&target),
                        black_box(&options),
                    )
                    .unwrap()
                })
            },
        );
        group.throughput(Throughput::Elements(ROWS as u64));
        group.bench_function(BenchmarkId::new("decode_and_convert", batch_size), |b| {
            b.iter(|| {
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                    .unwrap()
                    .with_batch_size(batch_size)
                    .build()
                    .unwrap();
                let mut rows = 0;
                for batch in reader {
                    let batch = batch.unwrap();
                    rows += batch.num_rows();
                    black_box(
                        spark_parquet_convert(
                            ColumnarValue::Array(Arc::clone(batch.column(0))),
                            &target,
                            &options,
                        )
                        .unwrap(),
                    );
                }
                assert_eq!(rows, ROWS);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
