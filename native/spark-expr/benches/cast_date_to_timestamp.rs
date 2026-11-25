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

use arrow::array::Date32Array;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{Cast, DateOffsets, EvalMode, SparkCastOptions};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8192;
const DAYS_IN_BATCH: usize = 2200;

fn day(year: i32, month: u32, day: u32) -> i32 {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .to_epoch_days()
}

fn region_offsets() -> Arc<DateOffsets> {
    // Model the size and offset ranges of a JVM table for a North American DST zone.
    // Exact transition dates are immaterial to the lookup benchmark.
    let mut starts = vec![i32::MIN];
    let mut offsets = vec![-28_800];
    for year in 1965..2430 {
        starts.extend([day(year, 3, 10), day(year, 11, 4)]);
        offsets.extend([-25_200, -28_800]);
    }
    Arc::new(DateOffsets::try_new(day(2030, 1, 1), starts, offsets).unwrap())
}

fn bench(c: &mut Criterion) {
    let start = day(2020, 1, 1);
    let mut days = (0..ROWS)
        .map(|row| start + (row * DAYS_IN_BATCH / ROWS) as i32)
        .collect::<Vec<_>>();
    let sorted = Date32Array::from(days.clone());
    days.shuffle(&mut StdRng::seed_from_u64(42));
    let shuffled = Date32Array::from(days);
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
    let batches = [("sorted", sorted), ("shuffled", shuffled)].map(|(order, dates)| {
        (
            order,
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dates)]).unwrap(),
        )
    });

    let mut group = c.benchmark_group("cast_date_to_timestamp");
    group.throughput(Throughput::Elements(ROWS as u64));
    for zone in ["UTC", "+05:30", "America/Los_Angeles"] {
        let mut options = SparkCastOptions::new(EvalMode::Legacy, zone);
        if zone == "America/Los_Angeles" {
            options.date_offsets = Some(region_offsets());
        }
        let cast = Cast::new(
            Arc::new(Column::new("d", 0)),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            options,
            None,
            None,
        );
        for (order, batch) in &batches {
            group.bench_function(format!("{zone}/{order}"), |b| {
                b.iter(|| black_box(cast.evaluate(black_box(batch)).unwrap()))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
