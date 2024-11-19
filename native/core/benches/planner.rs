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
// under the License.use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, RecordBatch, StringBuilder};

use comet::execution::datafusion::planner::PhysicalPlanner;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_proto::spark_operator::Operator;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("planner");

    group.bench_function("deserialize TPC-H q3 stage 18 shuffle", |b| {
        b.iter(|| black_box(deserialize_proto("testdata/plan_18_0.bin").unwrap()))
    });

    group.bench_function("plan TPC-H q3 stage 18 shuffle", |b| {
        let op = deserialize_proto("testdata/plan_18_0.bin").unwrap();
        b.iter(|| black_box(create_plan(&op).unwrap()))
    });

    group.finish();
}

fn create_plan(op: &Operator) -> Result<Arc<dyn ExecutionPlan>> {
    let planner = PhysicalPlanner::default();
    let mut inputs = vec![];
    let (_scans, plan) = planner.create_plan(op, &mut inputs)?;
    Ok(plan)
}

fn deserialize_proto(filename: &str) -> Result<Operator> {
    let bytes = read_plan_proto(filename);
    let spark_plan = comet::execution::serde::deserialize_op(bytes.as_slice())?;
    Ok(spark_plan)
}

fn read_plan_proto(filename: &str) -> Vec<u8> {
    let mut file = File::open(filename).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    buffer
}

fn config() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_millis(500))
        .warm_up_time(Duration::from_millis(500))
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
