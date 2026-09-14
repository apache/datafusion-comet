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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_levenshtein;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{string_array, NULL_RATIOS, ROW_COUNTS};

/// Generator pair for one dataset: the invariant right side plus a left-side
/// factory that takes the per-row index, so every dataset fills the same shape.
struct Dataset {
    label: &'static str,
    right: fn(usize) -> String,
    left: fn(usize) -> String,
}

/// ASCII: both operands are ASCII, so `is_ascii() && is_ascii()` enables the
/// byte-level fast path.
const ASCII: Dataset = Dataset {
    label: "ascii",
    right: |_| "sitting".to_string(),
    left: |_| "kitten".to_string(),
};

/// Non-ASCII: both operands are non-ASCII, so the fast path is skipped and the
/// Unicode `chars()` path runs. Both sides pay the `is_ascii()` scan.
const NON_ASCII: Dataset = Dataset {
    label: "non-ascii",
    right: |_| "smörgås".to_string(),
    left: |_| "naïve".to_string(),
};

/// Mixed: right is non-ASCII, left is ASCII. `s.is_ascii() && t.is_ascii()`
/// short-circuits on the second operand, so only the left scan is saved.
const MIXED: Dataset = Dataset {
    label: "mixed",
    right: |_| "café".to_string(),
    left: |_| "cafe".to_string(),
};

const DATASETS: [Dataset; 3] = [ASCII, NON_ASCII, MIXED];

fn criterion_benchmark(c: &mut Criterion) {
    for dataset in &DATASETS {
        let mut group = c.benchmark_group(format!("spark_levenshtein/{}", dataset.label));
        for rows in ROW_COUNTS {
            let right = string_array(rows, 0.0, |_| (dataset.right)(0));
            for (null_ratio, tag) in NULL_RATIOS {
                let left = string_array(rows, null_ratio, |_| (dataset.left)(0));
                let args = vec![
                    ColumnarValue::Array(left),
                    ColumnarValue::Array(Arc::clone(&right)),
                ];
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                    &args,
                    |b, args| b.iter(|| black_box(spark_levenshtein(black_box(args)).unwrap())),
                );
            }
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
