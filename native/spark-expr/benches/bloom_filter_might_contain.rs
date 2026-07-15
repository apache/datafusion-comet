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

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::murmur3::spark_compatible_murmur3_hash;
use datafusion_comet_spark_expr::BloomFilterMightContain;
use std::hint::black_box;
use std::sync::Arc;

/// Serialize a Spark V1 bloom filter (`[version][numHashFunctions][numWords][words...]`, all
/// big-endian) containing `items`, mirroring Spark's `BloomFilterImpl.putLong`.
fn serialize_filter(num_hash_functions: u32, num_words: usize, items: &[i64]) -> Vec<u8> {
    let mut words = vec![0u64; num_words];
    let bit_size = (num_words * 64) as i32;
    for item in items {
        let h1 = spark_compatible_murmur3_hash(item.to_le_bytes(), 0);
        let h2 = spark_compatible_murmur3_hash(item.to_le_bytes(), h1);
        for i in 1..=num_hash_functions {
            let mut combined = (h1 as i32).wrapping_add((i as i32).wrapping_mul(h2 as i32));
            if combined < 0 {
                combined = !combined;
            }
            let idx = (combined % bit_size) as usize;
            words[idx >> 6] |= 1u64 << (idx & 0x3f);
        }
    }

    let mut bytes = 1i32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&(num_hash_functions as i32).to_be_bytes());
    bytes.extend_from_slice(&(num_words as i32).to_be_bytes());
    for word in words {
        bytes.extend_from_slice(&word.to_be_bytes());
    }
    bytes
}

/// A batch of join keys, one in ten of them null, half of them present in the filter.
fn probe_values(num_rows: usize) -> (ArrayRef, Vec<i64>) {
    let mut values: Vec<Option<i64>> = Vec::with_capacity(num_rows);
    let mut inserted: Vec<i64> = Vec::with_capacity(num_rows / 2);
    // Simple LCG so the benchmark data is deterministic without a dependency on an RNG.
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    for row in 0..num_rows {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        if row % 10 == 9 {
            values.push(None);
            continue;
        }
        let value = state as i64;
        if row % 2 == 0 {
            inserted.push(value);
        }
        values.push(Some(value));
    }
    (Arc::new(Int64Array::from(values)), inserted)
}

fn criterion_benchmark(c: &mut Criterion) {
    let num_rows = 8192;
    let (values, inserted) = probe_values(num_rows);
    let return_field = Arc::new(Field::new("v", DataType::Boolean, true));

    let mut group = c.benchmark_group("bloom_filter_might_contain");
    // (label, num_hash_functions, num_words): a filter sized as Spark's runtime bloom filter
    // join pushdown would size it, and a saturated one where every probe walks all hashes.
    for (label, num_hash_functions, num_words) in
        [("sparse", 5u32, 16384usize), ("saturated", 8, 64)]
    {
        let filter_bytes = serialize_filter(num_hash_functions, num_words, &inserted);
        let filter: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Binary(Some(filter_bytes))));
        let udf = BloomFilterMightContain::try_new(filter).unwrap();

        group.bench_function(label, |b| {
            b.iter(|| {
                let args = ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::clone(&values))],
                    number_rows: num_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::new(ConfigOptions::default()),
                    arg_fields: vec![],
                };
                black_box(udf.invoke_with_args(args).unwrap());
            })
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
