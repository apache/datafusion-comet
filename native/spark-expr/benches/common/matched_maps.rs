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

//! Matched map inputs from CometShuffleBenchmark (PR #5788).

use arrow::array::{
    Array, ArrayRef, Int32Array, Int32Builder, MapArray, MapBuilder, StringBuilder, StructArray,
};
use arrow::datatypes::Field;
use criterion::{BenchmarkId, Criterion, Throughput};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{murmur3::create_murmur3_hashes, spark_map_sort};
use std::{hint::black_box, sync::Arc};

pub const ROWS: usize = 8192;

#[derive(Clone, Copy)]
// Each benchmark executable registers only its own subset of stages.
#[allow(dead_code)]
pub enum Stage {
    HashOnly,
    NormalizeOnly,
    NormalizeHash,
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::HashOnly => "hash_only",
            Self::NormalizeOnly => "normalize_only",
            Self::NormalizeHash => "normalize_hash",
        }
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Map,
    StructMapInt,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Self::Map => "map",
            Self::StructMapInt => "struct_map_int",
        }
    }
}

// Scala Long arithmetic wraps; pmod is applied to the signed result of mix64.
fn c1(row: usize) -> i32 {
    let mut z = (row as u64).wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    ((z ^ (z >> 31)) as i64).rem_euclid(1_000_000) as i32
}

fn maps(max_entries: usize, reversed: bool) -> ArrayRef {
    let mut builder = MapBuilder::new(
        Some(crate::common::map_field_names()),
        StringBuilder::new(),
        Int32Builder::new(),
    );
    for row in 0..ROWS {
        let value = c1(row);
        let count = if max_entries <= 1 {
            max_entries
        } else {
            2 + value as usize % (max_entries - 1)
        };
        for idx in 0..count {
            // Singleton is MAP(CAST(c1 AS STRING), c1); larger maps use sequence(1, count).
            let x = if max_entries == 1 {
                0
            } else if reversed {
                count - idx
            } else {
                idx + 1
            };
            let entry = value + x as i32;
            builder.keys().append_value(entry.to_string());
            builder.values().append_value(entry);
        }
        builder.append(true).unwrap();
    }
    Arc::new(builder.finish())
}

fn normalize(args: &[ColumnarValue]) -> ArrayRef {
    match spark_map_sort(args).unwrap() {
        ColumnarValue::Array(array) => array,
        _ => panic!("expected array"),
    }
}

fn wrap(map: ArrayRef, ints: &ArrayRef) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("m", map.data_type().clone(), true)),
            Arc::new(Field::new("i", ints.data_type().clone(), true)),
        ]
        .into(),
        vec![map, Arc::clone(ints)],
        None,
    ))
}

fn hashes(array: &ArrayRef) -> Vec<u32> {
    let mut hashes = vec![42; ROWS];
    create_murmur3_hashes(std::slice::from_ref(array), &mut hashes).unwrap();
    hashes
}

/// Shared registration keeps every stage's data and correctness checks identical.
/// Input construction and checks are untimed. Hash storage is allocated once; resetting it
/// to seed 42 is timed. Normalization output allocation/drop, and struct reconstruction in
/// normalize_hash, are timed. normalize_only measures mapsort itself (no struct wrapper).
pub fn bench_maps(c: &mut Criterion, stage: Stage) {
    let ints: ArrayRef = Arc::new(Int32Array::from_iter_values((0..ROWS).map(c1)));
    let mut group = c.benchmark_group(format!("matched_maps/{}", stage.name()));
    group.throughput(Throughput::Elements(ROWS as u64));
    // Zero entries means ROWS non-null empty maps, not an empty batch.
    for max_entries in [0, 1, 10, 50] {
        let forward = maps(max_entries, false);
        let reversed = maps(max_entries, true);
        let normalized = normalize(&[ColumnarValue::Array(Arc::clone(&forward))]);
        let normalized_reverse = normalize(&[ColumnarValue::Array(Arc::clone(&reversed))]);
        assert_eq!(normalized.to_data(), normalized_reverse.to_data());
        // Independently check lexical ordering and key/value alignment; numeric order does
        // not in general imply string order for unpadded decimal keys.
        let expected = normalized.as_any().downcast_ref::<MapArray>().unwrap();
        let keys = expected
            .keys()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let values = expected
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(expected.len(), ROWS);
        for (row, offsets) in expected.value_offsets().windows(2).enumerate() {
            let base = c1(row);
            let count = if max_entries <= 1 {
                max_entries
            } else {
                2 + base as usize % (max_entries - 1)
            };
            assert_eq!((offsets[1] - offsets[0]) as usize, count);
            let mut actual_values: Vec<_> = (offsets[0] as usize..offsets[1] as usize)
                .map(|i| values.value(i))
                .collect();
            actual_values.sort_unstable();
            let start = if max_entries == 1 { base } else { base + 1 };
            assert_eq!(
                actual_values,
                (start..start + count as i32).collect::<Vec<_>>()
            );
            for i in offsets[0] as usize..offsets[1] as usize {
                assert_eq!(keys.value(i), values.value(i).to_string());
                if i > offsets[0] as usize {
                    assert!(keys.value(i - 1) < keys.value(i));
                }
            }
        }
        // This fixed fixture demonstrates why normalization is needed. Do not require
        // every row to differ: Murmur3 collisions are possible for arbitrary inputs.
        if max_entries > 1 {
            assert_ne!(hashes(&forward), hashes(&reversed));
            assert_ne!(
                hashes(&wrap(Arc::clone(&forward), &ints)),
                hashes(&wrap(Arc::clone(&reversed), &ints))
            );
        } else {
            assert_eq!(forward.to_data(), reversed.to_data());
        }
        let mut field_hashes = vec![42; ROWS];
        create_murmur3_hashes(
            &[Arc::clone(&normalized), Arc::clone(&ints)],
            &mut field_hashes,
        )
        .unwrap();
        assert_eq!(field_hashes, hashes(&wrap(Arc::clone(&normalized), &ints)));
        for (order, raw) in [("forward", forward), ("reversed", reversed)] {
            let args = [ColumnarValue::Array(raw)];
            for shape in [Shape::Map, Shape::StructMapInt] {
                // mapsort is identical for either enclosing shape; measure it once.
                if matches!((stage, shape), (Stage::NormalizeOnly, Shape::StructMapInt)) {
                    continue;
                }
                group.bench_function(
                    BenchmarkId::new(shape.name(), format!("{max_entries}/{order}")),
                    |b| match stage {
                        Stage::NormalizeOnly => b.iter(|| {
                            black_box(normalize(black_box(&args)));
                        }),
                        Stage::HashOnly => {
                            // Both order labels intentionally hash the same pre-normalized
                            // bytes. They are repeated-measurement controls, not a test of
                            // raw hash order independence.
                            let input = match shape {
                                Shape::Map => Arc::clone(&normalized),
                                Shape::StructMapInt => wrap(Arc::clone(&normalized), &ints),
                            };
                            let mut buffer = vec![42; ROWS];
                            b.iter(|| {
                                buffer.fill(42);
                                create_murmur3_hashes(
                                    std::slice::from_ref(black_box(&input)),
                                    &mut buffer,
                                )
                                .unwrap();
                                black_box(&buffer);
                            });
                        }
                        Stage::NormalizeHash => {
                            let mut buffer = vec![42; ROWS];
                            // Dispatch outside timing, including whether a struct is rebuilt.
                            match shape {
                                Shape::Map => b.iter(|| {
                                    buffer.fill(42);
                                    let map = normalize(black_box(&args));
                                    create_murmur3_hashes(std::slice::from_ref(&map), &mut buffer)
                                        .unwrap();
                                    black_box(&buffer);
                                }),
                                Shape::StructMapInt => b.iter(|| {
                                    buffer.fill(42);
                                    let key = wrap(normalize(black_box(&args)), &ints);
                                    create_murmur3_hashes(std::slice::from_ref(&key), &mut buffer)
                                        .unwrap();
                                    black_box(&buffer);
                                }),
                            }
                        }
                    },
                );
            }
        }
    }
    group.finish();
}
