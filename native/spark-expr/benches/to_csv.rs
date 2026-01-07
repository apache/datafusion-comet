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

use arrow::array::{
    BooleanBuilder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_comet_spark_expr::struct_to_csv;
use std::hint::black_box;

fn create_struct_array(array_size: usize) -> StructArray {
    let fields = vec![
        Field::new("f1", DataType::Boolean, true),
        Field::new("f2", DataType::Int8, true),
        Field::new("f3", DataType::Int16, true),
        Field::new("f4", DataType::Int32, true),
        Field::new("f5", DataType::Int64, true),
        Field::new("f6", DataType::Utf8, true),
    ];
    let mut struct_builder = StructBuilder::from_fields(fields, array_size);
    for i in 0..array_size {
        struct_builder
            .field_builder::<BooleanBuilder>(0)
            .unwrap()
            .append_option(if i % 10 == 0 { None } else { Some(i % 2 == 0) });

        struct_builder
            .field_builder::<Int8Builder>(1)
            .unwrap()
            .append_option(if i % 10 == 0 {
                None
            } else {
                Some((i % 128) as i8)
            });

        struct_builder
            .field_builder::<Int16Builder>(2)
            .unwrap()
            .append_option(if i % 10 == 0 { None } else { Some(i as i16) });

        struct_builder
            .field_builder::<Int32Builder>(3)
            .unwrap()
            .append_option(if i % 10 == 0 { None } else { Some(i as i32) });

        struct_builder
            .field_builder::<Int64Builder>(4)
            .unwrap()
            .append_option(if i % 10 == 0 { None } else { Some(i as i64) });

        struct_builder
            .field_builder::<StringBuilder>(5)
            .unwrap()
            .append_option(if i % 10 == 0 {
                None
            } else {
                Some(format!("string_{}", i))
            });

        struct_builder.append(true);
    }
    struct_builder.finish()
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_size = 8192;
    let struct_array = create_struct_array(array_size);
    let default_delimiter = ",";
    let default_null_value = "";
    c.bench_function("to_csv", |b| {
        b.iter(|| {
            black_box(struct_to_csv(&struct_array, default_delimiter, default_null_value).unwrap())
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
