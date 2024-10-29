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

mod perf;

use std::sync::Arc;

use arrow::{array::ArrayData, buffer::Buffer};
use comet::parquet::{read::ColumnReader, util::jni::TypePromotionInfo};
use criterion::{criterion_group, criterion_main, Criterion};
use parquet::{
    basic::{Encoding, Type as PhysicalType},
    column::page::{PageIterator, PageReader},
    data_type::Int32Type,
    schema::types::{
        ColumnDescPtr, ColumnDescriptor, ColumnPath, PrimitiveTypeBuilder, SchemaDescPtr, TypePtr,
    },
};

use comet::parquet::util::test_common::page_util::{
    DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator,
};

use perf::FlamegraphProfiler;
use rand::{prelude::StdRng, Rng, SeedableRng};
use zstd::zstd_safe::WriteBuf;

fn bench(c: &mut Criterion) {
    let expected_num_values: usize = NUM_PAGES * VALUES_PER_PAGE;
    let mut group = c.benchmark_group("comet_parquet_read");
    let schema = build_test_schema();

    let pages = build_plain_int32_pages(schema.clone(), schema.column(0), 0.0);
    group.bench_function("INT/PLAIN/NOT_NULL", |b| {
        let t = TypePtr::new(
            PrimitiveTypeBuilder::new("f", PhysicalType::INT32)
                .with_length(4)
                .build()
                .unwrap(),
        );
        b.iter(|| {
            let cd = ColumnDescriptor::new(t.clone(), 0, 0, ColumnPath::from(Vec::new()));
            let promotion_info = TypePromotionInfo::new(PhysicalType::INT32, -1, -1, 32);
            let mut column_reader = TestColumnReader::new(
                cd,
                promotion_info,
                BATCH_SIZE,
                pages.clone(),
                expected_num_values,
            );

            let mut total = 0;
            for batch in column_reader.by_ref() {
                total += batch.len();
                ::std::mem::forget(batch);
            }
            assert_eq!(total, expected_num_values);
        });
    });
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(100))
}

criterion_group! {
    name = benches;
    config = profiled();
    targets = bench
}
criterion_main!(benches);

fn build_test_schema() -> SchemaDescPtr {
    use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
    let message_type = "
        message test_schema {
            REQUIRED INT32 c1;
            OPTIONAL INT32 c2;
        }
        ";
    parse_message_type(message_type)
        .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
        .unwrap()
}

fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

// test data params
const NUM_PAGES: usize = 1000;
const VALUES_PER_PAGE: usize = 10_000;
const BATCH_SIZE: usize = 4096;

fn build_plain_int32_pages(
    schema: SchemaDescPtr,
    column_desc: ColumnDescPtr,
    null_density: f32,
) -> impl PageIterator + Clone {
    let max_def_level = column_desc.max_def_level();
    let max_rep_level = column_desc.max_rep_level();
    let rep_levels = vec![0; VALUES_PER_PAGE];
    let mut rng = seedable_rng();
    let mut pages: Vec<parquet::column::page::Page> = Vec::new();
    let mut int32_value = 0;
    for _ in 0..NUM_PAGES {
        // generate page
        let mut values = Vec::with_capacity(VALUES_PER_PAGE);
        let mut def_levels = Vec::with_capacity(VALUES_PER_PAGE);
        for _ in 0..VALUES_PER_PAGE {
            let def_level = if rng.gen::<f32>() < null_density {
                max_def_level - 1
            } else {
                max_def_level
            };
            if def_level == max_def_level {
                int32_value += 1;
                values.push(int32_value);
            }
            def_levels.push(def_level);
        }
        let mut page_builder =
            DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
        page_builder.add_rep_levels(max_rep_level, &rep_levels);
        page_builder.add_def_levels(max_def_level, &def_levels);
        page_builder.add_values::<Int32Type>(Encoding::PLAIN, &values);
        pages.push(page_builder.consume());
    }

    // Since `InMemoryPageReader` is not exposed from parquet crate, here we use
    // `InMemoryPageIterator` instead which is a Iter<Iter<Page>>.
    InMemoryPageIterator::new(schema, column_desc, vec![pages])
}

struct TestColumnReader {
    inner: ColumnReader,
    pages: Box<dyn PageReader>,
    batch_size: usize,
    total_num_values: usize,
    total_num_values_read: usize,
    first_page_loaded: bool,
}

impl TestColumnReader {
    pub fn new(
        cd: ColumnDescriptor,
        promotion_info: TypePromotionInfo,
        batch_size: usize,
        mut page_iter: impl PageIterator + 'static,
        total_num_values: usize,
    ) -> Self {
        let reader = ColumnReader::get(cd, promotion_info, batch_size, false, false);
        let first = page_iter.next().unwrap().unwrap();
        Self {
            inner: reader,
            pages: first,
            batch_size,
            total_num_values,
            total_num_values_read: 0,
            first_page_loaded: false,
        }
    }

    fn load_page(&mut self) {
        if let Some(page) = self.pages.get_next_page().unwrap() {
            let num_values = page.num_values() as usize;
            let buffer = Buffer::from_slice_ref(page.buffer().as_slice());
            self.inner.set_page_v1(num_values, buffer, page.encoding());
        }
    }
}

impl Iterator for TestColumnReader {
    type Item = ArrayData;

    fn next(&mut self) -> Option<Self::Item> {
        if self.total_num_values_read >= self.total_num_values {
            return None;
        }

        if !self.first_page_loaded {
            self.load_page();
            self.first_page_loaded = true;
        }

        self.inner.reset_batch();
        let total = ::std::cmp::min(
            self.batch_size,
            self.total_num_values - self.total_num_values_read,
        );

        let mut left = total;
        while left > 0 {
            let (num_read, _) = self.inner.read_batch(left, 0);
            if num_read < left {
                self.load_page();
            }
            left -= num_read;
        }
        self.total_num_values_read += total;

        Some(self.inner.current_batch().unwrap())
    }
}
