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

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Field;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_comet_spark_expr::SparkArrayExtrema;

struct AllocationTracker;

thread_local! {
    // The extrema kernel runs synchronously. Ignore allocations from the test harness's threads
    // and from constructing the input, and record requests even if they are immediately freed.
    static LARGEST_ALLOCATION: Cell<Option<usize>> = const { Cell::new(None) };
}

fn record_allocation(size: usize) {
    LARGEST_ALLOCATION.with(|largest| {
        if let Some(previous) = largest.get() {
            largest.set(Some(previous.max(size)));
        }
    });
}

unsafe impl GlobalAlloc for AllocationTracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: AllocationTracker = AllocationTracker;

fn list(values: ArrayRef, offsets: Vec<i32>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ))
}

#[test]
fn tiny_nested_winners_do_not_allocate_for_large_losing_values() {
    let rows = 512;
    let loser_len = 16_384;
    for (is_min, winner) in [(true, None), (true, Some(-0.0)), (false, Some(2.0))] {
        let mut leaves = Vec::with_capacity(rows * (loser_len + 1));
        let mut offsets = vec![0];
        for _ in 0..rows {
            leaves.extend(winner);
            offsets.push(leaves.len() as i32);
            leaves.extend(std::iter::repeat_n(1.0, loser_len));
            offsets.push(leaves.len() as i32);
        }
        let children = list(Arc::new(Float64Array::from(leaves)), offsets);
        let input = list(children, (0..=rows).map(|row| (row * 2) as i32).collect());
        let udf = ScalarUDF::from(SparkArrayExtrema::new(is_min));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&input))],
            arg_fields: vec![Arc::new(Field::new(
                "input",
                input.data_type().clone(),
                true,
            ))],
            number_rows: rows,
            return_field: Arc::new(Field::new(
                "result",
                udf.return_type(&[input.data_type().clone()]).unwrap(),
                true,
            )),
            config_options: Arc::new(ConfigOptions::default()),
        };

        LARGEST_ALLOCATION.with(|largest| largest.set(Some(0)));
        let result = udf.invoke_with_args(args);
        let largest = LARGEST_ALLOCATION.with(|largest| largest.replace(None).unwrap());

        let result = result.unwrap().into_array(rows).unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(result.len(), rows);
        assert_eq!(result.null_count(), 0);
        for row in 0..rows {
            let value = result.value(row);
            let value = value.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(
                value
                    .iter()
                    .map(|v| v.map(f64::to_bits))
                    .collect::<Vec<_>>(),
                winner
                    .into_iter()
                    .map(|v| Some(v.to_bits()))
                    .collect::<Vec<_>>()
            );
        }
        // The result is only a few KiB. Arrow's input-average capacity estimate previously
        // requested 32 MiB before shrinking it, which retained-buffer assertions cannot detect.
        assert!(
            largest < 1024 * 1024,
            "largest allocation was {largest} bytes"
        );
    }
}
