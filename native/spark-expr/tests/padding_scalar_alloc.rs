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

use datafusion::arrow::array::{AsArray, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_lpad, spark_read_side_padding, spark_rpad};

struct AllocationTracker;

thread_local! {
    // Padding runs synchronously. Exclude input construction and other test threads,
    // but record temporary allocation requests even when their buffers are freed.
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

fn measured_padding(
    pad: fn(&[ColumnarValue]) -> Result<ColumnarValue>,
    args: &[ColumnarValue],
) -> ColumnarValue {
    LARGEST_ALLOCATION.with(|largest| largest.set(Some(0)));
    let result = pad(args);
    let largest = LARGEST_ALLOCATION.with(|largest| largest.replace(None).unwrap());
    // Outputs are at most a few KiB. A scalar broadcast previously requested
    // 64 MiB even for empty output; retained-buffer checks miss that allocation.
    assert!(
        largest < 1024 * 1024,
        "largest allocation was {largest} bytes"
    );
    result.unwrap()
}

fn string_scalar(large: bool, value: Option<String>) -> ScalarValue {
    if large {
        ScalarValue::LargeUtf8(value)
    } else {
        ScalarValue::Utf8(value)
    }
}

fn length_array(values: Vec<Option<i32>>) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(Int32Array::from(values)))
}

fn assert_array_result(result: ColumnarValue, large: bool, expected: &[Option<&str>]) {
    let ColumnarValue::Array(array) = result else {
        panic!("array lengths must produce an array");
    };
    assert_eq!(array.len(), expected.len());
    assert_eq!(
        array.null_count(),
        expected.iter().filter(|value| value.is_none()).count()
    );
    if large {
        assert_eq!(array.data_type(), &DataType::LargeUtf8);
        assert_eq!(
            array.as_string::<i64>().iter().collect::<Vec<_>>(),
            expected
        );
    } else {
        assert_eq!(array.data_type(), &DataType::Utf8);
        assert_eq!(
            array.as_string::<i32>().iter().collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn truncated_scalar_strings_do_not_allocate_broadcast_buffers() {
    let rows = 1024;
    for pad in [spark_lpad, spark_rpad] {
        for large in [false, true] {
            for pattern in [None, Some("öx")] {
                for lengths in [
                    vec![Some(0); rows],
                    vec![Some(3), Some(-1), None, Some(0)]
                        .into_iter()
                        .cycle()
                        .take(rows)
                        .collect(),
                ] {
                    let expected: Vec<_> = lengths
                        .iter()
                        .map(|length| match length {
                            Some(3) => Some("xxx"),
                            Some(_) => Some(""),
                            None => None,
                        })
                        .collect();
                    let mut args = vec![
                        ColumnarValue::Scalar(string_scalar(large, Some("x".repeat(64 * 1024)))),
                        length_array(lengths),
                    ];
                    if let Some(pattern) = pattern {
                        args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                            pattern.to_string(),
                        ))));
                    }
                    assert_array_result(measured_padding(pad, &args), large, &expected);
                }
            }
        }
    }
}

#[test]
fn null_pad_does_not_allocate_for_scalar_strings() {
    let rows = 1024;
    for pad in [spark_lpad, spark_rpad, spark_read_side_padding] {
        for large in [false, true] {
            for length in [Some(2 * 1024 * 1024), None] {
                let args = [
                    ColumnarValue::Scalar(string_scalar(large, Some("x".repeat(64 * 1024)))),
                    ColumnarValue::Scalar(ScalarValue::Int32(length)),
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ];
                let ColumnarValue::Scalar(result) = measured_padding(pad, &args) else {
                    panic!("scalar lengths must produce a scalar");
                };
                assert_eq!(result, string_scalar(large, None));
            }
            let args = [
                ColumnarValue::Scalar(string_scalar(large, Some("x".repeat(64 * 1024)))),
                length_array(
                    [Some(0), Some(3), Some(-1), None]
                        .into_iter()
                        .cycle()
                        .take(rows)
                        .collect(),
                ),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ];
            assert_array_result(measured_padding(pad, &args), large, &vec![None; rows]);
        }
    }
}

#[test]
fn null_scalar_strings_do_not_allocate_for_large_lengths() {
    // Bounded so removing the NULL guard fails without exhausting the runner.
    let large_length = 2 * 1024 * 1024;
    for pad in [spark_lpad, spark_rpad, spark_read_side_padding] {
        for large in [false, true] {
            for pattern in [None, Some(Some("öx")), Some(None)] {
                let mut args = vec![
                    ColumnarValue::Scalar(string_scalar(large, None)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(large_length))),
                ];
                if let Some(pattern) = pattern {
                    args.push(ColumnarValue::Scalar(ScalarValue::Utf8(
                        pattern.map(str::to_string),
                    )));
                }
                let ColumnarValue::Scalar(result) = measured_padding(pad, &args) else {
                    panic!("scalar lengths must produce a scalar");
                };
                assert_eq!(result, string_scalar(large, None));
                for lengths in [vec![Some(large_length), None, Some(0)], vec![]] {
                    let expected = vec![None; lengths.len()];
                    args[1] = length_array(lengths);
                    assert_array_result(measured_padding(pad, &args), large, &expected);
                }
            }
        }
    }
}

#[test]
fn null_scalar_lengths_do_not_copy_scalar_strings() {
    for pad in [spark_lpad, spark_rpad] {
        for large in [false, true] {
            let args = [
                ColumnarValue::Scalar(string_scalar(large, Some("x".repeat(2 * 1024 * 1024)))),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".to_string()))),
            ];
            let ColumnarValue::Scalar(result) = measured_padding(pad, &args) else {
                panic!("scalar lengths must produce a scalar");
            };
            assert_eq!(result, string_scalar(large, None));
        }
    }
}
