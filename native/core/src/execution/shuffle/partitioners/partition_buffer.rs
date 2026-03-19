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
    make_array, ArrayData, ArrayRef, BinaryArray, BooleanArray, BooleanBufferBuilder,
    LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, UInt32Array,
};
use arrow::buffer::{Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::take;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::Result;
use std::sync::Arc;

/// Per-partition typed column buffer for the scatter kernel.
pub(crate) enum ColumnBuffer {
    Boolean {
        values: BooleanBufferBuilder,
        nulls: BooleanBufferBuilder,
    },
    Fixed {
        values: MutableBuffer,
        byte_width: usize,
        nulls: BooleanBufferBuilder,
    },
    Variable {
        offsets: Vec<i32>,
        data: Vec<u8>,
        nulls: BooleanBufferBuilder,
    },
    LargeVariable {
        offsets: Vec<i64>,
        data: Vec<u8>,
        nulls: BooleanBufferBuilder,
    },
    Fallback {
        indices: Vec<u32>,
    },
}

impl ColumnBuffer {
    pub(crate) fn append_fixed(&mut self, bytes: &[u8]) {
        match self {
            ColumnBuffer::Fixed { values, .. } => {
                values.extend_from_slice(bytes);
            }
            _ => unreachable!("append_fixed called on non-Fixed variant"),
        }
    }

    pub(crate) fn append_variable(&mut self, bytes: &[u8]) {
        match self {
            ColumnBuffer::Variable { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as i32);
            }
            _ => unreachable!("append_variable called on non-Variable variant"),
        }
    }

    pub(crate) fn append_large_variable(&mut self, bytes: &[u8]) {
        match self {
            ColumnBuffer::LargeVariable { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as i64);
            }
            _ => unreachable!("append_large_variable called on non-LargeVariable variant"),
        }
    }

    pub(crate) fn append_bool(&mut self, value: bool) {
        match self {
            ColumnBuffer::Boolean { values, .. } => {
                values.append(value);
            }
            _ => unreachable!("append_bool called on non-Boolean variant"),
        }
    }

    pub(crate) fn append_fallback_index(&mut self, idx: u32) {
        match self {
            ColumnBuffer::Fallback { indices } => {
                indices.push(idx);
            }
            _ => unreachable!("append_fallback_index called on non-Fallback variant"),
        }
    }

    pub(crate) fn append_null_bit(&mut self, is_valid: bool) {
        match self {
            ColumnBuffer::Boolean { nulls, .. }
            | ColumnBuffer::Fixed { nulls, .. }
            | ColumnBuffer::Variable { nulls, .. }
            | ColumnBuffer::LargeVariable { nulls, .. } => {
                nulls.append(is_valid);
            }
            ColumnBuffer::Fallback { .. } => {
                unreachable!("append_null_bit called on Fallback variant")
            }
        }
    }

    pub(crate) fn memory_size(&self) -> usize {
        match self {
            ColumnBuffer::Boolean { values, nulls } => values.capacity() + nulls.capacity(),
            ColumnBuffer::Fixed { values, nulls, .. } => values.capacity() + nulls.capacity(),
            ColumnBuffer::Variable {
                offsets,
                data,
                nulls,
            } => {
                offsets.capacity() * std::mem::size_of::<i32>() + data.capacity() + nulls.capacity()
            }
            ColumnBuffer::LargeVariable {
                offsets,
                data,
                nulls,
            } => {
                offsets.capacity() * std::mem::size_of::<i64>() + data.capacity() + nulls.capacity()
            }
            ColumnBuffer::Fallback { indices } => indices.capacity() * std::mem::size_of::<u32>(),
        }
    }
}

/// Per-partition buffer that accumulates rows by scattering values into typed
/// column buffers.
pub(crate) struct PartitionBuffer {
    pub(crate) columns: Vec<ColumnBuffer>,
    pub(crate) row_count: usize,
    schema: SchemaRef,
}

impl PartitionBuffer {
    pub(crate) fn new(schema: SchemaRef, estimated_rows: usize) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Boolean => ColumnBuffer::Boolean {
                    values: BooleanBufferBuilder::new(estimated_rows),
                    nulls: BooleanBufferBuilder::new(estimated_rows),
                },
                DataType::Int8 | DataType::UInt8 => ColumnBuffer::Fixed {
                    values: MutableBuffer::new(estimated_rows),
                    byte_width: 1,
                    nulls: BooleanBufferBuilder::new(estimated_rows),
                },
                DataType::Int16 | DataType::UInt16 | DataType::Float16 => ColumnBuffer::Fixed {
                    values: MutableBuffer::new(estimated_rows * 2),
                    byte_width: 2,
                    nulls: BooleanBufferBuilder::new(estimated_rows),
                },
                DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => {
                    ColumnBuffer::Fixed {
                        values: MutableBuffer::new(estimated_rows * 4),
                        byte_width: 4,
                        nulls: BooleanBufferBuilder::new(estimated_rows),
                    }
                }
                DataType::Int64
                | DataType::UInt64
                | DataType::Float64
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Duration(_) => ColumnBuffer::Fixed {
                    values: MutableBuffer::new(estimated_rows * 8),
                    byte_width: 8,
                    nulls: BooleanBufferBuilder::new(estimated_rows),
                },
                DataType::Decimal128(_, _) => ColumnBuffer::Fixed {
                    values: MutableBuffer::new(estimated_rows * 16),
                    byte_width: 16,
                    nulls: BooleanBufferBuilder::new(estimated_rows),
                },
                DataType::Utf8 | DataType::Binary => ColumnBuffer::Variable {
                    offsets: vec![0i32],
                    data: vec![],
                    nulls: BooleanBufferBuilder::new(estimated_rows),
                },
                DataType::LargeUtf8 | DataType::LargeBinary => ColumnBuffer::LargeVariable {
                    offsets: vec![0i64],
                    data: vec![],
                    nulls: BooleanBufferBuilder::new(estimated_rows),
                },
                _ => ColumnBuffer::Fallback { indices: vec![] },
            })
            .collect();

        Self {
            columns,
            row_count: 0,
            schema,
        }
    }

    pub(crate) fn append_fixed(&mut self, col_idx: usize, bytes: &[u8], is_valid: bool) {
        self.columns[col_idx].append_fixed(bytes);
        self.columns[col_idx].append_null_bit(is_valid);
    }

    pub(crate) fn append_variable(&mut self, col_idx: usize, bytes: &[u8], is_valid: bool) {
        self.columns[col_idx].append_variable(bytes);
        self.columns[col_idx].append_null_bit(is_valid);
    }

    pub(crate) fn append_large_variable(&mut self, col_idx: usize, bytes: &[u8], is_valid: bool) {
        self.columns[col_idx].append_large_variable(bytes);
        self.columns[col_idx].append_null_bit(is_valid);
    }

    pub(crate) fn append_bool(&mut self, col_idx: usize, value: bool, is_valid: bool) {
        self.columns[col_idx].append_bool(value);
        self.columns[col_idx].append_null_bit(is_valid);
    }

    pub(crate) fn append_fallback_index(&mut self, col_idx: usize, idx: u32) {
        self.columns[col_idx].append_fallback_index(idx);
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.columns.iter().map(|c| c.memory_size()).sum()
    }

    #[allow(dead_code)]
    pub(crate) fn has_fallback_columns(&self) -> bool {
        self.columns
            .iter()
            .any(|c| matches!(c, ColumnBuffer::Fallback { .. }))
    }

    #[allow(dead_code)]
    pub(crate) fn clear(&mut self) {
        self.row_count = 0;
        for col in &mut self.columns {
            match col {
                ColumnBuffer::Boolean { values, nulls } => {
                    *values = BooleanBufferBuilder::new(0);
                    *nulls = BooleanBufferBuilder::new(0);
                }
                ColumnBuffer::Fixed { values, nulls, .. } => {
                    *values = MutableBuffer::new(0);
                    *nulls = BooleanBufferBuilder::new(0);
                }
                ColumnBuffer::Variable {
                    offsets,
                    data,
                    nulls,
                } => {
                    *offsets = vec![0i32];
                    data.clear();
                    *nulls = BooleanBufferBuilder::new(0);
                }
                ColumnBuffer::LargeVariable {
                    offsets,
                    data,
                    nulls,
                } => {
                    *offsets = vec![0i64];
                    data.clear();
                    *nulls = BooleanBufferBuilder::new(0);
                }
                ColumnBuffer::Fallback { indices } => {
                    indices.clear();
                }
            }
        }
    }

    pub(crate) fn flush(&mut self, fallback_batch: Option<&RecordBatch>) -> Result<RecordBatch> {
        let row_count = self.row_count;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());

        for (col_idx, col) in self.columns.iter_mut().enumerate() {
            let data_type = self.schema.field(col_idx).data_type().clone();
            let array: ArrayRef = match col {
                ColumnBuffer::Fixed { values, nulls, .. } => {
                    let buffer = Buffer::from(std::mem::replace(values, MutableBuffer::new(0)));
                    let mut builder = ArrayData::builder(data_type)
                        .len(row_count)
                        .add_buffer(buffer);
                    if !nulls.is_empty() {
                        builder = builder.null_bit_buffer(Some(nulls.finish().into_inner()));
                    }
                    let data = builder.build()?;
                    make_array(data)
                }
                ColumnBuffer::Variable {
                    offsets,
                    data,
                    nulls,
                } => {
                    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(
                        offsets,
                        vec![0i32],
                    )));
                    let values_buffer = Buffer::from(std::mem::take(data));
                    let null_buffer = if !nulls.is_empty() {
                        Some(NullBuffer::new(nulls.finish()))
                    } else {
                        None
                    };
                    match &data_type {
                        DataType::Utf8 => {
                            Arc::new(StringArray::new(offsets_buffer, values_buffer, null_buffer))
                                as ArrayRef
                        }
                        DataType::Binary => {
                            Arc::new(BinaryArray::new(offsets_buffer, values_buffer, null_buffer))
                                as ArrayRef
                        }
                        _ => unreachable!("Variable buffer with unexpected data type"),
                    }
                }
                ColumnBuffer::LargeVariable {
                    offsets,
                    data,
                    nulls,
                } => {
                    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(
                        offsets,
                        vec![0i64],
                    )));
                    let values_buffer = Buffer::from(std::mem::take(data));
                    let null_buffer = if !nulls.is_empty() {
                        Some(NullBuffer::new(nulls.finish()))
                    } else {
                        None
                    };
                    match &data_type {
                        DataType::LargeUtf8 => Arc::new(LargeStringArray::new(
                            offsets_buffer,
                            values_buffer,
                            null_buffer,
                        )) as ArrayRef,
                        DataType::LargeBinary => Arc::new(LargeBinaryArray::new(
                            offsets_buffer,
                            values_buffer,
                            null_buffer,
                        )) as ArrayRef,
                        _ => unreachable!("LargeVariable buffer with unexpected data type"),
                    }
                }
                ColumnBuffer::Boolean { values, nulls } => {
                    let values_buf = values.finish();
                    let null_buffer = if !nulls.is_empty() {
                        Some(NullBuffer::new(nulls.finish()))
                    } else {
                        None
                    };
                    Arc::new(BooleanArray::new(values_buf, null_buffer)) as ArrayRef
                }
                ColumnBuffer::Fallback { indices } => {
                    let fallback =
                        fallback_batch.expect("fallback_batch required for Fallback columns");
                    let idx_array = UInt32Array::from(std::mem::take(indices));
                    take(fallback.column(col_idx), &idx_array, None)?
                }
            };
            arrays.push(array);
        }

        self.row_count = 0;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn test_partition_buffer_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let mut buf = PartitionBuffer::new(Arc::clone(&schema), 100);

        // Append 3 rows manually
        // Row 0: i=1, s="hello", b=true, all valid
        buf.columns[0].append_fixed(&1i32.to_le_bytes());
        buf.columns[0].append_null_bit(true);
        buf.columns[1].append_variable(b"hello");
        buf.columns[1].append_null_bit(true);
        buf.columns[2].append_bool(true);
        buf.columns[2].append_null_bit(true);
        buf.row_count += 1;

        // Row 1: i=NULL, s="world", b=false
        buf.columns[0].append_fixed(&0i32.to_le_bytes());
        buf.columns[0].append_null_bit(false); // null
        buf.columns[1].append_variable(b"world");
        buf.columns[1].append_null_bit(true);
        buf.columns[2].append_bool(false);
        buf.columns[2].append_null_bit(true);
        buf.row_count += 1;

        // Row 2: i=42, s=NULL, b=true
        buf.columns[0].append_fixed(&42i32.to_le_bytes());
        buf.columns[0].append_null_bit(true);
        buf.columns[1].append_variable(b"");
        buf.columns[1].append_null_bit(false); // null
        buf.columns[2].append_bool(true);
        buf.columns[2].append_null_bit(true);
        buf.row_count += 1;

        let batch = buf.flush(None).unwrap();
        assert_eq!(batch.num_rows(), 3);

        // Check Int32 column
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col0.value(0), 1);
        assert!(col0.is_null(1));
        assert_eq!(col0.value(2), 42);

        // Check Utf8 column
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col1.value(0), "hello");
        assert_eq!(col1.value(1), "world");
        assert!(col1.is_null(2));

        // Check Boolean column
        let col2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col2.value(0));
        assert!(!col2.value(1));
        assert!(col2.value(2));

        // After flush, row_count should be 0
        assert_eq!(buf.row_count(), 0);
    }
}
