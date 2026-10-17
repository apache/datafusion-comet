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

//! The build side of a spilled bucket as one batch, for [`super::SpillingHashJoinExec`].
//!
//! `HashJoinExec` concatenates its build batches into one before it builds its table, and holds
//! the batches and their copy at once while it does. Given a single batch, it keeps the batch as
//! it is, because arrow's `concat` returns a lone array unchanged. So a spilled bucket's build
//! side is read back one batch at a time into buffers allocated once at their final size, from
//! the sizes recorded while the bucket's file was written. The bucket then needs about its own
//! size in memory, not twice that.
//!
//! Only flat columns are concatenated this way: fixed-width ones, booleans, and strings and
//! binaries with 32-bit or 64-bit offsets. A build side with any other column is read back batch
//! by batch.

use std::sync::Arc;

use arrow::array::builder::NullBufferBuilder;
use arrow::array::{
    make_array, Array, ArrayDataBuilder, ArrayRef, AsArray, BooleanBufferBuilder, OffsetSizeTrait,
    RecordBatch,
};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::{internal_err, Result};

/// The bytes each value takes in a column, for a column whose values all take the same.
fn fixed_width(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::FixedSizeBinary(width) => usize::try_from(*width).ok(),
        other => other.primitive_width(),
    }
}

/// Whether every column of `schema` can be concatenated into buffers of known size.
pub(super) fn supports(schema: &Schema) -> bool {
    schema.fields().iter().all(|field| {
        let data_type = field.data_type();
        fixed_width(data_type).is_some()
            || matches!(
                data_type,
                DataType::Boolean
                    | DataType::Utf8
                    | DataType::Binary
                    | DataType::LargeUtf8
                    | DataType::LargeBinary
            )
    })
}

/// Adds the value bytes of each variable-width column of `batch` to `value_bytes`.
pub(super) fn record_value_bytes(batch: &RecordBatch, value_bytes: &mut [usize]) {
    for (column, bytes) in batch.columns().iter().zip(value_bytes) {
        *bytes += match column.data_type() {
            DataType::Utf8 => used_bytes(column.as_string::<i32>().offsets()),
            DataType::LargeUtf8 => used_bytes(column.as_string::<i64>().offsets()),
            DataType::Binary => used_bytes(column.as_binary::<i32>().offsets()),
            DataType::LargeBinary => used_bytes(column.as_binary::<i64>().offsets()),
            _ => 0,
        };
    }
}

fn used_bytes<O: OffsetSizeTrait>(offsets: &[O]) -> usize {
    match (offsets.first(), offsets.last()) {
        (Some(first), Some(last)) => last.as_usize() - first.as_usize(),
        _ => 0,
    }
}

/// Concatenates batches into one, into buffers allocated once at their final size.
pub(super) struct Concatenator {
    schema: SchemaRef,
    rows: usize,
    columns: Vec<Column>,
}

enum Column {
    FixedWidth {
        width: usize,
        values: Vec<u8>,
        nulls: NullBufferBuilder,
    },
    Boolean {
        values: BooleanBufferBuilder,
        nulls: NullBufferBuilder,
    },
    Bytes32(Bytes<i32>),
    Bytes64(Bytes<i64>),
}

struct Bytes<O> {
    offsets: Vec<O>,
    values: Vec<u8>,
    nulls: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> Bytes<O> {
    fn new(rows: usize, value_bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(O::zero());
        Self {
            offsets,
            values: Vec::with_capacity(value_bytes),
            nulls: NullBufferBuilder::new(rows),
        }
    }

    fn push(&mut self, offsets: &[O], values: &[u8], nulls: Option<&NullBuffer>) -> Result<()> {
        let (first, last) = (offsets[0].as_usize(), offsets[offsets.len() - 1].as_usize());
        let base = self.values.len();
        if self.values.capacity() - base < last - first {
            return internal_err!("a spilled column holds more value bytes than were written");
        }
        self.values.extend_from_slice(&values[first..last]);
        for offset in &offsets[1..] {
            let offset = O::from_usize(base + offset.as_usize() - first);
            let Some(offset) = offset else {
                return internal_err!("a spilled column overflows its offsets");
            };
            self.offsets.push(offset);
        }
        append_nulls(&mut self.nulls, nulls, offsets.len() - 1);
        Ok(())
    }

    fn finish(mut self, data_type: &DataType, rows: usize) -> Result<ArrayRef> {
        let data = ArrayDataBuilder::new(data_type.clone())
            .len(rows)
            .add_buffer(Buffer::from_vec(self.offsets))
            .add_buffer(Buffer::from_vec(self.values))
            .nulls(self.nulls.finish())
            .build()?;
        Ok(make_array(data))
    }
}

fn append_nulls(builder: &mut NullBufferBuilder, nulls: Option<&NullBuffer>, rows: usize) {
    match nulls {
        Some(nulls) => builder.append_buffer(nulls),
        None => builder.append_n_non_nulls(rows),
    }
}

impl Concatenator {
    /// For batches of `schema` that hold `rows` rows together, and in each variable-width column
    /// the value bytes of `value_bytes`. `schema` must be one [`supports`].
    pub(super) fn new(schema: SchemaRef, rows: usize, value_bytes: &[usize]) -> Self {
        let columns = schema
            .fields()
            .iter()
            .zip(value_bytes)
            .map(|(field, bytes)| match field.data_type() {
                DataType::Boolean => Column::Boolean {
                    values: BooleanBufferBuilder::new(rows),
                    nulls: NullBufferBuilder::new(rows),
                },
                DataType::Utf8 | DataType::Binary => Column::Bytes32(Bytes::new(rows, *bytes)),
                DataType::LargeUtf8 | DataType::LargeBinary => {
                    Column::Bytes64(Bytes::new(rows, *bytes))
                }
                other => {
                    let width = fixed_width(other).expect("a supported column");
                    Column::FixedWidth {
                        width,
                        values: Vec::with_capacity(rows * width),
                        nulls: NullBufferBuilder::new(rows),
                    }
                }
            })
            .collect();
        Self {
            schema,
            rows,
            columns,
        }
    }

    /// Copies the rows of `batch` into the buffers.
    pub(super) fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        for (column, array) in self.columns.iter_mut().zip(batch.columns()) {
            let rows = array.len();
            match column {
                Column::FixedWidth {
                    width,
                    values,
                    nulls,
                } => {
                    let data = array.to_data();
                    let start = data.offset() * *width;
                    let bytes = &data.buffers()[0].as_slice()[start..start + rows * *width];
                    if values.capacity() - values.len() < bytes.len() {
                        return internal_err!("a spilled batch holds more rows than were written");
                    }
                    values.extend_from_slice(bytes);
                    append_nulls(nulls, array.nulls(), rows);
                }
                Column::Boolean { values, nulls } => {
                    let booleans = array.as_boolean().values();
                    values.append_packed_range(
                        booleans.offset()..booleans.offset() + rows,
                        booleans.values(),
                    );
                    append_nulls(nulls, array.nulls(), rows);
                }
                Column::Bytes32(bytes) => match array.data_type() {
                    DataType::Utf8 => {
                        let array = array.as_string::<i32>();
                        bytes.push(array.offsets(), array.values(), array.nulls())?
                    }
                    _ => {
                        let array = array.as_binary::<i32>();
                        bytes.push(array.offsets(), array.values(), array.nulls())?
                    }
                },
                Column::Bytes64(bytes) => match array.data_type() {
                    DataType::LargeUtf8 => {
                        let array = array.as_string::<i64>();
                        bytes.push(array.offsets(), array.values(), array.nulls())?
                    }
                    _ => {
                        let array = array.as_binary::<i64>();
                        bytes.push(array.offsets(), array.values(), array.nulls())?
                    }
                },
            }
        }
        Ok(())
    }

    /// The batch of every row pushed.
    pub(super) fn finish(self) -> Result<RecordBatch> {
        let rows = self.rows;
        let columns = self
            .columns
            .into_iter()
            .zip(self.schema.fields())
            .map(|(column, field)| -> Result<ArrayRef> {
                let data_type = field.data_type();
                match column {
                    Column::FixedWidth {
                        values, mut nulls, ..
                    } => {
                        let data = ArrayDataBuilder::new(data_type.clone())
                            .len(rows)
                            .add_buffer(Buffer::from_vec(values))
                            .nulls(nulls.finish())
                            .build()?;
                        Ok(make_array(data))
                    }
                    Column::Boolean {
                        mut values,
                        mut nulls,
                    } => {
                        let data = ArrayDataBuilder::new(DataType::Boolean)
                            .len(rows)
                            .add_buffer(values.finish().into_inner())
                            .nulls(nulls.finish())
                            .build()?;
                        Ok(make_array(data))
                    }
                    Column::Bytes32(bytes) => bytes.finish(data_type, rows),
                    Column::Bytes64(bytes) => bytes.finish(data_type, rows),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Int32Array,
        LargeStringArray, StringArray, TimestampMicrosecondArray,
    };
    use arrow::compute::concat_batches;
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion::common::utils::memory::get_record_batch_memory_size;

    use super::*;

    fn batch(start: usize, rows: usize) -> RecordBatch {
        let range = start..start + rows;
        let value = |row: usize| (row % 5 != 2).then_some(row);
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("d", DataType::Decimal128(20, 3), true),
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("l", DataType::LargeUtf8, false),
            Field::new("y", DataType::Binary, true),
            Field::new("f", DataType::FixedSizeBinary(3), true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from_iter(
                range.clone().map(|row| value(row).map(|row| row as i32)),
            )),
            Arc::new(
                Decimal128Array::from_iter(
                    range
                        .clone()
                        .map(|row| value(row).map(|row| row as i128 * 7)),
                )
                .with_precision_and_scale(20, 3)
                .unwrap(),
            ),
            Arc::new(
                TimestampMicrosecondArray::from_iter(
                    range.clone().map(|row| value(row).map(|row| row as i64)),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(BooleanArray::from_iter(
                range.clone().map(|row| value(row).map(|row| row % 3 == 0)),
            )),
            Arc::new(StringArray::from_iter(
                range
                    .clone()
                    .map(|row| value(row).map(|row| "s".repeat(row % 9))),
            )),
            Arc::new(LargeStringArray::from_iter_values(
                range.clone().map(|row| format!("large-{row}")),
            )),
            Arc::new(BinaryArray::from_iter(
                range
                    .clone()
                    .map(|row| value(row).map(|row| vec![row as u8; row % 4])),
            )),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    range.map(|row| value(row).map(|row| vec![row as u8; 3])),
                    3,
                )
                .unwrap(),
            ),
        ];
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn concatenates_like_concat_batches_into_buffers_of_final_size() -> Result<()> {
        // Batches as they come, sliced, and empty.
        let batches = vec![
            batch(0, 100),
            batch(100, 37).slice(5, 20),
            batch(200, 0),
            batch(300, 64),
        ];
        let schema = batches[0].schema();
        assert!(supports(&schema));
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        let mut value_bytes = vec![0; schema.fields().len()];
        for batch in &batches {
            record_value_bytes(batch, &mut value_bytes);
        }
        let mut concat = Concatenator::new(Arc::clone(&schema), rows, &value_bytes);
        for batch in &batches {
            concat.push(batch)?;
        }
        let actual = concat.finish()?;
        let expected = concat_batches(&schema, &batches)?;
        assert_eq!(actual, expected);
        // Every buffer was allocated at its final size, so the copy holds no more than
        // `concat_batches` makes, but for null bitmaps rounded up to 64 bytes.
        let bitmaps = schema.fields().len();
        assert!(
            get_record_batch_memory_size(&actual)
                <= get_record_batch_memory_size(&expected) + 64 * bitmaps
        );
        Ok(())
    }

    #[test]
    fn nested_and_dictionary_columns_are_not_supported() {
        let list = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        for data_type in [list, dictionary, DataType::Utf8View] {
            let schema = Schema::new(vec![Field::new("c", data_type, true)]);
            assert!(!supports(&schema));
        }
    }
}
