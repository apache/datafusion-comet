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

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct MonotonicallyIncreasingId {
    initial_offset: i64,
    current_offset: AtomicI64,
}

impl MonotonicallyIncreasingId {
    pub fn from_offset(offset: i64) -> Self {
        Self {
            initial_offset: offset,
            current_offset: AtomicI64::new(offset),
        }
    }

    pub fn from_partition_id(partition: i32) -> Self {
        Self::from_offset((partition as i64) << 33)
    }
}

impl Display for MonotonicallyIncreasingId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "monotonically_increasing_id()")
    }
}

impl PartialEq for MonotonicallyIncreasingId {
    fn eq(&self, other: &Self) -> bool {
        self.initial_offset == other.initial_offset
    }
}

impl Eq for MonotonicallyIncreasingId {}

impl Hash for MonotonicallyIncreasingId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.initial_offset.hash(state);
    }
}

impl PhysicalExpr for MonotonicallyIncreasingId {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let start = self
            .current_offset
            .fetch_add(batch.num_rows() as i64, Ordering::Relaxed);
        let end = start + batch.num_rows() as i64;
        let array_ref = Arc::new(Int64Array::from_iter_values(start..end));
        Ok(ColumnarValue::Array(array_ref))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::compute::concat;
    use arrow::{array::StringArray, datatypes::*};
    use datafusion::common::cast::as_int64_array;

    #[test]
    fn test_monotonically_increasing_id_single_batch() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let data = StringArray::from(vec![Some("foo"), None, None, Some("bar"), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)])?;
        let mid_expr = MonotonicallyIncreasingId::from_offset(0);
        let result = mid_expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_int64_array(&result)?;
        let expected = &Int64Array::from_iter_values(0..batch.num_rows() as i64);
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_monotonically_increasing_id_multi_batch() -> Result<()> {
        let first_batch_schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let first_batch_data = Int64Array::from(vec![Some(42), None]);
        let second_batch_schema = first_batch_schema.clone();
        let second_batch_data = Int64Array::from(vec![None, Some(-42), None]);
        let starting_offset: i64 = 100;
        let mid_expr = MonotonicallyIncreasingId::from_offset(starting_offset);
        let first_batch = RecordBatch::try_new(
            Arc::new(first_batch_schema),
            vec![Arc::new(first_batch_data)],
        )?;
        let first_batch_result = mid_expr
            .evaluate(&first_batch)?
            .into_array(first_batch.num_rows())?;
        let second_batch = RecordBatch::try_new(
            Arc::new(second_batch_schema),
            vec![Arc::new(second_batch_data)],
        )?;
        let second_batch_result = mid_expr
            .evaluate(&second_batch)?
            .into_array(second_batch.num_rows())?;
        let result_arrays: Vec<&dyn Array> = vec![
            as_int64_array(&first_batch_result)?,
            as_int64_array(&second_batch_result)?,
        ];
        let result_arrays = &concat(&result_arrays)?;
        let final_result = as_int64_array(result_arrays)?;
        let range_start = starting_offset;
        let range_end =
            starting_offset + first_batch.num_rows() as i64 + second_batch.num_rows() as i64;
        let expected = &Int64Array::from_iter_values(range_start..range_end);
        assert_eq!(final_result, expected);
        Ok(())
    }
}
