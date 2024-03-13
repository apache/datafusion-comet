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

use crate::{
    execution::datafusion::util::spark_bloom_filter::SparkBloomFilter, parquet::data_type::AsBytes,
};
use arrow::record_batch::RecordBatch;
use arrow_array::{BooleanArray, Int64Array};
use arrow_schema::DataType;
use datafusion::{common::Result, physical_plan::ColumnarValue};
use datafusion_common::{internal_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_physical_expr::{aggregate::utils::down_cast_any_ref, PhysicalExpr};
use once_cell::sync::OnceCell;
use std::{
    any::Any,
    fmt::Display,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// A physical expression that checks if a value might be in a bloom filter. It corresponds to the
/// Spark's `BloomFilterMightContain` expression.
#[derive(Debug)]
pub struct BloomFilterMightContain {
    pub bloom_filter_expr: Arc<dyn PhysicalExpr>,
    pub value_expr: Arc<dyn PhysicalExpr>,
    bloom_filter: OnceCell<Option<SparkBloomFilter>>,
}

impl Display for BloomFilterMightContain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "BloomFilterMightContain [bloom_filter_expr: {}, value_expr: {}]",
            self.bloom_filter_expr, self.value_expr
        )
    }
}

impl PartialEq<dyn Any> for BloomFilterMightContain {
    fn eq(&self, _other: &dyn Any) -> bool {
        down_cast_any_ref(_other)
            .downcast_ref::<Self>()
            .map(|other| {
                self.bloom_filter_expr.eq(&other.bloom_filter_expr)
                    && self.value_expr.eq(&other.value_expr)
            })
            .unwrap_or(false)
    }
}

impl BloomFilterMightContain {
    pub fn new(
        bloom_filter_expr: Arc<dyn PhysicalExpr>,
        value_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            bloom_filter_expr,
            value_expr,
            bloom_filter: Default::default(),
        }
    }
}

impl PhysicalExpr for BloomFilterMightContain {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &arrow_schema::Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &arrow_schema::Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        // lazily get the spark bloom filter
        if self.bloom_filter.get().is_none() {
            let bloom_filter_bytes = self.bloom_filter_expr.evaluate(batch)?;
            match bloom_filter_bytes {
                ColumnarValue::Array(_) => {
                    return internal_err!(
                        "Bloom filter expression must be evaluated as a scalar value"
                    );
                }
                ColumnarValue::Scalar(ScalarValue::Binary(v)) => {
                    let filter = v.map(|v| SparkBloomFilter::new_from_buf(v.as_bytes()));
                    self.bloom_filter.get_or_init(|| filter);
                }
                _ => {
                    return internal_err!("Bloom filter expression must be binary type");
                }
            }
        }
        let num_rows = batch.num_rows();
        let lazy_filter = self.bloom_filter.get().unwrap();
        if lazy_filter.is_none() {
            // when the bloom filter is null, we should return a boolean array with all nulls
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
                num_rows,
            ))))
        } else {
            let spark_filter = lazy_filter.as_ref().unwrap();
            let values = self.value_expr.evaluate(batch)?;
            match values {
                ColumnarValue::Array(array) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("value_expr must be evaluated as an int64 array");
                    Ok(ColumnarValue::Array(Arc::new(
                        spark_filter.might_contain_longs(array)?,
                    )))
                }
                ColumnarValue::Scalar(a) => match a {
                    ScalarValue::Int64(v) => {
                        let result = v.map(|v| spark_filter.might_contain_long(v));
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
                    }
                    _ => {
                        internal_err!(
                            "value_expr must be evaluated as an int64 array or a int64 scalar"
                        )
                    }
                },
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.bloom_filter_expr.clone(), self.value_expr.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(BloomFilterMightContain::new(
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.bloom_filter_expr.hash(&mut s);
        self.value_expr.hash(&mut s);
    }
}
