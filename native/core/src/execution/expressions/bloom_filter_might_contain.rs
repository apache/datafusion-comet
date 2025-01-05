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

use crate::{execution::util::spark_bloom_filter::SparkBloomFilter, parquet::data_type::AsBytes};
use arrow::record_batch::RecordBatch;
use arrow_array::cast::as_primitive_array;
use arrow_schema::{DataType, Schema};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{any::Any, fmt::Display, sync::Arc};

/// A physical expression that checks if a value might be in a bloom filter. It corresponds to the
/// Spark's `BloomFilterMightContain` expression.
#[derive(Debug, Eq)]
pub struct BloomFilterMightContain {
    pub bloom_filter_expr: Arc<dyn PhysicalExpr>,
    pub value_expr: Arc<dyn PhysicalExpr>,
    bloom_filter: Option<SparkBloomFilter>,
}

impl Hash for BloomFilterMightContain {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bloom_filter_expr.hash(state);
        self.value_expr.hash(state);
        self.bloom_filter.hash(state);
    }
}

impl PartialEq for BloomFilterMightContain {
    fn eq(&self, other: &Self) -> bool {
        self.bloom_filter_expr.eq(&other.bloom_filter_expr)
            && self.value_expr.eq(&other.value_expr)
            && self.bloom_filter.eq(&other.bloom_filter)
    }
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

fn evaluate_bloom_filter(
    bloom_filter_expr: &Arc<dyn PhysicalExpr>,
) -> Result<Option<SparkBloomFilter>> {
    // bloom_filter_expr must be a literal/scalar subquery expression, so we can evaluate it
    // with an empty batch with empty schema
    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let bloom_filter_bytes = bloom_filter_expr.evaluate(&batch)?;
    match bloom_filter_bytes {
        ColumnarValue::Scalar(ScalarValue::Binary(v)) => {
            Ok(v.map(|v| SparkBloomFilter::from(v.as_bytes())))
        }
        _ => internal_err!("Bloom filter expression should be evaluated as a scalar binary value"),
    }
}

impl BloomFilterMightContain {
    pub fn try_new(
        bloom_filter_expr: Arc<dyn PhysicalExpr>,
        value_expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Self> {
        // early evaluate the bloom_filter_expr to get the actual bloom filter
        let bloom_filter = evaluate_bloom_filter(&bloom_filter_expr)?;
        Ok(Self {
            bloom_filter_expr,
            value_expr,
            bloom_filter,
        })
    }
}

impl PhysicalExpr for BloomFilterMightContain {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.bloom_filter
            .as_ref()
            .map(|spark_filter| {
                let values = self.value_expr.evaluate(batch)?;
                match values {
                    ColumnarValue::Array(array) => {
                        let boolean_array =
                            spark_filter.might_contain_longs(as_primitive_array(&array));
                        Ok(ColumnarValue::Array(Arc::new(boolean_array)))
                    }
                    ColumnarValue::Scalar(ScalarValue::Int64(v)) => {
                        let result = v.map(|v| spark_filter.might_contain_long(v));
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
                    }
                    _ => internal_err!("value expression should be int64 type"),
                }
            })
            .unwrap_or_else(|| {
                // when the bloom filter is null, we should return null for all the input
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            })
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.bloom_filter_expr, &self.value_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(BloomFilterMightContain::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
        )?))
    }
}
