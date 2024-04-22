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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::errors::{CometError, CometResult};
use arrow::{
    compute::{cast_with_options, CastOptions},
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use arrow_array::{Array, ArrayRef, BooleanArray, GenericStringArray, OffsetSizeTrait};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{internal_err, Result as DataFusionResult, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;

use crate::execution::datafusion::expressions::utils::{
    array_with_timezone, down_cast_any_ref, spark_cast,
};

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");
static CAST_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

#[derive(Debug, Hash, PartialEq, Clone, Copy)]
pub enum EvalMode {
    Legacy,
    Ansi,
    Try,
}

#[derive(Debug, Hash)]
pub struct Cast {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub eval_mode: EvalMode,

    /// When cast from/to timezone related types, we need timezone, which will be resolved with
    /// session local timezone by an analyzer in Spark.
    pub timezone: String,
}

impl Cast {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
        timezone: String,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone,
            eval_mode,
        }
    }

    pub fn new_without_timezone(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone: "".to_string(),
            eval_mode,
        }
    }

    fn cast_array(&self, array: ArrayRef) -> DataFusionResult<ArrayRef> {
        let to_type = &self.data_type;
        let array = array_with_timezone(array, self.timezone.clone(), Some(to_type));
        let from_type = array.data_type();
        let cast_result = match (from_type, to_type) {
            (DataType::Utf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i32>(&array, self.eval_mode)?
            }
            (DataType::LargeUtf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i64>(&array, self.eval_mode)?
            }
            _ => cast_with_options(&array, to_type, &CAST_OPTIONS)?,
        };
        let result = spark_cast(cast_result, from_type, to_type);
        Ok(result)
    }

    fn spark_cast_utf8_to_boolean<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let array = from
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .unwrap();

        let output_array = array
            .iter()
            .map(|value| match value {
                Some(value) => match value.to_ascii_lowercase().trim() {
                    "t" | "true" | "y" | "yes" | "1" => Ok(Some(true)),
                    "f" | "false" | "n" | "no" | "0" => Ok(Some(false)),
                    _ if eval_mode == EvalMode::Ansi => Err(CometError::CastInvalidValue {
                        value: value.to_string(),
                        from_type: "STRING".to_string(),
                        to_type: "BOOLEAN".to_string(),
                    }),
                    _ => Ok(None),
                },
                _ => Ok(None),
            })
            .collect::<Result<BooleanArray, _>>()?;

        Ok(Arc::new(output_array))
    }
}

impl Display for Cast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast [data_type: {}, timezone: {}, child: {}, eval_mode: {:?}]",
            self.data_type, self.timezone, self.child, &self.eval_mode
        )
    }
}

impl PartialEq<dyn Any> for Cast {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.child.eq(&x.child)
                    && self.timezone.eq(&x.timezone)
                    && self.data_type.eq(&x.data_type)
                    && self.eval_mode.eq(&x.eval_mode)
            })
            .unwrap_or(false)
    }
}

impl PhysicalExpr for Cast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(self.cast_array(array)?)),
            ColumnarValue::Scalar(scalar) => {
                // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
                // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
                // here.
                let array = scalar.to_array()?;
                let scalar = ScalarValue::try_from_array(&self.cast_array(array)?, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(Cast::new(
                children[0].clone(),
                self.data_type.clone(),
                self.eval_mode,
                self.timezone.clone(),
            ))),
            _ => internal_err!("Cast should have exactly one child"),
        }
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.data_type.hash(&mut s);
        self.timezone.hash(&mut s);
        self.eval_mode.hash(&mut s);
        self.hash(&mut s);
    }
}
