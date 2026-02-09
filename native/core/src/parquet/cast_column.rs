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
use arrow::{
    array::{make_array, ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray},
    compute::CastOptions,
    datatypes::{DataType, FieldRef, Schema, TimeUnit},
    record_batch::RecordBatch,
};

use crate::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::Result as DataFusionResult;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{self, Display},
    hash::Hash,
    sync::Arc,
};

/// Returns true if two DataTypes are structurally equivalent (same data layout)
/// but may differ in field names within nested types.
fn types_differ_only_in_field_names(physical: &DataType, logical: &DataType) -> bool {
    match (physical, logical) {
        (DataType::List(pf), DataType::List(lf)) => {
            pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::LargeList(pf), DataType::LargeList(lf)) => {
            pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::Map(pf, p_sorted), DataType::Map(lf, l_sorted)) => {
            p_sorted == l_sorted
                && pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::Struct(pfields), DataType::Struct(lfields)) => {
            // For Struct types, field names are semantically meaningful (they
            // identify different columns), so we require name equality here.
            // This distinguishes from List/Map wrapper field names ("item" vs
            // "element") which are purely cosmetic.
            pfields.len() == lfields.len()
                && pfields.iter().zip(lfields.iter()).all(|(pf, lf)| {
                    pf.name() == lf.name()
                        && pf.is_nullable() == lf.is_nullable()
                        && (pf.data_type() == lf.data_type()
                            || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
                })
        }
        _ => false,
    }
}

/// Recursively relabel an array so its DataType matches `target_type`.
/// This only changes metadata (field names, nullability flags in nested fields);
/// it does NOT change the underlying buffer data.
fn relabel_array(array: ArrayRef, target_type: &DataType) -> ArrayRef {
    if array.data_type() == target_type {
        return array;
    }
    let data = array.to_data();
    let new_data = data
        .into_builder()
        .data_type(target_type.clone())
        .build()
        .expect("relabel_array: data layout must be compatible");
    make_array(new_data)
}

/// Casts a Timestamp(Microsecond) array to Timestamp(Millisecond) by dividing values by 1000.
/// Preserves the timezone from the target type.
fn cast_timestamp_micros_to_millis_array(
    array: &ArrayRef,
    target_tz: Option<Arc<str>>,
) -> ArrayRef {
    let micros_array = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("Expected TimestampMicrosecondArray");

    let millis_values: TimestampMillisecondArray = micros_array
        .iter()
        .map(|opt| opt.map(|v| v / 1000))
        .collect();

    // Apply timezone if present
    let result = if let Some(tz) = target_tz {
        millis_values.with_timezone(tz)
    } else {
        millis_values
    };

    Arc::new(result)
}

/// Casts a Timestamp(Microsecond) scalar to Timestamp(Millisecond) by dividing the value by 1000.
/// Preserves the timezone from the target type.
fn cast_timestamp_micros_to_millis_scalar(
    opt_val: Option<i64>,
    target_tz: Option<Arc<str>>,
) -> ScalarValue {
    let new_val = opt_val.map(|v| v / 1000);
    ScalarValue::TimestampMillisecond(new_val, target_tz)
}

#[derive(Debug, Clone, Eq)]
pub struct CometCastColumnExpr {
    /// The physical expression producing the value to cast.
    expr: Arc<dyn PhysicalExpr>,
    /// The physical field of the input column.
    input_physical_field: FieldRef,
    /// The field type required by query
    target_field: FieldRef,
    /// Options forwarded to [`cast_column`].
    cast_options: CastOptions<'static>,
    /// Spark parquet options for complex nested type conversions.
    /// When present, enables `spark_parquet_convert` as a fallback.
    parquet_options: Option<SparkParquetOptions>,
}

// Manually derive `PartialEq`/`Hash` as `Arc<dyn PhysicalExpr>` does not
// implement these traits by default for the trait object.
impl PartialEq for CometCastColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_physical_field.eq(&other.input_physical_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
            && self.parquet_options.eq(&other.parquet_options)
    }
}

impl Hash for CometCastColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_physical_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
        self.parquet_options.hash(state);
    }
}

impl CometCastColumnExpr {
    /// Create a new [`CometCastColumnExpr`].
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        physical_field: FieldRef,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            input_physical_field: physical_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
            parquet_options: None,
        }
    }

    /// Set Spark parquet options to enable complex nested type conversions.
    pub fn with_parquet_options(mut self, options: SparkParquetOptions) -> Self {
        self.parquet_options = Some(options);
        self
    }
}

impl Display for CometCastColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "COMET_CAST_COLUMN({} AS {})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for CometCastColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;

        // Use == (PartialEq) instead of equals_datatype because equals_datatype
        // ignores field names in nested types (Struct, List, Map). We need to detect
        // when field names differ (e.g., Struct("a","b") vs Struct("c","d")) so that
        // we can apply spark_parquet_convert for field-name-based selection.
        if value.data_type() == *self.target_field.data_type() {
            return Ok(value);
        }

        let input_physical_field = self.input_physical_field.data_type();
        let target_field = self.target_field.data_type();

        // Handle specific type conversions with custom casts
        match (input_physical_field, target_field) {
            // Timestamp(Microsecond) -> Timestamp(Millisecond)
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                DataType::Timestamp(TimeUnit::Millisecond, target_tz),
            ) => match value {
                ColumnarValue::Array(array) => {
                    let casted = cast_timestamp_micros_to_millis_array(&array, target_tz.clone());
                    Ok(ColumnarValue::Array(casted))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(opt_val, _)) => {
                    let casted = cast_timestamp_micros_to_millis_scalar(opt_val, target_tz.clone());
                    Ok(ColumnarValue::Scalar(casted))
                }
                _ => Ok(value),
            },
            // Nested types that differ only in field names (e.g., List element named
            // "item" vs "element", or Map entries named "key_value" vs "entries").
            // Re-label the array so the DataType metadata matches the logical schema.
            (physical, logical)
                if physical != logical
                    && types_differ_only_in_field_names(physical, logical) =>
            {
                match value {
                    ColumnarValue::Array(array) => {
                        let relabeled = relabel_array(array, logical);
                        Ok(ColumnarValue::Array(relabeled))
                    }
                    other => Ok(other),
                }
            }
            // Fallback: use spark_parquet_convert for complex nested type conversions
            // (e.g., List<Struct{a,b,c}> → List<Struct{a,c}>, Map field selection, etc.)
            _ => {
                if let Some(parquet_options) = &self.parquet_options {
                    let converted =
                        spark_parquet_convert(value, target_field, parquet_options)?;
                    Ok(converted)
                } else {
                    Ok(value)
                }
            }
        }
    }

    fn return_field(&self, _input_schema: &Schema) -> DataFusionResult<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().expect("CastColumnExpr child");
        let mut new_expr = Self::new(
            child,
            Arc::clone(&self.input_physical_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
        );
        if let Some(opts) = &self.parquet_options {
            new_expr = new_expr.with_parquet_options(opts.clone());
        }
        Ok(Arc::new(new_expr))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Column;

    #[test]
    fn test_cast_timestamp_micros_to_millis_array() {
        // Create a TimestampMicrosecond array with some values
        let micros_array: TimestampMicrosecondArray = vec![
            Some(1_000_000),  // 1 second in micros
            Some(2_500_000),  // 2.5 seconds in micros
            None,             // null value
            Some(0),          // zero
            Some(-1_000_000), // negative value (before epoch)
        ]
        .into();
        let array_ref: ArrayRef = Arc::new(micros_array);

        // Cast without timezone
        let result = cast_timestamp_micros_to_millis_array(&array_ref, None);
        let millis_array = result
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Expected TimestampMillisecondArray");

        assert_eq!(millis_array.len(), 5);
        assert_eq!(millis_array.value(0), 1000); // 1_000_000 / 1000
        assert_eq!(millis_array.value(1), 2500); // 2_500_000 / 1000
        assert!(millis_array.is_null(2));
        assert_eq!(millis_array.value(3), 0);
        assert_eq!(millis_array.value(4), -1000); // -1_000_000 / 1000
    }

    #[test]
    fn test_cast_timestamp_micros_to_millis_array_with_timezone() {
        let micros_array: TimestampMicrosecondArray = vec![Some(1_000_000), Some(2_000_000)].into();
        let array_ref: ArrayRef = Arc::new(micros_array);

        let target_tz: Option<Arc<str>> = Some(Arc::from("UTC"));
        let result = cast_timestamp_micros_to_millis_array(&array_ref, target_tz);
        let millis_array = result
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Expected TimestampMillisecondArray");

        assert_eq!(millis_array.value(0), 1000);
        assert_eq!(millis_array.value(1), 2000);
        // Verify timezone is preserved
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
        );
    }

    #[test]
    fn test_cast_timestamp_micros_to_millis_scalar() {
        // Test with a value
        let result = cast_timestamp_micros_to_millis_scalar(Some(1_500_000), None);
        assert_eq!(result, ScalarValue::TimestampMillisecond(Some(1500), None));

        // Test with null
        let null_result = cast_timestamp_micros_to_millis_scalar(None, None);
        assert_eq!(null_result, ScalarValue::TimestampMillisecond(None, None));

        // Test with timezone
        let target_tz: Option<Arc<str>> = Some(Arc::from("UTC"));
        let tz_result = cast_timestamp_micros_to_millis_scalar(Some(2_000_000), target_tz.clone());
        assert_eq!(
            tz_result,
            ScalarValue::TimestampMillisecond(Some(2000), target_tz)
        );
    }

    #[test]
    fn test_comet_cast_column_expr_evaluate_micros_to_millis_array() {
        // Create input schema with TimestampMicrosecond column
        let input_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let schema = Schema::new(vec![Arc::clone(&input_field)]);

        // Create target field with TimestampMillisecond
        let target_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ));

        // Create a column expression
        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));

        // Create the CometCastColumnExpr
        let cast_expr = CometCastColumnExpr::new(col_expr, input_field, target_field, None);

        // Create a record batch with TimestampMicrosecond data
        let micros_array: TimestampMicrosecondArray =
            vec![Some(1_000_000), Some(2_000_000), None].into();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(micros_array)]).unwrap();

        // Evaluate
        let result = cast_expr.evaluate(&batch).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let millis_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("Expected TimestampMillisecondArray");
                assert_eq!(millis_array.value(0), 1000);
                assert_eq!(millis_array.value(1), 2000);
                assert!(millis_array.is_null(2));
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[test]
    fn test_comet_cast_column_expr_evaluate_micros_to_millis_scalar() {
        // Create input schema with TimestampMicrosecond column
        let input_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let schema = Schema::new(vec![Arc::clone(&input_field)]);

        // Create target field with TimestampMillisecond
        let target_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ));

        // Create a literal expression that returns a scalar
        let scalar = ScalarValue::TimestampMicrosecond(Some(1_500_000), None);
        let literal_expr: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Literal::new(scalar));

        // Create the CometCastColumnExpr
        let cast_expr = CometCastColumnExpr::new(literal_expr, input_field, target_field, None);

        // Create an empty batch (scalar doesn't need data)
        let batch = RecordBatch::new_empty(Arc::new(schema));

        // Evaluate
        let result = cast_expr.evaluate(&batch).unwrap();

        match result {
            ColumnarValue::Scalar(s) => {
                assert_eq!(s, ScalarValue::TimestampMillisecond(Some(1500), None));
            }
            _ => panic!("Expected Scalar result"),
        }
    }
}
