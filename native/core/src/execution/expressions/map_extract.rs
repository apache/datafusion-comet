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

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{new_null_array, BooleanArray, RecordBatch};
use arrow::compute::{not, nullif};
use arrow::datatypes::{FieldRef, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{internal_err, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

/// Preserve Spark's left-to-right null short-circuit while using DataFusion's map lookup.
/// The planner still wraps the returned list in `ListExtract` to produce a single value.
#[derive(Debug)]
pub struct MapExtractExpr {
    function: ScalarFunctionExpr,
    config_options: Arc<ConfigOptions>,
}

impl MapExtractExpr {
    pub fn try_new(function: ScalarFunctionExpr) -> Result<Self> {
        if function.args().len() != 2 {
            return internal_err!("MapExtractExpr expects exactly two arguments");
        }
        let config_options = Arc::new(function.config_options().clone());
        Ok(Self {
            function,
            config_options,
        })
    }
}

impl Display for MapExtractExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.function, f)
    }
}

impl PartialEq for MapExtractExpr {
    fn eq(&self, other: &Self) -> bool {
        self.function.eq(&other.function)
    }
}

impl Eq for MapExtractExpr {}

impl Hash for MapExtractExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.hash(state);
    }
}

impl PhysicalExpr for MapExtractExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.function.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(ColumnarValue::Array(new_null_array(
                self.function.return_type(),
                0,
            )));
        }

        let args = self.function.args();
        let map = args[0].evaluate(batch)?;
        let all_null = match &map {
            ColumnarValue::Scalar(map) => map.is_null(),
            ColumnarValue::Array(map) => map.null_count() == num_rows,
        };
        if all_null {
            return Ok(ColumnarValue::Array(new_null_array(
                self.function.return_type(),
                num_rows,
            )));
        }

        let selection = match &map {
            ColumnarValue::Array(map) => map
                .nulls()
                .filter(|nulls| nulls.null_count() > 0)
                .map(|nulls| BooleanArray::new(nulls.inner().clone(), None)),
            ColumnarValue::Scalar(_) => None,
        };
        let key = match &selection {
            Some(selection) => args[1].evaluate_selection(batch, selection)?,
            None => args[1].evaluate(batch)?,
        };
        let arg_fields = args
            .iter()
            .map(|arg| arg.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;
        let result = self.function.fun().invoke_with_args(ScalarFunctionArgs {
            args: vec![map, key],
            arg_fields,
            number_rows: num_rows,
            return_field: self.function.return_field(batch.schema_ref())?,
            config_options: Arc::clone(&self.config_options),
        })?;

        // DataFusion's map_extract does not consult the map's row validity. In particular,
        // a scalar key stays scalar after evaluate_selection, and a null map may retain
        // nonempty entry offsets. Mask those rows even if the UDF found a matching entry.
        match selection {
            Some(selection) => {
                let result = result.into_array(num_rows)?;
                Ok(ColumnarValue::Array(nullif(
                    result.as_ref(),
                    &not(&selection)?,
                )?))
            }
            None => Ok(result),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.function.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let function = ScalarFunctionExpr::new(
            self.function.name(),
            Arc::new(self.function.fun().clone()),
            children,
            self.function.return_field(&Schema::empty())?,
            Arc::clone(&self.config_options),
        );
        Ok(Arc::new(Self::try_new(function)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{
        Array, ArrayRef, BooleanBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder,
        ListArray, MapBuilder,
    };
    use arrow::datatypes::{DataType, Int64Type};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion_comet_spark_expr::monotonically_increasing_id::MonotonicallyIncreasingId;
    use datafusion_comet_spark_expr::{create_query_context_map, ListExtract};
    use datafusion_functions_nested::map_extract::map_extract_udf;

    #[derive(Debug)]
    struct CountedExpr {
        child: Arc<dyn PhysicalExpr>,
        calls: Arc<AtomicUsize>,
    }

    impl Display for CountedExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(&self.child, f)
        }
    }

    impl PartialEq for CountedExpr {
        fn eq(&self, other: &Self) -> bool {
            self.child.eq(&other.child)
        }
    }

    impl Eq for CountedExpr {}

    impl Hash for CountedExpr {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.child.hash(state);
        }
    }

    impl PhysicalExpr for CountedExpr {
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }

        fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
            self.child.return_field(input_schema)
        }

        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.child.evaluate(batch)
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.child]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(Self {
                child: Arc::clone(&children[0]),
                calls: Arc::clone(&self.calls),
            }))
        }
    }

    fn map_batch(base: i32, first_valid: bool) -> Result<RecordBatch> {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int32Builder::new());
        for valid in [first_valid, true] {
            // Keep entries even in the null row, as Arrow permits.
            builder.keys().append_value(0);
            builder.values().append_value(base);
            builder.keys().append_value(1);
            builder.values().append_value(base + 1);
            builder.append(valid)?;
        }
        RecordBatch::try_from_iter(vec![("map", Arc::new(builder.finish()) as ArrayRef)])
            .map_err(Into::into)
    }

    fn map_expr(
        map: Arc<dyn PhysicalExpr>,
        key: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(MapExtractExpr::try_new(
            ScalarFunctionExpr::try_new(
                map_extract_udf(),
                vec![map, key],
                schema,
                Arc::new(ConfigOptions::default()),
            )?,
        )?))
    }

    fn single_value(map: Arc<dyn PhysicalExpr>) -> ListExtract {
        ListExtract::new(
            map,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            None,
            true,
            false,
            None,
            create_query_context_map(),
        )
    }

    #[test]
    fn test_map_extract_evaluates_map_once_and_advances_key_only_for_valid_maps() -> Result<()> {
        let first = map_batch(10, false)?;
        let calls = Arc::new(AtomicUsize::new(0));
        let lookup = map_expr(
            Arc::new(CountedExpr {
                child: Arc::new(Column::new("map", 0)),
                calls: Arc::clone(&calls),
            }),
            Arc::new(MonotonicallyIncreasingId::from_offset(0)),
            first.schema_ref(),
        )?;
        let expr = single_value(Arc::clone(&lookup));
        for (batch, expected) in [(first, 10), (map_batch(20, false)?, 21)] {
            lookup.evaluate(&batch.slice(0, 1))?;
            let before = calls.load(Ordering::Relaxed);
            lookup.evaluate(&batch.slice(0, 0))?;
            assert_eq!(calls.load(Ordering::Relaxed), before);
            let result = expr.evaluate(&batch)?.into_array(2)?;
            assert_eq!(calls.load(Ordering::Relaxed), before + 1);
            assert_eq!(
                result.to_data(),
                Int32Array::from(vec![None, Some(expected)]).to_data()
            );
        }
        Ok(())
    }

    #[test]
    fn test_map_extract_skips_throwing_key_for_null_map() -> Result<()> {
        let maps = map_batch(10, false)?;
        let index_list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(0)]),
            Some(vec![Some(0)]),
        ]);
        let batch = RecordBatch::try_from_iter(vec![
            ("map", Arc::clone(maps.column(0))),
            ("index_arr", Arc::new(index_list) as ArrayRef),
            ("index", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
        ])?;
        let key = Arc::new(ListExtract::new(
            Arc::new(Column::new("index_arr", 1)),
            Arc::new(Column::new("index", 2)),
            None,
            true,
            false,
            None,
            create_query_context_map(),
        ));
        assert!(key.evaluate(&batch).is_err());
        let expr = single_value(map_expr(
            Arc::new(Column::new("map", 0)),
            key,
            batch.schema_ref(),
        )?);
        let result = expr.evaluate(&batch)?.into_array(2)?;
        assert_eq!(
            result.to_data(),
            Int32Array::from(vec![None, Some(10)]).to_data()
        );

        let valid_maps = map_batch(10, true)?;
        let valid_batch = RecordBatch::try_new(
            batch.schema(),
            vec![
                Arc::clone(valid_maps.column(0)),
                Arc::clone(batch.column(1)),
                Arc::clone(batch.column(2)),
            ],
        )?;
        assert!(expr.evaluate(&valid_batch).is_err());
        Ok(())
    }

    #[test]
    fn test_map_extract_skips_key_for_null_and_empty_input() -> Result<()> {
        let batch = map_batch(10, false)?.slice(0, 1);
        let map_type = batch.column(0).data_type();
        let maps: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("map", 0)),
            Arc::new(Literal::new(ScalarValue::try_new_null(map_type)?)),
        ];
        for map in maps {
            let calls = Arc::new(AtomicUsize::new(0));
            let key = Arc::new(CountedExpr {
                child: Arc::new(BinaryExpr::new(
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                    Operator::Divide,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
                )),
                calls: Arc::clone(&calls),
            });
            let expr = map_expr(map, key, batch.schema_ref())?;
            for input in [&batch, &batch.slice(0, 0)] {
                let result = expr.evaluate(input)?.into_array(input.num_rows())?;
                assert!(matches!(result.data_type(), DataType::List(_)));
                assert_eq!(result.null_count(), input.num_rows());
            }
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
        Ok(())
    }

    #[test]
    fn test_map_extract_masks_null_map_entries_with_scalar_key() -> Result<()> {
        let batch = map_batch(10, false)?;
        let key: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(0))));
        let expr = single_value(map_expr(
            Arc::new(Column::new("map", 0)),
            Arc::clone(&key),
            batch.schema_ref(),
        )?);
        let result = expr.evaluate(&batch)?.into_array(2)?;
        assert_eq!(
            result.to_data(),
            Int32Array::from(vec![None, Some(10)]).to_data()
        );

        let scalar_map = ScalarValue::try_from_array(batch.column(0), 1)?;
        let lookup = map_expr(
            Arc::new(Literal::new(scalar_map.clone())),
            key,
            batch.schema_ref(),
        )?;
        assert!(matches!(lookup.evaluate(&batch)?, ColumnarValue::Scalar(_)));
        let expr = single_value(lookup);
        let result = expr.evaluate(&batch)?.into_array(2)?;
        assert_eq!(result.to_data(), Int32Array::from(vec![10, 10]).to_data());

        let keys = RecordBatch::try_from_iter(vec![(
            "key",
            Arc::new(Int64Array::from(vec![Some(0), Some(5), None])) as ArrayRef,
        )])?;
        let expr = single_value(map_expr(
            Arc::new(Literal::new(scalar_map)),
            Arc::new(Column::new("key", 0)),
            keys.schema_ref(),
        )?);
        let result = expr.evaluate(&keys)?.into_array(3)?;
        assert_eq!(
            result.to_data(),
            Int32Array::from(vec![Some(10), None, None]).to_data()
        );
        Ok(())
    }

    #[test]
    fn test_map_extract_handles_boolean_scalar_keys_on_null_maps() -> Result<()> {
        let mut builder = MapBuilder::new(None, BooleanBuilder::new(), Int32Builder::new());
        for valid in [false, true] {
            builder.keys().append_value(false);
            builder.values().append_value(10);
            builder.keys().append_value(true);
            builder.values().append_value(20);
            builder.append(valid)?;
        }
        let batch =
            RecordBatch::try_from_iter(vec![("map", Arc::new(builder.finish()) as ArrayRef)])?;
        for (key, expected) in [
            (Some(true), Some(20)),
            (Some(false), Some(10)),
            (None, None),
        ] {
            let expr = single_value(map_expr(
                Arc::new(Column::new("map", 0)),
                Arc::new(Literal::new(ScalarValue::Boolean(key))),
                batch.schema_ref(),
            )?);
            let result = expr.evaluate(&batch)?.into_array(2)?;
            assert_eq!(
                result.to_data(),
                Int32Array::from(vec![None, expected]).to_data()
            );
        }
        Ok(())
    }
}
