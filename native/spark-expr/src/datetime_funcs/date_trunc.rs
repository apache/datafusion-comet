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

use arrow::datatypes::DataType;
use datafusion::common::{
    utils::take_function_args, DataFusionError, Result, ScalarValue, ScalarValue::Utf8,
};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::kernels::temporal::{date_trunc_array_fmt_dyn, date_trunc_dyn};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Utf8],
                Volatility::Immutable,
            ),
            aliases: vec![],
        }
    }
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        "date_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [date, format] = take_function_args(self.name(), args.args)?;
        match (date, format) {
            (ColumnarValue::Array(date), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = date_trunc_dyn(&date, format)?;
                Ok(ColumnarValue::Array(result))
            }
            (date, ColumnarValue::Array(formats)) => {
                let date = date.into_array(formats.len())?;
                let result = date_trunc_array_fmt_dyn(&date, &formats)?;
                Ok(ColumnarValue::Array(result))
            }
            (ColumnarValue::Scalar(date_scalar), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let date_arr = date_scalar.to_array()?;
                let result = date_trunc_dyn(&date_arr, format)?;
                let scalar = ScalarValue::try_from_array(&result, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
            _ => Err(DataFusionError::Execution(
                "Invalid input to function DateTrunc. Expected (Date32, Utf8)".to_string(),
            )),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Date32Array, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    fn trunc_scalar_date(date: Option<i32>, formats: ArrayRef) -> ArrayRef {
        let result = SparkDateTrunc::new()
            .invoke_with_args(ScalarFunctionArgs {
                number_rows: formats.len(),
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Date32(date)),
                    ColumnarValue::Array(formats),
                ],
                arg_fields: vec![],
                return_field: Arc::new(Field::new("trunc", DataType::Date32, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("expected an array");
        };
        result
    }

    #[test]
    fn scalar_date_with_a_format_column() {
        let formats = || Arc::new(StringArray::from(vec!["year", "MM", "quarter", "Week"]));
        // 2024-06-15 becomes 2024-01-01, 2024-06-01, 2024-04-01 and 2024-06-10.
        let result = trunc_scalar_date(Some(19_889), formats());
        assert_eq!(
            result.as_ref(),
            &Date32Array::from(vec![19_723, 19_875, 19_814, 19_884])
        );

        // Before the epoch: 1969-12-31 becomes the start of its year, month, quarter and week.
        let result = trunc_scalar_date(Some(-1), formats());
        assert_eq!(
            result.as_ref(),
            &Date32Array::from(vec![-365, -31, -92, -3])
        );
    }

    #[test]
    fn scalar_date_with_dictionary_formats() {
        let formats = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1, 0, 1, 0]),
            Arc::new(StringArray::from(vec!["year", "month"])),
        )
        .unwrap();
        let result = trunc_scalar_date(Some(19_889), Arc::new(formats.slice(1, 3)));
        assert_eq!(
            result.as_ref(),
            &Date32Array::from(vec![19_875, 19_723, 19_875])
        );
    }

    #[test]
    fn scalar_date_preserves_nulls_and_empty_batches() {
        let formats = Arc::new(StringArray::from(vec!["year", "month"]));
        let result = trunc_scalar_date(None, formats);
        assert_eq!(result.as_ref(), &Date32Array::from(vec![None, None]));

        for date in [Some(19_889), None] {
            let result = trunc_scalar_date(date, Arc::new(StringArray::from(Vec::<&str>::new())));
            assert_eq!(result.as_ref(), &Date32Array::from(Vec::<i32>::new()));
        }
    }
}
