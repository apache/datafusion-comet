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

use crate::utils::array_with_timezone;
use arrow::compute::{date_part, DatePart};
use arrow::datatypes::{DataType, TimeUnit::Microsecond};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::{any::Any, fmt::Debug};

macro_rules! extract_date_part {
    ($struct_name:ident, $fn_name:expr, $date_part_variant:ident) => {
        #[derive(Debug)]
        pub struct $struct_name {
            signature: Signature,
            aliases: Vec<String>,
            timezone: String,
        }

        impl $struct_name {
            pub fn new(timezone: String) -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                    aliases: vec![],
                    timezone,
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
                Ok(match &arg_types[0] {
                    DataType::Dictionary(_, _) => {
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32))
                    }
                    _ => DataType::Int32,
                })
            }

            fn invoke_with_args(
                &self,
                args: ScalarFunctionArgs,
            ) -> datafusion::common::Result<ColumnarValue> {
                let args: [ColumnarValue; 1] = args.args.try_into().map_err(|_| {
                    internal_datafusion_err!(concat!($fn_name, " expects exactly one argument"))
                })?;

                match args {
                    [ColumnarValue::Array(array)] => {
                        let array = array_with_timezone(
                            array,
                            self.timezone.clone(),
                            Some(&DataType::Timestamp(
                                Microsecond,
                                Some(self.timezone.clone().into()),
                            )),
                        )?;
                        let result = date_part(&array, DatePart::$date_part_variant)?;
                        Ok(ColumnarValue::Array(result))
                    }
                    _ => Err(DataFusionError::Execution(
                        concat!($fn_name, "(scalar) should be fold in Spark JVM side.").to_string(),
                    )),
                }
            }

            fn aliases(&self) -> &[String] {
                &self.aliases
            }
        }
    };
}

extract_date_part!(SparkHour, "hour", Hour);
extract_date_part!(SparkMinute, "minute", Minute);
extract_date_part!(SparkSecond, "second", Second);
