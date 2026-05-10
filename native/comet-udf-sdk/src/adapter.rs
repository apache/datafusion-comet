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

//! Adapter: wrap any DataFusion `ScalarUDFImpl` as a `CometScalarUdf`.
//! Behind the `datafusion-adapter` feature so the default SDK build does
//! not pull in DataFusion.

use std::sync::Arc;

use arrow::array::ArrayRef;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility as DfVolatility,
};

use crate::error::CometUdfError;
use crate::types::{CometUdfSignature, Volatility};
use crate::CometScalarUdf;

/// Wrap an `Arc<dyn ScalarUDFImpl>` as a [`CometScalarUdf`].
///
/// Useful when the user already has a DataFusion UDF and wants to expose it
/// through Comet without re-implementing the trait.
///
/// # Errors
///
/// Returns [`CometUdfError`] if:
/// - The UDF's `TypeSignature` is not `TypeSignature::Exact`. The adapter
///   requires a concrete, fixed argument list to derive the Comet signature.
///   Use a Comet-native UDF for variadic / pattern signatures.
/// - The UDF's `return_type` method fails when called with the derived
///   argument types.
pub fn from_scalar_udf_impl(
    udf: Arc<dyn ScalarUDFImpl>,
) -> Result<impl CometScalarUdf, CometUdfError> {
    let sig = derive_signature(udf.as_ref())?;
    let config_options = Arc::new(ConfigOptions::default());
    Ok(DataFusionAdapter {
        udf,
        sig,
        config_options,
    })
}

struct DataFusionAdapter {
    udf: Arc<dyn ScalarUDFImpl>,
    sig: CometUdfSignature,
    config_options: Arc<ConfigOptions>,
}

impl CometScalarUdf for DataFusionAdapter {
    fn name(&self) -> &str {
        self.udf.name()
    }

    fn signature(&self) -> &CometUdfSignature {
        &self.sig
    }

    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
        let n = args.first().map(|a| a.len()).unwrap_or(0);
        let columnar: Vec<ColumnarValue> = args
            .iter()
            .map(|a| ColumnarValue::Array(a.clone()))
            .collect();
        let arg_fields: Vec<arrow::datatypes::FieldRef> = self
            .sig
            .args
            .iter()
            .map(|dt| Arc::new(arrow::datatypes::Field::new("arg", dt.clone(), true)))
            .collect();
        let return_field = Arc::new(arrow::datatypes::Field::new(
            "ret",
            self.sig.return_type.clone(),
            true,
        ));
        let result = self
            .udf
            .invoke_with_args(ScalarFunctionArgs {
                args: columnar,
                arg_fields,
                number_rows: n,
                return_field,
                config_options: self.config_options.clone(),
            })
            .map_err(|e| CometUdfError::new(e.to_string()))?;
        let array = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s
                .to_array_of_size(n)
                .map_err(|e| CometUdfError::new(e.to_string()))?,
        };
        Ok(array)
    }
}

fn derive_signature(udf: &dyn ScalarUDFImpl) -> Result<CometUdfSignature, CometUdfError> {
    use datafusion::logical_expr::TypeSignature;

    let volatility = match udf.signature().volatility {
        DfVolatility::Immutable => Volatility::Immutable,
        DfVolatility::Stable => Volatility::Stable,
        DfVolatility::Volatile => Volatility::Volatile,
    };
    let args = match &udf.signature().type_signature {
        TypeSignature::Exact(args) => args.clone(),
        other => {
            return Err(CometUdfError::new(format!(
                "DataFusion adapter requires a TypeSignature::Exact signature; \
                 got {other:?}. Use a Comet-native UDF for variadic / pattern signatures."
            )));
        }
    };
    let return_type = udf.return_type(&args).map_err(|e| {
        CometUdfError::new(format!(
            "DataFusion UDF '{}' return_type({:?}) failed: {e}",
            udf.name(),
            args
        ))
    })?;
    Ok(CometUdfSignature {
        args,
        return_type,
        volatility,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::DataType;
    use datafusion::common::Result as DfResult;
    use datafusion::logical_expr::{ScalarUDFImpl, Signature, Volatility as DfVolatility};
    use std::any::Any;

    /// A toy `ScalarUDFImpl` that adds 10 to its single Int64 input.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct AddTen {
        sig: Signature,
    }

    impl AddTen {
        fn new() -> Self {
            Self {
                sig: Signature::exact(vec![DataType::Int64], DfVolatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for AddTen {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "add_ten"
        }
        fn signature(&self) -> &Signature {
            &self.sig
        }
        fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
            // Materialize the single input as an Int64Array.
            let n = args.number_rows;
            let arr = match args.args.into_iter().next() {
                Some(ColumnarValue::Array(a)) => a,
                Some(ColumnarValue::Scalar(s)) => s.to_array_of_size(n)?,
                None => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        "no args".into(),
                    ))
                }
            };
            let int = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("expected Int64".into())
            })?;
            let out: Int64Array = int.iter().map(|v| v.map(|x| x + 10)).collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
    }

    #[test]
    fn adapter_invokes_underlying_udf() {
        let udf: Arc<dyn ScalarUDFImpl> = Arc::new(AddTen::new());
        let adapter = from_scalar_udf_impl(udf).unwrap();
        assert_eq!(adapter.name(), "add_ten");
        assert_eq!(adapter.signature().args, vec![DataType::Int64]);
        assert_eq!(adapter.signature().return_type, DataType::Int64);

        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let out = adapter.invoke(&[input]).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.values(), &[11, 12, 13]);
    }

    #[test]
    fn adapter_materializes_scalar_return() {
        use datafusion::common::ScalarValue;

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct ConstNinetyNine {
            sig: Signature,
        }
        impl ConstNinetyNine {
            fn new() -> Self {
                Self {
                    sig: Signature::exact(vec![DataType::Int64], DfVolatility::Immutable),
                }
            }
        }
        impl ScalarUDFImpl for ConstNinetyNine {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "const99"
            }
            fn signature(&self) -> &Signature {
                &self.sig
            }
            fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
                Ok(DataType::Int64)
            }
            fn invoke_with_args(&self, _: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(99))))
            }
        }

        let udf: Arc<dyn ScalarUDFImpl> = Arc::new(ConstNinetyNine::new());
        let adapter = from_scalar_udf_impl(udf).unwrap();
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let out = adapter.invoke(&[input]).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.values(), &[99, 99, 99]);
    }

    #[test]
    fn adapter_rejects_non_exact_signature() {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct VariadicUdf {
            sig: Signature,
        }
        impl VariadicUdf {
            fn new() -> Self {
                Self {
                    sig: Signature::variadic(vec![DataType::Int64], DfVolatility::Immutable),
                }
            }
        }
        impl ScalarUDFImpl for VariadicUdf {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "variadic"
            }
            fn signature(&self) -> &Signature {
                &self.sig
            }
            fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
                Ok(DataType::Int64)
            }
            fn invoke_with_args(&self, _: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
                unreachable!("should not be called");
            }
        }

        let udf: Arc<dyn ScalarUDFImpl> = Arc::new(VariadicUdf::new());
        let result = from_scalar_udf_impl(udf);
        assert!(result.is_err(), "expected Err for variadic signature");
        let err = result.err().unwrap();
        assert!(
            err.message.contains("Exact"),
            "error message should mention Exact: {}",
            err.message
        );
    }
}
