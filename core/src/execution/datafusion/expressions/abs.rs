use arrow::datatypes::DataType;
use arrow_schema::ArrowError;
use std::{any::Any, sync::Arc};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use datafusion_common::DataFusionError;
use datafusion_functions::math;

use crate::errors::CometError;

use super::EvalMode;

fn arithmetic_overflow_error(from_type: &str) -> CometError {
    CometError::ArithmeticOverflow {
        from_type: from_type.to_string(),
    }
}

#[derive(Debug)]
pub struct CometAbsFunc {
    inner_abs_func: Arc<dyn ScalarUDFImpl>,
    eval_mode: EvalMode,
    data_type_name: String
}

impl CometAbsFunc {
    pub fn new(eval_mode: EvalMode, data_type_name: String) -> Self {
        Self {
            inner_abs_func: math::abs().inner(),
            eval_mode,
            data_type_name
        }
    }
}

impl ScalarUDFImpl for CometAbsFunc {

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "abs"
    }

    fn signature(&self) -> &Signature {
        &self.inner_abs_func.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        self.inner_abs_func.return_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        match self.inner_abs_func.invoke(args) {
            Ok(result) => Ok(result),
            Err(DataFusionError::ArrowError(ArrowError::ComputeError(msg), trace)) 
            if msg.contains("overflow") => {
                if self.eval_mode == EvalMode::Legacy {
                    Ok(args[0].clone())
                } else {
                    let msg = arithmetic_overflow_error(&self.data_type_name).to_string();
                    Err(DataFusionError::ArrowError(ArrowError::ComputeError(msg), trace))
                }
            }
            other => other,
        }
    }
}

