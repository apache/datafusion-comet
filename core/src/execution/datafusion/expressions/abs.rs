use arrow::datatypes::DataType;
use std::{any::Any, sync::Arc};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use datafusion_common::DataFusionError;
use datafusion_functions::math;

use super::EvalMode;


#[derive(Debug)]
pub struct CometAbsFunc {
    inner_abs_func: Arc<dyn ScalarUDFImpl>,
    eval_mode: EvalMode,
}

impl CometAbsFunc {
    pub fn new(eval_mode: EvalMode) -> Self {
        Self {
            inner_abs_func: math::abs().inner(),
            eval_mode
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
            Err(err) => {
                if self.eval_mode == EvalMode::Legacy {
                    Ok(args[0].clone())
                } else {
                    Err(err)
                }
            }
        }
    }
}

