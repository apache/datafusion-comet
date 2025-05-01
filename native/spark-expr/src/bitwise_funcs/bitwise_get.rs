use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::{error::DataFusionError, logical_expr::ColumnarValue};

pub fn spark_bit_get(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "hex expects exactly two arguments".to_string(),
        ));
    }
    match (&args[0], &args[1]) {
        (ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(position))) => {
            match array.data_type() {
                DataType::Int8 => unimplemented!(),
                DataType::Int16 => unimplemented!(),
                DataType::Int32 => unimplemented!(),
                DataType::Int64 => unimplemented!(),
                _ => Err(DataFusionError::Execution(format!(
                    "Can't be evaluated because the expression's type is {:?}, not signed int",
                    array.data_type(),
                ))),
            }
        }
        _ => unimplemented!(),
    }
}
