use arrow_array::cast::AsArray;
use arrow_array::types::Decimal128Type;
use arrow_array::{ArrayRef, Decimal128Array};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;
use std::sync::Arc;

#[macro_export]
macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC() as i64);
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}

#[inline]
pub(crate) fn make_decimal_scalar(
    a: &Option<i128>,
    precision: u8,
    scale: i8,
    f: &dyn Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let result = ScalarValue::Decimal128(a.map(f), precision, scale);
    Ok(ColumnarValue::Scalar(result))
}

#[inline]
pub(crate) fn make_decimal_array(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    f: &dyn Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let array = array.as_primitive::<Decimal128Type>();
    let result: Decimal128Array = arrow::compute::kernels::arity::unary(array, f);
    let result = result.with_data_type(DataType::Decimal128(precision, scale));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[inline]
pub(crate) fn get_precision_scale(data_type: &DataType) -> (u8, i8) {
    let DataType::Decimal128(precision, scale) = data_type else {
        unreachable!()
    };
    (*precision, *scale)
}
