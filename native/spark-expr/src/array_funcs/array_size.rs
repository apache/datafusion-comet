use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::cast::as_list_array;
use datafusion_common::{internal_err, DataFusionError, Result as DataFusionResult};
use datafusion_expr_common::columnar_value::ColumnarValue;



#[derive(Debug, Eq)]
pub struct ArraySize {
    src_array_expr: Arc<dyn PhysicalExpr>,
}

impl PartialEq for ArraySize {
    fn eq(&self, other: &Self) -> bool {
        self.src_array_expr.eq(&other.src_array_expr)
    }
}

impl ArraySize {
    pub fn new(src_array_expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { src_array_expr }
    }
}

impl Display for ArraySize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArraySize [array: {:?}]", self.src_array_expr)
    }
}

impl Hash for ArraySize {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.src_array_expr.hash(state);
    }
}

impl PhysicalExpr for ArraySize {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Int32)
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        self.src_array_expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let array_value = self
            .src_array_expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        match array_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&array_value)?;
                let mut builder = Int32Array::builder(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(list_array.value_length(i));
                    }
                }
                let sizes_array = Int32Array::from(builder.finish());
                Ok(ColumnarValue::Array(Arc::new(sizes_array)))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unexpected data type in ArraySize: {:?}",
                array_value.data_type()
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.src_array_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(ArraySize::new(Arc::clone(&children[0])))),
            _ => internal_err!("ArraySize should have exactly one child"),
        }
    }
}
