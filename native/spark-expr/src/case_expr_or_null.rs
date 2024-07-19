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

use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::compute::nullif;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};

use datafusion_common::{exec_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, PhysicalExpr};

use crate::utils::down_cast_any_ref;

/// Specialization of `CASE WHEN .. THEN .. ELSE null END` where
/// the else condition is a null literal.
///
/// CaseWhenExprOrNull is only safe to use for expressions that do not
/// have side effects, and it is only suitable to use for expressions
/// that are inexpensive to compute (such as a column reference)
/// because it will be evaluated for all rows in the batch rather
/// than just the rows where the predicate is true.
///
/// The performance advantage of this expression is that it
/// avoids copying data and simply modifies the null bitmask
/// of the evaluated expression based on the inverse of the
/// predicate expression.
#[derive(Debug, Hash)]
pub struct CaseWhenExprOrNull {
    /// The WHEN predicate
    predicate: Arc<dyn PhysicalExpr>,
    /// The THEN expression
    expr: Arc<dyn PhysicalExpr>,
}

impl CaseWhenExprOrNull {
    pub fn new(predicate: Arc<dyn PhysicalExpr>, input: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            predicate,
            expr: input,
        }
    }
}

impl Display for CaseWhenExprOrNull {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ExprOrNull(predicate={}, expr={})",
            self.predicate, self.expr
        )
    }
}

impl DisplayAs for CaseWhenExprOrNull {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ExprOrNull(predicate={}, expr={})",
            self.predicate, self.expr
        )
    }
}

impl PhysicalExpr for CaseWhenExprOrNull {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.expr.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if let ColumnarValue::Array(bit_mask) = self.predicate.evaluate(batch)? {
            let bit_mask = bit_mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("predicate should evaluate to a boolean array");
            // invert the bitmask
            let bit_mask = arrow::compute::kernels::boolean::not(bit_mask)?;
            match self.expr.evaluate(batch)? {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(nullif(&array, &bit_mask)?)),
                ColumnarValue::Scalar(_) => exec_err!("expression did not evaluate to an array"),
            }
        } else {
            exec_err!("predicate did not evaluate to an array")
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.predicate.hash(&mut s);
        self.expr.hash(&mut s);
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for CaseWhenExprOrNull {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.predicate.eq(&x.predicate) && self.expr.eq(&x.expr))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, StringBuilder};
    use arrow_array::{Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, Operator};
    use datafusion_physical_expr::expressions::{BinaryExpr, CaseExpr};
    use datafusion_physical_expr_common::expressions::column::Column;
    use datafusion_physical_expr_common::expressions::Literal;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    use crate::CaseWhenExprOrNull;

    #[test]
    fn test() -> Result<()> {
        // create input data
        let mut c1 = Int32Builder::new();
        let mut c2 = StringBuilder::new();
        for i in 0..1000 {
            c1.append_value(i);
            if i % 7 == 0 {
                c2.append_null();
            } else {
                c2.append_value(&format!("string {i}"));
            }
        }
        let c1 = Arc::new(c1.finish());
        let c2 = Arc::new(c2.finish());
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![c1, c2]).unwrap();

        // CaseWhenExprOrNull should produce same results as CaseExpr
        let predicate = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::LtEq,
            make_lit_i32(250),
        ));
        let expr1 = CaseWhenExprOrNull::new(predicate.clone(), make_col("c2", 1));
        let expr2 = CaseExpr::try_new(None, vec![(predicate, make_col("c2", 1))], None)?;
        match (expr1.evaluate(&batch)?, expr2.evaluate(&batch)?) {
            (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
                assert_eq!(array1.len(), array2.len());
                assert_eq!(array1.null_count(), array2.null_count());
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn make_col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn make_lit_i32(n: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int32(Some(n))))
    }
}
