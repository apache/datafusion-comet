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

use crate::array_funcs::NestedPredicate;
use crate::{
    ArrayInsert, AtLeastNNonNulls, CaseWhenExpr, Cast, CheckedBinaryExpr, CreateNamedStruct,
    FromJson, GetArrayStructFields, GetStructField, IfExpr, ListExtract, NegativeExpr,
    NormalizeNestedFloats, NullShortCircuit, RLike, SparkArraysZipFunc, SparkComparison, Subquery,
    ToCsv, ToJson,
};
use arrow::array::{new_empty_array, Array, ArrayData, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{exec_err, internal_err, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr, Literal, NotExpr,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Evaluates scalar-struct consumers before repeating large variable-width fields.
///
/// GetStructField retains its batch-sized array contract. Constant consumers run on one row;
/// pure row-dependent consumers run on bounded slices before their results are concatenated.
/// Nothing is evaluated during planning, preserving conditional and ANSI NULL short-circuiting.
#[derive(Debug, Eq)]
pub struct ScalarSubqueryConsumer {
    inner: Arc<dyn PhysicalExpr>,
    constant: bool,
    max_rows: usize,
}

// Limit repeated scalar input payload, not the size of the user's final output. A single scalar
// can exceed this budget; in that case consume one row at a time.
const SCALAR_INPUT_BYTES: usize = 1024 * 1024;

impl ScalarSubqueryConsumer {
    pub fn wrap(inner: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        // Keep literals and direct field chains visible so a narrow field can be selected
        // before broadcasting. A field read over a computed struct is itself a consumer.
        if inner.is::<Literal>()
            || inner.is::<Self>()
            || inner
                .downcast_ref::<GetStructField>()
                .is_some_and(|field| field.literal_field().is_some())
        {
            return inner;
        }
        if let Some(properties) = consumer_properties(&inner) {
            if properties.has_scalar_struct
                && (properties.constant || properties.bytes != 0 || properties.unbounded_output)
            {
                return Arc::new(Self {
                    inner,
                    constant: properties.constant,
                    max_rows: if properties.unbounded_output {
                        1
                    } else {
                        (SCALAR_INPUT_BYTES / properties.bytes.max(1)).max(1)
                    },
                });
            }
        }
        inner
    }
}

struct ConsumerProperties {
    constant: bool,
    has_scalar_struct: bool,
    bytes: usize,
    unbounded_output: bool,
}

/// Explicitly recognize pure row-local trees. Unknown nodes and volatile functions cannot safely
/// be evaluated on a different batch. Columns require sliced evaluation.
fn consumer_properties(expr: &Arc<dyn PhysicalExpr>) -> Option<ConsumerProperties> {
    if let Some(value) = expr
        .downcast_ref::<GetStructField>()
        .and_then(GetStructField::literal_field)
    {
        return Some(ConsumerProperties {
            constant: true,
            has_scalar_struct: true,
            bytes: variable_bytes(&value.to_data()),
            unbounded_output: false,
        });
    }
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        let bytes = match literal.value() {
            ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => value.len(),
            ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => value.len(),
            ScalarValue::Struct(value) => variable_bytes(&value.to_data()),
            _ => 0,
        };
        return Some(ConsumerProperties {
            constant: true,
            has_scalar_struct: matches!(literal.value(), ScalarValue::Struct(_)),
            bytes,
            unbounded_output: false,
        });
    }
    // Spark materializes and registers this execution-context value before native execution.
    // Lookup still happens at runtime; it is independent of the current input row or batch.
    if expr.is::<Subquery>() {
        return Some(ConsumerProperties {
            constant: true,
            has_scalar_struct: false,
            bytes: 0,
            unbounded_output: false,
        });
    }
    if expr.is::<Column>() {
        return Some(ConsumerProperties {
            constant: false,
            has_scalar_struct: false,
            bytes: 0,
            unbounded_output: false,
        });
    }
    if let Some(expr) = expr.downcast_ref::<ScalarSubqueryConsumer>() {
        return consumer_properties(&expr.inner);
    }
    if let Some(expr) = expr.downcast_ref::<NullShortCircuit>() {
        return consumer_properties(expr.inner());
    }
    if let Some(fun) = expr.downcast_ref::<ScalarFunctionExpr>() {
        if fun.fun().signature().volatility != Volatility::Immutable {
            return None;
        }
    } else if !(expr.is::<AtLeastNNonNulls>()
        || expr.is::<ArrayInsert>()
        || expr.is::<GetArrayStructFields>()
        || expr.is::<SparkArraysZipFunc>()
        || expr.is::<NestedPredicate>()
        || expr.is::<NormalizeNestedFloats>()
        || expr.is::<FromJson>()
        || expr.is::<ToCsv>()
        || expr.is::<GetStructField>()
        || expr.is::<CastExpr>()
        || expr.is::<Cast>()
        || expr.is::<BinaryExpr>()
        || expr.is::<CheckedBinaryExpr>()
        || expr.is::<NegativeExpr>()
        || expr.is::<SparkComparison>()
        || expr.is::<IsNullExpr>()
        || expr.is::<IsNotNullExpr>()
        || expr.is::<NotExpr>()
        || expr.is::<InListExpr>()
        || expr.is::<LikeExpr>()
        || expr.is::<RLike>()
        || expr.is::<IfExpr>()
        || expr.is::<CaseWhenExpr>()
        || expr.is::<CreateNamedStruct>()
        || expr.is::<ListExtract>()
        || expr.is::<ToJson>())
    {
        return None;
    }
    let mut properties = ConsumerProperties {
        constant: true,
        has_scalar_struct: false,
        bytes: 0,
        // Input payload does not bound outputs such as repeat, padding, JSON or growing lists.
        // Propagate this even from constant children: a row-dependent IF can select them for
        // a whole batch before its parent reduces the result.
        unbounded_output: expr
            .downcast_ref::<ScalarFunctionExpr>()
            .map(ScalarFunctionExpr::return_type)
            .or_else(|| expr.downcast_ref::<Cast>().map(|cast| &cast.data_type))
            .or_else(|| expr.downcast_ref::<CastExpr>().map(CastExpr::cast_type))
            .is_some_and(|dt| {
                matches!(
                    dt,
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView
                ) || dt.is_nested()
            })
            || expr.is::<ToJson>()
            || expr.is::<ToCsv>()
            || expr.is::<FromJson>()
            || expr.is::<ArrayInsert>()
            || expr.is::<SparkArraysZipFunc>()
            || expr
                .downcast_ref::<ListExtract>()
                .is_some_and(|e| e.default_value().is_some()),
    };
    let default_value = expr
        .downcast_ref::<ListExtract>()
        .and_then(ListExtract::default_value);
    for child in expr.children().into_iter().chain(default_value) {
        let child = consumer_properties(child)?;
        properties.constant &= child.constant;
        properties.has_scalar_struct |= child.has_scalar_struct;
        properties.bytes = properties.bytes.saturating_add(child.bytes);
        properties.unbounded_output |= child.unbounded_output;
    }
    // Membership stops when the entire batch has matched. Only constant membership can
    // change batch boundaries without changing evaluation of later ANSI-error candidates.
    if !properties.constant
        && expr
            .downcast_ref::<NestedPredicate>()
            .is_some_and(NestedPredicate::is_membership)
    {
        return None;
    }
    properties.has_scalar_struct |= expr.is::<GetStructField>() && properties.constant;
    Some(properties)
}

fn variable_bytes(data: &ArrayData) -> usize {
    match data.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            data.buffers()[1].len()
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => data.get_buffer_memory_size(),
        _ => data
            .child_data()
            .iter()
            .map(variable_bytes)
            .fold(0, usize::saturating_add),
    }
}

impl Hash for ScalarSubqueryConsumer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl PartialEq for ScalarSubqueryConsumer {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Display for ScalarSubqueryConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScalarSubqueryConsumer({})", self.inner)
    }
}

impl PhysicalExpr for ScalarSubqueryConsumer {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                &self.data_type(batch.schema_ref())?,
            )));
        }
        if !self.constant {
            if rows <= self.max_rows {
                return self.inner.evaluate(batch);
            }
            let mut arrays: Vec<ArrayRef> = Vec::new();
            for offset in (0..rows).step_by(self.max_rows) {
                let len = self.max_rows.min(rows - offset);
                let array = self
                    .inner
                    .evaluate(&batch.slice(offset, len))?
                    .into_array(len)?;
                if array.len() != len {
                    return internal_err!(
                        "Scalar subquery consumer returned {} rows, expected {len}",
                        array.len()
                    );
                }
                arrays.push(array);
            }
            let arrays: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
            return Ok(ColumnarValue::Array(concat(&arrays)?));
        }
        let value = match self.inner.evaluate(&batch.slice(0, 1))? {
            ColumnarValue::Scalar(value) => value,
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    return internal_err!("Scalar subquery consumer returned {} rows", array.len());
                }
                if rows == 1 {
                    return Ok(ColumnarValue::Array(array));
                }
                ScalarValue::try_from_array(array.as_ref(), 0)?
            }
        };
        // A genuinely oversized final string/binary column cannot use 32-bit Arrow offsets.
        // Report it as an execution error rather than panicking in new_repeated.
        let bytes = match &value {
            ScalarValue::Utf8(Some(value)) => value.len(),
            ScalarValue::Binary(Some(value)) => value.len(),
            _ => 0,
        };
        if bytes
            .checked_mul(rows)
            .is_none_or(|n| n > i32::MAX as usize)
        {
            return exec_err!(
                "Scalar subquery consumer result exceeds Arrow's 32-bit offset limit"
            );
        }
        Ok(ColumnarValue::Array(value.to_array_of_size(rows)?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [inner]: [Arc<dyn PhysicalExpr>; 1] = children.try_into().map_err(|_| {
            datafusion::common::internal_datafusion_err!("Expected one scalar subquery consumer")
        })?;
        // Recheck after rewrites: constant and row-dependent consumers need different evaluation.
        Ok(Self::wrap(inner))
    }
}
