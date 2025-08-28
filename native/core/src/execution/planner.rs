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

//! Converts Spark physical plan to DataFusion physical plan

use crate::execution::operators::CopyMode;
use crate::{
    errors::ExpressionError,
    execution::{
        expressions::subquery::Subquery,
        operators::{CopyExec, ExecutionError, ExpandExec, ScanExec},
        serde::to_arrow_datatype,
        shuffle::ShuffleWriterExec,
    },
};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, DECIMAL128_MAX_PRECISION};
use datafusion::functions_aggregate::bit_and_or_xor::{bit_and_udaf, bit_or_udaf, bit_xor_udaf};
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::functions_aggregate::min_max::min_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::InputOrderMode;
use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef},
    common::DataFusionError,
    execution::FunctionRegistry,
    functions_aggregate::first_last::{FirstValue, LastValue},
    logical_expr::Operator as DataFusionOperator,
    physical_expr::{
        expressions::{
            in_list, BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, LikeExpr,
            Literal as DataFusionLiteral, NotExpr,
        },
        PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
    },
    physical_plan::{
        aggregates::{AggregateMode as DFAggregateMode, PhysicalGroupBy},
        joins::{utils::JoinFilter, HashJoinExec, PartitionMode, SortMergeJoinExec},
        limit::LocalLimitExec,
        projection::ProjectionExec,
        sorts::sort::SortExec,
        ExecutionPlan,
    },
    prelude::SessionContext,
};
use datafusion_comet_spark_expr::{
    create_comet_physical_fun, create_modulo_expr, create_negate_expr, BloomFilterAgg,
    BloomFilterMightContain, EvalMode, SparkHour, SparkMinute, SparkSecond,
};

use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::shuffle::{CometPartitioning, CompressionCodec};
use crate::execution::spark_plan::SparkPlan;
use crate::parquet::parquet_support::prepare_object_store_with_configs;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    JoinType as DFJoinType, NullEquality, ScalarValue,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::logical_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion::logical_expr::{
    AggregateUDF, ReturnFieldArgs, ScalarUDF, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition,
};
use datafusion::physical_expr::expressions::{Literal, StatsType};
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_expr::LexOrdering;

use crate::parquet::parquet_exec::init_datasource_exec;
use arrow::array::{
    BinaryBuilder, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, NullArray, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::buffer::BooleanBuffer;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion_comet_proto::spark_operator::SparkFilePartition;
use datafusion_comet_proto::{
    spark_expression::{
        self, agg_expr::ExprStruct as AggExprStruct, expr::ExprStruct, literal::Value, AggExpr,
        Expr, ScalarFunc,
    },
    spark_operator::{
        self, lower_window_frame_bound::LowerFrameBoundStruct, operator::OpStruct,
        upper_window_frame_bound::UpperFrameBoundStruct, BuildSide,
        CompressionCodec as SparkCompressionCodec, JoinType, Operator, WindowFrameType,
    },
    spark_partitioning::{partitioning::PartitioningStruct, Partitioning as SparkPartitioning},
};
use datafusion_comet_spark_expr::monotonically_increasing_id::MonotonicallyIncreasingId;
use datafusion_comet_spark_expr::{
    ArrayInsert, Avg, AvgDecimal, Cast, CheckOverflow, Correlation, Covariance, CreateNamedStruct,
    GetArrayStructFields, GetStructField, IfExpr, ListExtract, NormalizeNaNAndZero, RLike,
    RandExpr, RandnExpr, SparkCastOptions, Stddev, SubstringExpr, SumDecimal, TimestampTruncExpr,
    ToJson, UnboundColumn, Variance,
};
use itertools::Itertools;
use jni::objects::GlobalRef;
use num::{BigInt, ToPrimitive};
use object_store::path::Path;
use std::cmp::max;
use std::{collections::HashMap, sync::Arc};
use url::Url;

// For clippy error on type_complexity.
type PhyAggResult = Result<Vec<AggregateFunctionExpr>, ExecutionError>;
type PhyExprResult = Result<Vec<(Arc<dyn PhysicalExpr>, String)>, ExecutionError>;
type PartitionPhyExprResult = Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError>;

struct JoinParameters {
    pub left: Arc<SparkPlan>,
    pub right: Arc<SparkPlan>,
    pub join_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    pub join_filter: Option<JoinFilter>,
    pub join_type: DFJoinType,
}

#[derive(Default)]
struct BinaryExprOptions {
    pub is_integral_div: bool,
}

pub const TEST_EXEC_CONTEXT_ID: i64 = -1;

/// The query planner for converting Spark query plans to DataFusion query plans.
pub struct PhysicalPlanner {
    // The execution context id of this planner.
    exec_context_id: i64,
    partition: i32,
    session_ctx: Arc<SessionContext>,
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new(Arc::new(SessionContext::new()), 0)
    }
}

impl PhysicalPlanner {
    pub fn new(session_ctx: Arc<SessionContext>, partition: i32) -> Self {
        Self {
            exec_context_id: TEST_EXEC_CONTEXT_ID,
            session_ctx,
            partition,
        }
    }

    pub fn with_exec_id(self, exec_context_id: i64) -> Self {
        Self {
            exec_context_id,
            partition: self.partition,
            session_ctx: Arc::clone(&self.session_ctx),
        }
    }

    /// Return session context of this planner.
    pub fn session_ctx(&self) -> &Arc<SessionContext> {
        &self.session_ctx
    }

    /// get DataFusion PartitionedFiles from a Spark FilePartition
    fn get_partitioned_files(
        &self,
        partition: &SparkFilePartition,
    ) -> Result<Vec<PartitionedFile>, ExecutionError> {
        let mut files = Vec::with_capacity(partition.partitioned_file.len());
        partition.partitioned_file.iter().try_for_each(|file| {
            assert!(file.start + file.length <= file.file_size);

            let mut partitioned_file = PartitionedFile::new_with_range(
                String::new(), // Dummy file path.
                file.file_size as u64,
                file.start,
                file.start + file.length,
            );

            // Spark sends the path over as URL-encoded, parse that first.
            let url =
                Url::parse(file.file_path.as_ref()).map_err(|e| GeneralError(e.to_string()))?;
            // Convert that to a Path object to use in the PartitionedFile.
            let path = Path::from_url_path(url.path()).map_err(|e| GeneralError(e.to_string()))?;
            partitioned_file.object_meta.location = path;

            // Process partition values
            // Create an empty input schema for partition values because they are all literals.
            let empty_schema = Arc::new(Schema::empty());
            let partition_values: Result<Vec<_>, _> = file
                .partition_values
                .iter()
                .map(|partition_value| {
                    let literal =
                        self.create_expr(partition_value, Arc::<Schema>::clone(&empty_schema))?;
                    literal
                        .as_any()
                        .downcast_ref::<DataFusionLiteral>()
                        .ok_or_else(|| {
                            GeneralError("Expected literal of partition value".to_string())
                        })
                        .map(|literal| literal.value().clone())
                })
                .collect();
            let partition_values = partition_values?;

            partitioned_file.partition_values = partition_values;

            files.push(partitioned_file);
            Ok::<(), ExecutionError>(())
        })?;

        Ok(files)
    }

    /// Create a DataFusion physical expression from Spark physical expression
    pub(crate) fn create_expr(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            ExprStruct::Add(expr) => {
                // TODO respect ANSI eval mode
                // https://github.com/apache/datafusion-comet/issues/536
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                self.create_binary_expr(
                    expr.left.as_ref().unwrap(),
                    expr.right.as_ref().unwrap(),
                    expr.return_type.as_ref(),
                    DataFusionOperator::Plus,
                    input_schema,
                    eval_mode,
                )
            }
            ExprStruct::Subtract(expr) => {
                // TODO respect ANSI eval mode
                // https://github.com/apache/datafusion-comet/issues/535
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                self.create_binary_expr(
                    expr.left.as_ref().unwrap(),
                    expr.right.as_ref().unwrap(),
                    expr.return_type.as_ref(),
                    DataFusionOperator::Minus,
                    input_schema,
                    eval_mode,
                )
            }
            ExprStruct::Multiply(expr) => {
                // TODO respect ANSI eval mode
                // https://github.com/apache/datafusion-comet/issues/534
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                self.create_binary_expr(
                    expr.left.as_ref().unwrap(),
                    expr.right.as_ref().unwrap(),
                    expr.return_type.as_ref(),
                    DataFusionOperator::Multiply,
                    input_schema,
                    eval_mode,
                )
            }
            ExprStruct::Divide(expr) => {
                // TODO respect ANSI eval mode
                // https://github.com/apache/datafusion-comet/issues/533
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                self.create_binary_expr(
                    expr.left.as_ref().unwrap(),
                    expr.right.as_ref().unwrap(),
                    expr.return_type.as_ref(),
                    DataFusionOperator::Divide,
                    input_schema,
                    eval_mode,
                )
            }
            ExprStruct::IntegralDivide(expr) => {
                // TODO respect eval mode
                // https://github.com/apache/datafusion-comet/issues/533
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                self.create_binary_expr_with_options(
                    expr.left.as_ref().unwrap(),
                    expr.right.as_ref().unwrap(),
                    expr.return_type.as_ref(),
                    DataFusionOperator::Divide,
                    input_schema,
                    BinaryExprOptions {
                        is_integral_div: true,
                    },
                    eval_mode,
                )
            }
            ExprStruct::Remainder(expr) => {
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                // TODO add support for EvalMode::TRY
                // https://github.com/apache/datafusion-comet/issues/2021
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right =
                    self.create_expr(expr.right.as_ref().unwrap(), Arc::clone(&input_schema))?;

                let result = create_modulo_expr(
                    left,
                    right,
                    expr.return_type.as_ref().map(to_arrow_datatype).unwrap(),
                    input_schema,
                    eval_mode == EvalMode::Ansi,
                    &self.session_ctx.state(),
                );
                result.map_err(|e| GeneralError(e.to_string()))
            }
            ExprStruct::Eq(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Eq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Neq(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::NotEq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Gt(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Gt;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::GtEq(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::GtEq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Lt(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Lt;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::LtEq(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::LtEq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Bound(bound) => {
                let idx = bound.index as usize;
                if idx >= input_schema.fields().len() {
                    return Err(GeneralError(format!(
                        "Column index {idx} is out of bound. Schema: {input_schema}"
                    )));
                }
                let field = input_schema.field(idx);
                Ok(Arc::new(Column::new(field.name().as_str(), idx)))
            }
            ExprStruct::Unbound(unbound) => {
                let data_type = to_arrow_datatype(unbound.datatype.as_ref().unwrap());
                Ok(Arc::new(UnboundColumn::new(
                    unbound.name.as_str(),
                    data_type,
                )))
            }
            ExprStruct::IsNotNull(is_notnull) => {
                let child = self.create_expr(is_notnull.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(IsNotNullExpr::new(child)))
            }
            ExprStruct::IsNull(is_null) => {
                let child = self.create_expr(is_null.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(IsNullExpr::new(child)))
            }
            ExprStruct::And(and) => {
                let left =
                    self.create_expr(and.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(and.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::And;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Or(or) => {
                let left =
                    self.create_expr(or.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(or.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Or;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Literal(literal) => {
                let data_type = to_arrow_datatype(literal.datatype.as_ref().unwrap());
                let scalar_value = if literal.is_null {
                    match data_type {
                        DataType::Boolean => ScalarValue::Boolean(None),
                        DataType::Int8 => ScalarValue::Int8(None),
                        DataType::Int16 => ScalarValue::Int16(None),
                        DataType::Int32 => ScalarValue::Int32(None),
                        DataType::Int64 => ScalarValue::Int64(None),
                        DataType::Float32 => ScalarValue::Float32(None),
                        DataType::Float64 => ScalarValue::Float64(None),
                        DataType::Utf8 => ScalarValue::Utf8(None),
                        DataType::Date32 => ScalarValue::Date32(None),
                        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                            ScalarValue::TimestampMicrosecond(None, timezone)
                        }
                        DataType::Binary => ScalarValue::Binary(None),
                        DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, p, s),
                        DataType::Struct(fields) => ScalarStructBuilder::new_null(fields),
                        DataType::Map(f, s) => DataType::Map(f, s).try_into()?,
                        DataType::List(f) => DataType::List(f).try_into()?,
                        DataType::Null => ScalarValue::Null,
                        dt => {
                            return Err(GeneralError(format!("{dt:?} is not supported in Comet")))
                        }
                    }
                } else {
                    match literal.value.as_ref().unwrap() {
                        Value::BoolVal(value) => ScalarValue::Boolean(Some(*value)),
                        Value::ByteVal(value) => ScalarValue::Int8(Some(*value as i8)),
                        Value::ShortVal(value) => ScalarValue::Int16(Some(*value as i16)),
                        Value::IntVal(value) => match data_type {
                            DataType::Int32 => ScalarValue::Int32(Some(*value)),
                            DataType::Date32 => ScalarValue::Date32(Some(*value)),
                            dt => {
                                return Err(GeneralError(format!(
                                    "Expected either 'Int32' or 'Date32' for IntVal, but found {dt:?}"
                                )))
                            }
                        },
                        Value::LongVal(value) => match data_type {
                            DataType::Int64 => ScalarValue::Int64(Some(*value)),
                            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                                ScalarValue::TimestampMicrosecond(Some(*value), None)
                            }
                            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                                ScalarValue::TimestampMicrosecond(Some(*value), Some(tz))
                            }
                            dt => {
                                return Err(GeneralError(format!(
                                    "Expected either 'Int64' or 'Timestamp' for LongVal, but found {dt:?}"
                                )))
                            }
                        },
                        Value::FloatVal(value) => ScalarValue::Float32(Some(*value)),
                        Value::DoubleVal(value) => ScalarValue::Float64(Some(*value)),
                        Value::StringVal(value) => ScalarValue::Utf8(Some(value.clone())),
                        Value::BytesVal(value) => ScalarValue::Binary(Some(value.clone())),
                        Value::DecimalVal(value) => {
                            let big_integer = BigInt::from_signed_bytes_be(value);
                            let integer = big_integer.to_i128().ok_or_else(|| {
                                GeneralError(format!(
                                    "Cannot parse {big_integer:?} as i128 for Decimal literal"
                                ))
                            })?;

                            match data_type {
                                DataType::Decimal128(p, s) => {
                                    ScalarValue::Decimal128(Some(integer), p, s)
                                }
                                dt => {
                                    return Err(GeneralError(format!(
                                        "Decimal literal's data type should be Decimal128 but got {dt:?}"
                                    )))
                                }
                            }
                        },
                        Value::ListVal(values) => {
                            if let DataType::List(f) = data_type {
                                match f.data_type() {
                                    DataType::Null => {
                                        SingleRowListArrayBuilder::new(Arc::new(NullArray::new(values.clone().null_mask.len())))
                                            .build_list_scalar()
                                    }
                                    DataType::Boolean => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(BooleanArray::new(BooleanBuffer::from(vals.boolean_values), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Int8 => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Int8Array::new(vals.byte_values.iter().map(|&x| x as i8).collect::<Vec<_>>().into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Int16 => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Int16Array::new(vals.short_values.iter().map(|&x| x as i16).collect::<Vec<_>>().into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Int32 => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Int32Array::new(vals.int_values.into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Int64 => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Int64Array::new(vals.long_values.into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Float32 => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Float32Array::new(vals.float_values.into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Float64 => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Float64Array::new(vals.double_values.into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Timestamp(TimeUnit::Microsecond, None) => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(TimestampMicrosecondArray::new(vals.long_values.into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(TimestampMicrosecondArray::new(vals.long_values.into(), Some(vals.null_mask.into())).with_timezone(Arc::clone(tz))))
                                            .build_list_scalar()
                                    }
                                    DataType::Date32 => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Date32Array::new(vals.int_values.into(), Some(vals.null_mask.into()))))
                                            .build_list_scalar()
                                    }
                                    DataType::Binary => {
                                        // Using a builder as it is cumbersome to create BinaryArray from a vector with nulls
                                        // and calculate correct offsets
                                        let vals = values.clone();
                                        let item_capacity = vals.string_values.len();
                                        let data_capacity = vals.string_values.first().map(|s| s.len() * item_capacity).unwrap_or(0);
                                        let mut arr = BinaryBuilder::with_capacity(item_capacity, data_capacity);

                                        for (i, v) in vals.bytes_values.into_iter().enumerate() {
                                            if vals.null_mask[i] {
                                                arr.append_value(v);
                                            } else {
                                                arr.append_null();
                                            }
                                        }

                                        SingleRowListArrayBuilder::new(Arc::new(arr.finish()))
                                            .build_list_scalar()
                                    }
                                    DataType::Utf8 => {
                                        // Using a builder as it is cumbersome to create StringArray from a vector with nulls
                                        // and calculate correct offsets
                                        let vals = values.clone();
                                        let item_capacity = vals.string_values.len();
                                        let data_capacity = vals.string_values.first().map(|s| s.len() * item_capacity).unwrap_or(0);
                                        let mut arr = StringBuilder::with_capacity(item_capacity, data_capacity);

                                        for (i, v) in vals.string_values.into_iter().enumerate() {
                                            if vals.null_mask[i] {
                                                arr.append_value(v);
                                            } else {
                                                arr.append_null();
                                            }
                                        }

                                        SingleRowListArrayBuilder::new(Arc::new(arr.finish()))
                                            .build_list_scalar()
                                    }
                                    DataType::Decimal128(p, s) => {
                                        let vals = values.clone();
                                        SingleRowListArrayBuilder::new(Arc::new(Decimal128Array::new(vals.decimal_values.into_iter().map(|v| {
                                            let big_integer = BigInt::from_signed_bytes_be(&v);
                                            big_integer.to_i128().ok_or_else(|| {
                                                GeneralError(format!(
                                                    "Cannot parse {big_integer:?} as i128 for Decimal literal"
                                                ))
                                            }).unwrap()
                                        }).collect::<Vec<_>>().into(), Some(vals.null_mask.into())).with_precision_and_scale(*p, *s)?)).build_list_scalar()
                                    }
                                    dt => {
                                        return Err(GeneralError(format!(
                                            "DataType::List literal does not support {dt:?} type"
                                        )))
                                    }
                                }

                            } else {
                                return Err(GeneralError(format!(
                                    "Expected DataType::List but got {data_type:?}"
                                )))
                            }
                        }
                    }
                };
                Ok(Arc::new(DataFusionLiteral::new(scalar_value)))
            }
            ExprStruct::Cast(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                Ok(Arc::new(Cast::new(
                    child,
                    datatype,
                    SparkCastOptions::new(eval_mode, &expr.timezone, expr.allow_incompat),
                )))
            }
            ExprStruct::Hour(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let timezone = expr.timezone.clone();
                let args = vec![child];
                let comet_hour = Arc::new(ScalarUDF::new_from_impl(SparkHour::new(timezone)));
                let field_ref = Arc::new(Field::new("hour", DataType::Int32, true));
                let expr: ScalarFunctionExpr =
                    ScalarFunctionExpr::new("hour", comet_hour, args, field_ref);

                Ok(Arc::new(expr))
            }
            ExprStruct::Minute(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let timezone = expr.timezone.clone();
                let args = vec![child];
                let comet_minute = Arc::new(ScalarUDF::new_from_impl(SparkMinute::new(timezone)));
                let field_ref = Arc::new(Field::new("minute", DataType::Int32, true));
                let expr: ScalarFunctionExpr =
                    ScalarFunctionExpr::new("minute", comet_minute, args, field_ref);

                Ok(Arc::new(expr))
            }
            ExprStruct::Second(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let timezone = expr.timezone.clone();
                let args = vec![child];
                let comet_second = Arc::new(ScalarUDF::new_from_impl(SparkSecond::new(timezone)));
                let field_ref = Arc::new(Field::new("second", DataType::Int32, true));
                let expr: ScalarFunctionExpr =
                    ScalarFunctionExpr::new("second", comet_second, args, field_ref);

                Ok(Arc::new(expr))
            }
            ExprStruct::TruncTimestamp(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let format = self.create_expr(expr.format.as_ref().unwrap(), input_schema)?;
                let timezone = expr.timezone.clone();

                Ok(Arc::new(TimestampTruncExpr::new(child, format, timezone)))
            }
            ExprStruct::Substring(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                // Spark Substring's start is 1-based when start > 0
                let start = expr.start - i32::from(expr.start > 0);
                // substring negative len is treated as 0 in Spark
                let len = max(expr.len, 0);

                Ok(Arc::new(SubstringExpr::new(
                    child,
                    start as i64,
                    len as u64,
                )))
            }
            ExprStruct::Like(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

                Ok(Arc::new(LikeExpr::new(false, false, left, right)))
            }
            ExprStruct::Rlike(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                match right.as_any().downcast_ref::<Literal>().unwrap().value() {
                    ScalarValue::Utf8(Some(pattern)) => {
                        Ok(Arc::new(RLike::try_new(left, pattern)?))
                    }
                    _ => Err(GeneralError(
                        "RLike only supports scalar patterns".to_string(),
                    )),
                }
            }
            ExprStruct::CheckOverflow(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let fail_on_error = expr.fail_on_error;

                Ok(Arc::new(CheckOverflow::new(
                    child,
                    data_type,
                    fail_on_error,
                )))
            }
            ExprStruct::ScalarFunc(expr) => {
                let func = self.create_scalar_function_expr(expr, input_schema);
                match expr.func.as_ref() {
                    // DataFusion map_extract returns array of struct entries even if lookup by key
                    // Apache Spark wants a single value, so wrap the result into additional list extraction
                    "map_extract" => Ok(Arc::new(ListExtract::new(
                        func?,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                        None,
                        true,
                        false,
                    ))),
                    // DataFusion 49 hardcodes return type for MD5 built in function as UTF8View
                    // which is not yet supported in Comet
                    // Converting forcibly to UTF8. To be removed after UTF8View supported
                    "md5" => Ok(Arc::new(Cast::new(
                        func?,
                        DataType::Utf8,
                        SparkCastOptions::new_without_timezone(EvalMode::Try, true),
                    ))),
                    _ => func,
                }
            }
            ExprStruct::EqNullSafe(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::IsNotDistinctFrom;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::NeqNullSafe(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::IsDistinctFrom;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseAnd(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseAnd;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseOr(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseOr;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseXor(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseXor;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseShiftRight(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseShiftRight;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseShiftLeft(expr) => {
                let left =
                    self.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseShiftLeft;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            // https://github.com/apache/datafusion-comet/issues/666
            // ExprStruct::Abs(expr) => {
            //     let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
            //     let return_type = child.data_type(&input_schema)?;
            //     let args = vec![child];
            //     let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
            //     let comet_abs = Arc::new(ScalarUDF::new_from_impl(Abs::new(
            //         eval_mode,
            //         return_type.to_string(),
            //     )?));
            //     let expr = ScalarFunctionExpr::new("abs", comet_abs, args, return_type);
            //     Ok(Arc::new(expr))
            // }
            ExprStruct::CaseWhen(case_when) => {
                let when_then_pairs = case_when
                    .when
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .zip(
                        case_when
                            .then
                            .iter()
                            .map(|then| self.create_expr(then, Arc::clone(&input_schema))),
                    )
                    .try_fold(Vec::new(), |mut acc, (a, b)| {
                        acc.push((a?, b?));
                        Ok::<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>, ExecutionError>(
                            acc,
                        )
                    })?;

                let else_phy_expr = match &case_when.else_expr {
                    None => None,
                    Some(_) => Some(self.create_expr(
                        case_when.else_expr.as_ref().unwrap(),
                        Arc::clone(&input_schema),
                    )?),
                };

                create_case_expr(when_then_pairs, else_phy_expr, &input_schema)
            }
            ExprStruct::In(expr) => {
                let value =
                    self.create_expr(expr.in_value.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let list = expr
                    .lists
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                in_list(value, list, &expr.negated, input_schema.as_ref()).map_err(|e| e.into())
            }
            ExprStruct::If(expr) => {
                let if_expr =
                    self.create_expr(expr.if_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let true_expr =
                    self.create_expr(expr.true_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let false_expr =
                    self.create_expr(expr.false_expr.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(IfExpr::new(if_expr, true_expr, false_expr)))
            }
            ExprStruct::Not(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(NotExpr::new(child)))
            }
            ExprStruct::UnaryMinus(expr) => {
                let child: Arc<dyn PhysicalExpr> =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let result = create_negate_expr(child, expr.fail_on_error);
                result.map_err(|e| GeneralError(e.to_string()))
            }
            ExprStruct::NormalizeNanAndZero(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(NormalizeNaNAndZero::new(data_type, child)))
            }
            ExprStruct::Subquery(expr) => {
                let id = expr.id;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(Subquery::new(self.exec_context_id, id, data_type)))
            }
            ExprStruct::BloomFilterMightContain(expr) => {
                let bloom_filter_expr = self.create_expr(
                    expr.bloom_filter.as_ref().unwrap(),
                    Arc::clone(&input_schema),
                )?;

                // We only provide the values as argument, the bloom filter is created only in plan time.
                let value_expr = self.create_expr(expr.value.as_ref().unwrap(), input_schema)?;
                let args = vec![value_expr];
                let udf =
                    ScalarUDF::new_from_impl(BloomFilterMightContain::try_new(bloom_filter_expr)?);

                let field_ref = Arc::new(Field::new("might_contain", DataType::Boolean, true));
                let expr: ScalarFunctionExpr =
                    ScalarFunctionExpr::new("might_contain", Arc::new(udf), args, field_ref);
                Ok(Arc::new(expr))
            }
            ExprStruct::CreateNamedStruct(expr) => {
                let values = expr
                    .values
                    .iter()
                    .map(|expr| self.create_expr(expr, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;
                let names = expr.names.clone();
                Ok(Arc::new(CreateNamedStruct::new(values, names)))
            }
            ExprStruct::GetStructField(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                Ok(Arc::new(GetStructField::new(child, expr.ordinal as usize)))
            }
            ExprStruct::ToJson(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(ToJson::new(child, &expr.timezone)))
            }
            ExprStruct::ToPrettyString(expr) => {
                let mut spark_cast_options =
                    SparkCastOptions::new(EvalMode::Try, &expr.timezone, true);
                let null_string = "NULL";
                spark_cast_options.null_string = null_string.to_string();
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let cast = Arc::new(Cast::new(
                    Arc::clone(&child),
                    DataType::Utf8,
                    spark_cast_options,
                ));
                Ok(Arc::new(IfExpr::new(
                    Arc::new(IsNullExpr::new(child)),
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(
                        null_string.to_string(),
                    )))),
                    cast,
                )))
            }
            ExprStruct::ListExtract(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let ordinal =
                    self.create_expr(expr.ordinal.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let default_value = expr
                    .default_value
                    .as_ref()
                    .map(|e| self.create_expr(e, Arc::clone(&input_schema)))
                    .transpose()?;
                Ok(Arc::new(ListExtract::new(
                    child,
                    ordinal,
                    default_value,
                    expr.one_based,
                    expr.fail_on_error,
                )))
            }
            ExprStruct::GetArrayStructFields(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;

                Ok(Arc::new(GetArrayStructFields::new(
                    child,
                    expr.ordinal as usize,
                )))
            }
            ExprStruct::ArrayInsert(expr) => {
                let src_array_expr = self.create_expr(
                    expr.src_array_expr.as_ref().unwrap(),
                    Arc::clone(&input_schema),
                )?;
                let pos_expr =
                    self.create_expr(expr.pos_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let item_expr =
                    self.create_expr(expr.item_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                Ok(Arc::new(ArrayInsert::new(
                    src_array_expr,
                    pos_expr,
                    item_expr,
                    expr.legacy_negative_index,
                )))
            }
            ExprStruct::Rand(expr) => {
                let seed = expr.seed.wrapping_add(self.partition.into());
                Ok(Arc::new(RandExpr::new(seed)))
            }
            ExprStruct::Randn(expr) => {
                let seed = expr.seed.wrapping_add(self.partition.into());
                Ok(Arc::new(RandnExpr::new(seed)))
            }
            ExprStruct::SparkPartitionId(_) => Ok(Arc::new(DataFusionLiteral::new(
                ScalarValue::Int32(Some(self.partition)),
            ))),
            ExprStruct::MonotonicallyIncreasingId(_) => Ok(Arc::new(
                MonotonicallyIncreasingId::from_partition_id(self.partition),
            )),
            expr => Err(GeneralError(format!("Not implemented: {expr:?}"))),
        }
    }

    /// Create a DataFusion physical sort expression from Spark physical expression
    fn create_sort_expr<'a>(
        &'a self,
        spark_expr: &'a Expr,
        input_schema: SchemaRef,
    ) -> Result<PhysicalSortExpr, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            ExprStruct::SortOrder(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let descending = expr.direction == 1;
                let nulls_first = expr.null_ordering == 0;

                let options = SortOptions {
                    descending,
                    nulls_first,
                };

                Ok(PhysicalSortExpr {
                    expr: child,
                    options,
                })
            }
            expr => Err(GeneralError(format!("{expr:?} isn't a SortOrder"))),
        }
    }

    fn create_binary_expr(
        &self,
        left: &Expr,
        right: &Expr,
        return_type: Option<&spark_expression::DataType>,
        op: DataFusionOperator,
        input_schema: SchemaRef,
        eval_mode: EvalMode,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        self.create_binary_expr_with_options(
            left,
            right,
            return_type,
            op,
            input_schema,
            BinaryExprOptions::default(),
            eval_mode,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_binary_expr_with_options(
        &self,
        left: &Expr,
        right: &Expr,
        return_type: Option<&spark_expression::DataType>,
        op: DataFusionOperator,
        input_schema: SchemaRef,
        options: BinaryExprOptions,
        eval_mode: EvalMode,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let left = self.create_expr(left, Arc::clone(&input_schema))?;
        let right = self.create_expr(right, Arc::clone(&input_schema))?;
        match (
            &op,
            left.data_type(&input_schema),
            right.data_type(&input_schema),
        ) {
            (
                DataFusionOperator::Plus | DataFusionOperator::Minus | DataFusionOperator::Multiply,
                Ok(DataType::Decimal128(p1, s1)),
                Ok(DataType::Decimal128(p2, s2)),
            ) if ((op == DataFusionOperator::Plus || op == DataFusionOperator::Minus)
                && max(s1, s2) as u8 + max(p1 - s1 as u8, p2 - s2 as u8)
                    >= DECIMAL128_MAX_PRECISION)
                || (op == DataFusionOperator::Multiply && p1 + p2 >= DECIMAL128_MAX_PRECISION) =>
            {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
                // For some Decimal128 operations, we need wider internal digits.
                // Cast left and right to Decimal256 and cast the result back to Decimal128
                let left = Arc::new(Cast::new(
                    left,
                    DataType::Decimal256(p1, s1),
                    SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
                ));
                let right = Arc::new(Cast::new(
                    right,
                    DataType::Decimal256(p2, s2),
                    SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
                ));
                let child = Arc::new(BinaryExpr::new(left, op, right));
                Ok(Arc::new(Cast::new(
                    child,
                    data_type,
                    SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
                )))
            }
            (
                DataFusionOperator::Divide,
                Ok(DataType::Decimal128(_p1, _s1)),
                Ok(DataType::Decimal128(_p2, _s2)),
            ) => {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
                let func_name = if options.is_integral_div {
                    // Decimal256 division in Arrow may overflow, so we still need this variant of decimal_div.
                    // Otherwise, we may be able to reuse the previous case-match instead of here,
                    // see more: https://github.com/apache/datafusion-comet/pull/1428#discussion_r1972648463
                    "decimal_integral_div"
                } else {
                    "decimal_div"
                };
                let fun_expr = create_comet_physical_fun(
                    func_name,
                    data_type.clone(),
                    &self.session_ctx.state(),
                    None,
                )?;
                Ok(Arc::new(ScalarFunctionExpr::new(
                    func_name,
                    fun_expr,
                    vec![left, right],
                    Arc::new(Field::new(func_name, data_type, true)),
                )))
            }
            _ => {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
                if eval_mode == EvalMode::Try && data_type.is_integer() {
                    let op_str = match op {
                        DataFusionOperator::Plus => "checked_add",
                        DataFusionOperator::Minus => "checked_sub",
                        DataFusionOperator::Multiply => "checked_mul",
                        DataFusionOperator::Divide => "checked_div",
                        _ => {
                            todo!("Operator yet to be implemented!");
                        }
                    };
                    let fun_expr = create_comet_physical_fun(
                        op_str,
                        data_type.clone(),
                        &self.session_ctx.state(),
                        None,
                    )?;
                    Ok(Arc::new(ScalarFunctionExpr::new(
                        op_str,
                        fun_expr,
                        vec![left, right],
                        Arc::new(Field::new(op_str, data_type, true)),
                    )))
                } else {
                    Ok(Arc::new(BinaryExpr::new(left, op, right)))
                }
            }
        }
    }

    /// Create a DataFusion physical plan from Spark physical plan. There is a level of
    /// abstraction where a tree of SparkPlan nodes is returned. There is a 1:1 mapping from a
    /// protobuf Operator (that represents a Spark operator) to a native SparkPlan struct. We
    /// need this 1:1 mapping so that we can report metrics back to Spark. The native execution
    /// plan that is generated for each Operator is sometimes a single ExecutionPlan, but in some
    /// cases we generate a tree of ExecutionPlans and we need to collect metrics for all of these
    /// plans so we store references to them in the SparkPlan struct.
    ///
    /// `inputs` is a vector of input source IDs. It is used to create `ScanExec`s. Each `ScanExec`
    /// will be assigned a unique ID from `inputs` and the ID will be used to identify the input
    /// source at JNI API.
    ///
    /// Note that `ScanExec` will pull initial input batch during initialization. It is because we
    /// need to know the exact schema (not only data type but also dictionary-encoding) at
    /// `ScanExec`s. It is because some DataFusion operators, e.g., `ProjectionExec`, gets child
    /// operator schema during initialization and uses it later for `RecordBatch`. We may be
    /// able to get rid of it once `RecordBatch` relaxes schema check.
    ///
    /// Note that we return created `Scan`s which will be kept at JNI API. JNI calls will use it to
    /// feed in new input batch from Spark JVM side.
    pub(crate) fn create_plan<'a>(
        &'a self,
        spark_plan: &'a Operator,
        inputs: &mut Vec<Arc<GlobalRef>>,
        partition_count: usize,
    ) -> Result<(Vec<ScanExec>, Arc<SparkPlan>), ExecutionError> {
        let children = &spark_plan.children;
        match spark_plan.op_struct.as_ref().unwrap() {
            OpStruct::Projection(project) => {
                assert_eq!(children.len(), 1);
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;
                let exprs: PhyExprResult = project
                    .project_list
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, child.schema())
                            .map(|r| (r, format!("col_{idx}")))
                    })
                    .collect();
                let projection = Arc::new(ProjectionExec::try_new(
                    exprs?,
                    Arc::clone(&child.native_plan),
                )?);
                Ok((
                    scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, projection, vec![child])),
                ))
            }
            OpStruct::Filter(filter) => {
                assert_eq!(children.len(), 1);
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;
                let predicate =
                    self.create_expr(filter.predicate.as_ref().unwrap(), child.schema())?;

                let filter: Arc<dyn ExecutionPlan> = if filter.wrap_child_in_copy_exec {
                    Arc::new(FilterExec::try_new(
                        predicate,
                        Self::wrap_in_copy_exec(Arc::clone(&child.native_plan)),
                    )?)
                } else {
                    Arc::new(FilterExec::try_new(
                        predicate,
                        Arc::clone(&child.native_plan),
                    )?)
                };

                Ok((
                    scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, filter, vec![child])),
                ))
            }
            OpStruct::HashAgg(agg) => {
                assert_eq!(children.len(), 1);
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;

                let group_exprs: PhyExprResult = agg
                    .grouping_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, child.schema())
                            .map(|r| (r, format!("col_{idx}")))
                    })
                    .collect();

                let mut map_converter =
                    crate::execution::utils::HashAggregateMapConverter::default();

                // Currently DataFusion does not support grouping on Map type, as such pass the
                // `group_exprs` through `maybe_wrap_map_type_in_grouping_exprs` which canonicalizes
                // any Map type to a List of Struct types for grouping.
                let maybe_wrapped_group_exprs = map_converter
                    .maybe_wrap_map_type_in_grouping_exprs(
                        &self.session_ctx.state(),
                        group_exprs?,
                        child.schema(),
                    )?;

                let group_by = PhysicalGroupBy::new_single(maybe_wrapped_group_exprs);
                let schema = child.schema();

                let mode = if agg.mode == 0 {
                    DFAggregateMode::Partial
                } else {
                    DFAggregateMode::Final
                };

                let agg_exprs: PhyAggResult = agg
                    .agg_exprs
                    .iter()
                    .map(|expr| self.create_agg_expr(expr, Arc::clone(&schema)))
                    .collect();

                let num_agg = agg.agg_exprs.len();
                let aggr_expr = agg_exprs?.into_iter().map(Arc::new).collect();
                let aggregate: Arc<dyn ExecutionPlan> = Arc::new(
                    datafusion::physical_plan::aggregates::AggregateExec::try_new(
                        mode,
                        group_by,
                        aggr_expr,
                        vec![None; num_agg], // no filter expressions
                        Arc::clone(&child.native_plan),
                        Arc::clone(&schema),
                    )?,
                );

                // To maintain schema consistency, the `AggregateExec` output is passed through
                // `maybe_project_map_type_with_aggregation` which adds a projection that converts
                // any canonicalized Map back to its original Map type. Not doing so will
                // result in schema mismatch between Spark and DataFusion.
                let maybe_aggregate_with_project = map_converter
                    .maybe_project_map_type_with_aggregation(
                        &self.session_ctx.state(),
                        agg,
                        aggregate,
                    )?;

                let result_exprs: PhyExprResult = agg
                    .result_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, maybe_aggregate_with_project.schema())
                            .map(|r| (r, format!("col_{idx}")))
                    })
                    .collect();

                if agg.result_exprs.is_empty() {
                    Ok((
                        scans,
                        Arc::new(SparkPlan::new(
                            spark_plan.plan_id,
                            maybe_aggregate_with_project,
                            vec![child],
                        )),
                    ))
                } else {
                    // For final aggregation, DF's hash aggregate exec doesn't support Spark's
                    // aggregate result expressions like `COUNT(col) + 1`, but instead relying
                    // on additional `ProjectionExec` to handle the case. Therefore, here we'll
                    // add a projection node on top of the aggregate node.
                    //
                    // Note that `result_exprs` should only be set for final aggregation on the
                    // Spark side.
                    let projection = Arc::new(ProjectionExec::try_new(
                        result_exprs?,
                        Arc::clone(&maybe_aggregate_with_project),
                    )?);
                    Ok((
                        scans,
                        Arc::new(SparkPlan::new_with_additional(
                            spark_plan.plan_id,
                            projection,
                            vec![child],
                            vec![maybe_aggregate_with_project],
                        )),
                    ))
                }
            }
            OpStruct::Limit(limit) => {
                assert_eq!(children.len(), 1);
                let num = limit.limit;
                let offset: i32 = limit.offset;
                if num != -1 && offset > num {
                    return Err(GeneralError(format!(
                        "Invalid limit/offset combination: [{num}. {offset}]"
                    )));
                }
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;
                let limit: Arc<dyn ExecutionPlan> = if offset == 0 {
                    Arc::new(LocalLimitExec::new(
                        Arc::clone(&child.native_plan),
                        num as usize,
                    ))
                } else {
                    let fetch = if num == -1 {
                        None
                    } else {
                        Some((num - offset) as usize)
                    };
                    Arc::new(GlobalLimitExec::new(
                        Arc::clone(&child.native_plan),
                        offset as usize,
                        fetch,
                    ))
                };
                Ok((
                    scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, limit, vec![child])),
                ))
            }
            OpStruct::Sort(sort) => {
                assert_eq!(children.len(), 1);
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;

                let exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = sort
                    .sort_orders
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, child.schema()))
                    .collect();

                let fetch = sort.fetch.map(|num| num as usize);
                // SortExec caches batches so we need to make a copy of incoming batches. Also,
                // SortExec fails in some cases if we do not unpack dictionary-encoded arrays, and
                // it would be more efficient if we could avoid that.
                // https://github.com/apache/datafusion-comet/issues/963
                let child_copied = Self::wrap_in_copy_exec(Arc::clone(&child.native_plan));

                let mut sort_exec: Arc<dyn ExecutionPlan> = Arc::new(
                    SortExec::new(LexOrdering::new(exprs?).unwrap(), Arc::clone(&child_copied))
                        .with_fetch(fetch),
                );

                if let Some(skip) = sort.skip.filter(|&n| n > 0).map(|n| n as usize) {
                    sort_exec = Arc::new(GlobalLimitExec::new(sort_exec, skip, None));
                }

                Ok((
                    scans,
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        sort_exec,
                        vec![Arc::clone(&child)],
                    )),
                ))
            }
            OpStruct::NativeScan(scan) => {
                let data_schema = convert_spark_types_to_arrow_schema(scan.data_schema.as_slice());
                let required_schema: SchemaRef =
                    convert_spark_types_to_arrow_schema(scan.required_schema.as_slice());
                let partition_schema: SchemaRef =
                    convert_spark_types_to_arrow_schema(scan.partition_schema.as_slice());
                let projection_vector: Vec<usize> = scan
                    .projection_vector
                    .iter()
                    .map(|offset| *offset as usize)
                    .collect();

                // Convert the Spark expressions to Physical expressions
                let data_filters: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = scan
                    .data_filters
                    .iter()
                    .map(|expr| self.create_expr(expr, Arc::clone(&required_schema)))
                    .collect();

                let default_values: Option<HashMap<usize, ScalarValue>> = if !scan
                    .default_values
                    .is_empty()
                {
                    // We have default values. Extract the two lists (same length) of values and
                    // indexes in the schema, and then create a HashMap to use in the SchemaMapper.
                    let default_values: Result<Vec<ScalarValue>, DataFusionError> = scan
                        .default_values
                        .iter()
                        .map(|expr| {
                            let literal = self.create_expr(expr, Arc::clone(&required_schema))?;
                            let df_literal = literal
                                .as_any()
                                .downcast_ref::<DataFusionLiteral>()
                                .ok_or_else(|| {
                                GeneralError("Expected literal of default value.".to_string())
                            })?;
                            Ok(df_literal.value().clone())
                        })
                        .collect();
                    let default_values = default_values?;
                    let default_values_indexes: Vec<usize> = scan
                        .default_values_indexes
                        .iter()
                        .map(|offset| *offset as usize)
                        .collect();
                    Some(
                        default_values_indexes
                            .into_iter()
                            .zip(default_values)
                            .collect(),
                    )
                } else {
                    None
                };

                // Get one file from the list of files
                let one_file = scan
                    .file_partitions
                    .first()
                    .and_then(|f| f.partitioned_file.first())
                    .map(|f| f.file_path.clone())
                    .ok_or(GeneralError("Failed to locate file".to_string()))?;

                let object_store_options: HashMap<String, String> = scan
                    .object_store_options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let (object_store_url, _) = prepare_object_store_with_configs(
                    self.session_ctx.runtime_env(),
                    one_file,
                    &object_store_options,
                )?;

                // Generate file groups
                let mut file_groups: Vec<Vec<PartitionedFile>> =
                    Vec::with_capacity(partition_count);
                scan.file_partitions.iter().try_for_each(|partition| {
                    let files = self.get_partitioned_files(partition)?;
                    file_groups.push(files);
                    Ok::<(), ExecutionError>(())
                })?;

                // TODO: I think we can remove partition_count in the future, but leave for testing.
                assert_eq!(file_groups.len(), partition_count);
                let partition_fields: Vec<Field> = partition_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                    })
                    .collect_vec();
                let scan = init_datasource_exec(
                    required_schema,
                    Some(data_schema),
                    Some(partition_schema),
                    Some(partition_fields),
                    object_store_url,
                    file_groups,
                    Some(projection_vector),
                    Some(data_filters?),
                    default_values,
                    scan.session_timezone.as_str(),
                    scan.case_sensitive,
                )?;
                Ok((
                    vec![],
                    Arc::new(SparkPlan::new(spark_plan.plan_id, scan, vec![])),
                ))
            }
            OpStruct::Scan(scan) => {
                let data_types = scan.fields.iter().map(to_arrow_datatype).collect_vec();

                // If it is not test execution context for unit test, we should have at least one
                // input source
                if self.exec_context_id != TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
                    return Err(GeneralError("No input for scan".to_string()));
                }

                // Consumes the first input source for the scan
                let input_source =
                    if self.exec_context_id == TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
                        // For unit test, we will set input batch to scan directly by `set_input_batch`.
                        None
                    } else {
                        Some(inputs.remove(0))
                    };

                // The `ScanExec` operator will take actual arrays from Spark during execution
                let scan =
                    ScanExec::new(self.exec_context_id, input_source, &scan.source, data_types)?;

                Ok((
                    vec![scan.clone()],
                    Arc::new(SparkPlan::new(spark_plan.plan_id, Arc::new(scan), vec![])),
                ))
            }
            OpStruct::ShuffleWriter(writer) => {
                assert_eq!(children.len(), 1);
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;

                let partitioning = self
                    .create_partitioning(writer.partitioning.as_ref().unwrap(), child.schema())?;

                let codec = match writer.codec.try_into() {
                    Ok(SparkCompressionCodec::None) => Ok(CompressionCodec::None),
                    Ok(SparkCompressionCodec::Snappy) => Ok(CompressionCodec::Snappy),
                    Ok(SparkCompressionCodec::Zstd) => {
                        Ok(CompressionCodec::Zstd(writer.compression_level))
                    }
                    Ok(SparkCompressionCodec::Lz4) => Ok(CompressionCodec::Lz4Frame),
                    _ => Err(GeneralError(format!(
                        "Unsupported shuffle compression codec: {:?}",
                        writer.codec
                    ))),
                }?;

                let shuffle_writer = Arc::new(ShuffleWriterExec::try_new(
                    Self::wrap_in_copy_exec(Arc::clone(&child.native_plan)),
                    partitioning,
                    codec,
                    writer.output_data_file.clone(),
                    writer.output_index_file.clone(),
                    writer.tracing_enabled,
                )?);

                Ok((
                    scans,
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        shuffle_writer,
                        vec![Arc::clone(&child)],
                    )),
                ))
            }
            OpStruct::Expand(expand) => {
                assert_eq!(children.len(), 1);
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;

                let mut projections = vec![];
                let mut projection = vec![];

                expand.project_list.iter().try_for_each(|expr| {
                    let expr = self.create_expr(expr, child.schema())?;
                    projection.push(expr);

                    if projection.len() == expand.num_expr_per_project as usize {
                        projections.push(projection.clone());
                        projection = vec![];
                    }

                    Ok::<(), ExecutionError>(())
                })?;

                assert!(
                    !projections.is_empty(),
                    "Expand should have at least one projection"
                );

                let datatypes = projections[0]
                    .iter()
                    .map(|expr| expr.data_type(&child.schema()))
                    .collect::<Result<Vec<DataType>, _>>()?;
                let fields: Vec<Field> = datatypes
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| Field::new(format!("col_{idx}"), dt.clone(), true))
                    .collect();
                let schema = Arc::new(Schema::new(fields));

                // `Expand` operator keeps the input batch and expands it to multiple output
                // batches. However, `ScanExec` will reuse input arrays for the next
                // input batch. Therefore, we need to copy the input batch to avoid
                // the data corruption. Note that we only need to copy the input batch
                // if the child operator is `ScanExec`, because other operators after `ScanExec`
                // will create new arrays for the output batch.
                let input = Arc::clone(&child.native_plan);
                let expand = Arc::new(ExpandExec::new(projections, input, schema));
                Ok((
                    scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, expand, vec![child])),
                ))
            }
            OpStruct::SortMergeJoin(join) => {
                let (join_params, scans) = self.parse_join_parameters(
                    inputs,
                    children,
                    &join.left_join_keys,
                    &join.right_join_keys,
                    join.join_type,
                    &join.condition,
                    partition_count,
                )?;

                let sort_options = join
                    .sort_options
                    .iter()
                    .map(|sort_option| {
                        let sort_expr = self
                            .create_sort_expr(sort_option, join_params.left.schema())
                            .unwrap();
                        SortOptions {
                            descending: sort_expr.options.descending,
                            nulls_first: sort_expr.options.nulls_first,
                        }
                    })
                    .collect();

                let left = Self::wrap_in_copy_exec(Arc::clone(&join_params.left.native_plan));
                let right = Self::wrap_in_copy_exec(Arc::clone(&join_params.right.native_plan));

                let join = Arc::new(SortMergeJoinExec::try_new(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    join_params.join_on,
                    join_params.join_filter,
                    join_params.join_type,
                    sort_options,
                    // null doesn't equal to null in Spark join key. If the join key is
                    // `EqualNullSafe`, Spark will rewrite it during planning.
                    NullEquality::NullEqualsNothing,
                )?);

                if join.filter.is_some() {
                    // SMJ with join filter produces lots of tiny batches
                    let coalesce_batches: Arc<dyn ExecutionPlan> =
                        Arc::new(CoalesceBatchesExec::new(
                            Arc::<SortMergeJoinExec>::clone(&join),
                            self.session_ctx
                                .state()
                                .config_options()
                                .execution
                                .batch_size,
                        ));
                    Ok((
                        scans,
                        Arc::new(SparkPlan::new_with_additional(
                            spark_plan.plan_id,
                            coalesce_batches,
                            vec![
                                Arc::clone(&join_params.left),
                                Arc::clone(&join_params.right),
                            ],
                            vec![join],
                        )),
                    ))
                } else {
                    Ok((
                        scans,
                        Arc::new(SparkPlan::new(
                            spark_plan.plan_id,
                            join,
                            vec![
                                Arc::clone(&join_params.left),
                                Arc::clone(&join_params.right),
                            ],
                        )),
                    ))
                }
            }
            OpStruct::HashJoin(join) => {
                let (join_params, scans) = self.parse_join_parameters(
                    inputs,
                    children,
                    &join.left_join_keys,
                    &join.right_join_keys,
                    join.join_type,
                    &join.condition,
                    partition_count,
                )?;

                // HashJoinExec may cache the input batch internally. We need
                // to copy the input batch to avoid the data corruption from reusing the input
                // batch. We also need to unpack dictionary arrays, because the join operators
                // do not support them.
                let left = Self::wrap_in_copy_exec(Arc::clone(&join_params.left.native_plan));
                let right = Self::wrap_in_copy_exec(Arc::clone(&join_params.right.native_plan));

                let hash_join = Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    join_params.join_on,
                    join_params.join_filter,
                    &join_params.join_type,
                    None,
                    PartitionMode::Partitioned,
                    // null doesn't equal to null in Spark join key. If the join key is
                    // `EqualNullSafe`, Spark will rewrite it during planning.
                    NullEquality::NullEqualsNothing,
                )?);

                // If the hash join is build right, we need to swap the left and right
                if join.build_side == BuildSide::BuildLeft as i32 {
                    Ok((
                        scans,
                        Arc::new(SparkPlan::new(
                            spark_plan.plan_id,
                            hash_join,
                            vec![join_params.left, join_params.right],
                        )),
                    ))
                } else {
                    let swapped_hash_join =
                        hash_join.as_ref().swap_inputs(PartitionMode::Partitioned)?;

                    let mut additional_native_plans = vec![];
                    if swapped_hash_join.as_any().is::<ProjectionExec>() {
                        // a projection was added to the hash join
                        additional_native_plans.push(Arc::clone(swapped_hash_join.children()[0]));
                    }

                    Ok((
                        scans,
                        Arc::new(SparkPlan::new_with_additional(
                            spark_plan.plan_id,
                            swapped_hash_join,
                            vec![join_params.left, join_params.right],
                            additional_native_plans,
                        )),
                    ))
                }
            }
            OpStruct::Window(wnd) => {
                let (scans, child) = self.create_plan(&children[0], inputs, partition_count)?;
                let input_schema = child.schema();
                let sort_exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = wnd
                    .order_by_list
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, Arc::clone(&input_schema)))
                    .collect();

                let partition_exprs: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = wnd
                    .partition_by_list
                    .iter()
                    .map(|expr| self.create_expr(expr, Arc::clone(&input_schema)))
                    .collect();

                let sort_exprs = &sort_exprs?;
                let partition_exprs = &partition_exprs?;

                let window_expr: Result<Vec<Arc<dyn WindowExpr>>, ExecutionError> = wnd
                    .window_expr
                    .iter()
                    .map(|expr| {
                        self.create_window_expr(
                            expr,
                            Arc::clone(&input_schema),
                            partition_exprs,
                            sort_exprs,
                        )
                    })
                    .collect();

                let window_agg = Arc::new(BoundedWindowAggExec::try_new(
                    window_expr?,
                    Arc::clone(&child.native_plan),
                    InputOrderMode::Sorted,
                    !partition_exprs.is_empty(),
                )?);
                Ok((
                    scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, window_agg, vec![child])),
                ))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_join_parameters(
        &self,
        inputs: &mut Vec<Arc<GlobalRef>>,
        children: &[Operator],
        left_join_keys: &[Expr],
        right_join_keys: &[Expr],
        join_type: i32,
        condition: &Option<Expr>,
        partition_count: usize,
    ) -> Result<(JoinParameters, Vec<ScanExec>), ExecutionError> {
        assert_eq!(children.len(), 2);
        let (mut left_scans, left) = self.create_plan(&children[0], inputs, partition_count)?;
        let (mut right_scans, right) = self.create_plan(&children[1], inputs, partition_count)?;

        left_scans.append(&mut right_scans);

        let left_join_exprs: Vec<_> = left_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, left.schema()))
            .collect::<Result<Vec<_>, _>>()?;
        let right_join_exprs: Vec<_> = right_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, right.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        let join_on = left_join_exprs
            .into_iter()
            .zip(right_join_exprs)
            .collect::<Vec<_>>();

        let join_type = match join_type.try_into() {
            Ok(JoinType::Inner) => DFJoinType::Inner,
            Ok(JoinType::LeftOuter) => DFJoinType::Left,
            Ok(JoinType::RightOuter) => DFJoinType::Right,
            Ok(JoinType::FullOuter) => DFJoinType::Full,
            Ok(JoinType::LeftSemi) => DFJoinType::LeftSemi,
            Ok(JoinType::LeftAnti) => DFJoinType::LeftAnti,
            Err(_) => {
                return Err(GeneralError(format!(
                    "Unsupported join type: {join_type:?}"
                )));
            }
        };

        // Handle join filter as DataFusion `JoinFilter` struct
        let join_filter = if let Some(expr) = condition {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_fields = left_schema.fields();
            let right_fields = right_schema.fields();
            let all_fields: Vec<_> = left_fields
                .into_iter()
                .chain(right_fields)
                .cloned()
                .collect();
            let full_schema = Arc::new(Schema::new(all_fields));

            // Because we cast dictionary array to array in scan operator,
            // we need to change dictionary type to data type for join filter expression.
            let fields: Vec<_> = full_schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => Arc::new(Field::new(
                        f.name(),
                        val_type.as_ref().clone(),
                        f.is_nullable(),
                    )),
                    _ => Arc::clone(f),
                })
                .collect();

            let full_schema = Arc::new(Schema::new(fields));

            let physical_expr = self.create_expr(expr, full_schema)?;
            let (left_field_indices, right_field_indices) =
                expr_to_columns(&physical_expr, left_fields.len(), right_fields.len())?;
            let column_indices = JoinFilter::build_column_indices(
                left_field_indices.clone(),
                right_field_indices.clone(),
            );

            let filter_fields: Vec<Field> = left_field_indices
                .clone()
                .into_iter()
                .map(|i| left.schema().field(i).clone())
                .chain(
                    right_field_indices
                        .clone()
                        .into_iter()
                        .map(|i| right.schema().field(i).clone()),
                )
                // Because we cast dictionary array to array in scan operator,
                // we need to change dictionary type to data type for join filter expression.
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => {
                        Field::new(f.name(), val_type.as_ref().clone(), f.is_nullable())
                    }
                    _ => f.clone(),
                })
                .collect_vec();

            let filter_schema = Schema::new_with_metadata(filter_fields, HashMap::new());

            // Rewrite the physical expression to use the new column indices.
            // DataFusion's join filter is bound to intermediate schema which contains
            // only the fields used in the filter expression. But the Spark's join filter
            // expression is bound to the full schema. We need to rewrite the physical
            // expression to use the new column indices.
            let rewritten_physical_expr = rewrite_physical_expr(
                physical_expr,
                left_schema.fields.len(),
                right_schema.fields.len(),
                &left_field_indices,
                &right_field_indices,
            )?;

            Some(JoinFilter::new(
                rewritten_physical_expr,
                column_indices,
                filter_schema.into(),
            ))
        } else {
            None
        };

        Ok((
            JoinParameters {
                left: Arc::clone(&left),
                right: Arc::clone(&right),
                join_on,
                join_type,
                join_filter,
            },
            left_scans,
        ))
    }

    /// Wrap an ExecutionPlan in a CopyExec, which will unpack any dictionary-encoded arrays.
    fn wrap_in_copy_exec(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CopyExec::new(plan, CopyMode::UnpackOrClone))
    }

    /// Create a DataFusion physical aggregate expression from Spark physical aggregate expression
    fn create_agg_expr(
        &self,
        spark_expr: &AggExpr,
        schema: SchemaRef,
    ) -> Result<AggregateFunctionExpr, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            AggExprStruct::Count(expr) => {
                assert!(!expr.children.is_empty());
                // Using `count_udaf` from Comet is exceptionally slow for some reason, so
                // as a workaround we translate it to `SUM(IF(expr IS NOT NULL, 1, 0))`
                // https://github.com/apache/datafusion-comet/issues/744

                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                // create `IS NOT NULL expr` and join them with `AND` if there are multiple
                let not_null_expr: Arc<dyn PhysicalExpr> = children.iter().skip(1).fold(
                    Arc::new(IsNotNullExpr::new(Arc::clone(&children[0]))) as Arc<dyn PhysicalExpr>,
                    |acc, child| {
                        Arc::new(BinaryExpr::new(
                            acc,
                            DataFusionOperator::And,
                            Arc::new(IsNotNullExpr::new(Arc::clone(child))),
                        ))
                    },
                );

                let child = Arc::new(IfExpr::new(
                    not_null_expr,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                    Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
                ));

                AggregateExprBuilder::new(sum_udaf(), vec![child])
                    .schema(schema)
                    .alias("count")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Min(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let child = Arc::new(CastExpr::new(child, datatype.clone(), None));

                AggregateExprBuilder::new(min_udaf(), vec![child])
                    .schema(schema)
                    .alias("min")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Max(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let child = Arc::new(CastExpr::new(child, datatype.clone(), None));

                AggregateExprBuilder::new(max_udaf(), vec![child])
                    .schema(schema)
                    .alias("max")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Sum(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());

                let builder = match datatype {
                    DataType::Decimal128(_, _) => {
                        let func = AggregateUDF::new_from_impl(SumDecimal::try_new(datatype)?);
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    _ => {
                        // cast to the result data type of SUM if necessary, we should not expect
                        // a cast failure since it should have already been checked at Spark side
                        let child =
                            Arc::new(CastExpr::new(Arc::clone(&child), datatype.clone(), None));
                        AggregateExprBuilder::new(sum_udaf(), vec![child])
                    }
                };
                builder
                    .schema(schema)
                    .alias("sum")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Avg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let input_datatype = to_arrow_datatype(expr.sum_datatype.as_ref().unwrap());
                let builder = match datatype {
                    DataType::Decimal128(_, _) => {
                        let func =
                            AggregateUDF::new_from_impl(AvgDecimal::new(datatype, input_datatype));
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    _ => {
                        // cast to the result data type of AVG if the result data type is different
                        // from the input type, e.g. AVG(Int32). We should not expect a cast
                        // failure since it should have already been checked at Spark side.
                        let child: Arc<dyn PhysicalExpr> =
                            Arc::new(CastExpr::new(Arc::clone(&child), datatype.clone(), None));
                        let func = AggregateUDF::new_from_impl(Avg::new("avg", datatype));
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                };
                builder
                    .schema(schema)
                    .alias("avg")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::First(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(FirstValue::new());

                AggregateExprBuilder::new(Arc::new(func), vec![child])
                    .schema(schema)
                    .alias("first")
                    .with_ignore_nulls(expr.ignore_nulls)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Last(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(LastValue::new());

                AggregateExprBuilder::new(Arc::new(func), vec![child])
                    .schema(schema)
                    .alias("last")
                    .with_ignore_nulls(expr.ignore_nulls)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitAndAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_and_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_and")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitOrAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_or_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_or")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitXorAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_xor_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_xor")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Covariance(expr) => {
                let child1 =
                    self.create_expr(expr.child1.as_ref().unwrap(), Arc::clone(&schema))?;
                let child2 =
                    self.create_expr(expr.child2.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Covariance::new(
                            "covariance",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr(
                            "covariance",
                            schema,
                            vec![child1, child2],
                            func,
                        )
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Covariance::new(
                            "covariance_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr(
                            "covariance_pop",
                            schema,
                            vec![child1, child2],
                            func,
                        )
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for Variance"
                    ))),
                }
            }
            AggExprStruct::Variance(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Variance::new(
                            "variance",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("variance", schema, vec![child], func)
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Variance::new(
                            "variance_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("variance_pop", schema, vec![child], func)
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for Variance"
                    ))),
                }
            }
            AggExprStruct::Stddev(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Stddev::new(
                            "stddev",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("stddev", schema, vec![child], func)
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Stddev::new(
                            "stddev_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("stddev_pop", schema, vec![child], func)
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for stddev"
                    ))),
                }
            }
            AggExprStruct::Correlation(expr) => {
                let child1 =
                    self.create_expr(expr.child1.as_ref().unwrap(), Arc::clone(&schema))?;
                let child2 =
                    self.create_expr(expr.child2.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let func = AggregateUDF::new_from_impl(Correlation::new(
                    "correlation",
                    datatype,
                    expr.null_on_divide_by_zero,
                ));
                Self::create_aggr_func_expr("correlation", schema, vec![child1, child2], func)
            }
            AggExprStruct::BloomFilterAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let num_items =
                    self.create_expr(expr.num_items.as_ref().unwrap(), Arc::clone(&schema))?;
                let num_bits =
                    self.create_expr(expr.num_bits.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let func = AggregateUDF::new_from_impl(BloomFilterAgg::new(
                    Arc::clone(&num_items),
                    Arc::clone(&num_bits),
                    datatype,
                ));
                Self::create_aggr_func_expr("bloom_filter_agg", schema, vec![child], func)
            }
        }
    }

    /// Create a DataFusion windows physical expression from Spark physical expression
    fn create_window_expr<'a>(
        &'a self,
        spark_expr: &'a spark_operator::WindowExpr,
        input_schema: SchemaRef,
        partition_by: &[Arc<dyn PhysicalExpr>],
        sort_exprs: &[PhysicalSortExpr],
    ) -> Result<Arc<dyn WindowExpr>, ExecutionError> {
        let window_func_name: String;
        let window_args: Vec<Arc<dyn PhysicalExpr>>;
        if let Some(func) = &spark_expr.built_in_window_function {
            match &func.expr_struct {
                Some(ExprStruct::ScalarFunc(f)) => {
                    window_func_name = f.func.clone();
                    window_args = f
                        .args
                        .iter()
                        .map(|expr| self.create_expr(expr, Arc::clone(&input_schema)))
                        .collect::<Result<Vec<_>, ExecutionError>>()?;
                }
                other => {
                    return Err(GeneralError(format!(
                        "{other:?} not supported for window function"
                    )))
                }
            };
        } else if let Some(agg_func) = &spark_expr.agg_func {
            let result = self.process_agg_func(agg_func, Arc::clone(&input_schema))?;
            window_func_name = result.0;
            window_args = result.1;
        } else {
            return Err(GeneralError(
                "Both func and agg_func are not set".to_string(),
            ));
        }

        let window_func = match self.find_df_window_function(&window_func_name) {
            Some(f) => f,
            _ => {
                return Err(GeneralError(format!(
                    "{window_func_name} not supported for window function"
                )))
            }
        };

        let spark_window_frame = match spark_expr
            .spec
            .as_ref()
            .and_then(|inner| inner.frame_specification.as_ref())
        {
            Some(frame) => frame,
            _ => {
                return Err(ExecutionError::DeserializeError(
                    "Cannot deserialize window frame".to_string(),
                ))
            }
        };

        let units = match spark_window_frame.frame_type() {
            WindowFrameType::Rows => WindowFrameUnits::Rows,
            WindowFrameType::Range => WindowFrameUnits::Range,
        };

        let lower_bound: WindowFrameBound = match spark_window_frame
            .lower_bound
            .as_ref()
            .and_then(|inner| inner.lower_frame_bound_struct.as_ref())
        {
            Some(l) => match l {
                LowerFrameBoundStruct::UnboundedPreceding(_) => match units {
                    WindowFrameUnits::Rows => {
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None))
                    }
                    WindowFrameUnits::Range => {
                        WindowFrameBound::Preceding(ScalarValue::Int64(None))
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                LowerFrameBoundStruct::Preceding(offset) => {
                    let offset_value = offset.offset.abs();
                    match units {
                        WindowFrameUnits::Rows => WindowFrameBound::Preceding(ScalarValue::UInt64(
                            Some(offset_value as u64),
                        )),
                        WindowFrameUnits::Range => {
                            WindowFrameBound::Preceding(ScalarValue::Int64(Some(offset_value)))
                        }
                        WindowFrameUnits::Groups => {
                            return Err(GeneralError(
                                "WindowFrameUnits::Groups is not supported.".to_string(),
                            ));
                        }
                    }
                }
                LowerFrameBoundStruct::CurrentRow(_) => WindowFrameBound::CurrentRow,
            },
            None => match units {
                WindowFrameUnits::Rows => WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameUnits::Range => WindowFrameBound::Preceding(ScalarValue::Int64(None)),
                WindowFrameUnits::Groups => {
                    return Err(GeneralError(
                        "WindowFrameUnits::Groups is not supported.".to_string(),
                    ));
                }
            },
        };

        let upper_bound: WindowFrameBound = match spark_window_frame
            .upper_bound
            .as_ref()
            .and_then(|inner| inner.upper_frame_bound_struct.as_ref())
        {
            Some(u) => match u {
                UpperFrameBoundStruct::UnboundedFollowing(_) => match units {
                    WindowFrameUnits::Rows => {
                        WindowFrameBound::Following(ScalarValue::UInt64(None))
                    }
                    WindowFrameUnits::Range => {
                        WindowFrameBound::Following(ScalarValue::Int64(None))
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                UpperFrameBoundStruct::Following(offset) => match units {
                    WindowFrameUnits::Rows => {
                        WindowFrameBound::Following(ScalarValue::UInt64(Some(offset.offset as u64)))
                    }
                    WindowFrameUnits::Range => {
                        WindowFrameBound::Following(ScalarValue::Int64(Some(offset.offset)))
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                UpperFrameBoundStruct::CurrentRow(_) => WindowFrameBound::CurrentRow,
            },
            None => match units {
                WindowFrameUnits::Rows => WindowFrameBound::Following(ScalarValue::UInt64(None)),
                WindowFrameUnits::Range => WindowFrameBound::Following(ScalarValue::Int64(None)),
                WindowFrameUnits::Groups => {
                    return Err(GeneralError(
                        "WindowFrameUnits::Groups is not supported.".to_string(),
                    ));
                }
            },
        };

        let window_frame = WindowFrame::new_bounds(units, lower_bound, upper_bound);
        let lex_orderings = LexOrdering::new(sort_exprs.to_vec());
        let sort_phy_exprs = lex_orderings.as_deref().unwrap_or(&[]);

        datafusion::physical_plan::windows::create_window_expr(
            &window_func,
            window_func_name,
            &window_args,
            partition_by,
            sort_phy_exprs,
            window_frame.into(),
            input_schema.as_ref(),
            false, // TODO: Ignore nulls
        )
        .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
    }

    fn process_agg_func(
        &self,
        agg_func: &AggExpr,
        schema: SchemaRef,
    ) -> Result<(String, Vec<Arc<dyn PhysicalExpr>>), ExecutionError> {
        match &agg_func.expr_struct {
            Some(AggExprStruct::Count(expr)) => {
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&schema)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(("count".to_string(), children))
            }
            Some(AggExprStruct::Min(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok(("min".to_string(), vec![child]))
            }
            Some(AggExprStruct::Max(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok(("max".to_string(), vec![child]))
            }
            Some(AggExprStruct::Sum(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let arrow_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let datatype = child.data_type(&schema)?;

                let child = if datatype != arrow_type {
                    Arc::new(CastExpr::new(child, arrow_type.clone(), None))
                } else {
                    child
                };
                Ok(("sum".to_string(), vec![child]))
            }
            other => Err(GeneralError(format!(
                "{other:?} not supported for window function"
            ))),
        }
    }

    /// Find DataFusion's built-in window function by name.
    fn find_df_window_function(&self, name: &str) -> Option<WindowFunctionDefinition> {
        let registry = &self.session_ctx.state();
        registry
            .udaf(name)
            .map(WindowFunctionDefinition::AggregateUDF)
            .ok()
    }

    /// Create a DataFusion physical partitioning from Spark physical partitioning
    fn create_partitioning(
        &self,
        spark_partitioning: &SparkPartitioning,
        input_schema: SchemaRef,
    ) -> Result<CometPartitioning, ExecutionError> {
        match spark_partitioning.partitioning_struct.as_ref().unwrap() {
            PartitioningStruct::HashPartition(hash_partition) => {
                let exprs: PartitionPhyExprResult = hash_partition
                    .hash_expression
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .collect();
                Ok(CometPartitioning::Hash(
                    exprs?,
                    hash_partition.num_partitions as usize,
                ))
            }
            PartitioningStruct::RangePartition(range_partition) => {
                let exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = range_partition
                    .sort_orders
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, Arc::clone(&input_schema)))
                    .collect();
                let lex_ordering = LexOrdering::new(exprs?).unwrap();
                Ok(CometPartitioning::RangePartitioning(
                    lex_ordering,
                    range_partition.num_partitions as usize,
                    range_partition.sample_size as usize,
                ))
            }
            PartitioningStruct::SinglePartition(_) => Ok(CometPartitioning::SinglePartition),
        }
    }

    fn create_scalar_function_expr(
        &self,
        expr: &ScalarFunc,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let args = expr
            .args
            .iter()
            .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
            .collect::<Result<Vec<_>, _>>()?;

        let fun_name = &expr.func;
        let input_expr_types = args
            .iter()
            .map(|x| x.data_type(input_schema.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

        let (data_type, coerced_input_types) =
            match expr.return_type.as_ref().map(to_arrow_datatype) {
                Some(t) => (t, input_expr_types.clone()),
                None => {
                    let fun_name = match fun_name.as_ref() {
                        "read_side_padding" => "rpad", // use the same return type as rpad
                        other => other,
                    };
                    let func = self.session_ctx.udf(fun_name)?;
                    let coerced_types = func
                        .coerce_types(&input_expr_types)
                        .unwrap_or_else(|_| input_expr_types.clone());

                    let arg_fields = coerced_types
                        .iter()
                        .enumerate()
                        .map(|(i, dt)| Arc::new(Field::new(format!("arg{i}"), dt.clone(), true)))
                        .collect::<Vec<_>>();

                    // TODO this should try and find scalar
                    let arguments = args
                        .iter()
                        .map(|e| {
                            e.as_ref()
                                .as_any()
                                .downcast_ref::<Literal>()
                                .map(|lit| lit.value())
                        })
                        .collect::<Vec<_>>();

                    let args = ReturnFieldArgs {
                        arg_fields: &arg_fields,
                        scalar_arguments: &arguments,
                    };

                    let data_type = Arc::clone(&func.inner().return_field_from_args(args)?)
                        .data_type()
                        .clone();

                    (data_type, coerced_types)
                }
            };

        let fun_expr = create_comet_physical_fun(
            fun_name,
            data_type.clone(),
            &self.session_ctx.state(),
            None,
        )?;

        let args = args
            .into_iter()
            .zip(input_expr_types.into_iter().zip(coerced_input_types))
            .map(|(expr, (from_type, to_type))| {
                if from_type != to_type {
                    Arc::new(CastExpr::new(
                        expr,
                        to_type,
                        Some(CastOptions {
                            safe: false,
                            ..Default::default()
                        }),
                    ))
                } else {
                    expr
                }
            })
            .collect::<Vec<_>>();

        let scalar_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            fun_name,
            fun_expr,
            args.to_vec(),
            Arc::new(Field::new(fun_name, data_type, true)),
        ));

        Ok(scalar_expr)
    }

    fn create_aggr_func_expr(
        name: &str,
        schema: SchemaRef,
        children: Vec<Arc<dyn PhysicalExpr>>,
        func: AggregateUDF,
    ) -> Result<AggregateFunctionExpr, ExecutionError> {
        AggregateExprBuilder::new(Arc::new(func), children)
            .schema(schema)
            .alias(name)
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()
            .map_err(|e| e.into())
    }
}

impl From<DataFusionError> for ExecutionError {
    fn from(value: DataFusionError) -> Self {
        ExecutionError::DataFusionError(value.message().to_string())
    }
}

impl From<ExecutionError> for DataFusionError {
    fn from(value: ExecutionError) -> Self {
        DataFusionError::Execution(value.to_string())
    }
}

impl From<ExpressionError> for DataFusionError {
    fn from(value: ExpressionError) -> Self {
        DataFusionError::Execution(value.to_string())
    }
}

/// Collects the indices of the columns in the input schema that are used in the expression
/// and returns them as a pair of vectors, one for the left side and one for the right side.
fn expr_to_columns(
    expr: &Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
) -> Result<(Vec<usize>, Vec<usize>), ExecutionError> {
    let mut left_field_indices: Vec<usize> = vec![];
    let mut right_field_indices: Vec<usize> = vec![];

    expr.apply(&mut |expr: &Arc<dyn PhysicalExpr>| {
        Ok({
            if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                if column.index() > left_field_len + right_field_len {
                    return Err(DataFusionError::Internal(format!(
                        "Column index {} out of range",
                        column.index()
                    )));
                } else if column.index() < left_field_len {
                    left_field_indices.push(column.index());
                } else {
                    right_field_indices.push(column.index() - left_field_len);
                }
            }
            TreeNodeRecursion::Continue
        })
    })?;

    left_field_indices.sort();
    right_field_indices.sort();

    Ok((left_field_indices, right_field_indices))
}

/// A physical join filter rewritter which rewrites the column indices in the expression
/// to use the new column indices. See `rewrite_physical_expr`.
struct JoinFilterRewriter<'a> {
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &'a [usize],
    right_field_indices: &'a [usize],
}

impl JoinFilterRewriter<'_> {
    fn new<'a>(
        left_field_len: usize,
        right_field_len: usize,
        left_field_indices: &'a [usize],
        right_field_indices: &'a [usize],
    ) -> JoinFilterRewriter<'a> {
        JoinFilterRewriter {
            left_field_len,
            right_field_len,
            left_field_indices,
            right_field_indices,
        }
    }
}

impl TreeNodeRewriter for JoinFilterRewriter<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: Self::Node) -> datafusion::common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if column.index() < self.left_field_len {
                // left side
                let new_index = self
                    .left_field_indices
                    .iter()
                    .position(|&x| x == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in left field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index,
                ))))
            } else if column.index() < self.left_field_len + self.right_field_len {
                // right side
                let new_index = self
                    .right_field_indices
                    .iter()
                    .position(|&x| x + self.left_field_len == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in right field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index + self.left_field_indices.len(),
                ))))
            } else {
                Err(DataFusionError::Internal(format!(
                    "Column index {} out of range",
                    column.index()
                )))
            }
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Rewrites the physical expression to use the new column indices.
/// This is necessary when the physical expression is used in a join filter, as the column
/// indices are different from the original schema.
fn rewrite_physical_expr(
    expr: Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &[usize],
    right_field_indices: &[usize],
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    let mut rewriter = JoinFilterRewriter::new(
        left_field_len,
        right_field_len,
        left_field_indices,
        right_field_indices,
    );

    Ok(expr.rewrite(&mut rewriter).data()?)
}

fn from_protobuf_eval_mode(value: i32) -> Result<EvalMode, prost::UnknownEnumValue> {
    match spark_expression::EvalMode::try_from(value)? {
        spark_expression::EvalMode::Legacy => Ok(EvalMode::Legacy),
        spark_expression::EvalMode::Try => Ok(EvalMode::Try),
        spark_expression::EvalMode::Ansi => Ok(EvalMode::Ansi),
    }
}

fn convert_spark_types_to_arrow_schema(
    spark_types: &[spark_operator::SparkStructField],
) -> SchemaRef {
    let arrow_fields = spark_types
        .iter()
        .map(|spark_type| {
            Field::new(
                String::clone(&spark_type.name),
                to_arrow_datatype(spark_type.data_type.as_ref().unwrap()),
                spark_type.nullable,
            )
        })
        .collect_vec();
    let arrow_schema: SchemaRef = Arc::new(Schema::new(arrow_fields));
    arrow_schema
}

/// Create CASE WHEN expression and add casting as needed
fn create_case_expr(
    when_then_pairs: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    else_expr: Option<Arc<dyn PhysicalExpr>>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    let then_types: Vec<DataType> = when_then_pairs
        .iter()
        .map(|x| x.1.data_type(input_schema))
        .collect::<Result<Vec<_>, _>>()?;

    let else_type: Option<DataType> = else_expr
        .as_ref()
        .map(|x| Arc::clone(x).data_type(input_schema))
        .transpose()?
        .or(Some(DataType::Null));

    if let Some(coerce_type) = get_coerce_type_for_case_expression(&then_types, else_type.as_ref())
    {
        let cast_options = SparkCastOptions::new_without_timezone(EvalMode::Legacy, false);

        let when_then_pairs = when_then_pairs
            .iter()
            .map(|x| {
                let t: Arc<dyn PhysicalExpr> = Arc::new(Cast::new(
                    Arc::clone(&x.1),
                    coerce_type.clone(),
                    cast_options.clone(),
                ));
                (Arc::clone(&x.0), t)
            })
            .collect::<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>>();

        let else_phy_expr: Option<Arc<dyn PhysicalExpr>> = else_expr.clone().map(|x| {
            Arc::new(Cast::new(x, coerce_type.clone(), cast_options.clone()))
                as Arc<dyn PhysicalExpr>
        });
        Ok(Arc::new(CaseExpr::try_new(
            None,
            when_then_pairs,
            else_phy_expr,
        )?))
    } else {
        Ok(Arc::new(CaseExpr::try_new(
            None,
            when_then_pairs,
            else_expr.clone(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use futures::{poll, StreamExt};
    use std::{sync::Arc, task::Poll};

    use arrow::array::{Array, DictionaryArray, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
    };
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::{assert_batches_eq, physical_plan::common::collect, prelude::SessionContext};
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use crate::execution::{operators::InputBatch, planner::PhysicalPlanner};

    use crate::execution::operators::ExecutionError;
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkSchemaAdapterFactory;
    use datafusion_comet_proto::spark_expression::expr::ExprStruct;
    use datafusion_comet_proto::{
        spark_expression::expr::ExprStruct::*,
        spark_expression::Expr,
        spark_expression::{self, literal},
        spark_operator,
        spark_operator::{operator::OpStruct, Operator},
    };
    use datafusion_comet_spark_expr::EvalMode;

    #[test]
    fn test_unpack_dictionary_primitive() {
        let op_scan = Operator {
            plan_id: 0,
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![spark_expression::DataType {
                    type_id: 3, // Int32
                    type_info: None,
                }],
                source: "".to_string(),
            })),
        };

        let op = create_filter(op_scan, 3);
        let planner = PhysicalPlanner::default();
        let row_count = 100;

        // Create a dictionary array with 100 values, and use it as input to the execution.
        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = Int32Array::from(vec![0, 1, 2, 3]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut vec![], 1).unwrap();
        scans[0].set_input_batch(input_batch);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        // Separate thread to send the EOF signal once we've processed the only input batch
        runtime.spawn(async move {
            // Create a dictionary array with 100 values, and use it as input to the execution.
            let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
            let values = Int32Array::from(vec![0, 1, 2, 3]);
            let input_array = DictionaryArray::new(keys, Arc::new(values));
            let input_batch1 = InputBatch::Batch(vec![Arc::new(input_array)], row_count);
            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), row_count / 4);
                        // dictionary should be unpacked
                        assert!(matches!(batch.column(0).data_type(), DataType::Int32));
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    const STRING_TYPE_ID: i32 = 7;

    #[test]
    fn test_unpack_dictionary_string() {
        let op_scan = Operator {
            plan_id: 0,
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![spark_expression::DataType {
                    type_id: STRING_TYPE_ID, // String
                    type_info: None,
                }],
                source: "".to_string(),
            })),
        };

        let lit = spark_expression::Literal {
            value: Some(literal::Value::StringVal("foo".to_string())),
            datatype: Some(spark_expression::DataType {
                type_id: STRING_TYPE_ID,
                type_info: None,
            }),
            is_null: false,
        };

        let op = create_filter_literal(op_scan, STRING_TYPE_ID, lit);
        let planner = PhysicalPlanner::default();

        let row_count = 100;

        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = StringArray::from(vec!["foo", "bar", "hello", "comet"]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut vec![], 1).unwrap();

        // Scan's schema is determined by the input batch, so we need to set it before execution.
        scans[0].set_input_batch(input_batch);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        // Separate thread to send the EOF signal once we've processed the only input batch
        runtime.spawn(async move {
            // Create a dictionary array with 100 values, and use it as input to the execution.
            let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
            let values = StringArray::from(vec!["foo", "bar", "hello", "comet"]);
            let input_array = DictionaryArray::new(keys, Arc::new(values));
            let input_batch1 = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), row_count / 4);
                        // string/binary should still be packed with dictionary
                        assert!(matches!(
                            batch.column(0).data_type(),
                            DataType::Dictionary(_, _)
                        ));
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    #[tokio::test()]
    #[allow(clippy::field_reassign_with_default)]
    async fn to_datafusion_filter() {
        let op_scan = create_scan();
        let op = create_filter(op_scan, 0);
        let planner = PhysicalPlanner::default();

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut vec![], 1).unwrap();

        let scan = &mut scans[0];
        scan.set_input_batch(InputBatch::EOF);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let stream = datafusion_plan
            .native_plan
            .execute(0, Arc::clone(&task_ctx))
            .unwrap();
        let output = collect(stream).await.unwrap();
        assert!(output.is_empty());
    }

    #[tokio::test()]
    async fn from_datafusion_error_to_comet() {
        let err_msg = "exec error";
        let err = datafusion::common::DataFusionError::Execution(err_msg.to_string());
        let comet_err: ExecutionError = err.into();
        assert_eq!(comet_err.to_string(), "Error from DataFusion: exec error.");
    }

    // Creates a filter operator which takes an `Int32Array` and selects rows that are equal to
    // `value`.
    fn create_filter(child_op: spark_operator::Operator, value: i32) -> spark_operator::Operator {
        let lit = spark_expression::Literal {
            value: Some(literal::Value::IntVal(value)),
            datatype: Some(spark_expression::DataType {
                type_id: 3,
                type_info: None,
            }),
            is_null: false,
        };

        create_filter_literal(child_op, 3, lit)
    }

    fn create_filter_literal(
        child_op: spark_operator::Operator,
        type_id: i32,
        lit: spark_expression::Literal,
    ) -> spark_operator::Operator {
        let left = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(spark_expression::DataType {
                    type_id,
                    type_info: None,
                }),
            })),
        };
        let right = spark_expression::Expr {
            expr_struct: Some(Literal(lit)),
        };

        let expr = spark_expression::Expr {
            expr_struct: Some(Eq(Box::new(spark_expression::BinaryExpr {
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        };

        Operator {
            plan_id: 0,
            children: vec![child_op],
            op_struct: Some(OpStruct::Filter(spark_operator::Filter {
                predicate: Some(expr),
                wrap_child_in_copy_exec: false,
            })),
        }
    }

    #[test]
    fn spark_plan_metrics_filter() {
        let op_scan = create_scan();
        let op = create_filter(op_scan, 0);
        let planner = PhysicalPlanner::default();

        let (_scans, filter_exec) = planner.create_plan(&op, &mut vec![], 1).unwrap();

        assert_eq!("FilterExec", filter_exec.native_plan.name());
        assert_eq!(1, filter_exec.children.len());
        assert_eq!(0, filter_exec.additional_native_plans.len());
    }

    #[test]
    fn spark_plan_metrics_hash_join() {
        let op_scan = create_scan();
        let op_join = Operator {
            plan_id: 0,
            children: vec![op_scan.clone(), op_scan.clone()],
            op_struct: Some(OpStruct::HashJoin(spark_operator::HashJoin {
                left_join_keys: vec![create_bound_reference(0)],
                right_join_keys: vec![create_bound_reference(0)],
                join_type: 0,
                condition: None,
                build_side: 0,
            })),
        };

        let planner = PhysicalPlanner::default();

        let (_scans, hash_join_exec) = planner.create_plan(&op_join, &mut vec![], 1).unwrap();

        assert_eq!("HashJoinExec", hash_join_exec.native_plan.name());
        assert_eq!(2, hash_join_exec.children.len());
        assert_eq!("ScanExec", hash_join_exec.children[0].native_plan.name());
        assert_eq!("ScanExec", hash_join_exec.children[1].native_plan.name());
    }

    fn create_bound_reference(index: i32) -> Expr {
        Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index,
                datatype: Some(create_proto_datatype()),
            })),
        }
    }

    fn create_scan() -> Operator {
        Operator {
            plan_id: 0,
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![create_proto_datatype()],
                source: "".to_string(),
            })),
        }
    }

    fn create_proto_datatype() -> spark_expression::DataType {
        spark_expression::DataType {
            type_id: 3,
            type_info: None,
        }
    }

    #[test]
    fn test_create_array() {
        let session_ctx = SessionContext::new();
        session_ctx.register_udf(ScalarUDF::from(
            datafusion_functions_nested::make_array::MakeArray::new(),
        ));
        let task_ctx = session_ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::from(session_ctx), 0);

        // Create a plan for
        // ProjectionExec: expr=[make_array(col_0@0) as col_0]
        // ScanExec: source=[CometScan parquet  (unknown)], schema=[col_0: Int32]
        let op_scan = Operator {
            plan_id: 0,
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                ],
                source: "".to_string(),
            })),
        };

        let array_col = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
        };

        let array_col_1 = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 1,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
        };

        let projection = Operator {
            children: vec![op_scan],
            plan_id: 0,
            op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                project_list: vec![spark_expression::Expr {
                    expr_struct: Some(ExprStruct::ScalarFunc(spark_expression::ScalarFunc {
                        func: "make_array".to_string(),
                        args: vec![array_col, array_col_1],
                        return_type: None,
                    })),
                }],
            })),
        };

        let (mut scans, datafusion_plan) =
            planner.create_plan(&projection, &mut vec![], 1).unwrap();

        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        // Separate thread to send the EOF signal once we've processed the only input batch
        runtime.spawn(async move {
            let a = Int32Array::from(vec![0, 3]);
            let b = Int32Array::from(vec![1, 4]);
            let c = Int32Array::from(vec![2, 5]);
            let input_batch1 = InputBatch::Batch(vec![Arc::new(a), Arc::new(b), Arc::new(c)], 2);
            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), 2);
                        let expected = [
                            "+--------+",
                            "| col_0  |",
                            "+--------+",
                            "| [0, 1] |",
                            "| [3, 4] |",
                            "+--------+",
                        ];
                        assert_batches_eq!(expected, &[batch]);
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    #[test]
    fn test_array_repeat() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::from(session_ctx), 0);

        // Mock scan operator with 3 INT32 columns
        let op_scan = Operator {
            plan_id: 0,
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                ],
                source: "".to_string(),
            })),
        };

        // Mock expression to read a INT32 column with position 0
        let array_col = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
        };

        // Mock expression to read a INT32 column with position 1
        let array_col_1 = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 1,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
        };

        // Make a projection operator with array_repeat(array_col, array_col_1)
        let projection = Operator {
            children: vec![op_scan],
            plan_id: 0,
            op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                project_list: vec![spark_expression::Expr {
                    expr_struct: Some(ExprStruct::ScalarFunc(spark_expression::ScalarFunc {
                        func: "array_repeat".to_string(),
                        args: vec![array_col, array_col_1],
                        return_type: None,
                    })),
                }],
            })),
        };

        // Create a physical plan
        let (mut scans, datafusion_plan) =
            planner.create_plan(&projection, &mut vec![], 1).unwrap();

        // Start executing the plan in a separate thread
        // The plan waits for incoming batches and emitting result as input comes
        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        // create async channel
        let (tx, mut rx) = mpsc::channel(1);

        // Send data as input to the plan being executed in a separate thread
        runtime.spawn(async move {
            // create data batch
            // 0, 1, 2
            // 3, 4, 5
            // 6, null, null
            let a = Int32Array::from(vec![Some(0), Some(3), Some(6)]);
            let b = Int32Array::from(vec![Some(1), Some(4), None]);
            let c = Int32Array::from(vec![Some(2), Some(5), None]);
            let input_batch1 = InputBatch::Batch(vec![Arc::new(a), Arc::new(b), Arc::new(c)], 3);
            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        // Wait for the plan to finish executing and assert the result
        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        let expected = [
                            "+--------------+",
                            "| col_0        |",
                            "+--------------+",
                            "| [0]          |",
                            "| [3, 3, 3, 3] |",
                            "|              |",
                            "+--------------+",
                        ];
                        assert_batches_eq!(expected, &[batch]);
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    /// Executes a `test_data_query` SQL query
    /// and saves the result into a temp folder using parquet format
    /// Read the file back to the memory using a custom schema
    async fn make_parquet_data(
        test_data_query: &str,
        read_schema: Schema,
    ) -> Result<RecordBatch, DataFusionError> {
        let session_ctx = SessionContext::new();

        // generate test data in the temp folder
        let tmp_dir = TempDir::new()?;
        let test_path = tmp_dir.path().to_str().unwrap();

        let plan = session_ctx
            .sql(test_data_query)
            .await?
            .create_physical_plan()
            .await?;

        // Write a parquet file into temp folder
        session_ctx.write_parquet(plan, test_path, None).await?;

        // Register all parquet with temp data as file groups
        let mut file_groups: Vec<FileGroup> = vec![];
        for entry in std::fs::read_dir(test_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                if let Some(path_str) = path.to_str() {
                    file_groups.push(FileGroup::new(vec![PartitionedFile::from_path(
                        path_str.into(),
                    )?]));
                }
            }
        }

        let source = ParquetSource::default().with_schema_adapter_factory(Arc::new(
            SparkSchemaAdapterFactory::new(
                SparkParquetOptions::new(EvalMode::Ansi, "", false),
                None,
            ),
        ))?;

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, read_schema.into(), source)
                .with_file_groups(file_groups)
                .build();

        // Run native read
        let scan = Arc::new(DataSourceExec::new(Arc::new(file_scan_config.clone())));
        let result: Vec<_> = scan.execute(0, session_ctx.task_ctx())?.collect().await;
        Ok(result.first().unwrap().as_ref().unwrap().clone())
    }

    /*
    Testing a nested types scenario

    select arr[0].a, arr[0].c from (
        select array(named_struct('a', 1, 'b', 'n', 'c', 'x')) arr)
     */
    #[tokio::test]
    async fn test_nested_types_list_of_struct_by_index() -> Result<(), DataFusionError> {
        let test_data = "select make_array(named_struct('a', 1, 'b', 'n', 'c', 'x')) c0";

        // Define schema Comet reads with
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::List(
                Field::new(
                    "element",
                    DataType::Struct(Fields::from(vec![
                        Field::new("a", DataType::Int32, true),
                        Field::new("c", DataType::Utf8, true),
                    ] as Vec<Field>)),
                    true,
                )
                .into(),
            ),
            true,
        )]));

        let actual = make_parquet_data(test_data, required_schema).await?;

        let expected = [
            "+----------------+",
            "| c0             |",
            "+----------------+",
            "| [{a: 1, c: x}] |",
            "+----------------+",
        ];
        assert_batches_eq!(expected, &[actual]);

        Ok(())
    }

    /*
    Testing a nested types scenario map[struct, struct]

    select map_keys(m).b from (
        select map(named_struct('a', 1, 'b', 'n', 'c', 'x'), named_struct('a', 1, 'b', 'n', 'c', 'x')) m
     */
    #[tokio::test]
    async fn test_nested_types_map_keys() -> Result<(), DataFusionError> {
        let test_data = "select map([named_struct('a', 1, 'b', 'n', 'c', 'x')], [named_struct('a', 2, 'b', 'm', 'c', 'y')]) c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new(
                            "key",
                            DataType::Struct(Fields::from(vec![Field::new(
                                "b",
                                DataType::Utf8,
                                true,
                            )])),
                            false,
                        ),
                        Field::new(
                            "value",
                            DataType::Struct(Fields::from(vec![
                                Field::new("a", DataType::Int64, true),
                                Field::new("b", DataType::Utf8, true),
                                Field::new("c", DataType::Utf8, true),
                            ])),
                            true,
                        ),
                    ] as Vec<Field>)),
                    false,
                )
                .into(),
                false,
            ),
            true,
        )]));

        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = [
            "+------------------------------+",
            "| c0                           |",
            "+------------------------------+",
            "| {{b: n}: {a: 2, b: m, c: y}} |",
            "+------------------------------+",
        ];
        assert_batches_eq!(expected, std::slice::from_ref(&actual));

        Ok(())
    }

    // Read struct using schema where schema fields do not overlap with
    // struct fields
    #[tokio::test]
    async fn test_nested_types_extract_missing_struct_names_non_overlap(
    ) -> Result<(), DataFusionError> {
        let test_data = "select named_struct('a', 1, 'b', 'abc') c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Struct(Fields::from(vec![
                Field::new("c", DataType::Int64, true),
                Field::new("d", DataType::Utf8, true),
            ])),
            true,
        )]));
        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = ["+----+", "| c0 |", "+----+", "|    |", "+----+"];
        assert_batches_eq!(expected, &[actual]);
        Ok(())
    }

    // Read struct using custom schema to read just a single field from the struct
    #[tokio::test]
    async fn test_nested_types_extract_missing_struct_names_single_field(
    ) -> Result<(), DataFusionError> {
        let test_data = "select named_struct('a', 1, 'b', 'abc') c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)])),
            true,
        )]));
        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = [
            "+--------+",
            "| c0     |",
            "+--------+",
            "| {a: 1} |",
            "+--------+",
        ];
        assert_batches_eq!(expected, &[actual]);
        Ok(())
    }

    // Read struct using custom schema to handle a missing field
    #[tokio::test]
    async fn test_nested_types_extract_missing_struct_names_missing_field(
    ) -> Result<(), DataFusionError> {
        let test_data = "select named_struct('a', 1, 'b', 'abc') c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("x", DataType::Int64, true),
            ])),
            true,
        )]));
        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = [
            "+-------------+",
            "| c0          |",
            "+-------------+",
            "| {a: 1, x: } |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &[actual]);
        Ok(())
    }
}
