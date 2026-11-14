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

//! Stage graphs for the tests, built from Comet operators that need no files: `RangeScan` leaf
//! stages, and stages that return their input.

// Each test file uses its own subset.
#![allow(dead_code)]

use comet::execution::native_executors::COMET_VERSION;
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::ArrowPrimitiveType;
use datafusion_comet_proto::spark_expression::data_type::DataTypeId;
use datafusion_comet_proto::spark_expression::{
    self, expr::ExprStruct, literal::Value, BinaryExpr, BoundReference, Cast, Expr, IfExpr,
    Literal, MathExpr, NullOrdering, SortDirection, SortOrder,
};
use datafusion_comet_proto::spark_native_executors::{QueryRequest, Stage, StageInput};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, BuildSide, HashJoin, JoinType, Operator, Projection, RangeScan, Scan,
    ShuffleScan, Sort, SparkStructField,
};
use datafusion_comet_proto::spark_partitioning::{
    partitioning::PartitioningStruct, HashPartition, Partitioning, RangePartition, SinglePartition,
};
use datafusion_comet_proto::PROTO_FINGERPRINT;

/// A query of `stages` from a driver of this Comet build.
pub fn query(stages: Vec<Stage>) -> QueryRequest {
    QueryRequest {
        comet_version: COMET_VERSION.to_string(),
        proto_fingerprint: PROTO_FINGERPRINT.to_string(),
        stages,
        ..Default::default()
    }
}

fn data_type(type_id: DataTypeId) -> spark_expression::DataType {
    spark_expression::DataType {
        type_id: type_id as i32,
        type_info: None,
    }
}

pub fn long_type() -> spark_expression::DataType {
    data_type(DataTypeId::Int64)
}

pub fn int_type() -> spark_expression::DataType {
    data_type(DataTypeId::Int32)
}

pub fn double_type() -> spark_expression::DataType {
    data_type(DataTypeId::Double)
}

pub fn string_type() -> spark_expression::DataType {
    data_type(DataTypeId::String)
}

pub fn bool_type() -> spark_expression::DataType {
    data_type(DataTypeId::Bool)
}

/// Column `index` of an operator's input, of type `data_type`.
pub fn bound(index: i32, data_type: spark_expression::DataType) -> Expr {
    Expr {
        expr_struct: Some(ExprStruct::Bound(BoundReference {
            index,
            datatype: Some(data_type),
        })),
        ..Default::default()
    }
}

/// A nullable column `id` of type `data_type`, as a stage declares its output.
pub fn field(data_type: spark_expression::DataType) -> SparkStructField {
    SparkStructField {
        name: "id".to_string(),
        data_type: Some(data_type),
        nullable: true,
        ..Default::default()
    }
}

/// A stage that runs `range(0, num_elements)` in `num_slices` partitions, like `spark.range`.
pub fn range_stage(num_elements: i64, num_slices: i32) -> Stage {
    Stage {
        plan: Some(Operator {
            op_struct: Some(OpStruct::RangeScan(RangeScan {
                start: 0,
                step: 1,
                num_elements,
                num_slices,
            })),
            ..Default::default()
        }),
        num_partitions: num_slices as u32,
        output_schema: vec![field(long_type())],
        ..Default::default()
    }
}

/// A stage that returns what stage `producer` sends it through an exchange with `partitioning`.
/// `num_partitions` is the count the driver asks for, which is the exchange's.
pub fn reading_stage(producer: u32, partitioning: Partitioning, num_partitions: u32) -> Stage {
    Stage {
        plan: Some(Operator {
            op_struct: Some(OpStruct::Scan(Scan {
                fields: vec![long_type()],
                source: "test".to_string(),
            })),
            ..Default::default()
        }),
        inputs: vec![StageInput {
            producer,
            partitioning: Some(partitioning),
            broadcast: false,
        }],
        num_partitions,
        output_schema: vec![field(long_type())],
        ..Default::default()
    }
}

/// A stage that sorts what stage `producer` sends it through an exchange with `partitioning`, by
/// `order`, of a column of type `data_type`, and declares that order, as a driver does for a
/// stage that reads a range exchange.
pub fn sorting_stage(
    producer: u32,
    partitioning: Partitioning,
    order: Expr,
    data_type: spark_expression::DataType,
) -> Stage {
    sorting_stage_by(producer, partitioning, vec![order], vec![data_type])
}

/// Like [`sorting_stage`], for a producer with columns of `types`, sorted by `orders`.
pub fn sorting_stage_by(
    producer: u32,
    partitioning: Partitioning,
    orders: Vec<Expr>,
    types: Vec<spark_expression::DataType>,
) -> Stage {
    let scan = Operator {
        op_struct: Some(OpStruct::Scan(Scan {
            fields: types.clone(),
            source: "test".to_string(),
        })),
        ..Default::default()
    };
    Stage {
        plan: Some(Operator {
            children: vec![scan],
            op_struct: Some(OpStruct::Sort(Sort {
                sort_orders: orders.clone(),
                ..Default::default()
            })),
            ..Default::default()
        }),
        inputs: vec![StageInput {
            producer,
            partitioning: Some(partitioning),
            broadcast: false,
        }],
        num_partitions: 1,
        output_schema: types.into_iter().map(field).collect(),
        output_ordering: orders,
        ..Default::default()
    }
}

/// `input` with a projection of `exprs` on top of its block, producing columns of `types`.
pub fn projecting_stage(
    mut input: Stage,
    exprs: Vec<Expr>,
    types: Vec<spark_expression::DataType>,
) -> Stage {
    input.plan = Some(Operator {
        op_struct: Some(OpStruct::Projection(Projection {
            project_list: exprs,
        })),
        children: vec![input.plan.take().unwrap()],
        ..Default::default()
    });
    input.output_schema = types.into_iter().map(field).collect();
    input
}

fn expr(expr_struct: ExprStruct) -> Expr {
    Expr {
        expr_struct: Some(expr_struct),
        ..Default::default()
    }
}

pub fn long_literal(value: i64) -> Expr {
    expr(ExprStruct::Literal(Literal {
        value: Some(Value::LongVal(value)),
        datatype: Some(long_type()),
        is_null: false,
    }))
}

pub fn null_long() -> Expr {
    expr(ExprStruct::Literal(Literal {
        value: None,
        datatype: Some(long_type()),
        is_null: true,
    }))
}

pub fn remainder(left: Expr, right: Expr) -> Expr {
    expr(ExprStruct::Remainder(Box::new(MathExpr {
        left: Some(Box::new(left)),
        right: Some(Box::new(right)),
        return_type: Some(long_type()),
        ..Default::default()
    })))
}

pub fn equal(left: Expr, right: Expr) -> Expr {
    expr(ExprStruct::Eq(Box::new(BinaryExpr {
        left: Some(Box::new(left)),
        right: Some(Box::new(right)),
    })))
}

pub fn if_else(condition: Expr, then: Expr, otherwise: Expr) -> Expr {
    expr(ExprStruct::If(Box::new(IfExpr {
        if_expr: Some(Box::new(condition)),
        true_expr: Some(Box::new(then)),
        false_expr: Some(Box::new(otherwise)),
    })))
}

pub fn cast(child: Expr, data_type: spark_expression::DataType) -> Expr {
    expr(ExprStruct::Cast(Box::new(Cast {
        child: Some(Box::new(child)),
        datatype: Some(data_type),
        timezone: "UTC".to_string(),
        ..Default::default()
    })))
}

/// A sort key: column `index`, of type `data_type`.
pub fn sort_key(
    index: i32,
    data_type: spark_expression::DataType,
    descending: bool,
    nulls_first: bool,
) -> Expr {
    expr(ExprStruct::SortOrder(Box::new(SortOrder {
        child: Some(Box::new(bound(index, data_type))),
        direction: if descending {
            SortDirection::Descending
        } else {
            SortDirection::Ascending
        } as i32,
        null_ordering: if nulls_first {
            NullOrdering::NullsFirst
        } else {
            NullOrdering::NullsLast
        } as i32,
    })))
}

pub fn hash_on_first_column(num_partitions: i32) -> Partitioning {
    Partitioning {
        partitioning_struct: Some(PartitioningStruct::HashPartition(HashPartition {
            hash_expression: vec![bound(0, long_type())],
            num_partitions,
        })),
    }
}

pub fn single_partition() -> Partitioning {
    Partitioning {
        partitioning_struct: Some(PartitioningStruct::SinglePartition(SinglePartition {})),
    }
}

/// A range exchange as a driver sends it, with no bounds: sorting by `sort_orders`.
pub fn range_partitioning(num_partitions: i32, sort_orders: Vec<Expr>) -> Partitioning {
    Partitioning {
        partitioning_struct: Some(PartitioningStruct::RangePartition(RangePartition {
            sort_orders,
            num_partitions,
            ..Default::default()
        })),
    }
}

/// The first column, of type `data_type`, descending, as a sort key.
pub fn first_column_descending(data_type: spark_expression::DataType) -> Expr {
    Expr {
        expr_struct: Some(ExprStruct::SortOrder(Box::new(SortOrder {
            child: Some(Box::new(bound(0, data_type))),
            direction: SortDirection::Descending as i32,
            null_ordering: NullOrdering::NullsLast as i32,
        }))),
        ..Default::default()
    }
}

/// The values of the batches' single column, of type `T`, sorted.
pub fn sorted_values<T: ArrowPrimitiveType>(batches: &[RecordBatch]) -> Vec<T::Native>
where
    T::Native: Ord,
{
    let mut values: Vec<T::Native> = batches
        .iter()
        .flat_map(|batch| batch.column(0).as_primitive::<T>().values().to_vec())
        .collect();
    values.sort_unstable();
    values
}

/// A stage that joins what stages `probe` and `build` send it through hash exchanges of
/// `num_partitions` partitions on their first column, as an inner hash join built on the build
/// side, and returns both sides' columns.
pub fn joining_stage(probe: u32, build: u32, num_partitions: i32) -> Stage {
    let leaf = || Operator {
        op_struct: Some(OpStruct::ShuffleScan(ShuffleScan {
            fields: vec![long_type()],
            source: "test".to_string(),
        })),
        ..Default::default()
    };
    let input = |producer| StageInput {
        producer,
        partitioning: Some(hash_on_first_column(num_partitions)),
        broadcast: false,
    };
    Stage {
        plan: Some(Operator {
            children: vec![leaf(), leaf()],
            op_struct: Some(OpStruct::HashJoin(HashJoin {
                left_join_keys: vec![bound(0, long_type())],
                right_join_keys: vec![bound(0, long_type())],
                join_type: JoinType::Inner as i32,
                build_side: BuildSide::BuildRight as i32,
                ..Default::default()
            })),
            ..Default::default()
        }),
        inputs: vec![input(probe), input(build)],
        num_partitions: num_partitions as u32,
        output_schema: vec![field(long_type()), field(long_type())],
        ..Default::default()
    }
}
