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
    self, expr::ExprStruct, BoundReference, Expr, NullOrdering, SortDirection, SortOrder,
};
use datafusion_comet_proto::spark_native_executors::{QueryRequest, Stage, StageInput};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, Operator, RangeScan, Scan, SparkStructField,
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

/// The first column, descending, as a range exchange's sort key.
pub fn first_column_descending() -> Expr {
    Expr {
        expr_struct: Some(ExprStruct::SortOrder(Box::new(SortOrder {
            child: Some(Box::new(bound(0, long_type()))),
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
