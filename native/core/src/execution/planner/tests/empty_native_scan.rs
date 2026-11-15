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

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_comet_proto::spark_expression::{
    data_type::DataTypeId, expr::ExprStruct, literal::Value, DataType as SparkDataType, Expr,
    Literal,
};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, NativeScan, NativeScanCommon, Operator, SparkFilePartition,
    SparkPartitionedFile, SparkStructField,
};

use crate::execution::planner::PhysicalPlanner;

fn spark_field(
    name: &str,
    data_type: DataTypeId,
    nullable: bool,
    field_id: &str,
) -> SparkStructField {
    SparkStructField {
        name: name.to_string(),
        data_type: Some(SparkDataType {
            type_id: data_type as i32,
            type_info: None,
        }),
        nullable,
        metadata: HashMap::from([("parquet.field.id".to_string(), field_id.to_string())]),
    }
}

fn common(required_data_indices: &[usize], projection: Vec<i64>) -> NativeScanCommon {
    let data_schema = vec![
        spark_field("id", DataTypeId::Int32, false, "1"),
        spark_field("unused", DataTypeId::String, true, "2"),
        spark_field("payload", DataTypeId::Int64, true, "3"),
    ];
    NativeScanCommon {
        required_schema: required_data_indices
            .iter()
            .map(|&index| data_schema[index].clone())
            .collect(),
        data_schema,
        // Constant file metadata is appended alongside partition values by the JVM scan.
        partition_schema: vec![
            spark_field("part", DataTypeId::Int32, false, "4"),
            spark_field("_metadata.file_size", DataTypeId::Int64, false, "5"),
        ],
        projection_vector: projection,
        session_timezone: "UTC".to_string(),
        ..Default::default()
    }
}

fn literal(data_type: DataTypeId, value: Value) -> Expr {
    Expr {
        expr_struct: Some(ExprStruct::Literal(Literal {
            datatype: Some(SparkDataType {
                type_id: data_type as i32,
                type_info: None,
            }),
            value: Some(value),
            is_null: false,
        })),
        ..Default::default()
    }
}

fn plan_schema(common: &NativeScanCommon, empty: bool) -> SchemaRef {
    let files = if empty {
        vec![]
    } else {
        // Planning does not read the file. Only its presence selects the nonempty path.
        vec![SparkPartitionedFile {
            file_path: "file:///tmp/comet-empty-native-scan-schema.parquet".to_string(),
            start: 0,
            length: 16,
            file_size: 16,
            partition_values: vec![
                literal(DataTypeId::Int32, Value::IntVal(7)),
                literal(DataTypeId::Int64, Value::LongVal(16)),
            ],
        }]
    };
    let operator = Operator {
        op_struct: Some(OpStruct::NativeScan(NativeScan {
            common: Some(common.clone()),
            file_partition: Some(SparkFilePartition {
                partitioned_file: files,
            }),
            ..Default::default()
        })),
        ..Default::default()
    };
    let (_, _, plan) = PhysicalPlanner::default()
        .create_plan(&operator, &mut vec![], 1)
        .unwrap();
    plan.schema()
}

fn assert_schema_matches_nonempty(common: NativeScanCommon, expected: Vec<Field>) {
    let nonempty_schema = plan_schema(&common, false);
    let empty_schema = plan_schema(&common, true);
    assert_eq!(nonempty_schema.as_ref(), &Schema::new(expected));
    assert_eq!(empty_schema, nonempty_schema);
}

fn id_field() -> Field {
    Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
        "parquet.field.id".to_string(),
        "1".to_string(),
    )]))
}

fn payload_field() -> Field {
    Field::new("payload", DataType::Int64, true).with_metadata(HashMap::from([(
        "parquet.field.id".to_string(),
        "3".to_string(),
    )]))
}

#[test]
fn empty_scan_preserves_reordered_and_pruned_data_columns() {
    assert_schema_matches_nonempty(
        common(&[0, 2], vec![2, 0]),
        vec![payload_field(), id_field()],
    );
}

#[test]
fn empty_scan_preserves_partition_and_constant_metadata_projection() {
    // Match the nonempty path: data-field metadata is preserved, while partition
    // and constant metadata fields retain their names, types and nullability only.
    assert_schema_matches_nonempty(
        common(&[0, 2], vec![3, 2, 4, 0]),
        vec![
            Field::new("part", DataType::Int32, false),
            payload_field(),
            Field::new("_metadata.file_size", DataType::Int64, false),
            id_field(),
        ],
    );
}

#[test]
fn empty_scan_preserves_partition_only_projection() {
    assert_schema_matches_nonempty(
        common(&[], vec![3]),
        vec![Field::new("part", DataType::Int32, false)],
    );
}

#[test]
fn empty_scan_preserves_zero_column_projection() {
    assert_schema_matches_nonempty(common(&[], vec![]), vec![]);
}
