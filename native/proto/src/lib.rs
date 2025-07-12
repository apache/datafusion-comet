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

// The clippy throws an error if the reference clone not wrapped into `Arc::clone`
// The lint makes easier for code reader/reviewer separate references clones from more heavyweight ones
#![deny(clippy::clone_on_ref_ptr)]

// Include generated modules from .proto files.
#[allow(missing_docs)]
#[allow(clippy::large_enum_variant)]
pub mod spark_expression {
    include!(concat!("generated", "/spark.spark_expression.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_partitioning {
    include!(concat!("generated", "/spark.spark_partitioning.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_operator {
    include!(concat!("generated", "/spark.spark_operator.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_metric {
    include!(concat!("generated", "/spark.spark_metric.rs"));
}
