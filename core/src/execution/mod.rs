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

//! PoC of vectorization execution through JNI to Rust.
pub mod datafusion;
pub mod jni_api;

pub mod kernels; // for benchmarking

mod metrics;
pub mod operators;
pub mod serde;
mod timezone;
pub(crate) mod utils;

// Include generated modules from .proto files.
#[allow(missing_docs)]
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
