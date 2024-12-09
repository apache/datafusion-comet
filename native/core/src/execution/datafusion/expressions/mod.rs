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

//! Native DataFusion expressions

pub mod bitwise_not;
pub mod checkoverflow;
mod normalize_nan;
pub use normalize_nan::NormalizeNaNAndZero;

use crate::errors::CometError;
pub mod avg;
pub mod avg_decimal;
pub mod bloom_filter_agg;
pub mod bloom_filter_might_contain;
pub mod comet_scalar_funcs;
pub mod correlation;
pub mod covariance;
pub mod negative;
pub mod stddev;
pub mod strings;
pub mod subquery;
pub mod sum_decimal;
pub mod unbound;
pub mod variance;

pub use datafusion_comet_spark_expr::{EvalMode, SparkError};

fn arithmetic_overflow_error(from_type: &str) -> CometError {
    CometError::Spark(SparkError::ArithmeticOverflow {
        from_type: from_type.to_string(),
    })
}
