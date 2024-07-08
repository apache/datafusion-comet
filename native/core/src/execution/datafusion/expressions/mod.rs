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
pub mod cast;
pub mod checkoverflow;
pub mod if_expr;
mod normalize_nan;
pub mod scalar_funcs;
pub use normalize_nan::NormalizeNaNAndZero;
use prost::DecodeError;

use crate::{errors::CometError, execution::spark_expression};
pub mod abs;
pub mod avg;
pub mod avg_decimal;
pub mod bloom_filter_might_contain;
pub mod correlation;
pub mod covariance;
pub mod create_named_struct;
pub mod negative;
pub mod stats;
pub mod stddev;
pub mod strings;
pub mod subquery;
pub mod sum_decimal;
pub mod temporal;
pub mod unbound;
mod utils;
pub mod variance;
pub mod xxhash64;

#[derive(Debug, Hash, PartialEq, Clone, Copy)]
pub enum EvalMode {
    Legacy,
    Ansi,
    Try,
}

impl TryFrom<i32> for EvalMode {
    type Error = DecodeError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match spark_expression::EvalMode::try_from(value)? {
            spark_expression::EvalMode::Legacy => Ok(EvalMode::Legacy),
            spark_expression::EvalMode::Try => Ok(EvalMode::Try),
            spark_expression::EvalMode::Ansi => Ok(EvalMode::Ansi),
        }
    }
}

fn arithmetic_overflow_error(from_type: &str) -> CometError {
    CometError::ArithmeticOverflow {
        from_type: from_type.to_string(),
    }
}
