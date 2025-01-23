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

mod error;

mod kernels;
mod static_invoke;
pub use static_invoke::*;

mod struct_funcs;
pub use struct_funcs::{CreateNamedStruct, GetStructField};

mod json_funcs;
pub mod test_common;
pub mod timezone;
mod unbound;
pub use unbound::UnboundColumn;
mod predicate_funcs;
pub mod utils;
pub use predicate_funcs::{spark_isnan, RLike};

mod agg_funcs;
mod array_funcs;
mod bitwise_funcs;
mod comet_scalar_funcs;
pub mod hash_funcs;

mod string_funcs;

mod datetime_funcs;
pub use agg_funcs::*;

pub use cast::{spark_cast, Cast, SparkCastOptions};
mod conditional_funcs;
mod conversion_funcs;
mod math_funcs;

pub use array_funcs::*;
pub use bitwise_funcs::*;
pub use conditional_funcs::*;
pub use conversion_funcs::*;

pub use comet_scalar_funcs::create_comet_physical_fun;
pub use datetime_funcs::{
    spark_date_add, spark_date_sub, DateTruncExpr, HourExpr, MinuteExpr, SecondExpr,
    TimestampTruncExpr,
};
pub use error::{SparkError, SparkResult};
pub use hash_funcs::*;
pub use json_funcs::ToJson;
pub use math_funcs::{
    create_negate_expr, spark_ceil, spark_decimal_div, spark_floor, spark_hex, spark_make_decimal,
    spark_round, spark_unhex, spark_unscaled_value, CheckOverflow, NegativeExpr,
    NormalizeNaNAndZero,
};
pub use string_funcs::*;

/// Spark supports three evaluation modes when evaluating expressions, which affect
/// the behavior when processing input values that are invalid or would result in an
/// error, such as divide by zero errors, and also affects behavior when converting
/// between types.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum EvalMode {
    /// Legacy is the default behavior in Spark prior to Spark 4.0. This mode silently ignores
    /// or replaces errors during SQL operations. Operations resulting in errors (like
    /// division by zero) will produce NULL values instead of failing. Legacy mode also
    /// enables implicit type conversions.
    Legacy,
    /// Adheres to the ANSI SQL standard for error handling by throwing exceptions for
    /// operations that result in errors. Does not perform implicit type conversions.
    Ansi,
    /// Same as Ansi mode, except that it converts errors to NULL values without
    /// failing the entire query.
    Try,
}

pub(crate) fn arithmetic_overflow_error(from_type: &str) -> SparkError {
    SparkError::ArithmeticOverflow {
        from_type: from_type.to_string(),
    }
}
