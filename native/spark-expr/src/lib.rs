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

mod cast;
mod error;
mod if_expr;

mod avg;
pub use avg::Avg;
mod bitwise_not;
pub use bitwise_not::{bitwise_not, BitwiseNotExpr};
mod avg_decimal;
pub use avg_decimal::AvgDecimal;
mod checkoverflow;
pub use checkoverflow::CheckOverflow;
mod correlation;
pub use correlation::Correlation;
mod covariance;
pub use covariance::Covariance;
mod strings;
pub use strings::{Contains, EndsWith, Like, StartsWith, StringSpaceExpr, SubstringExpr};
mod kernels;
mod list;
mod regexp;
pub mod scalar_funcs;
mod schema_adapter;
pub use schema_adapter::SparkSchemaAdapterFactory;

pub mod spark_hash;
mod stddev;
pub use stddev::Stddev;
mod structs;
mod sum_decimal;
pub use sum_decimal::SumDecimal;
mod negative;
pub use negative::{create_negate_expr, NegativeExpr};
mod normalize_nan;
mod temporal;

pub mod test_common;
pub mod timezone;
mod to_json;
mod unbound;
pub use unbound::UnboundColumn;
pub mod utils;
pub use normalize_nan::NormalizeNaNAndZero;

mod variance;
pub use variance::Variance;
mod comet_scalar_funcs;
pub use cast::{spark_cast, Cast, SparkCastOptions};
pub use comet_scalar_funcs::create_comet_physical_fun;
pub use error::{SparkError, SparkResult};
pub use if_expr::IfExpr;
pub use list::{ArrayInsert, GetArrayStructFields, ListExtract};
pub use regexp::RLike;
pub use structs::{CreateNamedStruct, GetStructField};
pub use temporal::{DateTruncExpr, HourExpr, MinuteExpr, SecondExpr, TimestampTruncExpr};
pub use to_json::ToJson;

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
