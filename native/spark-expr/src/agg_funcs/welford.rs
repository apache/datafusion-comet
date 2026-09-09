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

//! Welford-style online update/merge math shared by the per-row
//! `Accumulator` and the vectorized `GroupsAccumulator` implementations of
//! variance/stddev/covariance/correlation. Counts are `f64` to match
//! Spark's wire-format state.

use arrow::buffer::NullBuffer;
use datafusion::physical_expr::expressions::StatsType;

#[inline]
pub(crate) fn variance_update(count: f64, mean: f64, m2: f64, value: f64) -> (f64, f64, f64) {
    let new_count = count + 1.0;
    let delta1 = value - mean;
    let new_mean = delta1 / new_count + mean;
    let delta2 = value - new_mean;
    let new_m2 = m2 + delta1 * delta2;
    (new_count, new_mean, new_m2)
}

#[inline]
pub(crate) fn variance_retract(count: f64, mean: f64, m2: f64, value: f64) -> (f64, f64, f64) {
    let new_count = count - 1.0;
    let delta1 = mean - value;
    let new_mean = delta1 / new_count + mean;
    let delta2 = new_mean - value;
    let new_m2 = m2 - delta1 * delta2;
    (new_count, new_mean, new_m2)
}

#[inline]
pub(crate) fn variance_merge(
    count_a: f64,
    mean_a: f64,
    m2_a: f64,
    count_b: f64,
    mean_b: f64,
    m2_b: f64,
) -> (f64, f64, f64) {
    let new_count = count_a + count_b;
    let new_mean = mean_a * count_a / new_count + mean_b * count_b / new_count;
    let delta = mean_a - mean_b;
    let new_m2 = m2_a + m2_b + delta * delta * count_a * count_b / new_count;
    (new_count, new_mean, new_m2)
}

#[inline]
pub(crate) fn covariance_update(
    count: f64,
    mean1: f64,
    mean2: f64,
    c: f64,
    v1: f64,
    v2: f64,
) -> (f64, f64, f64, f64) {
    let new_count = count + 1.0;
    let delta1 = v1 - mean1;
    let new_mean1 = delta1 / new_count + mean1;
    let delta2 = v2 - mean2;
    let new_mean2 = delta2 / new_count + mean2;
    let new_c = delta1 * (v2 - new_mean2) + c;
    (new_count, new_mean1, new_mean2, new_c)
}

#[inline]
pub(crate) fn covariance_retract(
    count: f64,
    mean1: f64,
    mean2: f64,
    c: f64,
    v1: f64,
    v2: f64,
) -> (f64, f64, f64, f64) {
    let new_count = count - 1.0;
    let delta1 = mean1 - v1;
    let new_mean1 = delta1 / new_count + mean1;
    let delta2 = mean2 - v2;
    let new_mean2 = delta2 / new_count + mean2;
    let new_c = c - delta1 * (new_mean2 - v2);
    (new_count, new_mean1, new_mean2, new_c)
}

/// Compute the per-group result and a null buffer for a Welford-style
/// moment (variance `m2` or covariance `algo_const`) following Spark
/// semantics shared by variance/covariance grouped accumulators:
/// count == 0 => null, count == 1 && Sample => NaN or null depending on
/// `null_on_divide_by_zero`, otherwise `numerator / divisor`.
pub(crate) fn finalize_moments(
    counts: Vec<f64>,
    numerators: Vec<f64>,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
) -> (Vec<f64>, NullBuffer) {
    let mut values = Vec::with_capacity(counts.len());
    let mut validity = Vec::with_capacity(counts.len());

    for (count, numerator) in counts.into_iter().zip(numerators) {
        if count == 0.0 {
            values.push(0.0);
            validity.push(false);
            continue;
        }
        let divisor = match stats_type {
            StatsType::Population => count,
            StatsType::Sample if count > 1.0 => count - 1.0,
            StatsType::Sample => {
                if null_on_divide_by_zero {
                    values.push(0.0);
                    validity.push(false);
                } else {
                    values.push(f64::NAN);
                    validity.push(true);
                }
                continue;
            }
        };
        values.push(numerator / divisor);
        validity.push(true);
    }

    (values, NullBuffer::from(validity))
}

#[inline]
#[allow(clippy::too_many_arguments)]
pub(crate) fn covariance_merge(
    count_a: f64,
    mean1_a: f64,
    mean2_a: f64,
    c_a: f64,
    count_b: f64,
    mean1_b: f64,
    mean2_b: f64,
    c_b: f64,
) -> (f64, f64, f64, f64) {
    let new_count = count_a + count_b;
    let new_mean1 = mean1_a * count_a / new_count + mean1_b * count_b / new_count;
    let new_mean2 = mean2_a * count_a / new_count + mean2_b * count_b / new_count;
    let delta1 = mean1_a - mean1_b;
    let delta2 = mean2_a - mean2_b;
    let new_c = c_a + c_b + delta1 * delta2 * count_a * count_b / new_count;
    (new_count, new_mean1, new_mean2, new_c)
}

/// Online update for the first four central moments `[n, avg, m2, m3, m4]`. Direct port of
/// Spark's `CentralMomentAgg.updateExpressionsDef` for `momentOrder = 4`.
///
/// The order-4 recurrence subsumes the order-2 one above, so `skewness` (`momentOrder = 3`)
/// is an `evaluate` on top of this same state rather than another copy of the algebra.
#[inline]
pub(crate) fn moments4_update(
    n: f64,
    avg: f64,
    m2: f64,
    m3: f64,
    m4: f64,
    value: f64,
) -> (f64, f64, f64, f64, f64) {
    let new_n = n + 1.0;
    let delta = value - avg;
    let delta_n = delta / new_n;
    let new_avg = avg + delta_n;
    let new_m2 = m2 + delta * (delta - delta_n);
    let delta2 = delta * delta;
    let delta_n2 = delta_n * delta_n;
    let new_m3 = m3 - 3.0 * delta_n * new_m2 + delta * (delta2 - delta_n2);
    let new_m4 = m4 - 4.0 * delta_n * new_m3 - 6.0 * delta_n2 * new_m2
        + delta * (delta * delta2 - delta_n * delta_n2);
    (new_n, new_avg, new_m2, new_m3, new_m4)
}

/// Merge two partial `[n, avg, m2, m3, m4]` states. Direct port of Spark's
/// `CentralMomentAgg.mergeExpressions` for `momentOrder = 4`.
#[inline]
#[allow(clippy::too_many_arguments)]
pub(crate) fn moments4_merge(
    n1: f64,
    avg1: f64,
    m2_1: f64,
    m3_1: f64,
    m4_1: f64,
    n2: f64,
    avg2: f64,
    m2_2: f64,
    m3_2: f64,
    m4_2: f64,
) -> (f64, f64, f64, f64, f64) {
    let new_n = n1 + n2;
    let delta = avg2 - avg1;
    let delta_n = if new_n == 0.0 { 0.0 } else { delta / new_n };
    let new_avg = avg1 + delta_n * n2;
    let new_m2 = m2_1 + m2_2 + delta * delta_n * n1 * n2;
    let new_m3 = m3_1
        + m3_2
        + delta_n * delta_n * delta * n1 * n2 * (n1 - n2)
        + 3.0 * delta_n * (n1 * m2_2 - n2 * m2_1);
    let new_m4 = m4_1
        + m4_2
        + delta_n * delta_n * delta_n * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2)
        + 6.0 * delta_n * delta_n * (n1 * n1 * m2_2 + n2 * n2 * m2_1)
        + 4.0 * delta_n * (n1 * m3_2 - n2 * m3_1);
    (new_n, new_avg, new_m2, new_m3, new_m4)
}
