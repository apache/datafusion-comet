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

//! Proleptic Gregorian calendar conversions, using Howard Hinnant's algorithms
//! (https://howardhinnant.github.io/date_algorithms.html) as adapted in DataFusion 55.1.0's
//! date_trunc.rs, where they are private. Hand-rolled rather than chrono because chrono stops at
//! year 262142 and Spark and Iceberg values reach further.

/// Days since 1970-01-01 for a proleptic Gregorian date, like Java's `LocalDate.toEpochDay`.
/// `month` must be in 1..=12. The result is linear in `day`, so a day past the end of the month
/// rolls into the next month. Years within about 2.5e16 of zero stay within `i64`.
// Inlined because per-row callers in other crates cannot inline it otherwise without LTO.
#[inline]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_index = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Proleptic Gregorian `(year, month, day)` for a day count since 1970-01-01, like Java's
/// `LocalDate.ofEpochDay`, with `month` in 1..=12 and `day` in 1..=31. The inverse of
/// `days_from_civil` for any day at least 719468 below `i64::MAX`.
// Inlined for the same reason as days_from_civil.
#[inline]
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn days_in_month(year: i64, month: i64) -> i64 {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        match month {
            2 if leap => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        }
    }

    #[test]
    fn walks_four_eras_day_by_day() {
        // Four 400-year eras from 1170-01-01 cross every century leap rule and era boundary.
        let (mut year, mut month, mut day) = (1170, 1, 1);
        for days in -292_194..-292_194 + 4 * 146_097 {
            assert_eq!(civil_from_days(days), (year, month, day), "day {days}");
            assert_eq!(days_from_civil(year, month, day), days);
            day += 1;
            if day > days_in_month(year, month) {
                day = 1;
                month += 1;
                if month > 12 {
                    month = 1;
                    year += 1;
                }
            }
        }
    }

    #[test]
    fn repeats_every_era_out_to_the_i32_day_range() {
        // Every 400 years has the same 146097 days, which reaches the i32 extremes without a sweep.
        for days in [-719_529, -1, 0, 59, 789, 146_096] {
            let (year, month, day) = civil_from_days(days);
            for era in -14_700..=14_700 {
                let shifted = days + era * 146_097;
                assert_eq!(civil_from_days(shifted), (year + era * 400, month, day));
                assert_eq!(days_from_civil(year + era * 400, month, day), shifted);
            }
        }
    }

    #[test]
    fn matches_known_dates() {
        for (days, date) in [
            (0, (1970, 1, 1)),
            (-1, (1969, 12, 31)),
            (59, (1970, 3, 1)),
            (789, (1972, 2, 29)),
            (-25_567, (1900, 1, 1)),
            (-25_509, (1900, 2, 28)),
            (-25_508, (1900, 3, 1)),
            (-719_468, (0, 3, 1)),
            (-719_528, (0, 1, 1)),
            (-719_529, (-1, 12, 31)),
            (i64::from(i32::MIN), (-5_877_641, 6, 23)),
            (i64::from(i32::MAX), (5_881_580, 7, 11)),
        ] {
            assert_eq!(civil_from_days(days), date, "day {days}");
            assert_eq!(days_from_civil(date.0, date.1, date.2), days);
        }
    }

    #[test]
    fn rolls_days_past_the_month_end_forward() {
        assert_eq!(days_from_civil(1900, 2, 29), days_from_civil(1900, 3, 1));
        assert_eq!(days_from_civil(1970, 1, 32), 31);
        assert_eq!(days_from_civil(1999, 12, 32), days_from_civil(2000, 1, 1));
    }
}
