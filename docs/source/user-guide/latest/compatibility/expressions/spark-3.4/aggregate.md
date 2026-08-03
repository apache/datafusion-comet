<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Aggregate Expressions

<!--BEGIN:EXPR_COMPAT[aggregate]-->

## ApproximatePercentile

The following cases are not supported by Comet and always fall back to Spark, regardless of any `allowIncompatible` setting:

- The percentage argument must be a foldable literal.
- The accuracy argument must be a foldable literal.
- Only byte, short, int, long, float, and double input types are supported.

## Average

The following cases are not supported by Comet and always fall back to Spark, regardless of any `allowIncompatible` setting:

- YearMonthIntervalType and DayTimeIntervalType inputs are not supported

## CollectSet

The following incompatibilities cause `CollectSet` to fall back to Spark by default. Set `spark.comet.expression.CollectSet.allowIncompatible=true` to enable Comet acceleration despite these differences.

- Comet deduplicates NaN values (treats `NaN == NaN`) while Spark treats each NaN as a distinct value. When `spark.comet.exec.strictFloatingPoint=true`, `collect_set` on floating-point types falls back to Spark unless `spark.comet.expression.CollectSet.allowIncompatible=true` is set.

## First

The following differences from Spark are always present and do not require any additional configuration:

- This function is not deterministic. Results may not match Spark.

## HyperLogLogPlusPlus

The following cases are not supported by Comet and always fall back to Spark, regardless of any `allowIncompatible` setting:

- Input type must be boolean, integral, floating-point, decimal, date/time, string, or binary
- `DecimalType` with precision > 18 is not supported
- Collated (non-UTF8_BINARY) strings are not supported

## Last

The following differences from Spark are always present and do not require any additional configuration:

- This function is not deterministic. Results may not match Spark.

## Percentile

The following cases are not supported by Comet and always fall back to Spark, regardless of any `allowIncompatible` setting:

- An array of percentages is not supported.
- The percentage argument must be a literal.
- A frequency argument is not supported.
- Descending order in `WITHIN GROUP (ORDER BY ... DESC)` is not supported.
- Only numeric input types are supported.
<!--END:EXPR_COMPAT-->
