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

# Supported Spark Expressions

Comet supports the following Spark expressions. See the [Comet Compatibility Guide] for details on known
incompatibilities and unsupported cases.

All expressions are enabled by default, but most can be disabled by setting
`spark.comet.expression.EXPRNAME.enabled=false`, where `EXPRNAME` is the expression name as specified in
the following tables, such as `Length`, or `StartsWith`. See the [Comet Configuration Guide] for a full list
of expressions that be disabled.

## Conditional Expressions

| Expression | SQL                                         |
| ---------- | ------------------------------------------- |
| CaseWhen   | `CASE WHEN expr THEN expr ELSE expr END`    |
| If         | `IF(predicate_expr, true_expr, false_expr)` |

## Predicate Expressions

| Expression         | SQL           |
| ------------------ | ------------- |
| And                | `AND`         |
| EqualTo            | `=`           |
| EqualNullSafe      | `<=>`         |
| GreaterThan        | `>`           |
| GreaterThanOrEqual | `>=`          |
| ILike              | `ILIKE`       |
| In                 | `IN`          |
| InSet              | `IN (...)`    |
| IsNotNull          | `IS NOT NULL` |
| IsNull             | `IS NULL`     |
| LessThan           | `<`           |
| LessThanOrEqual    | `<=`          |
| Not                | `NOT`         |
| Or                 | `OR`          |

## String Functions

| Expression      |
| --------------- |
| Ascii           |
| BitLength       |
| Chr             |
| Concat          |
| ConcatWs        |
| Contains        |
| Decode          |
| EndsWith        |
| InitCap         |
| Left            |
| Length          |
| Like            |
| Lower           |
| OctetLength     |
| Reverse         |
| Right           |
| RLike           |
| Split           |
| StartsWith      |
| StringInstr     |
| StringRepeat    |
| StringReplace   |
| StringLPad      |
| StringRPad      |
| StringSpace     |
| StringTranslate |
| StringTrim      |
| StringTrimBoth  |
| StringTrimLeft  |
| StringTrimRight |
| Substring       |
| SubstringIndex  |
| Upper           |

## JSON Functions

| Expression    |
| ------------- |
| GetJsonObject |

## Date/Time Functions

| Expression       | SQL                          |
| ---------------- | ---------------------------- |
| ConvertTimezone  | `convert_timezone`           |
| CurrentTimeZone  | `current_timezone`           |
| DateAdd          | `date_add`                   |
| DateDiff         | `datediff`                   |
| DateFormat       | `date_format`                |
| DateFromUnixDate | `date_from_unix_date`        |
| DateSub          | `date_sub`                   |
| DatePart         | `date_part(field, source)`   |
| Days             | `days`                       |
| Extract          | `extract(field FROM source)` |
| FromUnixTime     | `from_unixtime`              |
| Hour             | `hour`                       |
| LastDay          | `last_day`                   |
| LocalTimestamp   | `localtimestamp`             |
| MakeDate         | `make_date`                  |
| MakeTime         | `make_time`                  |
| Minute           | `minute`                     |
| NextDay          | `next_day`                   |
| Second           | `second`                     |
| TimestampSeconds | `timestamp_seconds`          |
| TruncDate        | `trunc`                      |
| TruncTimestamp   | `date_trunc`                 |
| UnixDate         | `unix_date`                  |
| UnixTimestamp    | `unix_timestamp`             |
| Year             | `year`                       |
| Month            | `month`                      |
| DayOfMonth       | `day`/`dayofmonth`           |
| DayOfWeek        | `dayofweek`                  |
| WeekDay          | `weekday`                    |
| DayOfYear        | `dayofyear`                  |
| WeekOfYear       | `weekofyear`                 |
| Quarter          | `quarter`                    |
| ToTime           | `to_time`                    |
| TryToTime        | `try_to_time`                |

## Math Expressions

| Expression     | SQL            |
| -------------- | -------------- |
| Abs            | `abs`          |
| Acos           | `acos`         |
| Acosh          | `acosh`        |
| Add            | `+`            |
| Asin           | `asin`         |
| Asinh          | `asinh`        |
| Atan           | `atan`         |
| Atan2          | `atan2`        |
| Atanh          | `atanh`        |
| Bin            | `bin`          |
| BRound         | `bround`       |
| Cbrt           | `cbrt`         |
| Ceil           | `ceil`         |
| Cos            | `cos`          |
| Cosh           | `cosh`         |
| Cot            | `cot`          |
| Csc            | `csc`          |
| Divide         | `/`            |
| Exp            | `exp`          |
| Expm1          | `expm1`        |
| Factorial      | `factorial`    |
| Floor          | `floor`        |
| Hex            | `hex`          |
| IntegralDivide | `div`          |
| IsNaN          | `isnan`        |
| Log            | `log`          |
| Log2           | `log2`         |
| Log10          | `log10`        |
| Multiply       | `*`            |
| Pi             | `pi`           |
| Pow            | `power`        |
| Rand           | `rand`         |
| Randn          | `randn`        |
| Remainder      | `%`            |
| Rint           | `rint`         |
| Round          | `round`        |
| Sec            | `sec`          |
| Signum         | `signum`       |
| Sin            | `sin`          |
| Sinh           | `sinh`         |
| Sqrt           | `sqrt`         |
| Subtract       | `-`            |
| Tan            | `tan`          |
| Tanh           | `tanh`         |
| ToDegrees      | `degrees`      |
| ToRadians      | `radians`      |
| TryAdd         | `try_add`      |
| TryDivide      | `try_div`      |
| TryMultiply    | `try_mul`      |
| TrySubtract    | `try_sub`      |
| UnaryMinus     | `-`            |
| Unhex          | `unhex`        |
| WidthBucket    | `width_bucket` |

## Hashing Functions

| Expression  |
| ----------- |
| Crc32       |
| Md5         |
| Murmur3Hash |
| Sha1        |
| Sha2        |
| XxHash64    |

## Bitwise Expressions

| Expression   | SQL  |
| ------------ | ---- |
| BitwiseAnd   | `&`  |
| BitwiseCount |      |
| BitwiseGet   |      |
| BitwiseOr    | `\|` |
| BitwiseNot   | `~`  |
| BitwiseXor   | `^`  |
| ShiftLeft    | `<<` |
| ShiftRight   | `>>` |

## Aggregate Expressions

| Expression    | SQL        |
| ------------- | ---------- |
| Average       |            |
| BitAndAgg     |            |
| BitOrAgg      |            |
| BitXorAgg     |            |
| BoolAnd       | `bool_and` |
| BoolOr        | `bool_or`  |
| CollectSet    |            |
| Corr          |            |
| Count         |            |
| CountIf       | `count_if` |
| CovPopulation |            |
| CovSample     |            |
| First         |            |
| Last          |            |
| Max           |            |
| Min           |            |
| StddevPop     |            |
| StddevSamp    |            |
| Sum           |            |
| VariancePop   |            |
| VarianceSamp  |            |

## Window Functions

```{warning}
Window support is disabled by default due to known correctness issues. Tracking issue: [#2721](https://github.com/apache/datafusion-comet/issues/2721).
```

Comet supports using the following aggregate functions within window contexts with PARTITION BY and ORDER BY clauses.

| Expression |
| ---------- |
| Count      |
| Max        |
| Min        |
| Sum        |

**Note:** Dedicated window functions such as `rank`, `dense_rank`, `row_number`, `lag`, `lead`, `ntile`, `cume_dist`, `percent_rank`, and `nth_value` are not currently supported and will fall back to Spark.

## Array Expressions

| Expression     |
| -------------- |
| ArrayAppend    |
| ArrayCompact   |
| ArrayContains  |
| ArrayDistinct  |
| ArrayExcept    |
| ArrayFilter    |
| ArrayInsert    |
| ArrayIntersect |
| ArrayJoin      |
| ArrayMax       |
| ArrayMin       |
| ArrayPosition  |
| ArrayRemove    |
| ArrayRepeat    |
| ArraysZip      |
| ArrayUnion     |
| ArraysOverlap  |
| CreateArray    |
| ElementAt      |
| Flatten        |
| GetArrayItem   |
| Size           |
| SortArray      |

## Map Expressions

| Expression     |
| -------------- |
| GetMapValue    |
| MapContainsKey |
| MapEntries     |
| MapFromArrays  |
| MapFromEntries |
| MapKeys        |
| MapValues      |
| StringToMap    |

## Struct Expressions

| Expression           |
| -------------------- |
| CreateNamedStruct    |
| GetArrayStructFields |
| GetStructField       |
| JsonToStructs        |
| StructsToJson        |

## URL Functions

| Expression   |
| ------------ |
| TryUrlDecode |
| UrlDecode    |
| UrlEncode    |

## Conversion Expressions

| Expression |
| ---------- |
| Cast       |

## SortOrder

| Expression |
| ---------- |
| NullsFirst |
| NullsLast  |
| Ascending  |
| Descending |

## Other

| Expression                   |
| ---------------------------- |
| Alias                        |
| AttributeReference           |
| BloomFilterMightContain      |
| Coalesce                     |
| CheckOverflow                |
| KnownFloatingPointNormalized |
| Literal                      |
| MakeDecimal                  |
| MonotonicallyIncreasingID    |
| NormalizeNaNAndZero          |
| PromotePrecision             |
| RegExpReplace                |
| ScalarSubquery               |
| SparkPartitionID             |
| ToPrettyString               |
| UnscaledValue                |

[Comet Configuration Guide]: configs.md
[Comet Compatibility Guide]: compatibility/expressions/index.md
