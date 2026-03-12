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

Comet supports the following Spark expressions. Unless noted otherwise, all expressions are Spark-compatible and will
produce the same results as Spark. For expressions with known incompatibilities, Comet will fall back to Spark by
default. These can be forced to run natively by setting `spark.comet.expression.EXPRNAME.allowIncompatible=true`.

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
| LessThan           | `<`           |
| LessThanOrEqual    | `<=`          |
| In                 | `IN`          |
| IsNotNull          | `IS NOT NULL` |
| IsNull             | `IS NULL`     |
| InSet              | `IN (...)`    |
| Not                | `NOT`         |
| Or                 | `OR`          |

## String Functions

| Expression      | Notes                                                                                                                            |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Ascii           |                                                                                                                                  |
| BitLength       |                                                                                                                                  |
| Chr             |                                                                                                                                  |
| Concat          | Only string inputs are supported                                                                                                 |
| ConcatWs        |                                                                                                                                  |
| Contains        |                                                                                                                                  |
| EndsWith        |                                                                                                                                  |
| InitCap         | Not Spark compatible. Behavior is different in some cases, such as hyphenated names.                                             |
| Left            | Length argument must be a literal value                                                                                          |
| Length          |                                                                                                                                  |
| Like            |                                                                                                                                  |
| Lower           | Not Spark compatible. Results can vary depending on locale and character set. Requires `spark.comet.caseConversion.enabled=true` |
| OctetLength     |                                                                                                                                  |
| Reverse         |                                                                                                                                  |
| Right           | Length argument must be a literal value                                                                                          |
| RLike           | Not Spark compatible. Uses Rust regexp engine, which has different behavior to Java regexp engine                                |
| StartsWith      |                                                                                                                                  |
| StringInstr     |                                                                                                                                  |
| StringRepeat    | Negative argument for number of times to repeat causes exception                                                                 |
| StringReplace   |                                                                                                                                  |
| StringLPad      |                                                                                                                                  |
| StringRPad      |                                                                                                                                  |
| StringSpace     |                                                                                                                                  |
| StringSplit     | Not Spark compatible. Regex engine differences between Java and Rust                                                             |
| StringTranslate |                                                                                                                                  |
| StringTrim      |                                                                                                                                  |
| StringTrimBoth  |                                                                                                                                  |
| StringTrimLeft  |                                                                                                                                  |
| StringTrimRight |                                                                                                                                  |
| Substring       |                                                                                                                                  |
| Upper           | Not Spark compatible. Results can vary depending on locale and character set. Requires `spark.comet.caseConversion.enabled=true` |

## Date/Time Functions

| Expression     | SQL                          | Notes                                                                                                                                                            |
| -------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DateAdd        | `date_add`                   |                                                                                                                                                                  |
| DateDiff       | `datediff`                   |                                                                                                                                                                  |
| DateFormat     | `date_format`                | Partial support. Only specific format patterns are supported.                                                                                                    |
| DateSub        | `date_sub`                   |                                                                                                                                                                  |
| DatePart       | `date_part(field, source)`   | Spark decomposes into individual functions. Supported values of `field`: `year`/`month`/`week`/`day`/`dayofweek`/`dayofweek_iso`/`doy`/`quarter`/`hour`/`minute` |
| Extract        | `extract(field FROM source)` | Spark decomposes into individual functions. Supported values of `field`: `year`/`month`/`week`/`day`/`dayofweek`/`dayofweek_iso`/`doy`/`quarter`/`hour`/`minute` |
| FromUnixTime   | `from_unixtime`              | Not Spark compatible. Does not support format, supports only -8334601211038 <= sec <= 8210266876799                                                              |
| Hour           | `hour`                       |                                                                                                                                                                  |
| LastDay        | `last_day`                   |                                                                                                                                                                  |
| MakeDate       | `make_date`                  |                                                                                                                                                                  |
| Minute         | `minute`                     |                                                                                                                                                                  |
| NextDay        | `next_day`                   |                                                                                                                                                                  |
| Second         | `second`                     |                                                                                                                                                                  |
| TruncDate      | `trunc`                      |                                                                                                                                                                  |
| TruncTimestamp | `date_trunc`                 |                                                                                                                                                                  |
| UnixDate       | `unix_date`                  |                                                                                                                                                                  |
| UnixTimestamp  | `unix_timestamp`             |                                                                                                                                                                  |
| Year           | `year`                       |                                                                                                                                                                  |
| Month          | `month`                      |                                                                                                                                                                  |
| DayOfMonth     | `day`/`dayofmonth`           |                                                                                                                                                                  |
| DayOfWeek      | `dayofweek`                  |                                                                                                                                                                  |
| WeekDay        | `weekday`                    |                                                                                                                                                                  |
| DayOfYear      | `dayofyear`                  |                                                                                                                                                                  |
| WeekOfYear     | `weekofyear`                 |                                                                                                                                                                  |
| Quarter        | `quarter`                    |                                                                                                                                                                  |

## Math Expressions

| Expression     | SQL       | Notes                             |
| -------------- | --------- | --------------------------------- |
| Abs            | `abs`     |                                   |
| Acos           | `acos`    |                                   |
| Add            | `+`       |                                   |
| Asin           | `asin`    |                                   |
| Atan           | `atan`    |                                   |
| Atan2          | `atan2`   |                                   |
| BRound         | `bround`  |                                   |
| Ceil           | `ceil`    |                                   |
| Cos            | `cos`     |                                   |
| Cosh           | `cosh`    |                                   |
| Cot            | `cot`     |                                   |
| Divide         | `/`       |                                   |
| Exp            | `exp`     |                                   |
| Expm1          | `expm1`   |                                   |
| Floor          | `floor`   |                                   |
| Hex            | `hex`     |                                   |
| IntegralDivide | `div`     |                                   |
| IsNaN          | `isnan`   |                                   |
| Log            | `log`     |                                   |
| Log2           | `log2`    |                                   |
| Log10          | `log10`   |                                   |
| Multiply       | `*`       |                                   |
| Pow            | `power`   |                                   |
| Rand           | `rand`    |                                   |
| Randn          | `randn`   |                                   |
| Remainder      | `%`       |                                   |
| Round          | `round`   |                                   |
| Signum         | `signum`  |                                   |
| Sin            | `sin`     |                                   |
| Sinh           | `sinh`    |                                   |
| Sqrt           | `sqrt`    |                                   |
| Subtract       | `-`       |                                   |
| Tan            | `tan`     |                                   |
| Tanh           | `tanh`    |                                   |
| TryAdd         | `try_add` | Only integer inputs are supported |
| TryDivide      | `try_div` | Only integer inputs are supported |
| TryMultiply    | `try_mul` | Only integer inputs are supported |
| TrySubtract    | `try_sub` | Only integer inputs are supported |
| UnaryMinus     | `-`       |                                   |
| Unhex          | `unhex`   |                                   |

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

| Expression    | SQL        | Notes                                                                                  |
| ------------- | ---------- | -------------------------------------------------------------------------------------- |
| Average       |            | Not supported in ANSI mode                                                             |
| BitAndAgg     |            |                                                                                        |
| BitOrAgg      |            |                                                                                        |
| BitXorAgg     |            |                                                                                        |
| BoolAnd       | `bool_and` | Spark decomposes to Min/Max on boolean columns                                         |
| BoolOr        | `bool_or`  | Spark decomposes to Min/Max on boolean columns                                         |
| Corr          |            |                                                                                        |
| Count         |            |                                                                                        |
| CovPopulation |            |                                                                                        |
| CovSample     |            |                                                                                        |
| First         |            | Not Spark compatible. This function is not deterministic. Results may not match Spark. |
| Last          |            | Not Spark compatible. This function is not deterministic. Results may not match Spark. |
| Max           |            |                                                                                        |
| Min           |            |                                                                                        |
| StddevPop     |            |                                                                                        |
| StddevSamp    |            |                                                                                        |
| Sum           |            | Not supported in ANSI mode                                                             |
| VariancePop   |            |                                                                                        |
| VarianceSamp  |            |                                                                                        |

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

| Expression     | Notes                                                                                                                                                                           |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ArrayAppend    | Not Spark compatible                                                                                                                                                            |
| ArrayCompact   | Not Spark compatible                                                                                                                                                            |
| ArrayContains  | Not Spark compatible. Returns null instead of false for empty arrays with literal values ([#3346](https://github.com/apache/datafusion-comet/issues/3346))                      |
| ArrayDistinct  | Not Spark compatible. Comet first sorts then removes duplicates while Spark preserves the original order.                                                                       |
| ArrayExcept    | Not Spark compatible                                                                                                                                                            |
| ArrayFilter    | Only supports case where function is `IsNotNull`                                                                                                                                |
| ArrayInsert    | Not Spark compatible                                                                                                                                                            |
| ArrayIntersect | Not Spark compatible                                                                                                                                                            |
| ArrayJoin      | Not Spark compatible                                                                                                                                                            |
| ArrayMax       |                                                                                                                                                                                 |
| ArrayMin       |                                                                                                                                                                                 |
| ArrayRemove    | Not Spark compatible. Returns null when element is null instead of removing null elements ([#3173](https://github.com/apache/datafusion-comet/issues/3173))                     |
| ArrayRepeat    | Not Spark compatible                                                                                                                                                            |
| ArrayUnion     | Not Spark compatible. Comet sorts the input arrays before performing the union, while Spark preserves the order of the first array and appends unique elements from the second. |
| ArraysOverlap  | Not Spark compatible                                                                                                                                                            |
| CreateArray    |                                                                                                                                                                                 |
| ElementAt      | Input must be an array. Map inputs are not supported.                                                                                                                           |
| Flatten        |                                                                                                                                                                                 |
| GetArrayItem   |                                                                                                                                                                                 |
| Size           | Only array inputs are supported. Map inputs are not supported.                                                                                                                  |

## Map Expressions

| Expression     | Notes                                                             |
| -------------- | ----------------------------------------------------------------- |
| GetMapValue    |                                                                   |
| MapContainsKey |                                                                   |
| MapEntries     |                                                                   |
| MapFromArrays  |                                                                   |
| MapFromEntries | Not Spark compatible. Binary key or value types are not supported |
| MapKeys        |                                                                   |
| MapValues      |                                                                   |

## Struct Expressions

| Expression           | Notes                                                                                    |
| -------------------- | ---------------------------------------------------------------------------------------- |
| CreateNamedStruct    |                                                                                          |
| GetArrayStructFields |                                                                                          |
| GetStructField       |                                                                                          |
| JsonToStructs        | Not Spark compatible. Partial support. Requires explicit schema.                         |
| StructsToCsv         | Not Spark compatible. Complex, Date, Timestamp, and Binary types may produce differences |
| StructsToJson        |                                                                                          |

## Conversion Expressions

| Expression | Notes                                                                                                                 |
| ---------- | --------------------------------------------------------------------------------------------------------------------- |
| Cast       | Depends on specific cast. See the [Comet Compatibility Guide] for list of supported cast expressions and known issues |

## SortOrder

| Expression |
| ---------- |
| NullsFirst |
| NullsLast  |
| Ascending  |
| Descending |

## Other

| Expression                   | Notes                                                                                             |
| ---------------------------- | ------------------------------------------------------------------------------------------------- |
| Alias                        |                                                                                                   |
| AttributeReference           |                                                                                                   |
| BloomFilterMightContain      |                                                                                                   |
| Coalesce                     |                                                                                                   |
| CheckOverflow                |                                                                                                   |
| KnownFloatingPointNormalized |                                                                                                   |
| Literal                      |                                                                                                   |
| MakeDecimal                  |                                                                                                   |
| MonotonicallyIncreasingID    |                                                                                                   |
| NormalizeNaNAndZero          |                                                                                                   |
| PromotePrecision             |                                                                                                   |
| RegExpReplace                | Not Spark compatible. Uses Rust regexp engine, which has different behavior to Java regexp engine |
| ScalarSubquery               |                                                                                                   |
| SparkPartitionID             |                                                                                                   |
| ToPrettyString               |                                                                                                   |
| UnscaledValue                |                                                                                                   |

[Comet Configuration Guide]: configs.md
[Comet Compatibility Guide]: compatibility.md
