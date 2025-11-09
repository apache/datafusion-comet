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

Comet supports the following Spark expressions. Expressions that are marked as Spark-compatible will either run
natively in Comet and provide the same results as Spark, or will fall back to Spark for cases that would not
be compatible.

All expressions are enabled by default, but most can be disabled by setting
`spark.comet.expression.EXPRNAME.enabled=false`, where `EXPRNAME` is the expression name as specified in
the following tables, such as `Length`, or `StartsWith`. See the [Comet Configuration Guide] for a full list
of expressions that be disabled.

Expressions that are not Spark-compatible will fall back to Spark by default and can be enabled by setting
`spark.comet.expression.EXPRNAME.allowIncompatible=true`.

It is also possible to specify `spark.comet.expression.allowIncompatible=true` to enable all
incompatible expressions.

## Conditional Expressions

| Expression | SQL                                         | Spark-Compatible? |
| ---------- | ------------------------------------------- | ----------------- |
| CaseWhen   | `CASE WHEN expr THEN expr ELSE expr END`    | Yes               |
| If         | `IF(predicate_expr, true_expr, false_expr)` | Yes               |

## Predicate Expressions

| Expression         | SQL           | Spark-Compatible? |
| ------------------ | ------------- | ----------------- |
| And                | `AND`         | Yes               |
| EqualTo            | `=`           | Yes               |
| EqualNullSafe      | `<=>`         | Yes               |
| GreaterThan        | `>`           | Yes               |
| GreaterThanOrEqual | `>=`          | Yes               |
| LessThan           | `<`           | Yes               |
| LessThanOrEqual    | `<=`          | Yes               |
| In                 | `IN`          | Yes               |
| IsNotNull          | `IS NOT NULL` | Yes               |
| IsNull             | `IS NULL`     | Yes               |
| InSet              | `IN (...)`    | Yes               |
| Not                | `NOT`         | Yes               |
| Or                 | `OR`          | Yes               |

## String Functions

| Expression      | Spark-Compatible? | Compatibility Notes                                                                                        |
| --------------- | ----------------- | ---------------------------------------------------------------------------------------------------------- |
| Ascii           | Yes               |                                                                                                            |
| BitLength       | Yes               |                                                                                                            |
| Chr             | Yes               |                                                                                                            |
| Concat          | Yes               | Only string inputs are supported                                                                           |
| ConcatWs        | Yes               |                                                                                                            |
| Contains        | Yes               |                                                                                                            |
| EndsWith        | Yes               |                                                                                                            |
| InitCap         | No                | Behavior is different in some cases, such as hyphenated names.                                             |
| Length          | Yes               |                                                                                                            |
| Like            | Yes               |                                                                                                            |
| Lower           | No                | Results can vary depending on locale and character set. Requires `spark.comet.caseConversion.enabled=true` |
| OctetLength     | Yes               |                                                                                                            |
| Reverse         | Yes               |                                                                                                            |
| RLike           | No                | Uses Rust regexp engine, which has different behavior to Java regexp engine                                |
| StartsWith      | Yes               |                                                                                                            |
| StringInstr     | Yes               |                                                                                                            |
| StringRepeat    | Yes               | Negative argument for number of times to repeat causes exception                                           |
| StringReplace   | Yes               |                                                                                                            |
| StringLPad      | Yes               |                                                                                                            |
| StringRPad      | Yes               |                                                                                                            |
| StringSpace     | Yes               |                                                                                                            |
| StringTranslate | Yes               |                                                                                                            |
| StringTrim      | Yes               |                                                                                                            |
| StringTrimBoth  | Yes               |                                                                                                            |
| StringTrimLeft  | Yes               |                                                                                                            |
| StringTrimRight | Yes               |                                                                                                            |
| Substring       | Yes               |                                                                                                            |
| Upper           | No                | Results can vary depending on locale and character set. Requires `spark.comet.caseConversion.enabled=true` |

## Date/Time Functions

| Expression     | SQL                          | Spark-Compatible? | Compatibility Notes                                                                                                  |
| -------------- | ---------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| DateAdd        | `date_add`                   | Yes               |                                                                                                                      |
| DateSub        | `date_sub`                   | Yes               |                                                                                                                      |
| DatePart       | `date_part(field, source)`   | Yes               | Supported values of `field`: `year`/`month`/`week`/`day`/`dayofweek`/`dayofweek_iso`/`doy`/`quarter`/`hour`/`minute` |
| Extract        | `extract(field FROM source)` | Yes               | Supported values of `field`: `year`/`month`/`week`/`day`/`dayofweek`/`dayofweek_iso`/`doy`/`quarter`/`hour`/`minute` |
| FromUnixTime   | `from_unixtime`              | No                | Does not support format, supports only -8334601211038 <= sec <= 8210266876799                                        |
| Hour           | `hour`                       | Yes               |                                                                                                                      |
| Minute         | `minute`                     | Yes               |                                                                                                                      |
| Second         | `second`                     | Yes               |                                                                                                                      |
| TruncDate      | `trunc`                      | Yes               |                                                                                                                      |
| TruncTimestamp | `trunc_date`                 | Yes               |                                                                                                                      |
| Year           | `year`                       | Yes               |                                                                                                                      |
| Month          | `month`                      | Yes               |                                                                                                                      |
| DayOfMonth     | `day`/`dayofmonth`           | Yes               |                                                                                                                      |
| DayOfWeek      | `dayofweek`                  | Yes               |                                                                                                                      |
| WeekDay        | `weekday`                    | Yes               |                                                                                                                      |
| DayOfYear      | `dayofyear`                  | Yes               |                                                                                                                      |
| WeekOfYear     | `weekofyear`                 | Yes               |                                                                                                                      |
| Quarter        | `quarter`                    | Yes               |                                                                                                                      |

## Math Expressions

| Expression     | SQL       | Spark-Compatible? | Compatibility Notes               |
|----------------|-----------|-------------------|-----------------------------------|
| Abs            | `abs`     | Yes               |                                   |
| Acos           | `acos`    | Yes               |                                   |
| Add            | `+`       | Yes               |                                   |
| Asin           | `asin`    | Yes               |                                   |
| Atan           | `atan`    | Yes               |                                   |
| Atan2          | `atan2`   | Yes               |                                   |
| BRound         | `bround`  | Yes               |                                   |
| Ceil           | `ceil`    | Yes               |                                   |
| Cos            | `cos`     | Yes               |                                   |
| Cot            | `cot`     | Yes               |                                   |
| Divide         | `/`       | Yes               |                                   |
| Exp            | `exp`     | Yes               |                                   |
| Expm1          | `expm1`   | Yes               |                                   |
| Floor          | `floor`   | Yes               |                                   |
| Hex            | `hex`     | Yes               |                                   |
| IntegralDivide | `div`     | Yes               |                                   |
| IsNaN          | `isnan`   | Yes               |                                   |
| Log            | `log`     | Yes               |                                   |
| Log2           | `log2`    | Yes               |                                   |
| Log10          | `log10`   | Yes               |                                   |
| Multiply       | `*`       | Yes               |                                   |
| Pow            | `power`   | Yes               |                                   |
| Rand           | `rand`    | Yes               |                                   |
| Randn          | `randn`   | Yes               |                                   |
| Remainder      | `%`       | Yes               |                                   |
| Round          | `round`   | Yes               |                                   |
| Signum         | `signum`  | Yes               |                                   |
| Sin            | `sin`     | Yes               |                                   |
| Sqrt           | `sqrt`    | Yes               |                                   |
| Subtract       | `-`       | Yes               |                                   |
| Tan            | `tan`     | Yes               |                                   |
| TryAdd         | `try_add` | Yes               | Only integer inputs are supported |
| TryDivide      | `try_div` | Yes               | Only integer inputs are supported |
| TryMultiply    | `try_mul` | Yes               | Only integer inputs are supported |
| TrySubtract    | `try_sub` | Yes               | Only integer inputs are supported |
| UnaryMinus     | `-`       | Yes               |                                   |
| Unhex          | `unhex`   | Yes               |                                   |

## Hashing Functions

| Expression  | Spark-Compatible? |
| ----------- | ----------------- |
| Md5         | Yes               |
| Murmur3Hash | Yes               |
| Sha2        | Yes               |
| XxHash64    | Yes               |

## Bitwise Expressions

| Expression   | SQL  | Spark-Compatible? |
| ------------ | ---- | ----------------- |
| BitwiseAnd   | `&`  | Yes               |
| BitwiseCount |      | Yes               |
| BitwiseGet   |      | Yes               |
| BitwiseOr    | `\|` | Yes               |
| BitwiseNot   | `~`  | Yes               |
| BitwiseXor   | `^`  | Yes               |
| ShiftLeft    | `<<` | Yes               |
| ShiftRight   | `>>` | Yes               |

## Aggregate Expressions

| Expression    | SQL        | Spark-Compatible?         | Compatibility Notes                                              |
| ------------- | ---------- | ------------------------- | ---------------------------------------------------------------- |
| Average       |            | Yes, except for ANSI mode |                                                                  |
| BitAndAgg     |            | Yes                       |                                                                  |
| BitOrAgg      |            | Yes                       |                                                                  |
| BitXorAgg     |            | Yes                       |                                                                  |
| BoolAnd       | `bool_and` | Yes                       |                                                                  |
| BoolOr        | `bool_or`  | Yes                       |                                                                  |
| Corr          |            | Yes                       |                                                                  |
| Count         |            | Yes                       |                                                                  |
| CovPopulation |            | Yes                       |                                                                  |
| CovSample     |            | Yes                       |                                                                  |
| First         |            | No                        | This function is not deterministic. Results may not match Spark. |
| Last          |            | No                        | This function is not deterministic. Results may not match Spark. |
| Max           |            | Yes                       |                                                                  |
| Min           |            | Yes                       |                                                                  |
| StddevPop     |            | Yes                       |                                                                  |
| StddevSamp    |            | Yes                       |                                                                  |
| Sum           |            | Yes, except for ANSI mode |                                                                  |
| VariancePop   |            | Yes                       |                                                                  |
| VarianceSamp  |            | Yes                       |                                                                  |

## Array Expressions

| Expression     | Spark-Compatible? | Compatibility Notes                                                                                                                                                                       |
| -------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ArrayAppend    | No                |                                                                                                                                                                                           |
| ArrayCompact   | No                |                                                                                                                                                                                           |
| ArrayContains  | Yes               |                                                                                                                                                                                           |
| ArrayDistinct  | No                | Behaves differently than spark. Comet first sorts then removes duplicates while Spark preserves the original order.                                                                       |
| ArrayExcept    | No                |                                                                                                                                                                                           |
| ArrayFilter    | Yes               | Only supports case where function is `IsNotNull`                                                                                                                                          |
| ArrayInsert    | No                |                                                                                                                                                                                           |
| ArrayIntersect | No                |                                                                                                                                                                                           |
| ArrayJoin      | No                |                                                                                                                                                                                           |
| ArrayMax       | Yes               |                                                                                                                                                                                           |
| ArrayMin       | Yes               |                                                                                                                                                                                           |
| ArrayRemove    | Yes               |                                                                                                                                                                                           |
| ArrayRepeat    | No                |                                                                                                                                                                                           |
| ArrayUnion     | No                | Behaves differently than spark. Comet sorts the input arrays before performing the union, while Spark preserves the order of the first array and appends unique elements from the second. |
| ArraysOverlap  | No                |                                                                                                                                                                                           |
| CreateArray    | Yes               |                                                                                                                                                                                           |
| ElementAt      | Yes               | Input must be an array. Map inputs are not supported.                                                                                                                                     |
| Flatten        | Yes               |                                                                                                                                                                                           |
| GetArrayItem   | Yes               |                                                                                                                                                                                           |

## Map Expressions

| Expression    | Spark-Compatible? |
| ------------- | ----------------- |
| GetMapValue   | Yes               |
| MapKeys       | Yes               |
| MapEntries    | Yes               |
| MapValues     | Yes               |
| MapFromArrays | Yes               |

## Struct Expressions

| Expression           | Spark-Compatible? |
| -------------------- | ----------------- |
| CreateNamedStruct    | Yes               |
| GetArrayStructFields | Yes               |
| GetStructField       | Yes               |
| StructsToJson        | Yes               |

## Conversion Expressions

| Expression | Spark-Compatible         | Compatibility Notes                                                                         |
| ---------- | ------------------------ | ------------------------------------------------------------------------------------------- |
| Cast       | Depends on specific cast | See the [Comet Compatibility Guide] for list of supported cast expressions and known issues |

## SortOrder

| Expression  | Spark-Compatible? | Compatibility Notes                                                         |
|-------------| ----------------- | --------------------------------------------------------------------------- |
| NullsFirst  | Yes               |                                                                             |
| NullsLast   | Yes               |                                                                             |
| Ascending   | Yes               |                                                                             |
| Descending  | Yes               |                                                                             |

## Other

| Expression                   | Spark-Compatible? | Compatibility Notes                                                         |
|------------------------------| ----------------- | --------------------------------------------------------------------------- |
| Alias                        | Yes               |                                                                             |
| AttributeReference           | Yes               |                                                                             |
| BloomFilterMightContain      | Yes               |                                                                             |
| Coalesce                     | Yes               |                                                                             |
| CheckOverflow                | Yes               |                                                                             |
| KnownFloatingPointNormalized | Yes               |                                                                             |
| Literal                      | Yes               |                                                                             |
| MakeDecimal                  | Yes               |                                                                             |
| MonotonicallyIncreasingID    | Yes               |                                                                             |
| NormalizeNaNAndZero          | Yes               |                                                                             |
| PromotePrecision             | Yes               |                                                                             |
| RegExpReplace                | No                | Uses Rust regexp engine, which has different behavior to Java regexp engine |
| ScalarSubquery               | Yes               |                                                                             |
| SparkPartitionID             | Yes               |                                                                             |
| ToPrettyString               | Yes               |                                                                             |
| UnscaledValue                | Yes               |                                                                             |

[Comet Configuration Guide]: configs.md
[Comet Compatibility Guide]: compatibility.md
