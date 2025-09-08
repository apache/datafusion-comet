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

Expressions that are not Spark-compatible can be enabled by setting `spark.comet.expression.allowIncompatible=true`.

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
| ConcatWs        | Yes               |                                                                                                            |
| Contains        | Yes               |                                                                                                            |
| EndsWith        | Yes               |                                                                                                            |
| InitCap         | No                | Requires `spark.comet.exec.initCap.enabled=true`                                                           |
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

| Expression     | SQL                          | Spark-Compatible? | Compatibility Notes                                                           |
| -------------- | ---------------------------- | ----------------- | ----------------------------------------------------------------------------- |
| DateAdd        | `date_add`                   | Yes               |                                                                               |
| DateSub        | `date_sub`                   | Yes               |                                                                               |
| DatePart       | `date_part(field, source)`   | Yes               | Only `year` is supported                                                      |
| Extract        | `extract(field FROM source)` | Yes               | Only `year` is supported                                                      |
| FromUnixTime   | `from_unixtime`              | No                | Does not support format, supports only -8334601211038 <= sec <= 8210266876799 |
| Hour           | `hour`                       | Yes               |                                                                               |
| Minute         | `minute`                     | Yes               |                                                                               |
| Second         | `second`                     | Yes               |                                                                               |
| TruncDate      | `trunc`                      | Yes               |                                                                               |
| TruncTimestamp | `trunc_date`                 | Yes               |                                                                               |
| Year           | `year`                       | Yes               |                                                                               |

## Math Expressions

| Expression     | SQL       | Spark-Compatible?          | Compatibility Notes               |
| -------------- | --------- | -------------------------- | --------------------------------- |
| Acos           | `acos`    | Yes                        |                                   |
| Add            | `+`       | Yes                        | ANSI mode is not supported.       |
| Asin           | `asin`    | Yes                        |                                   |
| Atan           | `atan`    | Yes                        |                                   |
| Atan2          | `atan2`   | Yes                        |                                   |
| BRound         | `bround`  | Yes                        | ANSI mode is not supported.       |
| Ceil           | `ceil`    | Yes                        |                                   |
| Cos            | `cos`     | Yes                        |                                   |
| Divide         | `/`       | Yes                        | ANSI mode is not supported.       |
| Exp            | `exp`     | Yes                        |                                   |
| Expm1          | `expm1`   | Yes                        |                                   |
| Floor          | `floor`   | Yes                        |                                   |
| Hex            | `hex`     | Yes                        |                                   |
| IntegralDivide | `div`     | Yes, except for ANSI mode. | ANSI mode is not supported.       |
| IsNaN          | `isnan`   | Yes                        |                                   |
| Log            | `log`     | Yes                        |                                   |
| Log2           | `log2`    | Yes                        |                                   |
| Log10          | `log10`   | Yes                        |                                   |
| Multiply       | `*`       | Yes                        | ANSI mode is not supported.       |
| Pow            | `power`   | Yes                        |                                   |
| Rand           | `rand`    | Yes                        |                                   |
| Randn          | `randn`   | Yes                        |                                   |
| Remainder      | `%`       | Yes                        | ANSI mode is not supported.       |
| Round          | `round`   | Yes                        | ANSI mode is not supported.       |
| Signum         | `signum`  | Yes                        |                                   |
| Sin            | `sin`     | Yes                        |                                   |
| Sqrt           | `sqrt`    | Yes                        |                                   |
| Subtract       | `-`       | Yes                        | ANSI mode is not supported.       |
| Tan            | `tan`     | Yes                        |                                   |
| TryAdd         | `try_add` | Yes                        | Only integer inputs are supported |
| TryDivide      | `try_div` | Yes                        | Only integer inputs are supported |
| TryMultiply    | `try_mul` | Yes                        | Only integer inputs are supported |
| TrySubtract    | `try_sub` | Yes                        | Only integer inputs are supported |
| UnaryMinus     | `-`       | Yes                        |                                   |
| Unhex          | `unhex`   | Yes                        |                                   |

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
| ElementAt      | Yes               | Arrays only                                                                                                                                                                               |
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

## Other

| Expression                   | Spark-Compatible? | Compatibility Notes                                                         |
| ---------------------------- | ----------------- | --------------------------------------------------------------------------- |
| Alias                        | Yes               |                                                                             |
| AttributeRefernce            | Yes               |                                                                             |
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

[Comet Compatibility Guide]: compatibility.md
