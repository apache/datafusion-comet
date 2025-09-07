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

The following Spark expressions are currently available. Any known compatibility issues are noted in the following tables.

Expressions that are marked as Spark-compatible will either run natively in Comet and provide the same
results as Spark, or will fall back to Spark for cases that would not be compatible.

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

| Expression      | Spark-Compatible? | Compatibility Notes                                              |
| --------------- | ----------------- | ---------------------------------------------------------------- |
| Ascii           |                   |                                                                  |
| BitLength       |                   |                                                                  |
| Chr             |                   |                                                                  |
| ConcatWs        |                   |                                                                  |
| Contains        |                   |                                                                  |
| EndsWith        |                   |                                                                  |
| InitCap         |                   |                                                                  |
| Length          |                   |                                                                  |
| Like            |                   |                                                                  |
| Lower           |                   |                                                                  |
| OctetLength     |                   |                                                                  |
| Reverse         |                   |                                                                  |
| RLike           |                   |                                                                  |
| StartsWith      |                   |                                                                  |
| StringInstr     |                   |                                                                  |
| StringRepeat    |                   | Negative argument for number of times to repeat causes exception |
| StringReplace   |                   |                                                                  |
| StringRPad      |                   |                                                                  |
| StringSpace     |                   |                                                                  |
| StringTranslate |                   |                                                                  |
| StringTrim      |                   |                                                                  |
| StringTrimBoth  |                   |                                                                  |
| StringTrimLeft  |                   |                                                                  |
| StringTrimRight |                   |                                                                  |
| Substring       |                   |                                                                  |
| Upper           |                   |                                                                  |

## Date/Time Functions

| Expression     | Spark-Compatible? | Compatibility Notes                                                           |
| -------------- | ----------------- | ----------------------------------------------------------------------------- |
| DateAdd        |                   |                                                                               |
| DateSub        |                   |                                                                               |
| DatePart       |                   | Only `year` is supported                                                      |
| Extract        |                   | Only `year` is supported                                                      |
| FromUnixTime   |                   | Does not support format, supports only -8334601211038 <= sec <= 8210266876799 |
| Hour           |                   |                                                                               |
| Minute         |                   |                                                                               |
| Second         |                   |                                                                               |
| TruncDate      |                   |                                                                               |
| TruncTimestamp |                   |                                                                               |
| Year           |                   |                                                                               |

## Math Expressions

| Expression     | SQL       | Spark-Compatible?          | Compatibility Notes                                                                                                                                                                                                                                |
| -------------- | --------- | -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Acos           | `acos`    |                            |                                                                                                                                                                                                                                                    |
| Add            | `+`       |
| Asin           | `asin`    |                            |                                                                                                                                                                                                                                                    |
| Atan           | `atan`    |                            |                                                                                                                                                                                                                                                    |
| Atan2          | `atan2`   |                            |                                                                                                                                                                                                                                                    |
| BRound         | `bround`  |                            |                                                                                                                                                                                                                                                    |
| Ceil           | `ceil`    |                            |                                                                                                                                                                                                                                                    |
| Cos            | `cos`     |                            |                                                                                                                                                                                                                                                    |
| Divide         | `/`       |                            |                                                                                                                                                                                                                                                    |
| Exp            | `exp`     |                            |                                                                                                                                                                                                                                                    |
| Expm1          | `expm1`   |                            |                                                                                                                                                                                                                                                    |
| Floor          | `floor`   |                            |                                                                                                                                                                                                                                                    |
| Hex            | `hex`     |                            |                                                                                                                                                                                                                                                    |
| IntegralDivide | `div`     | Yes, except for ANSI mode. | All operands are cast to DecimalType (in case the input type is not already decima type) with precision 19 and scale 0. Please set `spark.comet.expression.allowIncompatible` to `true` to enable DataFusion’s cast operation for LongType inputs. |
| IsNaN          | `isnan`   |                            |                                                                                                                                                                                                                                                    |
| Log            | `log`     |                            |                                                                                                                                                                                                                                                    |
| Log2           | `log2`    |                            |                                                                                                                                                                                                                                                    |
| Log10          | `log10`   |                            |                                                                                                                                                                                                                                                    |
| Multiply       | `*`       |                            |                                                                                                                                                                                                                                                    |
| Pow            | `power`   |                            |                                                                                                                                                                                                                                                    |
| Rand           | `rand`    |                            |                                                                                                                                                                                                                                                    |
| Randn          | `randn`   |                            |                                                                                                                                                                                                                                                    |
| Remainder      | `%`       |                            |                                                                                                                                                                                                                                                    |
| Round          | `round`   |                            |                                                                                                                                                                                                                                                    |
| Signum         | `signum`  |                            |                                                                                                                                                                                                                                                    |
| Sin            | `sin`     |                            |                                                                                                                                                                                                                                                    |
| Sqrt           | `sqrt`    |                            |                                                                                                                                                                                                                                                    |
| Subtract       | `-`       |                            |                                                                                                                                                                                                                                                    |
| Tan            | `tan`     |                            |                                                                                                                                                                                                                                                    |
| TryAdd         | `try_add` | Yes                        | Only integer inputs are supported                                                                                                                                                                                                                  |
| TryDivide      | `try_div` | Yes                        | Only integer inputs are supported                                                                                                                                                                                                                  |
| TryMultiply    | `try_mul` | Yes                        | Only integer inputs are supported                                                                                                                                                                                                                  |
| TrySubtract    | `try_sub` | Yes                        | Only integer inputs are supported                                                                                                                                                                                                                  |
| UnaryMinus     | `-`       |                            |                                                                                                                                                                                                                                                    |
| Unhex          | `unhex`   |                            |                                                                                                                                                                                                                                                    |

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

| Expression    | SQL        | Spark-Compatible? |
| ------------- | ---------- | ----------------- |
| Average       |            |                   |
| BitAndAgg     |            |                   |
| BitOrAgg      |            |                   |
| BitXorAgg     |            |                   |
| BoolAnd       | `bool_and` |                   |
| BoolOr        | `bool_or`  |                   |
| Corr          |            |                   |
| Count         |            |                   |
| CovPopulation |            |                   |
| CovSample     |            |                   |
| First         |            |                   |
| Last          |            |                   |
| Max           |            |                   |
| Min           |            |                   |
| StddevPop     |            |                   |
| StddevSamp    |            |                   |
| Sum           |            |                   |
| VariancePop   |            |                   |
| VarianceSamp  |            |                   |

## Array Expressions

| Expression     | Spark-Compatible? | Compatibility Notes                                                                                                                                                                            |
| -------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ArrayAppend    | No                |                                                                                                                                                                                                |
| ArrayCompact   | No                |                                                                                                                                                                                                |
| ArrayContains  | Yes               |                                                                                                                                                                                                |
| ArrayDistinct  | No                | Behaves differently than spark. DataFusion first sorts then removes duplicates while spark preserves the original order.                                                                       |
| ArrayExcept    | No                |                                                                                                                                                                                                |
| ArrayInsert    | No                |                                                                                                                                                                                                |
| ArrayIntersect | No                |                                                                                                                                                                                                |
| ArrayJoin      | No                |                                                                                                                                                                                                |
| ArrayMax       | Yes               |                                                                                                                                                                                                |
| ArrayMin       | Yes               |                                                                                                                                                                                                |
| ArrayRemove    | Yes               |                                                                                                                                                                                                |
| ArrayRepeat    | No                |                                                                                                                                                                                                |
| ArrayUnion     | No                | Behaves differently than spark. DataFusion sorts the input arrays before performing the union, while spark preserves the order of the first array and appends unique elements from the second. |
| ArraysOverlap  | No                |                                                                                                                                                                                                |
| CreateArray    | Yes               |                                                                                                                                                                                                |
| ElementAt      | Yes               | Arrays only                                                                                                                                                                                    |
| Flatten        | Yes               |                                                                                                                                                                                                |
| GetArrayItem   | Yes               |                                                                                                                                                                                                |

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

| Expression | Spark-Compatible         | Compatibility Notes                                                             |
| ---------- | ------------------------ | ------------------------------------------------------------------------------- |
| Cast       | Depends on specific cast | See compatibility guide for list of supported cast expressions and known issues |

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
