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

## Conditional Expressions

| Expression | Notes |
| ---------- | ----- |
| CaseWhen   |       |
| If         |       |

## Predicate Expressions

| Expression                | Notes |
| ------------------------- | ----- |
| And                       |       |
| EqualTo (`=`)             |       |
| EqualNullSafe (`<=>`)     |       |
| GreaterThan (`>`)         |       |
| GreaterThanOrEqual (`>=`) |       |
| LessThan (`<`)            |       |
| LessThanOrEqual (`<=`)    |       |
| In (`IN`)                 |       |
| IsNotNull (`IS NOT NULL`) |       |
| IsNull (`IS NULL`)        |       |
| InSet                     |       |
| Not                       |       |
| Or                        |       |

## String Functions

| Expression      | Notes                                                            |
| --------------- | ---------------------------------------------------------------- |
| Ascii           |                                                                  |
| BitLength       |                                                                  |
| Chr             |                                                                  |
| ConcatWs        |                                                                  |
| Contains        |                                                                  |
| EndsWith        |                                                                  |
| InitCap         |                                                                  |
| Length          |                                                                  |
| Like            |                                                                  |
| Lower           |                                                                  |
| OctetLength     |                                                                  |
| Reverse         |                                                                  |
| RLike           |                                                                  |
| StartsWith      |                                                                  |
| StringInstr     |                                                                  |
| StringRepeat    | Negative argument for number of times to repeat causes exception |
| StringReplace   |                                                                  |
| StringRPad      |                                                                  |
| StringSpace     |                                                                  |
| StringTranslate |                                                                  |
| StringTrim      |                                                                  |
| StringTrimBoth  |                                                                  |
| StringTrimLeft  |                                                                  |
| StringTrimRight |                                                                  |
| Substring       |                                                                  |
| Upper           |                                                                  |

## Date/Time Functions

| Expression     | Notes                                                                         |
| -------------- | ----------------------------------------------------------------------------- |
| DateAdd        |                                                                               |
| DateSub        |                                                                               |
| DatePart       | Only `year` is supported                                                      |
| Extract        | Only `year` is supported                                                      |
| FromUnixTime   | Does not support format, supports only -8334601211038 <= sec <= 8210266876799 |
| Hour           |                                                                               |
| Minute         |                                                                               |
| Second         |                                                                               |
| TruncDate      |                                                                               |
| TruncTimestamp |                                                                               |
| Year           |                                                                               |

## Math Expressions

| Expression     | SQL       | Notes                                                                                                                                                                                                                                              |
| -------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Acos           | `acos`    |                                                                                                                                                                                                                                                    |
| Add            | `+`       |
| Asin           | `asin`    |                                                                                                                                                                                                                                                    |
| Atan           | `atan`    |                                                                                                                                                                                                                                                    |
| Atan2          | `atan2`   |                                                                                                                                                                                                                                                    |
| BRound         | `bround`  |                                                                                                                                                                                                                                                    |
| Ceil           | `ceil`    |                                                                                                                                                                                                                                                    |
| Cos            | `cos`     |                                                                                                                                                                                                                                                    |
| Divide         | `/`       |                                                                                                                                                                                                                                                    |
| Exp            | `exp`     |                                                                                                                                                                                                                                                    |
| Expm1          | `expm1`   |                                                                                                                                                                                                                                                    |
| Floor          | `floor`   |                                                                                                                                                                                                                                                    |
| Hex            | `hex`     |                                                                                                                                                                                                                                                    |
| IntegralDivide | `div`     | All operands are cast to DecimalType (in case the input type is not already decima type) with precision 19 and scale 0. Please set `spark.comet.expression.allowIncompatible` to `true` to enable DataFusion’s cast operation for LongType inputs. |
| IsNaN          | `isnan`   |                                                                                                                                                                                                                                                    |
| Log            | `log`     |                                                                                                                                                                                                                                                    |
| Log2           | `log2`    |                                                                                                                                                                                                                                                    |
| Log10          | `log10`   |                                                                                                                                                                                                                                                    |
| Multiply       | `*`       |                                                                                                                                                                                                                                                    |
| Pow            | `power`   |                                                                                                                                                                                                                                                    |
| Rand           | `rand`    |                                                                                                                                                                                                                                                    |
| Randn          | `randn`   |                                                                                                                                                                                                                                                    |
| Remainder      | `%`       |                                                                                                                                                                                                                                                    |
| Round          | `round`   |                                                                                                                                                                                                                                                    |
| Signum         | `signum`  |                                                                                                                                                                                                                                                    |
| Sin            | `sin`     |                                                                                                                                                                                                                                                    |
| Sqrt           | `sqrt`    |                                                                                                                                                                                                                                                    |
| Subtract       | `-`       |                                                                                                                                                                                                                                                    |
| Tan            | `tan`     |                                                                                                                                                                                                                                                    |
| TryAdd         | `try_add` | Adds operands (IntegerTypes only) or returns NULL in case of overflow                                                                                                                                                                              |
| TryDivide      | `try_div` | Subtracts operands (IntegerTypes only) or returns NULL in case of overflow                                                                                                                                                                         |
| TryMultiply    | `try_mul` | Multiplies operands (IntegerTypes only) or returns NULL in case of overflow                                                                                                                                                                        |
| TrySubtract    | `try_sub` | Subtracts operands (IntegerTypes only) or returns NULL in case of overflow                                                                                                                                                                         |
| UnaryMinus     | `-`       |                                                                                                                                                                                                                                                    |
| Unhex          | `unhex`   |                                                                                                                                                                                                                                                    |

## Hashing Functions

| Expression  | Notes |
| ----------- | ----- |
| Md5         |       |
| Murmur3Hash |       |
| Sha2        |       |
| XxHash64    |       |

## Bitwise Expressions

| Expression        | Notes |
| ----------------- | ----- |
| BitwiseAnd (`&`)  |       |
| BitwiseCount      |       |
| BitwiseGet        |       |
| BitwiseOr (`\|`)  |       |
| BitwiseNot (`~`)  |       |
| BitwiseXor (`^`)  |       |
| ShiftLeft (`<<`)  |       |
| ShiftRight (`>>`) |       |

## Aggregate Expressions

| Expression           | Notes |
| -------------------- | ----- |
| Average              |       |
| BitAndAgg            |       |
| BitOrAgg             |       |
| BitXorAgg            |       |
| BoolAnd (`bool_and`) |       |
| BoolOr (`bool_or`)   |       |
| Corr                 |       |
| Count                |       |
| CovPopulation        |       |
| CovSample            |       |
| First                |       |
| Last                 |       |
| Max                  |       |
| Min                  |       |
| StddevPop            |       |
| StddevSamp           |       |
| Sum                  |       |
| VariancePop          |       |
| VarianceSamp         |       |

## Array Expressions

| Expression     | Notes                                                                                                                                                                                                        |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| ArrayAppend    | Experimental                                                                                                                                                                                                 |
| ArrayCompact   | Experimental                                                                                                                                                                                                 |
| ArrayContains  | Experimental                                                                                                                                                                                                 |
| ArrayDistinct  | Experimental: behaves differently than spark. Datafusion first sorts then removes duplicates while spark preserves the original order.                                                                       |
| ArrayExcept    | Experimental                                                                                                                                                                                                 |
| ArrayInsert    | Experimental                                                                                                                                                                                                 |
| ArrayIntersect | Experimental                                                                                                                                                                                                 |
| ArrayJoin      | Experimental                                                                                                                                                                                                 |
| ArrayMax       | Experimental                                                                                                                                                                                                 |
| ArrayMin       |                                                                                                                                                                                                              |
| ArrayRemove    |                                                                                                                                                                                                              |
| ArrayRepeat    | Experimental                                                                                                                                                                                                 |
| ArraysOverlap  | Experimental                                                                                                                                                                                                 |
| ArrayUnion     | Experimental: behaves differently than spark. Datafusion sorts the input arrays before performing the union, while spark preserves the order of the first array and appends unique elements from the second. |
| CreateArray    |                                                                                                                                                                                                              |
| ElementAt      | Arrays only                                                                                                                                                                                                  |
| Flatten        |                                                                                                                                                                                                              |
| GetArrayItem   |                                                                                                                                                                                                              |

## Map Expressions

| Expression    | Notes |
| ------------- | ----- |
| GetMapValue   |       |
| MapKeys       |       |
| MapEntries    |       |
| MapValues     |       |
| MapFromArrays |       |

## Struct Expressions

| Expression           | Notes |
| -------------------- | ----- |
| CreateNamedStruct    |       |
| GetArrayStructFields |       |
| GetStructField       |       |
| StructsToJson        |       |

## Conversion Expressions

| Expression | Notes                                                                           |
| ---------- | ------------------------------------------------------------------------------- |
| Cast       | See compatibility guide for list of supported cast expressions and known issues |

## Other

| Expression                   | Notes                                  |
| ---------------------------- | -------------------------------------- |
| Alias                        |                                        |
| AttributeRefernce            |                                        |
| BloomFilterMightContain      |                                        |
| Coalesce                     |                                        |
| CheckOverflow                |                                        |
| KnownFloatingPointNormalized |                                        |
| Literal                      | Literal values of supported data types |
| MakeDecimal                  |                                        |
| MonotonicallyIncreasingID    |                                        |
| NormalizeNaNAndZero          |                                        |
| PromotePrecision             |                                        |
| RegExpReplace                |                                        |
| ScalarSubquery               |                                        |
| SparkPartitionID             |                                        |
| ToPrettyString               |                                        |
| UnscaledValue                |                                        |
