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

## Literal Values

| Expression                             | Notes |
| -------------------------------------- | ----- |
| Literal values of supported data types |       |

## Arithmetic Expressions

| Expression             | Notes                                                                                                                                                                                                                                              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Add (`+`)              |                                                                                                                                                                                                                                                    |
| Subtract (`-`)         |                                                                                                                                                                                                                                                    |
| Multiply (`*`)         |                                                                                                                                                                                                                                                    |
| Divide (`/`)           |                                                                                                                                                                                                                                                    |
| IntegralDivide (`div`) | All operands are cast to DecimalType (in case the input type is not already decima type) with precision 19 and scale 0. Please set `spark.comet.expression.allowIncompatible` to `true` to enable DataFusion’s cast operation for LongType inputs. |
| Remainder (`%`)        |                                                                                                                                                                                                                                                    |
| UnaryMinus (`-`)       |                                                                                                                                                                                                                                                    |

## Binary Try Arithmetic

| Expression | Notes                                                                      |
| ---------- | -------------------------------------------------------------------------- |
| `try_add`  | Adds operands (IntegerTypes only) or results NULL incase of overflow       |
| `try_sub`  | Subtracts operands (IntegerTypes only) or results NULL incase of overflow  |
| `try_mul`  | Multiplies operands (IntegerTypes only) or results NULL incase of overflow |
| `try_div`  | Subtracts operands (IntegerTypes only) or results NULL incase of overflow  |

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

| Expression | Notes |
| ---------- | ----- |
| Acos       |       |
| Asin       |       |
| Atan       |       |
| Atan2      |       |
| Ceil       |       |
| Cos        |       |
| Exp        |       |
| Expm1      |       |
| Floor      |       |
| Hex        |       |
| IsNaN      |       |
| Log        |       |
| Log2       |       |
| Log10      |       |
| Pow        |       |
| Rand       |       |
| Randn      |       |
| Round      |       |
| Signum     |       |
| Sin        |       |
| Sqrt       |       |
| Tan        |       |
| Unhex      |       |

## Hashing Functions

| Expression  | Notes |
| ----------- | ----- |
| Md5         |       |
| Murmur3Hash |       |
| Sha2        |       |
| XxHash64    |       |

## Bitwise Expressions

| Expression           | Notes |
| -------------------- | ----- |
| BitwiseAnd (`&`)     |       |
| BitwiseCount         |       |
| BitwiseGet           |       |
| BitwiseOr (`\|`)     |       |
| BitwiseNot (`~`)     |       |
| BitwiseXor (`^`)     |       |
| ShiftLeft (`<<`)     |       |
| ShiftRight (`>>`)    |       |
| BoolAnd (`bool_and`) |       |
| BoolOr (`bool_or`)   |       |

## Aggregate Expressions

| Expression    | Notes |
| ------------- | ----- |
| Avg           |       |
| BitAndAgg     |       |
| BitOrAgg      |       |
| BitXorAgg     |       |
| Corr          |       |
| Count         |       |
| CovPopulation |       |
| CovSample     |       |
| First         |       |
| Last          |       |
| Max           |       |
| Min           |       |
| StddevPop     |       |
| StddevSamp    |       |
| Sum           |       |
| VariancePop   |       |
| VarianceSamp  |       |

## Arrays

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

## Maps

| Expression    | Notes |
| ------------- | ----- |
| GetMapValue   |       |
| MapKeys       |       |
| MapEntries    |       |
| MapValues     |       |
| MapFromArrays |       |

## Structs

| Expression        | Notes |
| ----------------- | ----- |
| CreateNamedStruct |       |
| GetStructField    |       |
| StructsToJson     |       |

## Other

| Expression              | Notes                                                                           |
| ----------------------- | ------------------------------------------------------------------------------- |
| Cast                    | See compatibility guide for list of supported cast expressions and known issues |
| BloomFilterMightContain |                                                                                 |
| ScalarSubquery          |                                                                                 |
| Coalesce                |                                                                                 |
| NormalizeNaNAndZero     |                                                                                 |
| ToPrettyString          |                                                                                 |
