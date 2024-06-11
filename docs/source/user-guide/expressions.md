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

## Unary Arithmetic

| Expression       | Notes |
| ---------------- | ----- |
| UnaryMinus (`-`) |       |

## Binary Arithmeticx

| Expression      | Notes                                               |
| --------------- | --------------------------------------------------- |
| Add (`+`)       |                                                     |
| Subtract (`-`)  |                                                     |
| Multiply (`*`)  |                                                     |
| Divide (`/`)    |                                                     |
| Remainder (`%`) | Comet produces `NaN` instead of `NULL` for `% -0.0` |

## Conditional Expressions

| Expression | Notes |
| ---------- | ----- |
| CaseWhen   |       |
| If         |       |

## Comparison

| Expression                | Notes |
| ------------------------- | ----- |
| EqualTo (`=`)             |       |
| EqualNullSafe (`<=>`)     |       |
| GreaterThan (`>`)         |       |
| GreaterThanOrEqual (`>=`) |       |
| LessThan (`<`)            |       |
| LessThanOrEqual (`<=`)    |       |
| IsNull (`IS NULL`)        |       |
| IsNotNull (`IS NOT NULL`) |       |
| In (`IN`)                 |       |

## String Functions

| Expression      | Notes                                                                                                       |
| --------------- | ----------------------------------------------------------------------------------------------------------- |
| Ascii           |                                                                                                             |
| BitLength       |                                                                                                             |
| Chr             |                                                                                                             |
| ConcatWs        |                                                                                                             |
| Contains        |                                                                                                             |
| EndsWith        |                                                                                                             |
| InitCap         |                                                                                                             |
| Instr           |                                                                                                             |
| Length          |                                                                                                             |
| Like            |                                                                                                             |
| Lower           |                                                                                                             |
| OctetLength     |                                                                                                             |
| Repeat          | Negative argument for number of times to repeat causes exception                                            |
| Replace         |                                                                                                             |
| Reverse         |                                                                                                             |
| StartsWith      |                                                                                                             |
| StringSpace     |                                                                                                             |
| StringTrim      |                                                                                                             |
| StringTrimBoth  |                                                                                                             |
| StringTrimLeft  |                                                                                                             |
| StringTrimRight |                                                                                                             |
| Substring       |                                                                                                             |
| Translate       |                                                                                                             |
| Upper           |                                                                                                             |

## Date/Time Functions

| Expression     | Notes                    |
| -------------- | ------------------------ |
| DatePart       | Only `year` is supported |
| Extract        | Only `year` is supported |
| Hour           |                          |
| Minute         |                          |
| Second         |                          |
| TruncDate      |                          |
| TruncTimestamp |                          |
| Year           |                          |

## Math Expressions

| Expression | Notes                                                               |
| ---------- | ------------------------------------------------------------------- |
| Abs        |                                                                     |
| Acos       |                                                                     |
| Asin       |                                                                     |
| Atan       |                                                                     |
| Atan2      |                                                                     |
| Ceil       |                                                                     |
| Cos        |                                                                     |
| Exp        |                                                                     |
| Floor      |                                                                     |
| Log        | log(0) will produce `-Infinity` unlike Spark which returns `null`   |
| Log2       | log2(0) will produce `-Infinity` unlike Spark which returns `null`  |
| Log10      | log10(0) will produce `-Infinity` unlike Spark which returns `null` |
| Pow        |                                                                     |
| Round      |                                                                     |
| Signum     | Signum does not differentiate between `0.0` and `-0.0`              |
| Sin        |                                                                     |
| Sqrt       |                                                                     |
| Tan        |                                                                     |

## Hashing Functions

| Expression | Notes |
| ---------- | ----- |
| Md5        |       |
| Hash       |       |
| Sha2       |       |
| XxHash64   |       |

## Boolean Expressions

| Expression | Notes |
| ---------- | ----- |
| And        |       |
| Or         |       |
| Not        |       |

## Bitwise Expressions

| Expression           | Notes |
| -------------------- | ----- |
| ShiftLeft (`<<`)     |       |
| ShiftRight (`>>`)    |       |
| BitAnd (`&`)         |       |
| BitOr (`\|`)         |       |
| BitXor (`^`)         |       |
| BitwiseNot (`~`)     |       |
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

## Other

| Expression              | Notes                                                                           |
| ----------------------- | ------------------------------------------------------------------------------- |
| Cast                    | See compatibility guide for list of supported cast expressions and known issues |
| BloomFilterMightContain |                                                                                 |
| ScalarSubquery          |                                                                                 |
| Coalesce                |                                                                                 |
| NormalizeNaNAndZero     |                                                                                 |
