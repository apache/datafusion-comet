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

## Arithmetic

| Expression                                  | Notes                                                                               |
| ------------------------------------------- | ----------------------------------------------------------------------------------- |
| Literal values of supported data types      |                                                                                     |
| Unary Arithmetic (`+`, `-`)                 |                                                                                     |
| Binary Arithmetic (`+`, `-`, `*`, `/`, `%`) | Some operations will throw an overflow exception even when ANSI mode is not enabled |

## Conditional Expressions

| Expression | Notes |
| ---------- | ----- |
| Case When  |       |
| If         |       |

## Comparison

| Expression         | Notes |
| ------------------ | ----- |
| EqualTo            |       |
| EqualNullSafe      |       |
| GreaterThan        |       |
| GreaterThanOrEqual |       |
| LessThan           |       |
| LessThanOrEqual    |       |
| IsNull             |       |
| IsNotNull          |       |
| In                 |       |

## String Functions

| Expression             | Notes                                                            |
| ---------------------- |------------------------------------------------------------------|
| Substring              |                                                                  |
| StringSpace            |                                                                  |
| Like                   |                                                                  |
| Contains               |                                                                  |
| Startswith             |                                                                  |
| Endswith               |                                                                  |
| Ascii                  |                                                                  |
| Bit_length             |                                                                  |
| Octet_length           |                                                                  |
| Upper                  |                                                                  |
| Lower                  |                                                                  |
| Chr                    |                                                                  |
| Initcap                |                                                                  |
| Trim/Btrim/Ltrim/Rtrim |                                                                  |
| Concat_ws              |                                                                  |
| Repeat                 | Negative argument for number of times to repeat causes exception |
| Length                 |                                                                  |
| Reverse                |                                                                  |
| Instr                  |                                                                  |
| Replace                |                                                                  |
| Translate              |                                                                  |

## Date/Time Functions

| Expression | Notes                    |
| ---------- | ------------------------ |
| Year       |                          |
| Hour       |                          |
| Minute     |                          |
| Second     |                          |
| date_part  | Only `year` is supported |
| extract    | Only `year` is supported |

## Math Expressions

| Expression | Notes |
| ---------- | ----- |
| Abs        |       |
| Acos       |       |
| Asin       |       |
| Atan       |       |
| Atan2      |       |
| Cos        |       |
| Exp        |       |
| Ln         |       |
| Log10      |       |
| Log2       |       |
| Pow        |       |
| Round      |       |
| Signum     |       |
| Sin        |       |
| Sqrt       |       |
| Tan        |       |
| Ceil       |       |
| Floor      |       |

## Hashing Functions

| Expression | Notes |
| ---------- | ----- |
| Md5        |       |
| Sha2       |       |
| Hash       |       |
| Xxhash64   |       |

## Boolean Expressions

| Expression | Notes |
| ---------- | ----- |
| And        |       |
| Or         |       |
| Not        |       |

## Bitwise Expressions

| Expression           | Notes |
| -------------------- | ----- |
| Shiftright/Shiftleft |       |
| BitAnd               |       |
| BitOr                |       |
| BitXor               |       |
| BoolAnd              |       |
| BoolOr               |       |

## Aggregate Expressions

| Expression    | Notes |
| ------------- | ----- |
| Count         |       |
| Sum           |       |
| Max           |       |
| Min           |       |
| Avg           |       |
| First         |       |
| Last          |       |
| CovPopulation |       |
| CovSample     |       |
| VariancePop   |       |
| VarianceSamp  |       |
| StddevPop     |       |
| StddevSamp    |       |
| Corr          |       |

## Other

| Expression              | Notes                                                                           |
| ----------------------- | ------------------------------------------------------------------------------- |
| Cast                    | See compatibility guide for list of supported cast expressions and known issues |
| BloomFilterMightContain |                                                                                 |
| Coalesce                |                                                                                 |
