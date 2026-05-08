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
| LessThan           | `<`           |
| LessThanOrEqual    | `<=`          |
| In                 | `IN`          |
| IsNotNull          | `IS NOT NULL` |
| IsNull             | `IS NULL`     |
| InSet              | `IN (...)`    |
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
| EndsWith        |
| InitCap         |
| Left            |
| Length          |
| Like            |
| Lower           |
| OctetLength     |
| Reverse         |
| RLike           |
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
| Upper           |

## JSON Functions

| Expression    |
| ------------- |
| GetJsonObject |

## Date/Time Functions

| Expression     | SQL                          |
| -------------- | ---------------------------- |
| DateAdd        | `date_add`                   |
| DateDiff       | `datediff`                   |
| DateFormat     | `date_format`                |
| DateSub        | `date_sub`                   |
| DatePart       | `date_part(field, source)`   |
| Days           | `days`                       |
| Extract        | `extract(field FROM source)` |
| FromUnixTime   | `from_unixtime`              |
| Hour           | `hour`                       |
| LastDay        | `last_day`                   |
| Minute         | `minute`                     |
| Second         | `second`                     |
| TruncDate      | `trunc`                      |
| TruncTimestamp | `date_trunc`                 |
| UnixDate       | `unix_date`                  |
| UnixTimestamp  | `unix_timestamp`             |
| Year           | `year`                       |
| Month          | `month`                      |
| DayOfMonth     | `day`/`dayofmonth`           |
| DayOfWeek      | `dayofweek`                  |
| WeekDay        | `weekday`                    |
| DayOfYear      | `dayofyear`                  |
| WeekOfYear     | `weekofyear`                 |
| Quarter        | `quarter`                    |

## Math Expressions

| Expression     | SQL       |
| -------------- | --------- |
| Abs            | `abs`     |
| Acos           | `acos`    |
| Add            | `+`       |
| Asin           | `asin`    |
| Atan           | `atan`    |
| Atan2          | `atan2`   |
| BRound         | `bround`  |
| Ceil           | `ceil`    |
| Cos            | `cos`     |
| Cosh           | `cosh`    |
| Cot            | `cot`     |
| Divide         | `/`       |
| Exp            | `exp`     |
| Expm1          | `expm1`   |
| Floor          | `floor`   |
| Hex            | `hex`     |
| IntegralDivide | `div`     |
| IsNaN          | `isnan`   |
| Log            | `log`     |
| Log2           | `log2`    |
| Log10          | `log10`   |
| Multiply       | `*`       |
| Pow            | `power`   |
| Rand           | `rand`    |
| Randn          | `randn`   |
| Remainder      | `%`       |
| Round          | `round`   |
| Signum         | `signum`  |
| Sin            | `sin`     |
| Sinh           | `sinh`    |
| Sqrt           | `sqrt`    |
| Subtract       | `-`       |
| Tan            | `tan`     |
| Tanh           | `tanh`    |
| TryAdd         | `try_add` |
| TryDivide      | `try_div` |
| TryMultiply    | `try_mul` |
| TrySubtract    | `try_sub` |
| UnaryMinus     | `-`       |
| Unhex          | `unhex`   |

## Hashing Functions

| Expression  |
| ----------- |
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
| ArrayRemove    |
| ArrayRepeat    |
| ArrayUnion     |
| ArraysOverlap  |
| CreateArray    |
| ElementAt      |
| Flatten        |
| GetArrayItem   |

## Map Expressions

| Expression    |
| ------------- |
| GetMapValue   |
| MapKeys       |
| MapEntries    |
| MapValues     |
| MapFromArrays |
| StringToMap   |

## Struct Expressions

| Expression           |
| -------------------- |
| CreateNamedStruct    |
| GetArrayStructFields |
| GetStructField       |
| JsonToStructs        |
| StructsToJson        |

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
