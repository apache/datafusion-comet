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

# math_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## `%`

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Remainder(left, right, evalMode)` signature identical across versions. Native path uses Rust `spark_modulo` UDF; non-ANSI returns NULL on divide-by-zero, ANSI raises `DIVIDE_BY_ZERO` / `REMAINDER_BY_ZERO`. `CometRemainder` rejects `EvalMode.TRY`, so `try_mod` (Spark 4.0+) falls back to Spark (https://github.com/apache/datafusion-comet/issues/4484).

## `*`

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Multiply(left, right, evalMode)` signature identical. Decimal results exceeding `DECIMAL128_MAX_PRECISION` go through `WideDecimalBinaryExpr` (Decimal256 intermediate); smaller decimals and primitives use DataFusion `BinaryExpr`. ANSI integer overflow uses Rust `checked_mul`. Interval multiplication falls back.

## `+`

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Add(left, right, evalMode)` with the same Decimal / ANSI plumbing as `*`. `Date + Int8/16/32` dispatches to the Rust `date_add` UDF to work around DataFusion's Date32 + Interval-only kernel.

## `-`

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Subtract(left, right, evalMode)` mirrors `+`. `Date - Int8/16/32` uses the Rust `date_sub` UDF.

## `/`

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Divide(left, right, evalMode)`. Non-ANSI mode wraps the divisor in `If(EqualTo(right, 0), null, right)` so DataFusion never throws. Decimal output is wrapped in `CheckOverflow(failOnError = ANSI)`; ANSI surfaces `NUMERIC_VALUE_OUT_OF_RANGE`, non-ANSI returns NULL.

## abs

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Abs(child, failOnError)` over `NumericType` plus the two interval types. `failOnError` (ANSI) is propagated to the native `abs` UDF, which throws `ARITHMETIC_OVERFLOW` on `Int.MinValue` / `Long.MinValue` / Decimal MIN. `DayTimeIntervalType` and `YearMonthIntervalType` fall back to Spark. Spark 4.0 / 4.1 do the `NullIntolerant` -> `nullIntolerant: Boolean` refactor; behaviour unchanged.

## acos

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.acos, "ACOS")` unchanged across versions; wired as `CometScalarFunction("acos")` to DataFusion's `acos` UDF. NaN for `|x| > 1`.

## acosh

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `StrictMath.log(x + sqrt(x*x - 1))` unchanged across versions. NaN for `x < 1`. Routes to DataFusion's `acosh`.

## asin

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.asin, "ASIN")` unchanged. NaN for `|x| > 1`.

## asinh

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): special-cases `Double.NegativeInfinity` to avoid `log(NaN)`, otherwise `StrictMath.log(x + sqrt(x*x + 1))`. Identical across versions.

## atan

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.atan, "ATAN")` unchanged.

## atan2

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `BinaryMathExpression(math.atan2, "ATAN2")` with both inputs adjusted by `+0.0` to flip `-0.0` to `+0.0`. `CometAtan2` reproduces this by wrapping each child in `Add(child, Literal.default(child.dataType))` before dispatching to DataFusion's `atan2`.

## atanh

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `0.5 * (log1p(x) - log1p(-x))` (SPARK-28519). NaN for `|x| > 1`, +/-Infinity for `x = +/-1`.

## bin

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Bin(child)` over `LongType -> StringType`. Spark 4.x gains `DefaultStringProducingExpression` and the `nullIntolerant: Boolean` refactor; no behaviour change. Routes to datafusion-spark `SparkBin`.

## cbrt

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): passthrough to DataFusion `cbrt`.

## ceil

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): one-arg `ceil(expr)` supported (`LongType` / `DoubleType` / `DecimalType` with scale >= 0). Decimal with negative scale falls back at convert time. The two-arg `ceil(expr, scale)` form (`RoundCeil`) is not wired and falls back to Spark.

## ceiling

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Ceil`. Same support as `ceil`.

## cos

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.cos, "COS")` unchanged across versions.

## cosh

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.cosh, "COSH")` unchanged.

## cot

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `1 / math.tan(x)`. DataFusion's `cot` is also `1.0 / tan(x)`, so the result matches.

## csc

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `1 / math.sin(x)`. Routed to datafusion-spark's `SparkCsc` (registered in `jni_api.rs`).

## degrees

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.toDegrees, "DEGREES")` unchanged across versions.

## div

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `IntegralDivide(left, right, evalMode)`. Non-decimal operands are cast to `DecimalType(19, 0)`; result is recomputed per `IntegralDivide.resultDecimalType`, wrapped in `CheckOverflow`, then cast to `Long`. ANSI overflow for `Long.MinValue div -1` and decimal-overflow ANSI cases are covered by existing tests.

## e

- Foldable; rewritten to a literal by ConstantFolding (like `pi`).

## exp

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(StrictMath.exp, "EXP")` unchanged. ULP-level differences vs DataFusion `exp` are possible but unflagged.

## expm1

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(StrictMath.expm1, "EXPM1")` unchanged.

## factorial

- 3.4.3 (audited 2026-05-15): identical to v3.5.8.
- 3.5.8 (audited 2026-05-15): canonical reference; `extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant`. Returns NULL for NULL input or values outside `[0, 20]`.
- 4.0.1 (audited 2026-05-15): `NullIntolerant` trait replaced by `nullIntolerant: Boolean` method override; behavior unchanged.
- 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## floor

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): mirror of `ceil`. Two-arg `floor(expr, scale)` form (`RoundFloor`) falls back to Spark.

## greatest

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): NULL-skipping variadic. Wired as `CometScalarFunction("greatest")` to DataFusion's `GreatestFunc`. Comet does not gate input types, so interval inputs and other Spark-only orderings rely on the native UDF accepting them; no explicit fallback path.

## hex

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): accepts `LongType` / `BinaryType` / `StringType`. Spark 4.x widens `StringType` to `StringTypeWithCollation` and preserves collation in `dataType`; `CometHex` passes `expr.dataType` to native `SparkHex`, which always returns `Utf8` -- collation propagation may diverge on Spark 4.x.

## least

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): mirror of `greatest`; same caveats. Spark 4.1.1 adds `contextIndependentFoldable` (no Comet impact).

## ln

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Log`. Comet wires through `CometLog` to DataFusion `ln` with a `nullIfNegative` rewrite to match Spark's NULL behaviour for `x <= 0`.

## log

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): one-arg `log(x)` -> `CometLog` (DataFusion `ln`); two-arg `log(base, x)` -> `CometLogarithm` (custom `spark_log` UDF, returns NULL when `base <= 0` or `x <= 0` to match `Logarithm.nullSafeEval`).

## log10

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryLogExpression(StrictMath.log10, "LOG10")`; returns NULL for `x <= 0`. Possible ULP differences from `StrictMath`.

## log2

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryLogExpression(StrictMath.log(x) / StrictMath.log(2), "LOG2")`; returns NULL for `x <= 0`.

## mod

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Remainder`. Same support as `%`.

## negative

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMinus(child, failOnError)` -> Rust `NegativeExpr`. ANSI overflow is detected for `Int8/Int16/Int32/Int64` and `IntervalYearMonth/IntervalDayTime`. Float / Double / Decimal cannot overflow on negate. Spark 4.0 `NullIntolerant` -> `nullIntolerant: Boolean` refactor; no impact.

## pi

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `LeafMathExpression(math.Pi, "PI")`; foldable, so Spark `ConstantFolding` rewrites it to a `Literal` before Comet sees the plan. The `CometScalarFunction("pi")` registration is exercised only when `ConstantFolding` is excluded.

## positive

- Spark 3.4.3, 3.5.8 (audited 2026-05-27): `UnaryPositive(child)` is a regular expression. There is no Comet serde for `UnaryPositive`, so projections containing `+col` silently disable Comet for the projection on 3.4/3.5.
- Spark 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryPositive` is `RuntimeReplaceable` with `replacement = child`; the optimizer removes it before Comet sees the plan, so the gap is transparent on 4.x.

## pow

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Pow(left, right) extends BinaryMathExpression(StrictMath.pow, "POWER")`; routes to DataFusion `pow`. ULP-level differences possible.

## power

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Pow`. Same support as `pow`.

## radians

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.toRadians, "RADIANS")` unchanged across versions.

## rand

- See `misc_funcs / rand`.

## randn

- See `misc_funcs / randn`.

## rint

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.rint, "ROUND")` with `funcName = "rint"`. Passthrough to DataFusion `rint` (round-half-to-even).

## round

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): HALF_UP rounding for integer / decimal. Float / Double child types always fall back to Spark because `BigDecimal`-via-`toString` rounding cannot be precisely matched (documented inline in `CometRound`). ANSI `failOnError` is propagated for integer overflow. `BRound` (HALF_EVEN) is not wired.

## sec

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `1 / math.cos(x)`. Routed to datafusion-spark's `SparkSec`.

## shiftleft

- See `bitwise_funcs / <<` (audited in PR #4479). Same support as the operator alias added in 4.0.

## sign

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Signum`. Same support as `signum`.

## signum

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Signum(child)` over `DoubleType`. Spark also restricts to the two interval types via `inputTypes`; Comet handles only the `Double` case via DataFusion `signum`.

## sin

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.sin, "SIN")` unchanged.

## sinh

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.sinh, "SINH")` unchanged.

## sqrt

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.sqrt, "SQRT")` unchanged.

## tan

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.tan, "TAN")` unchanged.

## tanh

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.tanh, "TANH")` unchanged.

## try_add

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `TryAdd` is `RuntimeReplaceable` and rewrites to `Add(.., EvalMode.TRY)` for numeric inputs (datetime / interval go through `TryEval(Add(.., ANSI))` and fall back). Numeric path uses the Rust `checked_add` UDF, returning NULL on overflow. Decimal goes through `WideDecimalBinaryExpr` with `EvalMode.Try`.

## try_divide

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `TryDivide` rewrites to `Divide(.., EvalMode.TRY)`. The `nullIfWhenPrimitive` wrapper swaps zero divisors to NULL; integer / float divide uses `checked_div`; decimal uses `decimal_div` + `CheckOverflow(failOnError = false)` returning NULL.

## try_multiply

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): rewrites to `Multiply(.., EvalMode.TRY)`. Integer path uses `checked_mul`; decimal uses `WideDecimalBinaryExpr` with `EvalMode.Try`, returning NULL on overflow.

## try_subtract

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): rewrites to `Subtract(.., EvalMode.TRY)`. Integer path uses `checked_sub`; decimal uses `WideDecimalBinaryExpr` as needed.

## unhex

- Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Unhex(child, failOnError)`. Spark 4.x widens input to `StringTypeWithCollation` and wraps the inner call in try/catch; Comet `CometUnhex` forwards `failOnError` to native `spark_unhex` but does not gate on collation.

## width_bucket

- Spark 3.5.8 (audited 2026-05-27): introduced; not available in 3.4.3.
- Spark 4.0.1, 4.1.1 (audited 2026-05-27): same semantics; `NullIntolerant` -> `nullIntolerant: Boolean` refactor.
- Known limitation: wired via per-version `CometExprShim` rather than a `CometExpressionSerde`, so it bypasses the support-level framework and the auto-generated compatibility doc (https://github.com/apache/datafusion-comet/issues/4485). Native path uses datafusion-spark `SparkWidthBucket`; interval input types are not exercised by Comet tests.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
