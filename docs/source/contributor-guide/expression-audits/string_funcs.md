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

# string_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## ascii

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringType -> IntegerType`; `nullSafeEval` returns `codePointAt(0)` of the first char, or `0` for the empty string. Wired via `CometScalarFunction("ascii")` and resolved to DataFusion `ascii` (`chars().next() as i32`); first-code-point semantics match for ASCII, BMP, and supplementary code points.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; behaviour unchanged for `UTF8_BINARY`. Comet does not propagate collation, so non-default collations may diverge silently (https://github.com/apache/datafusion-comet/issues/4496).

## bit_length

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `(StringType|BinaryType) -> IntegerType`; eval returns `numBytes * 8` for strings and `.length * 8` for binary.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).
- `BinaryType` input is reported `Unsupported` in `getSupportLevel`, so `bit_length(<binary>)` falls back to Spark cleanly (DataFusion's `BitLengthFunc` signature accepts string types only).

## btrim

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrimBoth` is `RuntimeReplaceable` and rewritten to `StringTrim(srcStr, trimStr)` before serde runs. Support is provided by the `trim` entry; no dedicated serde registration.
- Spark 4.0.1 (audited 2026-05-27): `StringTrim` (the rewrite target) routes through `CollationSupport.StringTrim.exec` and uses `StringTypeNonCSAICollation(supportsTrimCollation = true)`; semantics unchanged for `UTF8_BINARY`. Non-default collations may diverge in Comet (https://github.com/apache/datafusion-comet/issues/4496).

## char

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Chr(LongType) -> StringType`; `lon < 0` returns `""`, else `((lon & 0xFF) as char).toString` (so `chr(256)` and `chr(0)` both return `\u0000`).
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`. Resolves natively to `datafusion_spark::function::string::char::CharFunc`, which mirrors Spark's negative-input and `& 0xFF` semantics.

## char_length

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): registry alias of `Length`. Same support as `length`.
- Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Length`.

## character_length

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): registry alias of `Length`. Same support as `length`.
- Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Length`.

## chr

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): registry alias of `Chr`. Same support as `char`.
- Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Chr`.

## concat_ws

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Seq[Expression] -> StringType`; NULL separator yields NULL, NULL element values are skipped, children can be `StringType` or `ArrayType(StringType)`. Comet serde rewrites a NULL-literal separator to a NULL of the result type and bails out on all-foldable inputs so Spark's `ConstantFolding` handles them; otherwise delegates to DataFusion `concat_ws`.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation` / `AbstractArrayType`; `dataType` becomes `children.head.dataType` (collation-derived). Semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## contains

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `UTF8String.contains` on `StringType`; the parser routes `(BinaryType, BinaryType)` to `BinaryPredicate`, so Comet only ever sees the String form.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.Contains.exec(..., collationId)`; behaviour identical for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## decode

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringDecode(bin, charset)` evaluated directly; invalid sequences silently substitute replacement characters via `new String(bytes, charset)`.
- Spark 4.0.1 (audited 2026-05-27): refactored to `RuntimeReplaceable` whose `replacement` is a `StaticInvoke(StringDecode.decode, bin, charset, legacyCharsets, legacyErrorAction)`; the 4-arg form raises on malformed input unless legacy flags are set.
- `decode` runs through the codegen dispatcher on all versions (Spark 3.x via `CometStringDecode`, Spark 4.0 via the `StaticInvoke` replacement routed to `CometStaticInvokeCodegenDispatch`), so Spark's own evaluation runs inside the Comet pipeline. This honours the `charset` argument and the Spark 4.0 `legacyCharsets` / `legacyErrorAction` flags, and falls back to Spark when the dispatcher is disabled.
- Known limitation: because there is no `CometExpressionSerde[StringDecode]` registration, `decode` does not surface in the auto-generated compatibility docs (https://github.com/apache/datafusion-comet/issues/4466).

## endswith

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `UTF8String.endsWith` on `StringType`; binary form routed to `BinaryPredicate` before Comet.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.EndsWith.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## initcap

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `string.toLowerCase.toTitleCase` on `UTF8String`; word boundary is Java `Character.isWhitespace`. Comet routes to DataFusion `initcap`, which splits on `!is_alphanumeric()` (hyphens, apostrophes, and punctuation all split words), so Comet is unconditionally `Incompatible` (https://github.com/apache/datafusion-comet/issues/1052).
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.InitCap.exec` (collation- and ICU-aware) and propagates `child.dataType`. Comet ignores collation; 3.x divergences persist plus collation/ICU mismatches (https://github.com/apache/datafusion-comet/issues/4496).

## instr

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringInstr(str, substr) -> IntegerType`; returns `string.indexOf(sub, 0) + 1` (1-based, 0 when not found, 1 on empty substring). Resolves to DataFusion `strpos` (alias `instr`) with matching semantics.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringInstr.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## lcase

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): registry alias of `Lower`. Same support as `lower`.
- Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Lower`.

## left

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `RuntimeReplaceable` with `replacement = Substring(str, Literal(1), len)`; accepts `StringType` or `BinaryType` plus `IntegerType`. Comet serde rewrites to a `Substring` proto with `start=1, len=lenValue`. `getSupportLevel` declares `Unsupported` for non-literal `len` so the dispatcher falls back uniformly.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened with `StringTypeWithCollation`; behaviour unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## len

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): registry alias of `Length`. Same support as `length`.
- Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Length`.

## length

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `(StringType|BinaryType) -> IntegerType`; eval returns `numChars` for strings and `.length` for binary. `BinaryType` input falls back via `Unsupported` (DataFusion's `character_length` accepts string types only).
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; semantics unchanged. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## lower

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. JVM default-locale `toLowerCase` on `UTF8String`. Comet routes to DataFusion `lower` (Rust Unicode default case mapping, no locale awareness) and is unconditionally `Incompatible`; users opt in via the standard `spark.comet.expression.Lower.allowIncompatible=true`.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.Lower.exec(v, collationId, useICU)` with `SQLConf.ICU_CASE_MAPPINGS_ENABLED`; `inputTypes` widened to `StringTypeWithCollation`. Comet ignores collation and ICU mode, so non-default collations or `ICU_CASE_MAPPINGS_ENABLED=true` diverge even after opting in (https://github.com/apache/datafusion-comet/issues/2190).

## lpad

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringLPad(str, len, pad) -> StringType`; `len <= 0` returns the empty string, empty `pad` returns `str` unchanged, NULL inputs propagate. Comet serde requires `str` to be a column and `pad` to be a literal; otherwise falls back.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`; `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`. Semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).
- Known limitation: `lpad(<binary>, ...)` is rewritten by Spark to `BinaryPad / StaticInvoke(ByteArray.lpad)` before serde runs and always falls back to Spark.

## ltrim

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrimLeft` extends `String2TrimExpression`; no-arg form strips ASCII space `0x20` only. The two-arg parser form `ltrim(trimStr, srcStr)` is swapped to `(srcStr, Option(trimStr))` by Spark's secondary constructor, so children match DataFusion `ltrim(str, chars)`.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTrimLeft.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## octet_length

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `(StringType|BinaryType) -> IntegerType`; eval returns `numBytes` for strings and `.length` for binary.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).
- `BinaryType` input is reported `Unsupported` in `getSupportLevel`, so `octet_length(<binary>)` falls back to Spark cleanly (DataFusion's `OctetLengthFunc` signature accepts string types only).

## regexp_replace

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `RegExpReplace(subject, regexp, rep, pos)` with foldable `pos > 0`; uses Java `Pattern`. Comet supports only `pos = 1` (other offsets fall back) and injects a `'g'` flag because DataFusion's `regexp_replace` stops at the first match by default.
- Spark 4.0.1 (audited 2026-05-27): adds raw-string literal support at the parser level and `nullIntolerant: Boolean = true`; runtime semantics unchanged.
- Known limitation: regex semantics differ (Rust `regex` crate vs Java `Pattern`); `RegExp.isSupportedPattern` currently returns `false` for every pattern, so the path always requires `spark.comet.expression.regexp.allowIncompatible=true`.

## repeat

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringRepeat(str, times)` with `nullSafeEval(s, n) = s.repeat(n)`; `UTF8String.repeat` returns the empty string for `n <= 0`. Comet casts `times` to `LongType` and delegates to DataFusion `repeat`, which mirrors Spark for negative counts.
- Spark 4.0.1 (audited 2026-05-27): adds `nullIntolerant: Boolean` field; `dataType` becomes `str.dataType` (collation-tracking). Semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## replace

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringReplace(src, search, replace)`; when `search` is empty, Spark returns `src` unchanged (short-circuit on `search.numBytes == 0`). DataFusion `replace` instead inserts `replace` between every character, so `CometStringReplace.getSupportLevel` marks `Incompatible(Some(reason))` when `search` is a literal empty string and falls back to Spark by default (https://github.com/apache/datafusion-comet/issues/4497).
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringReplace.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## right

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `RuntimeReplaceable` with `replacement = If(IsNull(str), null, If(len <= 0, "", Substring(str, -len, len)))`; accepts `StringType` plus `IntegerType`. Comet serde rewrites positive `len` to a `Substring` proto with `start=-len, len=len`; for `len <= 0` it builds an `If(IsNull(str), null, "")` proto chain to preserve NULL propagation. `getSupportLevel` declares `Unsupported` for non-literal `len` so the dispatcher falls back uniformly.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened with collation; uses `UnaryMinus(len, failOnError = false)` to avoid integer-overflow exceptions on `len = Int.MinValue`. Semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## rpad

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringRPad(str, len, pad) -> StringType`; same edge-case behaviour as `lpad` (negative len, empty pad, NULL propagation). Comet serde requires column `str` and literal `pad`.
- Spark 4.0.1 (audited 2026-05-27): same evolution as `lpad`; default-pad literal type tightened; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).
- Known limitation: same `BinaryPad / StaticInvoke` rewrite as `lpad` causes `rpad(<binary>, ...)` to fall back.

## rtrim

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrimRight` extends `String2TrimExpression`; semantically symmetric to `ltrim`.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTrimRight.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## space

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringSpace(IntegerType) -> StringType`; negative input yields the empty string. Resolves natively to `datafusion_spark::function::string::space::SparkSpace`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `nullIntolerant: Boolean` override.

## split

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringSplit(str, regex, limit)`; `limit > 0` permits at most `limit-1` splits, `limit <= 0` is unlimited. Comet registers `split` as a custom UDF (`native/spark-expr/src/string_funcs/split.rs`) using the Rust `regex` crate, and is unconditionally `Incompatible` due to regex-engine differences.
- Spark 4.0.1 (audited 2026-05-27): wraps the regex via `CollationSupport.collationAwareRegex` and changes `dataType` to `ArrayType(str.dataType, ...)`. Comet does not honour collation flags (https://github.com/apache/datafusion-comet/issues/4496).

## startswith

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `UTF8String.startsWith` on `StringType`; binary form routed to `BinaryPredicate`.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StartsWith.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## substr

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): registry alias of `Substring`. Same support as `substring`.
- Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Substring`.

## substring

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `TernaryExpression`; two-arg form defaults `len = Integer.MAX_VALUE`; supports `StringType` and `BinaryType`. Comet serializes to a dedicated `Substring` proto. `getSupportLevel` declares `Unsupported` when either `pos` or `len` is not a `Literal` so the dispatcher falls back uniformly.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened with `StringTypeWithCollation`; semantics unchanged for `UTF8_BINARY`. Native `SubstringExpr` implements Spark's negative-start clamping and is exercised against ASCII, multibyte UTF-8, emoji, decomposed and Telugu inputs. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## substring_index

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `TernaryExpression(StringType, StringType, IntegerType) -> StringType`. Comet casts `count` to `LongType` and delegates to DataFusion's `substr_index` UDF (alias `substring_index`).
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.SubstringIndex.exec` and propagates `strExpr.dataType`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## translate

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringTranslate(src, from, to)`; `UTF8String.translate(dict)` is code-point based, and any character mapped explicitly to U+0000 in `to` is also deleted.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTranslate.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).
- Marked `Incompatible`: DataFusion's `translate` iterates over Unicode graphemes (Spark uses code points) and substitutes U+0000 instead of treating it as a deletion sentinel. It falls back to Spark by default and runs natively only when incompatible expressions are explicitly allowed.

## trim

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrim` no-arg form strips ASCII space `0x20` only (matches DataFusion `btrim`'s default); two-arg form's children are `(srcStr, trimStr)` after Spark's secondary-constructor swap.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTrim.exec` and uses `StringTypeNonCSAICollation`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4496).

## ucase

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): registry alias of `Upper`. Same support as `upper`.
- Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Upper`.

## upper

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. JVM default-locale `toUpperCase` on `UTF8String`. Comet routes to DataFusion `upper` (Rust Unicode default case mapping, no locale awareness) and is unconditionally `Incompatible`; users opt in via the standard `spark.comet.expression.Upper.allowIncompatible=true`.
- Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.Upper.exec(v, collationId, useICU)` with `SQLConf.ICU_CASE_MAPPINGS_ENABLED`. Comet does not propagate collation or ICU mode; non-default collations or `ICU_CASE_MAPPINGS_ENABLED=true` diverge even after opting in (https://github.com/apache/datafusion-comet/issues/2190).

[Spark Expression Support]: ../../user-guide/latest/expressions.md
