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

# Regular Expressions

Comet evaluates Spark regular-expression expressions (`rlike`, `regexp_replace`, `split`,
`regexp_extract`, `regexp_extract_all`, `regexp_instr`) two ways:

- **Codegen dispatcher** — Spark's own `doGenCode` for the expression runs inside Comet's
  Arrow-direct codegen dispatcher (the same dispatcher used by Comet's `ScalaUDF` codegen path).
  This is 100% compatible with Spark, at the cost of one JNI round-trip per batch. It is enabled by
  default (`spark.comet.exec.scalaUDF.codegen.enabled=true`); if the dispatcher is disabled, regex
  expressions fall back to Spark. This is the default for every regex expression except an
  in-subset `rlike` literal (see below).
- **Native (rust) engine** — the Rust [`regex`] crate, run natively with no JNI overhead. It is
  faster but has different semantics from Java regex (see below). For `rlike`, a plan-time
  analyzer admits a conservative subset of `UTF8_BINARY` literal patterns and runs those natively
  **by default**. Every other `rlike` pattern, and every other regex expression, still requires
  that expression's `allowIncompatible` flag. `regexp_instr` has no native implementation and
  always runs through the codegen dispatcher.

| SQL                  | Native (rust) opt-in config                                 |
| -------------------- | ----------------------------------------------------------- |
| `rlike`              | `spark.comet.expression.RLike.allowIncompatible`            |
| `regexp_replace`     | `spark.comet.expression.RegExpReplace.allowIncompatible`    |
| `regexp_extract`     | `spark.comet.expression.RegExpExtract.allowIncompatible`    |
| `regexp_extract_all` | `spark.comet.expression.RegExpExtractAll.allowIncompatible` |
| `split`              | `spark.comet.expression.StringSplit.allowIncompatible`      |

`spark.comet.expression.RLike.allowIncompatible` only forces the native Rust path for literal
patterns the analyzer cannot prove equivalent to Java regex. It is not needed for in-subset
literals, and it does not apply to non-literal or NULL patterns.

When the native path is selected but a case has no native implementation (for example a
non-scalar `rlike` pattern, `regexp_replace` with a non-1 offset, or `regexp_extract` with a
non-literal pattern or idx), Comet routes that case through the codegen dispatcher.

## Disabling Comet for individual regex expressions

Each regex expression has a per-class `spark.comet.expression.<ClassName>.enabled` flag (default
`true`) that disables Comet's serde for that expression and forces a Spark fallback. This is
useful for narrowing a regression or comparing performance on a single operator without changing
the engine selector:

| Expression           | Config                                                  |
| -------------------- | ------------------------------------------------------- |
| `rlike`              | `spark.comet.expression.RLike.enabled=false`            |
| `regexp_extract`     | `spark.comet.expression.RegExpExtract.enabled=false`    |
| `regexp_extract_all` | `spark.comet.expression.RegExpExtractAll.enabled=false` |
| `regexp_instr`       | `spark.comet.expression.RegExpInStr.enabled=false`      |
| `regexp_replace`     | `spark.comet.expression.RegExpReplace.enabled=false`    |
| `split`              | `spark.comet.expression.StringSplit.enabled=false`      |

## Choosing an engine

|                      | Rust engine                                                                                                         | Codegen dispatcher                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Compatibility**    | In-subset `rlike` literals match Java regex; other patterns may differ (see below)                                  | 100% compatible with Spark                                                                                          |
| **Feature coverage** | `rlike`, `regexp_replace`, `split`, `regexp_extract`, `regexp_extract_all` natively; `regexp_instr` via fallthrough | All regexp expressions (`rlike`, `regexp_extract`, `regexp_extract_all`, `regexp_instr`, `regexp_replace`, `split`) |
| **Performance**      | Fully native, no JNI overhead                                                                                       | One JNI round-trip per batch (Arrow vectors stay columnar)                                                          |
| **Pattern support**  | Linear-time subset only                                                                                             | All Java regex features (backreferences, lookaround, etc.)                                                          |
| **`rlike` default**  | Used automatically for analyzer-admitted `UTF8_BINARY` literals                                                     | Used for every other `rlike` pattern                                                                                |

The **Rust engine** is faster but cannot match Java regex semantics for every pattern. For `rlike`,
Comet therefore runs only the analyzer-admitted subset natively by default. Setting
`spark.comet.expression.RLike.allowIncompatible=true` forces the Rust path for other literal
patterns and declares acceptance of any remaining differences. The other regex expressions still
require their own `allowIncompatible` flag.

The **codegen dispatcher** is enabled by `spark.comet.exec.scalaUDF.codegen.enabled`, so it can be
disabled globally to fall back to Spark for out-of-subset regex expressions.

## Why the engines differ

Java's `java.util.regex` is a backtracking engine in the Perl/PCRE family. It supports the full range of
features that style of engine provides, including some whose worst-case running time grows exponentially with
the input.

Rust's [`regex`] crate is a finite-automaton engine in the [RE2] family. It deliberately omits features that
cannot be implemented with a guarantee of linear-time matching. In exchange, every pattern it does accept runs
in time linear in the size of the input. This is the same trade-off RE2, Go's `regexp`, and several other
engines make.

The practical consequence is that Java accepts a strictly larger set of patterns than the Rust engine, and
several constructs that look the same in source have different semantics on the two sides.

## Features supported by Java but not by the Rust engine

Patterns that use any of the following will not compile in Comet's Rust engine and must run on Spark (or use
the Java engine):

- **Backreferences** such as `\1`, `\2`, or `\k<name>`. The Rust engine has no backtracking and cannot match
  a previously captured group.
- **Lookaround**, including lookahead (`(?=...)`, `(?!...)`) and lookbehind (`(?<=...)`, `(?<!...)`).
- **Atomic groups** (`(?>...)`).
- **Possessive quantifiers** (`*+`, `++`, `?+`, `{n,m}+`). Rust supports greedy and lazy quantifiers but not
  possessive.
- **Embedded code, conditionals, and recursion** such as `(?(cond)yes|no)` or `(?R)`. Rust accepts none of
  these.

## Features that exist on both sides but behave differently

Even where both engines accept a construct, the matching behavior is not always the same.

- **Unicode-aware character classes.** In the Rust engine, `\d`, `\w`, `\s`, and `.` are Unicode-aware by
  default, so `\d` matches every digit codepoint defined by Unicode rather than only `0`-`9`. Java's defaults
  match ASCII only and require the `UNICODE_CHARACTER_CLASS` flag (or `(?U)` inline) to switch to Unicode
  semantics. The same pattern can therefore match a different set of characters on each side.
- **Line terminators.** In multiline mode, Java treats `\r`, `\n`, `\r\n`, and a few additional Unicode line
  separators as line boundaries by default. The Rust engine treats only `\n` as a line boundary unless CRLF
  mode is enabled. `^`, `$`, and `.` (with `(?s)` off) all depend on this definition.
- **Case-insensitive matching.** Both engines support `(?i)`, but Java's default is ASCII case folding while
  the Rust engine uses full Unicode simple case folding when Unicode mode is on. Patterns that match characters
  outside ASCII can produce different results.
- **POSIX character classes.** The Rust engine supports `[[:alpha:]]` style POSIX classes inside bracket
  expressions but not Java's `\p{Alpha}` shorthand. Java accepts both. Unicode property escapes (`\p{L}`,
  `\p{Greek}`, etc.) are supported by both engines but cover slightly different sets of properties.
- **Octal and Unicode escapes.** Java accepts `\0nnn` for octal and `\uXXXX` for a BMP codepoint. Rust uses
  `\x{...}` for arbitrary codepoints and does not accept Java's bare `\uXXXX` form.
- **Empty matches in `split`.** Spark's `StringSplit`, which is built on Java's regex, includes leading empty
  strings produced by zero-width matches at the start of the input. The Rust engine's `split` follows different
  rules, so split results can differ in edge cases involving empty matches even when the pattern itself is
  identical on both sides.

## When the Rust engine is safe

Comet's plan-time analyzer admits a conservative whitelist of `rlike` literals and runs those on
the Rust engine by default: printable ASCII literals, simple ASCII character classes, greedy
quantifiers (`*`, `+`, `?`, `{n}`, `{n,}`, `{n,m}`), capturing and non-capturing groups, and
alternation. Anchors (`^`, `$`), `.`, `\d` / `\w` / `\s`, inline flags, lookaround, Rust-only
class set operators (`&&`, `~~`, `--`), unescaped class delimiters used as atoms or range
endpoints, and counted, aggregate, or nested patterns that exceed a conservative compile-size
budget stay on the Java engine. Any unrecognized construct also stays on the Java engine.

For `regexp_replace`, `split`, `regexp_extract`, and `regexp_extract_all`, the native path is
still opt-in via `allowIncompatible`. If you are confident those patterns fit the same ASCII,
non-anchored shape, opting in is generally safe.

For anything that uses backreferences, lookaround, or relies on Java's specific Unicode or
line-handling defaults, use the Java engine.

[`java.util.regex`]: https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
[`regex`]: https://docs.rs/regex/latest/regex/
[RE2]: https://github.com/google/re2/wiki/Syntax
