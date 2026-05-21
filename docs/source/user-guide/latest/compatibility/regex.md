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

Comet provides two regexp engines for evaluating regular expressions: a **Rust engine** that uses the Rust
[`regex`] crate natively, and an experimental **Java engine** that calls back into the JVM via Comet's JVM
UDF framework. The engine is selected with `spark.comet.exec.regexp.engine`, which accepts:

- `java` (default) — route through the Java engine for full Spark compatibility. Requires
  `spark.comet.jvmUdf.enabled=true`; otherwise regex expressions fall back to Spark with an explanatory
  message.
- `rust` — run the Rust engine when an expression has a native implementation. Setting this is itself
  the opt-in for the semantic differences between Java and Rust regex (no separate `allowIncompatible`
  flag needed). Expressions without a native Rust implementation (`regexp_extract`,
  `regexp_extract_all`, `regexp_instr`) fall back to Spark.

The JVM UDF framework is experimental and disabled by default. With pure defaults
(`engine=java`, `jvmUdf.enabled=false`), all regex expressions fall back to Spark.

## Choosing an engine

|                      | Rust engine                             | Java engine (experimental, default)                                                                                 |
| -------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Compatibility**    | Differs from Java regex (see below)     | 100% compatible with Spark                                                                                          |
| **Feature coverage** | `rlike`, `regexp_replace`, `split` only | All regexp expressions (`rlike`, `regexp_extract`, `regexp_extract_all`, `regexp_instr`, `regexp_replace`, `split`) |
| **Performance**      | Fully native, no JNI overhead           | One JNI round-trip per batch (Arrow vectors stay columnar)                                                          |
| **Pattern support**  | Linear-time subset only                 | All Java regex features (backreferences, lookaround, etc.)                                                          |

The **Rust engine** is faster but cannot match Java regex semantics for every pattern. Because the engine
choice is itself the opt-in, setting `spark.comet.exec.regexp.engine=rust` declares acceptance of those
differences without a separate per-expression flag.

The **Java engine** is the default but the underlying JVM UDF framework is experimental and gated behind
`spark.comet.jvmUdf.enabled=true`; the behavior, configuration, and supported expressions may change in
future releases.

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

For most ASCII-only, non-anchored patterns that use only literal characters, simple character classes, and
ordinary quantifiers, the two engines produce the same results. If you are confident your patterns fit this
shape and want to avoid the JNI overhead of the Java engine, switching to the Rust engine with
`allowIncompatible=true` is generally safe.

For anything that uses backreferences, lookaround, or relies on Java's specific Unicode or line-handling
defaults, use the experimental Java engine.

[`java.util.regex`]: https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
[`regex`]: https://docs.rs/regex/latest/regex/
[RE2]: https://github.com/google/re2/wiki/Syntax
