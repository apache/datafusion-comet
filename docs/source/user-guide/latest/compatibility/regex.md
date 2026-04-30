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

Spark evaluates regular expressions with Java's [`java.util.regex`] engine. Comet evaluates them with the Rust
[`regex`] crate. The two engines have different feature sets and different default behavior, so the same pattern
may produce different results, fail to compile, or match different input on each side.

Because the differences are pattern-dependent and not always obvious, Comet falls back to Spark for any
expression that uses a regular expression by default. To opt in to native evaluation, set
`spark.comet.expression.regexp.allowIncompatible=true`. This is appropriate when you have verified that your
patterns are simple enough that the two engines agree, or when you are willing to accept the difference.

The expressions affected by this fallback include `RLike`, `RegExpReplace`, `StringSplit`, and `Like` (which
Comet translates to a regular expression internally).

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

Patterns that use any of the following will not compile in Comet's native engine and must run on Spark:

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
  default, so `\d` matches every digit codepoint defined by Unicode rather than only `0`–`9`. Java's defaults
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

## When opting in is safe

For most ASCII-only, non-anchored patterns that use only literal characters, simple character classes, and
ordinary quantifiers, the two engines produce the same results. If you are confident your patterns fit this
shape, enabling `spark.comet.expression.regexp.allowIncompatible=true` is generally safe. For anything that
uses backreferences, lookaround, or relies on Java's specific Unicode or line-handling defaults, leave the
fallback in place.

[`java.util.regex`]: https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
[`regex`]: https://docs.rs/regex/latest/regex/
[RE2]: https://github.com/google/re2/wiki/Syntax
