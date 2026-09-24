<!--
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

# Java regex parity fixtures

The committed `spark/src/test/resources/regex/rlike-java-fixtures.json` records
Java `Pattern.compile(pattern).matcher(subject).find()` results for patterns
admitted by `CometRegex`. Rust's `test_rlike_java_fixtures` exercises the actual
`RLike` expression with scalar and UTF-8 array inputs against these answers.
It neither starts a JVM nor generates expected results during the test. The
crate's existing JNI build/link requirements still apply; see [Development](development.md).

## Regenerating

From the repository root, use JDK 17:

```shell
java dev/GenerateRegexFixtures.java spark/src/test/resources/regex/rlike-java-fixtures.json
java dev/GenerateRegexFixtures.java /tmp/rlike-java-fixtures.json
cmp spark/src/test/resources/regex/rlike-java-fixtures.json /tmp/rlike-java-fixtures.json
```

The initial oracle was generated with Eclipse Adoptium JDK `17.0.20.1+1`.
The JSON records the actual vendor and runtime version. Output order is stable,
uses explicit UTF-8 and JSON escapes, and contains no timestamp. Supplementary
characters are encoded as JSON surrogate pairs and decoded as Unicode strings.
Regeneration with the same JDK produces identical bytes.

The generator has no external dependencies. Change its fixed case lists or
bounded composition loops to extend coverage, then regenerate and review the
JSON diff. Expected values must always come from Java, never from Rust or manual
edits. All subjects are non-null; the existing kernel tests cover null propagation.

## Coverage

Cases cover literals, every admitted metacharacter escape, positive and negated
classes, ranges, capturing and non-capturing groups, alternation, concatenation,
greedy and counted quantifiers, empty groups/branches/matches, and combinations
of these constructs. Subjects include ASCII, non-ASCII, combining characters,
supplementary code points, control characters, and newline variants.

Dedicated cases reach group depth 32, counted bound 256, quantifier depth 8,
and structural expansion 4095/4096, including capture and class costs. These
use targeted subjects instead of the full subject cross product to bound Java
backtracking work. Structural costs are admission heuristics, not a proof of
Rust compilation success. Finite fixtures cannot prove equivalence for all patterns.

`CometRegexSuite` checks that every fixture is still admitted and that the
current Java engine agrees with the stored oracle. `CometRegexParitySuite`
continues to verify Spark/native routing end to end.

## Validation and upgrades

```shell
(cd native && cargo test -p datafusion-comet-spark-expr rlike)
make core
./mvnw test -Dtest=none -Dsuites="org.apache.comet.expressions.CometRegexSuite,org.apache.comet.CometRegexParitySuite"
```

When upgrading `regex`, run the Rust test against the existing committed oracle.
A compilation error or mismatched answer must fail with the pattern, subject,
expected answer, and actual answer/error. Investigate the difference before
changing either the whitelist or fixtures; do not regenerate answers merely to
make an upgrade pass. Likewise, a JVM test failure after a JDK upgrade requires
review of the Java behavior change before replacing the baseline.
