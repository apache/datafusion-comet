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

# Comparison of Comet and Gluten

This document provides a comparison of the Comet and Gluten projects to help guide users who are looking to choose
between them. This document is likely biased because the Comet community maintains it.

We recommend trying out both Comet and Gluten to see which is the best fit for your needs.

This document is based on Comet 0.16.0 and Gluten 1.6.0.

## Architecture

Comet and Gluten have very similar architectures. Both are Spark plugins that translate Spark physical plans to
a serialized representation and pass the serialized plan to native code for execution.

Gluten serializes the plans using the Substrait format and has an extensible architecture that supports execution
against multiple engines. Velox and Clickhouse are currently supported, but Velox is more widely used.

Comet serializes the plans in a proprietary Protocol Buffer format. Execution is delegated to Apache DataFusion. Comet
does not plan to support multiple engines, but rather focus on a tight integration between Spark and DataFusion.

## Underlying Execution Engine: DataFusion vs Velox

One of the main differences between Comet and Gluten is the choice of native execution engine.

Gluten uses Velox, which is an open-source C++ vectorized query engine created by Meta.

Comet uses Apache DataFusion, which is an open-source vectorized query engine implemented in Rust and is governed by the
Apache Software Foundation.

Velox and DataFusion are both mature query engines that are growing in popularity.

From the point of view of the usage of these query engines in Gluten and Comet, the most significant difference is
the choice of implementation language (Rust vs C++) and this may be the main factor that users should consider when
choosing a solution. For users wishing to implement UDFs in Rust, Comet would likely be a better choice. For users
wishing to implement UDFs in C++, Gluten would likely be a better choice.

The choice of language also has implications for robustness. Rust provides memory safety guarantees at compile
time, eliminating entire classes of bugs such as use-after-free, buffer overflows, and data races that a C++ engine
must guard against manually at runtime. For a component that runs inside every Spark executor and processes
untrusted data, this reduces the risk of memory-corruption crashes and security vulnerabilities. DataFusion achieves
this safety without a garbage collector, so there is no additional runtime overhead compared to C++.

If users are just interested in speeding up their existing Spark jobs and do not need to implement UDFs in native
code, then we suggest benchmarking with both solutions and choosing the fastest one for your use case.

![github-stars-datafusion-velox.png](/_static/images/github-stars-datafusion-velox.png)

## Community and Governance

Comet is developed within the Apache DataFusion project, and its native execution is built directly on the
DataFusion query engine. DataFusion is a broad, vendor-neutral community governed by the Apache Software Foundation
and used by many downstream projects beyond Comet. This means that engine-level improvements such as new operators,
optimizer rules, and performance work are shared across the whole DataFusion ecosystem: work done for other
DataFusion users benefits Comet, and work done for Comet benefits them in turn.

Velox is also an open-source project with contributors from multiple organizations, but its development has been
primarily driven by Meta. Users evaluating long-term adoption may want to weigh the governance model and breadth of
the community behind each engine alongside the technical differences.

## Spark Version Support

Both projects target a similar set of Spark releases.

Comet supports Spark 3.4, 3.5, 4.0, and 4.1 in production builds, with an experimental build also published for
the Spark 4.2 preview. See the [Spark version compatibility guide] for the exact patch versions and
JDK/Scala combinations.

[Spark version compatibility guide]: /user-guide/latest/compatibility/spark-versions.md

Gluten supports Spark 3.3, 3.4, 3.5, 4.0, and 4.1.

## ANSI Mode

Spark 4.0 enables ANSI SQL semantics by default, which changes how arithmetic overflow, invalid casts, division by
zero, and similar error conditions are handled. This is one area where the two projects currently differ.

Comet implements ANSI semantics for the expressions it supports natively, including arithmetic overflow checks,
ANSI cast behavior, and `try_*` variants. Queries running with `spark.sql.ansi.enabled=true` continue to be accelerated.
See the [Comet Compatibility Guide] for details on which expressions have full ANSI coverage.

The Gluten Velox backend documents that ANSI mode is not supported and that any query executed with ANSI enabled
will fall back to vanilla Spark. See the [Gluten Velox limitations] page for the current status.

[Gluten Velox limitations]: https://apache.github.io/gluten/velox-backend-limitations.html

For users adopting Spark 4.0 without disabling ANSI mode, this difference can have a significant impact on the
fraction of a workload that runs natively.

## Table Format Support

Both projects can accelerate queries against Apache Iceberg tables, but they take different approaches and Gluten
covers a broader set of table formats overall.

Comet provides a native Iceberg scan built on iceberg-rust. It has been tested with Iceberg 1.5 through 1.10 and
supports Iceberg spec v1 and v2, schema evolution, time travel and branch reads, positional and equality deletes
on merge-on-read tables, REST catalogs, and S3-compatible object storage. Iceberg writes still go through Spark.
Comet does not currently provide native integrations for Delta Lake, Hudi, or Paimon. See the
[Comet Iceberg guide] for the full list of supported features and known limitations.

[Comet Iceberg guide]: /user-guide/latest/iceberg.md

Gluten ships dedicated modules for Iceberg, Delta Lake (2.0 through 4.0), Hudi, and Paimon. Users who need native
acceleration for Delta, Hudi, or Paimon will find broader coverage in Gluten today.

## Compatibility

Comet relies on the full Spark SQL test suite (consisting of more than 24,000 tests) as well its own unit and
integration tests to ensure compatibility with Spark. Features that are known to have compatibility differences with
Spark are disabled by default, but users can opt in. See the [Comet Compatibility Guide] for more information.

[Comet Compatibility Guide]: https://datafusion.apache.org/comet/user-guide/latest/compatibility/index.html

Gluten also aims to provide compatibility with Spark, and includes a subset of the Spark SQL tests in its own test
suite. See the Gluten [Velox backend limitations] page for known gaps, including notes on case sensitivity, regular
expression dialect (RE2 vs `java.util.regex`), NaN handling, and timestamp encodings.

[Velox backend limitations]: https://apache.github.io/gluten/velox-backend-limitations.html

## Codegen Dispatch

Comet has a feature called the codegen dispatcher that has no direct equivalent in Gluten. When an expression has a
native implementation with known semantic differences from Spark, or has no native implementation at all, Comet can
run Spark's own generated code for that expression inside the native pipeline. Data is passed to the JVM in Arrow
format, evaluated using Spark's byte-exact logic, and returned to the native pipeline for the rest of the query.

This matters for two reasons:

- **Correctness by default.** Expressions that are only partially compatible with Spark (for example, regular
  expressions, JSON functions, and some datetime and array functions) route through the codegen dispatcher by
  default, so they produce results that match Spark exactly. The faster native path is opt-in per expression for
  users who accept its differences.
- **Fewer full fallbacks.** Because the dispatcher keeps evaluation inside the native pipeline instead of falling
  back to whole-stage Spark execution, a single unsupported or incompatible expression does not force an entire
  query stage back onto vanilla Spark. This keeps more of the plan running natively than a strict native-or-fallback
  model would.

By contrast, Gluten falls back to vanilla Spark for expressions and operators that its Velox backend does not
support. See the [Comet Compatibility Guide] for more detail on how the codegen dispatcher works and which
expressions use it.

## Regular Expression Compatibility

Regular expressions are a good example of where the codegen dispatcher gives Comet an architectural advantage.
Spark's regex semantics come from the JVM's `java.util.regex` engine, which supports features such as
backreferences and lookaround. Because Comet's codegen dispatcher runs Spark's own generated Java code, Comet can
evaluate `rlike`, `regexp_replace`, `regexp_extract`, `regexp_extract_all`, `regexp_instr`, and `split` with 100%
compatibility with Spark by default, including every pattern feature the JVM engine supports. Comet also offers a
faster native Rust regex engine as an opt-in per expression for users who can accept its semantic differences.

Gluten's Velox backend evaluates regular expressions using the C++ RE2 engine. RE2 uses a different dialect from
`java.util.regex` and deliberately omits features such as backreferences and lookaround, so it cannot fully match
Spark's regex semantics regardless of configuration. This is a fundamental consequence of using a native C++ regex
engine rather than the JVM engine, and it is documented in the [Gluten Velox limitations] page. Comet's codegen
dispatcher sidesteps this class of incompatibility entirely by keeping Spark's own engine in the loop.

## Scala and Java UDF Support

The same codegen dispatcher lets Comet run Spark's existing Scala and Java scalar UDFs inside the native pipeline
without any rewrite. A UDF's compiled code executes on Arrow data passed from the native pipeline, and, crucially,
the presence of a UDF does not force the enclosing operator back to vanilla Spark: the surrounding native operators
keep running natively while only the UDF itself is evaluated on the JVM. This covers functions registered via
`udf(...)`, `spark.udf.register(...)`, and SQL `CREATE FUNCTION ... AS 'com.example.MyUDF'`, including UDFs over
complex nested types and UDFs composed with other Catalyst expressions and higher-order functions.

This means users can adopt Comet without giving up their existing Scala and Java UDFs, and without rewriting them in
the engine's native language. Gluten's Velox backend supports native C++ UDFs but does not run arbitrary existing
JVM UDFs in the native pipeline, so plans containing them typically fall back to Spark. See the
[Comet Scala and Java UDF guide] for the full list of supported and unsupported cases.

[Comet Scala and Java UDF guide]: /user-guide/latest/scala_java_udfs.md

## Performance

AWS Labs published an [independent benchmark] comparing Comet 0.16.0 with Gluten 1.6.0 on a TPC-DS 3TB workload,
running on Amazon EKS with Graviton4 instances. The study concluded that the two accelerators deliver similar
overall performance, with Gluten finishing roughly 9% faster than Comet across the full query set.

[independent benchmark]: https://awslabs.github.io/data-on-eks/docs/benchmarks/spark-gluten-velox-comet-benchmark

The headline number masks wide per-query variation: some queries were significantly faster on Gluten (for example,
large fact-table joins where Gluten uses a shuffled hash join strategy), while others were significantly faster on
Comet (for example, CPU-bound scan and aggregate pipelines). We expect Comet performance to continue improving over
time and for this gap to close.

Although TPC-DS and TPC-H are good benchmarks for operators such as joins and aggregates, they don't necessarily
represent real-world queries, especially for ETL use cases. For example, there are limited complex types involved
and little string manipulation, regular expressions, or other advanced expressions. We recommend running your own
benchmarks based on your existing Spark jobs.

The scripts that were used to generate Comet's own TPC-H and TPC-DS results can be found
[here](https://github.com/apache/datafusion-comet/tree/main/benchmarks/tpc).

## Ease of Development & Contributing

Setting up a local development environment with Comet is generally easier than with Gluten due to Rust's package
management capabilities vs the complexities around installing C++ dependencies.

## Summary

Comet and Gluten are both good solutions for accelerating Spark jobs, and independent benchmarking shows they
deliver similar overall performance. Comet currently has an edge for users on Spark 4.0 with ANSI mode enabled, for
users who want a fully native Iceberg scan path, and through its codegen dispatcher, which keeps partially compatible
or unsupported expressions running inside the native pipeline rather than falling back to Spark. Gluten holds a
small performance lead in the AWS Labs TPC-DS benchmark and offers broader native integration with Delta Lake, Hudi,
and Paimon, plus a second backend in ClickHouse. We recommend trying both to see which is the best fit for your needs.
