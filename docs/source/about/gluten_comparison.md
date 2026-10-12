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

## Architecture

Comet and Gluten have very similar architectures. Both are Spark plugins that translate Spark physical plans to
a serialized representation and pass the serialized plan to native code for execution.

Gluten serializes the plans using the Substrait format and has an extensible architecture that supports execution
against multiple engines. Gluten 1.7.0 supports Velox and ClickHouse. A third backend, Bolt, a C++ engine from
ByteDance that is derived from Velox, has been added to Gluten's main branch but is not yet in a release. The rest of
this page compares Comet with Gluten's Velox backend.

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

## Community and Governance

Comet is developed within the Apache DataFusion project, and its native execution is built directly on the
DataFusion query engine. DataFusion is a broad, vendor-neutral community governed by the Apache Software Foundation
and used by many downstream projects beyond Comet. This means that engine-level improvements such as new operators,
optimizer rules, and performance work are shared across the whole DataFusion ecosystem: work done for other
DataFusion users benefits Comet, and work done for Comet benefits them in turn.

Velox is also an open-source project with contributors from multiple organizations, but its development has been
primarily driven by Meta. Contributing to Velox additionally requires signing a Contributor License Agreement: the
Velox [contributing guide] instructs contributors to sign the Meta [CLA] before their contributions can be accepted.
Comet and DataFusion follow the standard Apache Software Foundation contribution model and do not require a per-contributor
CLA. Users evaluating long-term adoption may want to weigh the governance model, contribution process, and breadth of
the community behind each engine alongside the technical differences.

[contributing guide]: https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md
[CLA]: https://code.facebook.com/cla

## Spark Version Support

Both projects target a similar set of Spark releases.

Comet supports Spark 3.4, 3.5, 4.0, 4.1, and 4.2 in production builds. See the
[Spark version compatibility guide] for the exact patch versions and JDK/Scala combinations.

[Spark version compatibility guide]: /user-guide/latest/compatibility/spark-versions.md

Gluten 1.7.0 supports Spark 3.3, 3.4, 3.5, 4.0, and 4.1.

## ANSI Mode

Spark 4.0 enables ANSI SQL semantics by default, which changes how arithmetic overflow, invalid casts, division by
zero, and similar error conditions are handled. This is one area where the two projects currently differ.

Comet implements ANSI semantics for the expressions it supports natively, including arithmetic overflow checks,
ANSI cast behavior, and `try_*` variants. Queries running with `spark.sql.ansi.enabled=true` continue to be accelerated.
See the [Comet Compatibility Guide] for details on which expressions have full ANSI coverage.

The Gluten Velox backend documents that ANSI mode is not supported: by default, any query executed with ANSI enabled
falls back to vanilla Spark. Setting `spark.gluten.sql.ansiFallback.enabled=false` makes Gluten attempt to run such
queries natively, but Gluten's developer documentation notes that the results do not yet match Spark. See the
[Gluten Velox limitations] page for the current status.

[Gluten Velox limitations]: https://apache.github.io/gluten/velox-backend-limitations.html

For users adopting Spark 4.0 without disabling ANSI mode, this difference can have a significant impact on the
fraction of a workload that runs natively.

## Table Format Support

Both projects can accelerate queries against Apache Iceberg tables, but they take different approaches and Gluten
covers a broader set of table formats overall.

Comet provides a native Iceberg scan built on iceberg-rust. It has been tested with Iceberg 1.5 and 1.8 through 1.11
and supports Iceberg spec v1, v2, and v3, schema evolution, time travel and branch reads, positional and equality
deletes and deletion vectors, encrypted v3 tables, REST catalogs, and S3-compatible object storage. Comet can also
write Iceberg data files natively through iceberg-rust, as an experimental feature that is disabled by default (see
[Comet Iceberg writes]). Comet does not currently provide native integrations for Delta Lake, Hudi, or Paimon. See
the [Comet Iceberg guide] for the full list of supported features and known limitations.

[Comet Iceberg guide]: /user-guide/latest/iceberg.md
[Comet Iceberg writes]: /user-guide/latest/iceberg-writes.md

Gluten ships dedicated modules for Iceberg, Delta Lake (2.3 through 4.1, depending on the Spark version), Hudi, and
Paimon. Its [Iceberg documentation] says that reads of unpartitioned tables are offloaded, including tables with
position deletes, while reads of partitioned tables mostly fall back to Spark, as do equality deletes and all writes.
Users who need native acceleration for Delta, Hudi, or Paimon will find broader coverage in Gluten today.

[Iceberg documentation]: https://github.com/apache/gluten/blob/v1.7.0/docs/get-started/VeloxIceberg.md

## Compatibility

Comet relies on the full Spark SQL test suite (consisting of more than 24,000 tests) as well its own unit and
integration tests to ensure compatibility with Spark. Features that are known to have compatibility differences with
Spark are disabled by default, but users can opt in. See the [Comet Compatibility Guide] for more information.

[Comet Compatibility Guide]: https://datafusion.apache.org/comet/user-guide/latest/compatibility/index.html

Gluten also aims to provide compatibility with Spark, and includes a subset of the Spark SQL tests in its own test
suite. See the Gluten [Velox backend limitations] page for known gaps, such as differences in the regular expression
dialect (RE2 vs `java.util.regex`).

[Velox backend limitations]: https://apache.github.io/gluten/velox-backend-limitations.html

## Codegen Dispatch

When an expression has a native implementation with known semantic differences from Spark, or has no native
implementation at all, Comet can run Spark's own generated code for that expression inside the native pipeline. Data
is passed to the JVM in Arrow format, evaluated using Spark's byte-exact logic, and returned to the native pipeline for
the rest of the query. Comet calls this the codegen dispatcher.

This matters for two reasons:

- **Correctness by default.** Expressions that are only partially compatible with Spark (for example, regular
  expressions, JSON functions, and some datetime and array functions) route through the codegen dispatcher by
  default, so they produce results that match Spark exactly. The faster native path is opt-in per expression for
  users who accept its differences.
- **Fewer fallbacks.** Because the dispatcher keeps evaluation inside the native pipeline, a single unsupported or
  incompatible scalar expression does not force its operator back onto vanilla Spark.

Gluten's closest equivalent is partial projection, which has been enabled by default since Gluten 1.3.0. When a
projection contains expressions that the backend cannot run, including Scala and Hive UDFs, Gluten splits the
projection so that vanilla Spark evaluates only those expressions and the rest of the projection runs natively.
Partial projection applies only to projections, so, for example, a filter that contains an unsupported expression
still falls back to Spark. See the [Comet Compatibility Guide] for more detail on how the codegen dispatcher works and
which expressions use it.

## Regular Expression Compatibility

Regular expressions are a good example of where the codegen dispatcher gives Comet an architectural advantage.
Spark's regex semantics come from the JVM's `java.util.regex` engine, which supports features such as
backreferences and lookaround. Because Comet's codegen dispatcher runs Spark's own generated Java code, Comet can
evaluate `rlike`, `regexp_replace`, `regexp_extract`, `regexp_extract_all`, `regexp_instr`, and `split` with 100%
compatibility with Spark by default, including every pattern feature the JVM engine supports. Comet also offers a
faster native Rust regex engine as an opt-in per expression for users who can accept its semantic differences.

Gluten's Velox backend evaluates regular expressions using the C++ RE2 engine, which uses a different dialect from
`java.util.regex` and deliberately omits features such as backreferences and lookaround. Gluten falls back to Spark
for patterns that RE2 cannot compile, so those return correct results without acceleration. Patterns that RE2 accepts
run natively and can differ from Spark in edge cases: for example, `\s` does not match the vertical tab character.
Setting `spark.gluten.sql.fallbackRegexpExpressions=true` sends every regular expression function to Spark. See the
[Gluten Velox limitations] page for details. Comet's codegen dispatcher avoids this trade-off by running Spark's own
regex engine inside the native pipeline.

## Scala and Java UDF Support

The same codegen dispatcher lets Comet run Spark's existing Scala and Java scalar UDFs inside the native pipeline
without any rewrite. A UDF's compiled code executes on Arrow data passed from the native pipeline, and, crucially,
the presence of a UDF does not force the enclosing operator back to vanilla Spark: the surrounding native operators
keep running natively while only the UDF itself is evaluated on the JVM. This covers functions registered via
`udf(...)`, `spark.udf.register(...)`, and SQL `CREATE FUNCTION ... AS 'com.example.MyUDF'`, including UDFs over
complex nested types and UDFs composed with other Catalyst expressions and higher-order functions.

This means users can adopt Comet without giving up their existing Scala and Java UDFs, and without rewriting them in
the engine's native language. Hive UDFs are not covered and still fall back to Spark. Gluten's Velox backend supports
native C++ UDFs, and its partial projection (see [Codegen Dispatch](#codegen-dispatch)) evaluates Scala and Hive UDFs
in a projection with vanilla Spark while the rest of the projection runs natively. A UDF in a filter makes the filter
fall back to Spark. See the [Comet Scala and Java UDF guide] for the full list of supported and unsupported cases.

[Comet Scala and Java UDF guide]: /user-guide/latest/scala_java_udfs.md

## Performance

AWS Labs published an [independent benchmark] comparing Comet 0.16.0 with Gluten 1.6.0 on a TPC-DS 3TB workload,
running on Amazon EKS with Graviton4 instances. The study concluded that the two accelerators deliver similar
overall performance, with Gluten finishing roughly 9% faster than Comet across the full query set.

[independent benchmark]: https://awslabs.github.io/data-on-eks/docs/benchmarks/spark-gluten-velox-comet-benchmark

The headline number masks wide per-query variation: some queries were significantly faster on Gluten (for example,
large fact-table joins where Gluten uses a shuffled hash join strategy), while others were significantly faster on
Comet (for example, CPU-bound scan and aggregate pipelines).

AWS Labs has also published separate runs of each accelerator against the same Spark baseline on the same TPC-DS 3TB
Parquet workload. [Comet 1.0.0] ran it 1.57× faster than Spark (and 1.75× faster on Iceberg tables), and
[Gluten 1.6.0] ran it 1.63× faster. We expect Comet performance to continue improving over time and for this gap to
close.

[Comet 1.0.0]: https://awslabs.github.io/data-on-eks/docs/benchmarks/spark-datafusion-comet-benchmark
[Gluten 1.6.0]: https://awslabs.github.io/data-on-eks/docs/benchmarks/spark-gluten-velox-benchmark

Although TPC-DS and TPC-H are good benchmarks for operators such as joins and aggregates, they don't necessarily
represent real-world queries, especially for ETL use cases. For example, there are limited complex types involved
and little string manipulation, regular expressions, or other advanced expressions. We recommend running your own
benchmarks based on your existing Spark jobs.

## Ease of Development & Contributing

Setting up a local development environment with Comet is generally easier than with Gluten due to Rust's package
management capabilities vs the complexities around installing C++ dependencies.

## Summary

Comet and Gluten are both good solutions for accelerating Spark jobs, and independent benchmarking shows they
deliver similar overall performance. Comet currently has an edge for users on Spark 4.0 with ANSI mode enabled, for
Iceberg workloads, and through its codegen dispatcher, which runs partially compatible or unsupported expressions with
Spark's exact semantics inside the native pipeline. Gluten holds a small performance lead in the AWS Labs TPC-DS
benchmarks and offers broader native integration with Delta Lake, Hudi, and Paimon, plus a second backend in
ClickHouse. We recommend trying both to see which is the best fit for your needs.
