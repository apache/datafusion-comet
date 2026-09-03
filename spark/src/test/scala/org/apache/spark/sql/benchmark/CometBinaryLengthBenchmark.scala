/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * One binary-input shape under benchmark. Each shape is materialized as its own column in the
 * source table so a single write covers the whole grid.
 *
 * @param name
 *   short label for the shape
 * @param column
 *   name of the source column holding the shape
 * @param width
 *   payload width in bytes for the non-null rows
 * @param nullPercent
 *   percentage of rows that are NULL
 */
case class BinaryLengthShape(name: String, column: String, width: Int, nullPercent: Int) {

  /** SQL that materializes this shape as a `binary` column. */
  def sql: String = {
    // RPAD to an exact width over the row id keeps values distinct (so Parquet does not collapse
    // the column to a single dictionary entry) while holding the payload width fixed. The digits
    // and the pad character are ASCII, so the UTF-8 encoding is exactly `width` bytes.
    val payload = s"CAST(RPAD(CAST(value AS STRING), $width, 'x') AS BINARY)"
    val expr = if (nullPercent == 0) {
      payload
    } else {
      s"CASE WHEN PMOD(value, 100) < $nullPercent THEN CAST(NULL AS BINARY) ELSE $payload END"
    }
    s"$expr AS $column"
  }
}

// spotless:off
/**
 * Benchmark to measure performance of Comet `length` / `bit_length` / `octet_length` on
 * `BinaryType` input, across the two paths the serdes can take. Added in response to the review
 * request on the PR that mixed `CodegenDispatchFallback` into the three serdes.
 * `CometStringExpressionBenchmark` covers these three roots only on `StringType`, which takes the
 * native DataFusion kernel and so never exercises the binary route.
 *
 * Three arms, all cases of the same `Benchmark` so they share warmup, iteration count, data and
 * SQL settings by construction:
 *
 *   - `Spark` -- Comet off entirely, the reference.
 *   - `Comet (Spark fallback)` -- Comet on with the codegen dispatcher disabled. Binary input is
 *     `Unsupported` and, with the dispatcher off, `CodegenDispatchFallback` produces no marker,
 *     so the enclosing projection falls back to Spark. This is the path this PR replaces, and it
 *     is what the pre-PR build does unconditionally.
 *   - `Comet (codegen dispatch)` -- Comet on with the dispatcher enabled, i.e. the path this PR
 *     adds: Spark's own `doGenCode` (`numBytes()`) compiled into a per-batch kernel that reads
 *     the Arrow vector directly, with no transition out of the Comet pipeline.
 *
 * The shapes span payload width (8 / 64 / 1024 bytes) at a fixed null fraction, and null fraction
 * (0% / 50% / 90%) at a fixed width. Both axes matter, and the measured result is that the
 * dispatcher path is the slower of the two Comet arms:
 *
 *   - Payload width drives the size of the gap. The kernel's `getBinary` allocates a `byte[]` and
 *     copies the whole payload out of the Arrow buffer per row (see `emitBinaryBodyUnsafe` in
 *     `CometBatchKernelCodegenInput`), because that is what Spark's generated `numBytes()` reads.
 *     So a `length` that should be an offset subtraction pays for the full value, and the cost the
 *     dispatch arm adds over the Spark-fallback arm tracks the payload: tens of ms at 8-64 B,
 *     several hundred at 1 KB, where the dispatch arm also runs about twice as slow as Spark.
 *   - Null fraction pulls the other way. The kernel short-circuits null rows ahead of the
 *     generated code, so a skipped row costs no copy and the gap shrinks as nulls rise -- at the
 *     same 64 B width it is several times smaller at 90% nulls than at 0%. It narrows sharply but
 *     does not close: the dispatch arm stays the slower of the two Comet arms at every shape here.
 *
 * Keeping the projection inside the Comet pipeline is therefore not automatically a win for these
 * roots; on wide binary it is a regression against the Spark fallback it replaces.
 *
 * Each case prints its physical plan and a digest of its result set before the timings, so the
 * report shows which operators each arm actually ran and that all three arms agree on the output.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometBinaryLengthBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometBinaryLengthBenchmark-**results.txt".
 */
// spotless:on
object CometBinaryLengthBenchmark extends CometBenchmarkBase {

  private val shapes = List(
    BinaryLengthShape("width_8B", "b_w8", width = 8, nullPercent = 0),
    BinaryLengthShape("width_64B", "b_w64", width = 64, nullPercent = 0),
    BinaryLengthShape("width_1KB", "b_w1024", width = 1024, nullPercent = 0),
    BinaryLengthShape("width_64B_50pct_null", "b_w64_n50", width = 64, nullPercent = 50),
    BinaryLengthShape("width_64B_90pct_null", "b_w64_n90", width = 64, nullPercent = 90))

  private val roots = List("length", "bit_length", "octet_length")

  /**
   * Excluding `ConstantFolding` matches the rest of the expression benchmarks. Nothing in these
   * queries is foldable (every root reads a column), but the exclusion is applied to all three
   * arms so the optimizer configuration is identical across them.
   */
  private val noConstantFolding =
    SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConstantFolding.ruleName

  private val sparkConfigs = Seq(noConstantFolding, CometConf.COMET_ENABLED.key -> "false")

  private def cometConfigs(dispatch: Boolean): Seq[(String, String)] = Seq(
    noConstantFolding,
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString)

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    // Twice the 1M default the other expression benchmarks use. `length` on binary is O(1) per
    // row, so what separates the two Comet arms is only the per-row cost of leaving the pipeline
    // (`CometColumnarToRow` then a Spark `Project`) versus staying in it (`CometProject`). The row
    // count has to be large enough that fixed query-submission cost does not drown that out: at
    // 8192 rows every arm reports hundreds of ns/row, which is overhead rather than expression
    // work.
    val values = 2 * 1024 * 1024 // 2M rows

    runBenchmarkWithTable("Binary length expressions", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(dir, spark.sql(s"SELECT ${shapes.map(_.sql).mkString(", ")} FROM $tbl"))

          for (root <- roots; shape <- shapes) {
            val name = s"$root ${shape.name}"
            val query = s"select $root(${shape.column}) as v from parquetV1Table"
            runBenchmark(name) {
              runBinaryLengthModes(name, v, query)
            }
          }
        }
      }
    }
  }

  /** Runs the three arms for a single root/shape pair. */
  private def runBinaryLengthModes(name: String, cardinality: Long, query: String): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    val arms = Seq(
      ("Spark", sparkConfigs),
      ("Comet (Spark fallback)", cometConfigs(dispatch = false)),
      ("Comet (codegen dispatch)", cometConfigs(dispatch = true)))

    // Report the plan and the result digest for every arm before timing anything. This both
    // documents what each arm ran and guards the comparison: an arm that silently planned
    // differently, or that disagreed on its output, is visible in the results file rather than
    // showing up as an unexplained speedup.
    val digests = arms.map { case (label, configs) =>
      label -> describe(benchmark, label, query, configs)
    }
    val (referenceLabel, reference) = digests.head
    digests.tail.foreach { case (label, result) =>
      if (result != reference) {
        report(
          benchmark,
          s"""WARNING: "$label" does not agree with "$referenceLabel". The arms below are not
             |computing the same thing, so the timings are not comparable.
             |  $referenceLabel: $reference
             |  $label: $result""".stripMargin)
      }
    }

    arms.foreach { case (label, configs) =>
      benchmark.addCase(label) { _ =>
        withSQLConf(configs: _*) {
          spark.sql(query).noop()
        }
      }
    }

    benchmark.run()
  }

  /**
   * Runs `query` under `configs`, writes its physical plan and a digest of its output to the
   * results file, and returns the digest so the caller can compare arms.
   */
  private def describe(
      benchmark: Benchmark,
      label: String,
      query: String,
      configs: Seq[(String, String)]): String = withSQLConf(configs: _*) {
    val df = spark.sql(query)
    // Execute the benchmarked query itself rather than the digest query below, so the plan
    // reported is the one the timings measure. `noop()` runs it without collecting. Execute
    // before reading the plan: AQE only settles the final plan once the query has run.
    df.noop()
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    val summary = digest(query)
    report(benchmark, s"$label plan:\n${plan.treeString}$summary")
    summary
  }

  /**
   * A one-line summary of `query`'s output, printed under each arm's plan and compared across
   * arms. Aggregated in Spark rather than collected to the driver: at this row count, pulling
   * every value back for a row-by-row comparison would cost more than the benchmark it guards.
   * Count, null count, sum and range together are enough to catch an arm that computes a
   * different length, or that returns a value where another returns NULL.
   */
  private def digest(query: String): String = {
    val row = spark
      .sql(s"""SELECT COUNT(1), COUNT(v), SUM(CAST(v AS BIGINT)), MIN(v), MAX(v)
              |FROM ($query)""".stripMargin)
      .collect()
      .head
    s"result: rows=${row.get(0)} nulls=${row.getLong(0) - row.getLong(1)} " +
      s"sum=${row.get(2)} min=${row.get(3)} max=${row.get(4)}"
  }

  /**
   * Writes a line to the benchmark results file as well as the console. `Benchmark.out` tees the
   * two, so writing through it keeps this ordered against the results table `Benchmark.run`
   * writes to the same stream.
   *
   * This mirrors `CometBenchmarkBase.warn`, which is private to the base. Kept local rather than
   * widening the shared base's API, since no other benchmark reports plans: only this one was
   * asked to show which path each arm took.
   */
  private def report(benchmark: Benchmark, message: String): Unit = {
    val border = "-" * 80
    benchmark.out.println(s"\n$border\n$message\n$border")
  }
}
