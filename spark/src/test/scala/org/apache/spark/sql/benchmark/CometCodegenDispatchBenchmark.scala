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

import java.nio.charset.StandardCharsets

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Benchmark of the expressions that the JVM codegen dispatcher picks up when no native handler
 * exists: `StaticInvoke` outside `CometStaticInvoke`'s allowlist, and `Invoke`, which has no
 * allowlist at all. Every case here fell the whole projection back to Spark before that catch-all
 * existed, so the interesting comparison is not Comet against Spark but
 *
 *   - `codegen dispatch` -- the expression runs as a Janino-compiled kernel reading and writing
 *     Arrow vectors inside the Comet pipeline, and
 *   - `dispatch off` -- `spark.comet.exec.scalaUDF.codegen.enabled=false`, which is exactly the
 *     behaviour that shipped before: the enclosing projection falls back to Spark, so the plan
 *     pays a columnar-to-row conversion and runs every other expression in the projection under
 *     whole-stage codegen.
 *
 * Both arms run Spark's own implementation of the function itself -- the dispatcher compiles
 * `Expression.doGenCode` -- so the difference between them is the cost of the bridge (expression
 * transport, per-batch argument binding, Arrow output allocation) set against the cost of losing
 * the operator to Spark. A pure Spark case is included as a third reference point.
 *
 * A `dispatch off (repeat)` case repeats the baseline at the end of every table. It measures the
 * same work as the first row, so the spread between the two is this machine's noise floor for
 * that table; ignore any difference between the other rows that is smaller than that spread.
 *
 * Compilation is a one-time cost, and the steady-state tables warm up before timing, so it does
 * not appear there. The `first use` table at the top measures it directly: the same query run
 * twice back to back on a cold kernel, after the dispatcher itself has been warmed on an
 * unrelated expression.
 *
 * Before timing anything, every case is run through all three arms and the rows are compared, so
 * a timing cannot come from an arm that computed something else.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCodegenDispatchBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCodegenDispatchBenchmark-**results.txt".
 */
object CometCodegenDispatchBenchmark extends CometBenchmarkBase {

  /** Fewer rows than one Comet batch, so the query is a single batch of real work. */
  private val SmallRows = 1024

  /** ~128 batches at the default `spark.comet.batchSize`. */
  private val LargeRows = 1024 * 1024

  /**
   * @param name
   *   Case name, as it appears in the results table.
   * @param query
   *   The query to time. Every argument is a column so that the expression is evaluated per row.
   * @param available
   *   False when the Spark version under test does not lower this function to a `StaticInvoke` /
   *   `Invoke`, in which case there is nothing for the dispatcher to pick up.
   * @param extraConfigs
   *   Applied to all three arms, so they never account for a difference between them.
   */
  private case class DispatchCase(
      name: String,
      query: String,
      available: Boolean = true,
      extraConfigs: Seq[(String, String)] = Nil)

  private def cases: Seq[DispatchCase] = Seq(
    // `lpad` / `rpad` on binary input lowers to `StaticInvoke(ByteArray, funcName, ...)` on every
    // supported Spark version.
    DispatchCase("lpad(binary)", "select lpad(c_bin, 24, c_pad) from parquetV1Table"),
    DispatchCase("rpad(binary)", "select rpad(c_bin, 24, c_pad) from parquetV1Table"),
    // Spark 4.0+ lowers `encode`, and the `utf-8` form of `to_binary`, to
    // `StaticInvoke(Encode, "encode", ...)`. On 3.x they are ordinary expressions.
    DispatchCase(
      "encode(utf-8)",
      "select encode(c_str, 'utf-8') from parquetV1Table",
      isSpark40Plus),
    DispatchCase(
      "to_binary(utf-8)",
      "select to_binary(c_str, 'utf-8') from parquetV1Table",
      isSpark40Plus),
    // Spark 4.1's `to_time` with a format lowers to an evaluator-backed `Invoke`, the receiver
    // call the `Invoke` half of the catch-all exists for. `spark.sql.timeType.enabled` defaults
    // to `Utils.isTesting`, so a benchmark JVM has to opt in the way Spark's own test runs do,
    // or `ToTime.checkInputDataTypes` rejects the call during analysis.
    DispatchCase(
      "to_time(fmt)",
      "select to_time(c_time, 'HH:mm:ss') from parquetV1Table",
      isSpark41Plus,
      Seq("spark.sql.timeType.enabled" -> "true")),
    // The case the catch-all is really about: one unhandled expression used to cost the whole
    // projection, including the three expressions next to it that do have native kernels.
    DispatchCase(
      "mixed projection",
      "select length(c_str), c_long + 1, substring(c_str, 1, 4), lpad(c_bin, 24, c_pad) " +
        "from parquetV1Table"),
    // ...and the same thing one operator further out. With the projection gone the aggregate
    // above it has a row-based child, so the partial aggregate, the exchange and the final
    // aggregate all leave the Comet pipeline with it. `c_pad` has 100 distinct values, so the
    // grouping is cheap and the per-row call is still what dominates. AQE is off and the
    // shuffle is one partition so that both arms plan the same shape every iteration, and so
    // that the plan check is not looking at `AQEShuffleRead`.
    DispatchCase(
      "group by dispatch",
      "select lpad(c_pad, 8, c_pad) as k, count(*) from parquetV1Table group by 1",
      available = true,
      Seq(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1")))
    .filter(_.available)

  private def noConstantFolding: (String, String) =
    SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRulesWith(ConstantFolding.ruleName)

  private def sparkConfigs(c: DispatchCase): Seq[(String, String)] =
    Seq(noConstantFolding, CometConf.COMET_ENABLED.key -> "false") ++ c.extraConfigs

  /** Comet on, dispatcher on: the behaviour this benchmark is validating. */
  private def dispatchConfigs(c: DispatchCase): Seq[(String, String)] =
    cometConfigs(c, dispatch = true)

  /** Comet on, dispatcher off: the behaviour that shipped before the catch-all. */
  private def fallbackConfigs(c: DispatchCase): Seq[(String, String)] =
    cometConfigs(c, dispatch = false)

  private def cometConfigs(c: DispatchCase, dispatch: Boolean): Seq[(String, String)] = Seq(
    noConstantFolding,
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString) ++ c.extraConfigs

  private val DispatchCaseName = "Comet, codegen dispatch"
  private val FallbackCaseName = "Comet, dispatch off (Spark fallback)"
  private val SparkCaseName = "Spark (Comet disabled)"

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val selected = cases
    runBenchmark("Codegen dispatch: environment") {
      emitEnvironment(selected)
    }
    if (selected.isEmpty) {
      return
    }

    // The kernels must be uncompiled when `runFirstUse` starts, so it runs before anything else
    // executes these queries: `CodeGenerator.compile` caches on the generated source JVM-wide,
    // and a single earlier execution would make every "first use" number a cache hit.
    withCorpus(SmallRows) {
      runBenchmark(s"Codegen dispatch: first use, $SmallRows rows") {
        runFirstUse(selected)
      }
      selected.foreach(verifyArmsAgree(_, SmallRows))
      selected.foreach(runSteadyState(_, SmallRows))
    }
    withCorpus(LargeRows) {
      selected.foreach(verifyArmsAgree(_, LargeRows))
      selected.foreach(runSteadyState(_, LargeRows))
    }
  }

  /**
   * The reader of a results file cannot see the confs the numbers were produced under, and for
   * this benchmark the batch size and the dispatcher conf are the whole point.
   */
  private def emitEnvironment(selected: Seq[DispatchCase]): Unit = {
    emit(s"Spark version: ${spark.version}")
    emit(
      s"Java version: ${System.getProperty("java.version")} " +
        s"(${System.getProperty("java.vm.name")})")
    emit(s"Scala version: ${scala.util.Properties.versionNumberString}")
    emit(s"spark.master: ${spark.conf.get("spark.master", "<unset>")}")
    emit(
      s"${CometConf.COMET_BATCH_SIZE.key}: " +
        CometConf.COMET_BATCH_SIZE.get(spark.sessionState.conf))
    emit(
      s"${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}: " +
        spark.conf.get(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key))
    emit(s"Dispatcher conf: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}")
    emit("Steady-state tables: Spark's Benchmark defaults -- 2s of untimed warmup per case, then")
    emit(
      "  at least 2 iterations and at least 2s of timed iterations; the table reports best and")
    emit("  average of the timed iterations.")
    emit(s"Row counts: $SmallRows (sub-batch) and $LargeRows (multi-batch).")
    val skipped =
      Seq("encode(utf-8) / to_binary(utf-8)" -> isSpark40Plus, "to_time(fmt)" -> isSpark41Plus)
        .collect { case (name, false) => name }
    if (skipped.nonEmpty) {
      emit(
        "Not lowered to StaticInvoke/Invoke on this Spark version, skipped: " +
          skipped.mkString(", "))
    }
    emit(s"Cases: ${selected.map(_.name).mkString(", ")}")
  }

  /**
   * Cost of the first execution of a query whose kernel has never been compiled, against the
   * second execution of the same query.
   *
   * The dispatcher is warmed on an expression that is not in the case list first, so that Janino,
   * `CodeGenerator`, the Arrow bridge and the FFI boundary are all loaded and JIT-warm before the
   * first case runs and the difference is dominated by compiling that one kernel. It is still an
   * upper bound on the compile: the first case in the list absorbs whatever class loading the
   * warmup missed, and any case that reaches machinery no earlier case did -- the grouped case is
   * the first to shuffle -- pays for that here too.
   */
  private def runFirstUse(selected: Seq[DispatchCase]): Unit = {
    // `rlike` routes through the same dispatcher and is not one of the cases below.
    val warmup = DispatchCase("warmup", "select c_str rlike '[0-9]+' from parquetV1Table")
    (0 until 5).foreach(_ => runQuery(warmup.query, dispatchConfigs(warmup)))

    emit(
      f"${"case"}%-24s  ${"1st run (ms)"}%14s  ${"2nd run (ms)"}%14s  " +
        f"${"one-time (ms)"}%14s  ${"compiles"}%9s")
    emit("-" * 84)
    selected.foreach { c =>
      CometScalaUDFCodegen.resetStats()
      val cold = timeMillis(runQuery(c.query, dispatchConfigs(c)))
      val compiles = CometScalaUDFCodegen.stats().compileCount
      val warm = timeMillis(runQuery(c.query, dispatchConfigs(c)))
      emit(f"${c.name}%-24s  $cold%14.1f  $warm%14.1f  ${cold - warm}%14.1f  $compiles%9d")
    }
    emit("")
    emit("`compiles` counts dispatcher cache misses, which is one per task. The Janino work")
    emit("itself is deduplicated JVM-wide by Spark's CodeGenerator source cache, so only the")
    emit("first task to reach a given kernel source pays for it -- and a case whose bound tree")
    emit("matches an earlier case's, as `to_binary(utf-8)` matches `encode(utf-8)`, is a hit on")
    emit("that cache and reports a one-time cost near zero.")
  }

  /**
   * Fails if the three arms disagree on `query`. Rows are compared as a sorted multiset rather
   * than positionally, because the grouped case shuffles and its output order is a property of
   * the plan, which is the one thing that differs between the arms.
   */
  private def verifyArmsAgree(c: DispatchCase, rows: Int): Unit = {
    def collect(configs: Seq[(String, String)]): Array[String] = {
      // Assigned to a local rather than returned from the block: Spark 3.4 and 3.5 declare
      // `SQLHelper.withSQLConf` as returning `Unit`; only Spark 4 has the result-returning form.
      var collected: Array[Row] = Array.empty
      withSQLConf(configs: _*) {
        collected = spark.sql(c.query).collect()
      }
      // `Row.equals` compares binary columns by reference, so normalize before comparing.
      collected
        .map(
          _.toSeq
            .map {
              case bytes: Array[Byte] => bytes.mkString("[", ",", "]")
              case other => String.valueOf(other)
            }
            .mkString("|"))
        .sorted
    }

    val expected = collect(sparkConfigs(c))
    Seq(DispatchCaseName -> dispatchConfigs(c), FallbackCaseName -> fallbackConfigs(c)).foreach {
      case (armName, configs) =>
        val actual = collect(configs)
        assert(
          expected.length == actual.length,
          s"${c.name} @ $rows rows: Spark produced ${expected.length} rows, " +
            s"$armName ${actual.length}")
        expected.indices.find(i => expected(i) != actual(i)).foreach { i =>
          throw new AssertionError(
            s"${c.name} @ $rows rows: row $i differs -- Spark ${expected(i)}, " +
              s"$armName ${actual(i)}")
        }
    }
  }

  private def runSteadyState(c: DispatchCase, rows: Int): Unit = {
    runBenchmark(s"${c.name} -- $rows rows") {
      val benchmark = new Benchmark(s"${c.name} -- $rows rows", rows, output = output)
      checkPlans(benchmark, c)
      // The dispatch-off arm goes first so the `Relative` column reads as the speedup this
      // change buys over the behaviour that shipped before it.
      benchmark.addCase(FallbackCaseName)(_ => runQuery(c.query, fallbackConfigs(c)))
      benchmark.addCase(DispatchCaseName)(_ => runQuery(c.query, dispatchConfigs(c)))
      benchmark.addCase(SparkCaseName)(_ => runQuery(c.query, sparkConfigs(c)))
      benchmark.addCase(s"$FallbackCaseName (repeat)")(_ => runQuery(c.query, fallbackConfigs(c)))
      benchmark.run()
    }
  }

  /**
   * Warns rather than fails, so one Spark version lowering a function differently degrades the
   * table to a note instead of aborting the run. A case where the dispatch arm is not fully
   * native, or where the dispatch-off arm is, is not measuring what its name says.
   */
  private def checkPlans(benchmark: Benchmark, c: DispatchCase): Unit = {
    var dispatchNonComet: Option[String] = None
    var fallbackIsFullyComet = false
    var dispatcherRan = false
    withSQLConf(dispatchConfigs(c): _*) {
      CometScalaUDFCodegen.resetStats()
      val df = spark.sql(c.query)
      df.noop()
      val stats = CometScalaUDFCodegen.stats()
      dispatcherRan = stats.compileCount + stats.cacheHitCount > 0
      dispatchNonComet =
        findFirstNonCometOperator(stripAQEPlan(df.queryExecution.executedPlan)).map(_.nodeName)
    }
    withSQLConf(fallbackConfigs(c): _*) {
      val df = spark.sql(c.query)
      df.noop()
      fallbackIsFullyComet =
        findFirstNonCometOperator(stripAQEPlan(df.queryExecution.executedPlan)).isEmpty
    }
    dispatchNonComet.foreach(op =>
      warn(
        benchmark,
        "WARNING: the codegen-dispatch plan is not fully Comet native (first " +
          s"non-Comet operator: $op), so that case is partly measuring Spark."))
    if (!dispatcherRan) {
      warn(
        benchmark,
        "WARNING: the codegen dispatcher did not run for this query, so the two " +
          "Comet cases below are measuring the same plan.")
    }
    if (fallbackIsFullyComet) {
      warn(
        benchmark,
        "WARNING: the dispatch-off plan is fully Comet native, so this case is " +
          "not exercising the operator fallback it is meant to be compared against.")
    }
  }

  private def runQuery(query: String, configs: Seq[(String, String)]): Unit =
    withSQLConf(configs: _*) {
      spark.sql(query).noop()
    }

  private def timeMillis(f: => Unit): Double = {
    val start = System.nanoTime()
    f
    (System.nanoTime() - start) / 1e6
  }

  /** Builds `parquetV1Table` with `rows` rows of the corpus and drops it afterwards. */
  private def withCorpus(rows: Int)(f: => Unit): Unit = {
    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        spark.range(rows).createOrReplaceTempView(tbl)
        prepareTable(dir, spark.sql(corpusQuery))
        f
      }
    }
  }

  /**
   * Every column varies per row, so no argument to a benchmarked expression is loop-invariant and
   * neither engine can hoist the call out of the row loop.
   */
  private def corpusQuery: String = {
    val columns = Seq(
      "c_str" -> "REPEAT(CAST(id AS STRING), 4)",
      "c_bin" -> "CAST(REPEAT(CAST(id AS STRING), 4) AS BINARY)",
      // Short, so `lpad` / `rpad` have padding to do on most rows.
      "c_pad" -> "CAST(CAST(PMOD(id, 100) AS STRING) AS BINARY)",
      "c_long" -> "id",
      "c_time" -> ("CONCAT(LPAD(CAST(PMOD(id, 24) AS STRING), 2, '0'), ':', " +
        "LPAD(CAST(PMOD(id, 60) AS STRING), 2, '0'), ':', " +
        "LPAD(CAST(PMOD(id * 7, 60) AS STRING), 2, '0'))"))
    s"SELECT ${columns.map { case (name, expr) => s"$expr AS $name" }.mkString(", ")} FROM $tbl"
  }

  /** Writes a warning to the results file as well as the console, ordered against the table. */
  private def warn(benchmark: Benchmark, message: String): Unit = {
    val border = "=" * 80
    benchmark.out.println(s"\n$border\n$message\n$border")
  }

  /** [[Benchmark]] tees console and results file; this benchmark's own tables need the same. */
  private def emit(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes(StandardCharsets.UTF_8)))
  }
}
