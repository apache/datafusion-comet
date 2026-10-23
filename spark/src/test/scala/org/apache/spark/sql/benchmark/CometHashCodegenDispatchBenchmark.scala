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
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Benchmark of non-foldable `sha2(payload, numBits)` through `CodegenDispatchFallback`.
 *
 * The dispatcher runs Spark's `doGenCode` inside the Comet pipeline. With dispatch disabled, the
 * projection falls back to Spark. The mixed projection also measures native expressions alongside
 * `sha2`; it does not imply that SHA-2 itself executes natively.
 *
 * A pure Spark case is included as a third reference point. A `dispatch off (repeat)` case
 * repeats the baseline at the end of every table so the spread between the two is this machine's
 * noise floor.
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
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometHashCodegenDispatchBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometHashCodegenDispatchBenchmark-**results.txt".
 */
object CometHashCodegenDispatchBenchmark extends CometBenchmarkBase {

  /** Fewer rows than one Comet batch, so the query is a single batch of real work. */
  private val SmallRows = 1024

  /** ~128 batches at the default `spark.comet.batchSize`. */
  private val LargeRows = 1024 * 1024

  /**
   * @param name
   *   Case name, as it appears in the results table.
   * @param query
   *   The query to time. Every argument is a column so that the expression is evaluated per row.
   */
  private case class DispatchCase(name: String, query: String)

  private def cases: Seq[DispatchCase] = Seq(
    DispatchCase("sha2(payload, numBits)", "select sha2(c_str, c_bits) from parquetV1Table"),
    DispatchCase(
      "sha2 mixed projection",
      "select length(c_str), c_int + 1, hash(c_int), sha2(c_str, c_bits) from parquetV1Table"))

  private def noConstantFolding: (String, String) =
    SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRulesWith(ConstantFolding.ruleName)

  private def sparkConfigs: Seq[(String, String)] =
    Seq(noConstantFolding, CometConf.COMET_ENABLED.key -> "false")

  /** Comet on, dispatcher on: the behaviour this benchmark is validating. */
  private def dispatchConfigs: Seq[(String, String)] =
    cometConfigs(dispatch = true)

  /** Comet on, dispatcher off: the enclosing projection falls back to Spark. */
  private def fallbackConfigs: Seq[(String, String)] =
    cometConfigs(dispatch = false)

  private def cometConfigs(dispatch: Boolean): Seq[(String, String)] = Seq(
    noConstantFolding,
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString)

  private val DispatchCaseName = "Comet, codegen dispatch"
  private val FallbackCaseName = "Comet, dispatch off (Spark fallback)"
  private val SparkCaseName = "Spark (Comet disabled)"

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val selected = cases
    runBenchmark("SHA-2 codegen dispatch: environment") {
      emitEnvironment(selected)
    }

    // The kernels must be uncompiled when `runFirstUse` starts, so it runs before anything else
    // executes these queries: `CodeGenerator.compile` caches on the generated source JVM-wide,
    // and a single earlier execution would make the "first use" number a cache hit. Only the
    // isolated case is measured here: the mixed projection reuses the same SHA-2 kernel.
    withCorpus(SmallRows) {
      runBenchmark(s"SHA-2 codegen dispatch: first use, $SmallRows rows") {
        runFirstUse(selected.take(1))
      }
      selected.foreach(verifyArmsAgree(_, SmallRows))
      selected.foreach(runSteadyState(_, SmallRows))
    }
    withCorpus(LargeRows) {
      selected.foreach(verifyArmsAgree(_, LargeRows))
      selected.foreach(runSteadyState(_, LargeRows))
    }
  }

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
    emit("Payloads include nulls, empty strings and varying lengths; numBits varies per row.")
    emit("Both Comet arms use Spark codegen for SHA-2; dispatch keeps the projection in Comet.")
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
   * warmup missed. It is a query-level measurement, not an isolated compiler measurement.
   */
  private def runFirstUse(selected: Seq[DispatchCase]): Unit = {
    // `rlike` routes through the same dispatcher and is not one of the cases below.
    val warmup = DispatchCase("warmup", "select c_str rlike '[0-9]+' from parquetV1Table")
    (0 until 5).foreach(_ => runQuery(warmup.query, dispatchConfigs))

    emit(
      f"${"case"}%-24s  ${"1st run (ms)"}%14s  ${"2nd run (ms)"}%14s  " +
        f"${"one-time (ms)"}%14s  ${"compiles"}%9s")
    emit("-" * 84)
    selected.foreach { c =>
      CometScalaUDFCodegen.resetStats()
      val cold = timeMillis(runQuery(c.query, dispatchConfigs))
      val compiles = CometScalaUDFCodegen.stats().compileCount
      val warm = timeMillis(runQuery(c.query, dispatchConfigs))
      emit(f"${c.name}%-24s  $cold%14.1f  $warm%14.1f  ${cold - warm}%14.1f  $compiles%9d")
    }
    emit("")
    emit("`compiles` counts dispatcher cache misses, which is one per task. The Janino work")
    emit("itself is deduplicated JVM-wide by Spark's CodeGenerator source cache, so only the")
    emit("first task to reach a given kernel source pays for it.")
  }

  /**
   * Fails if the three arms disagree on `query`. Rows are compared as a sorted multiset because
   * output order is not guaranteed across the different plans.
   */
  private def verifyArmsAgree(c: DispatchCase, rows: Int): Unit = {
    def collect(configs: Seq[(String, String)]): Array[String] = {
      // Assigned to a local rather than returned from the block: Spark 3.4 and 3.5 declare
      // `SQLHelper.withSQLConf` as returning `Unit`; only Spark 4 has the result-returning form.
      var collected: Array[Row] = Array.empty
      withSQLConf(configs: _*) {
        collected = spark.sql(c.query).collect()
      }
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

    val expected = collect(sparkConfigs)
    Seq(DispatchCaseName -> dispatchConfigs, FallbackCaseName -> fallbackConfigs).foreach {
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
      checkPlans(c)
      // The dispatch-off arm goes first so the `Relative` column reads as the speedup this
      // change buys over falling the enclosing operator back to Spark.
      benchmark.addCase(FallbackCaseName)(_ => runQuery(c.query, fallbackConfigs))
      benchmark.addCase(DispatchCaseName)(_ => runQuery(c.query, dispatchConfigs))
      benchmark.addCase(SparkCaseName)(_ => runQuery(c.query, sparkConfigs))
      benchmark.addCase(s"$FallbackCaseName (repeat)")(_ => runQuery(c.query, fallbackConfigs))
      benchmark.run()
    }
  }

  /** Refuses to time a case whose actual routing does not match its label. */
  private def checkPlans(c: DispatchCase): Unit = {
    withSQLConf(dispatchConfigs: _*) {
      CometScalaUDFCodegen.resetStats()
      val df = spark.sql(c.query)
      df.noop()
      val stats = CometScalaUDFCodegen.stats()
      assert(stats.compileCount + stats.cacheHitCount > 0, s"dispatcher did not run: $stats")
      val nonComet = findFirstNonCometOperator(stripAQEPlan(df.queryExecution.executedPlan))
      assert(nonComet.isEmpty, s"dispatch projection fell back to Spark: $nonComet")
    }
    withSQLConf(fallbackConfigs: _*) {
      CometScalaUDFCodegen.resetStats()
      val df = spark.sql(c.query)
      df.noop()
      assert(
        findFirstNonCometOperator(stripAQEPlan(df.queryExecution.executedPlan)).nonEmpty,
        "dispatch-off case did not fall back to Spark")
      assert(CometScalaUDFCodegen.stats().totalLookups == 0, "dispatcher ran while disabled")
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
   * Inputs vary per row, including nulls and empty payloads, so neither engine can hoist the
   * expression out of the row loop.
   */
  private def corpusQuery: String = {
    val columns = Seq(
      "c_str" -> ("CASE WHEN pmod(id, 31) = 0 THEN NULL WHEN pmod(id, 31) = 1 THEN '' " +
        "ELSE REPEAT(CAST(id AS STRING), CAST(pmod(id, 16) AS INT) + 1) END"),
      "c_int" -> "CAST(id AS INT)",
      "c_bits" -> "element_at(array(0, 224, 256, 384, 512, 128, NULL), CAST(pmod(id, 7) AS INT) + 1)")
    s"SELECT ${columns.map { case (name, expr) => s"$expr AS $name" }.mkString(", ")} FROM $tbl"
  }

  /** [[Benchmark]] tees console and results file; this benchmark's own tables need the same. */
  private def emit(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes(StandardCharsets.UTF_8)))
  }
}
