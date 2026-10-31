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
 * Benchmark of native non-foldable `sha2(payload, numBits)` against Spark.
 *
 * Both Comet arms execute SHA-2 natively, with the codegen dispatcher enabled and disabled. The
 * mixed projection also measures native expressions alongside `sha2`.
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

  /** Native execution with the dispatcher enabled. */
  private def dispatchConfigs: Seq[(String, String)] =
    cometConfigs(dispatch = true)

  /** Native execution with the dispatcher disabled. */
  private def nativeConfigs: Seq[(String, String)] =
    cometConfigs(dispatch = false)

  private def cometConfigs(dispatch: Boolean): Seq[(String, String)] = Seq(
    noConstantFolding,
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString)

  private val DispatchCaseName = "Comet native, dispatch on"
  private val NativeCaseName = "Comet native, dispatch off"
  private val SparkCaseName = "Spark (Comet disabled)"

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val selected = cases
    runBenchmark("SHA-2 native: environment") {
      emitEnvironment(selected)
    }

    withCorpus(SmallRows) {
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
    emit("Both Comet arms execute SHA-2 natively without using the dispatcher.")
    emit(s"Cases: ${selected.map(_.name).mkString(", ")}")
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
    Seq(DispatchCaseName -> dispatchConfigs, NativeCaseName -> nativeConfigs).foreach {
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
      val benchmark = new Benchmark(s"${c.name} -- $rows rows", rows.toLong, output = output)
      checkPlans(c)
      // Spark goes first so `Relative` reports the native speedup.
      benchmark.addCase(SparkCaseName)(_ => runQuery(c.query, sparkConfigs))
      benchmark.addCase(NativeCaseName)(_ => runQuery(c.query, nativeConfigs))
      benchmark.addCase(DispatchCaseName)(_ => runQuery(c.query, dispatchConfigs))
      benchmark.run()
    }
  }

  /** Refuses to time a case whose actual routing does not match its label. */
  private def checkPlans(c: DispatchCase): Unit = {
    Seq(dispatchConfigs, nativeConfigs).foreach { configs =>
      withSQLConf(configs: _*) {
        CometScalaUDFCodegen.resetStats()
        val df = spark.sql(c.query)
        df.noop()
        val nonComet = findFirstNonCometOperator(stripAQEPlan(df.queryExecution.executedPlan))
        assert(nonComet.isEmpty, s"native projection fell back to Spark: $nonComet")
        assert(CometScalaUDFCodegen.stats().totalLookups == 0, "native SHA-2 used the dispatcher")
      }
    }
  }

  private def runQuery(query: String, configs: Seq[(String, String)]): Unit =
    withSQLConf(configs: _*) {
      spark.sql(query).noop()
    }

  /** Builds `parquetV1Table` with `rows` rows of the corpus and drops it afterwards. */
  private def withCorpus(rows: Int)(f: => Unit): Unit = {
    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        spark.range(rows.toLong).createOrReplaceTempView(tbl)
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
