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
import org.apache.spark.sql.catalyst.expressions.MapFromArrays
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Measures retaining the Comet projection via the LAST_WIN codegen dispatcher against falling
 * that projection back to Spark. Pure Spark is a third reference. The incompatible native
 * implementation is disabled in every arm. Unique/duplicate keys, arrays of length 4/256, and
 * standalone/mixed projections form eight tables.
 *
 * Inputs are varying String-key/Long-value columns in prepared Parquet, not constant arrays. The
 * optional argument is the input-element budget per table (default 1048576): large arrays use
 * fewer rows to bound memory. Compare arms within a table, not row rates across array sizes. Data
 * generation, full-result comparisons and plan/runtime checks are outside timing. Timing uses
 * noop(), which materializes the projection, and includes scanning and query execution.
 *
 * Spark's Benchmark warms each arm for 2 seconds, then times at least 2 iterations and at least 2
 * seconds. This measures steady state, not first-use compilation. The repeated dispatch-off
 * baseline at the end helps expose drift; differences smaller than that spread or the reported
 * standard deviation are not evidence of a speedup. Repeat the suite for independent samples.
 *
 * Run from the repository root (the make target builds a release library first):
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometMapFromArraysBenchmark BENCH_HEAP=4g
 * }}}
 * Append `-- 262144` for a smaller corpus. Results are written under spark/benchmarks/. The
 * shared benchmark base uses local[1], so runtime dispatcher counters are visible to the driver.
 */
object CometMapFromArraysBenchmark extends CometBenchmarkBase {

  private val FunctionName = "map_from_arrays"
  private val ExpressionName = classOf[MapFromArrays].getSimpleName

  private case class Arm(name: String, comet: Boolean, dispatch: Boolean)

  private val Fallback =
    Arm("Comet, dispatch off (Spark fallback)", comet = true, dispatch = false)
  private val Dispatch = Arm("Comet, codegen dispatch", comet = true, dispatch = true)
  private val Spark = Arm("Spark (Comet disabled)", comet = false, dispatch = false)

  private case class MapCase(
      rows: Int,
      arrayLength: Int,
      duplicateKeys: Boolean,
      mixed: Boolean) {
    val name: String = {
      val keys = if (duplicateKeys) "duplicate" else "unique"
      val projection = if (mixed) "mixed projection" else "standalone"
      s"$FunctionName: $keys keys, length $arrayLength, $projection -- $rows rows"
    }

    val query: String = {
      val neighbors = if (mixed) ", id + 1, length(label), substring(label, 1, 4)" else ""
      s"SELECT map_from_arrays(k, v)$neighbors FROM parquetV1Table"
    }
  }

  private def configs(arm: Arm): Seq[(String, String)] = Seq(
    SQLConf.MAP_KEY_DEDUP_POLICY.key -> "LAST_WIN",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRulesWith(ConstantFolding.ruleName),
    CometConf.getExprEnabledConfigKey(ExpressionName) -> "true",
    CometConf.getExprAllowIncompatConfigKey(ExpressionName) -> "false",
    CometConf.COMET_ENABLED.key -> arm.comet.toString,
    CometConf.COMET_EXEC_ENABLED.key -> arm.comet.toString,
    CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
    CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> arm.dispatch.toString)

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    require(mainArgs.length <= 1, "Usage: CometMapFromArraysBenchmark [input-element-budget]")
    val elements = mainArgs.headOption.map(_.toInt).getOrElse(1024 * 1024)
    require(elements >= 256, "The input-element budget must be at least 256")
    require(spark.sparkContext.isLocal, "Runtime dispatcher verification requires local mode")
    runBenchmark(s"$FunctionName: environment") {
      emitEnvironment(elements)
    }
    for {
      arrayLength <- Seq(4, 256)
      duplicateKeys <- Seq(false, true)
    } {
      val rows = elements / arrayLength
      withCorpus(rows, arrayLength, duplicateKeys) {
        Seq(false, true).foreach { mixed =>
          val c = MapCase(rows, arrayLength, duplicateKeys, mixed)
          runBenchmark(c.name) {
            verifyArmsAgree(c)
            val benchmark = new Benchmark(c.name, rows, output = output)
            Seq(Fallback, Dispatch, Spark, Fallback.copy(name = Fallback.name + " (repeat)"))
              .foreach { arm =>
                benchmark.addCase(arm.name) { _ =>
                  withSQLConf(configs(arm): _*) {
                    spark.sql(c.query).noop()
                  }
                }
              }
            benchmark.run()
          }
        }
      }
    }
  }

  /**
   * Compare maps independent of their Scala collection iteration order and rows as a multiset.
   */
  private def verifyArmsAgree(c: MapCase): Unit = {
    def collect(arm: Arm): Array[String] = {
      // Spark 3.x's withSQLConf returns Unit; do not return a value from its block.
      var rows: Array[Row] = Array.empty
      withSQLConf(configs(arm): _*) {
        CometScalaUDFCodegen.resetStats()
        val df = spark.sql(c.query)
        rows = df.collect()
        checkPlan(c, arm, stripAQEPlan(df.queryExecution.executedPlan))
        val lookups = CometScalaUDFCodegen.stats().totalLookups
        require(
          (lookups > 0) == arm.dispatch,
          s"${c.name}: ${arm.name} had $lookups dispatcher lookups; refusing to time it")
      }
      require(rows.length == c.rows, s"${c.name}: ${arm.name} produced ${rows.length} rows")
      rows.map { row =>
        val entries = row.getMap[String, Long](0).toSeq.sortBy(_._1)
        entries.mkString("[", ",", "]") + row.toSeq.drop(1).mkString("|", "|", "")
      }.sorted
    }

    val expected = collect(Spark)
    Seq(Fallback, Dispatch).foreach { arm =>
      require(
        expected.sameElements(collect(arm)),
        s"${c.name}: ${arm.name} differs from Spark; refusing to time it")
    }
    emit("Verified all rows against Spark, expected plans and runtime dispatcher activity.")
  }

  /** Abort instead of publishing a result under a misleading execution-path label. */
  private def checkPlan(c: MapCase, arm: Arm, plan: SparkPlan): Unit = {
    val explain = new ExtendedExplainInfo()
    val dispatched = explain.getCodegenDispatchExpressions(plan)
    val native = explain.getNativeExpressions(plan)
    val sparkMapProjection = plan.exists {
      case p: ProjectExec =>
        p.projectList.exists(_.exists(_.isInstanceOf[MapFromArrays]))
      case _ => false
    }
    val hasComet = plan.exists(_.nodeName.startsWith("Comet"))
    val correctRoute = if (arm.dispatch) {
      findFirstNonCometOperator(plan).isEmpty &&
      dispatched == Seq(FunctionName) && !native.contains(FunctionName) && !sparkMapProjection &&
      (!c.mixed || Seq("length", "substring").forall(native.contains))
    } else {
      sparkMapProjection && dispatched.isEmpty && !native.contains(FunctionName) &&
      hasComet == arm.comet
    }
    require(
      correctRoute,
      s"${c.name}: unexpected ${arm.name} plan; refusing to time it.\n" +
        s"Native expressions: $native; dispatched: $dispatched\n${plan.treeString}")
  }

  private def withCorpus(rows: Int, arrayLength: Int, duplicateKeys: Boolean)(
      f: => Unit): Unit = {
    val distinctKeys = if (duplicateKeys) arrayLength / 2 else arrayLength
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        withSQLConf(configs(Spark): _*) {
          val df = spark
            .range(rows)
            .selectExpr(
              "id",
              "concat('row-', cast(id as string)) as label",
              s"transform(sequence(0, ${arrayLength - 1}), " +
                s"x -> concat(cast(id as string), ':', cast(x % $distinctKeys as string))) as k",
              s"transform(sequence(0, ${arrayLength - 1}), " +
                s"x -> id * $arrayLength + x) as v")
          prepareTable(dir, df)
        }
        f
      }
    }
  }

  private def emitEnvironment(elements: Int): Unit = {
    emit(s"Spark: ${spark.version}; Scala: ${scala.util.Properties.versionNumberString}")
    emit(s"Java: ${System.getProperty("java.version")} (${System.getProperty("java.vm.name")})")
    emit(s"OS: ${System.getProperty("os.name")} ${System.getProperty("os.arch")}")
    emit(s"Master: ${spark.sparkContext.master}; JVM max heap: ${Runtime.getRuntime.maxMemory()}")
    emit(s"Input-element budget: $elements; lengths: 4, 256; rows = budget / length")
    emit(
      "Duplicate keys: half as many distinct keys, each occurring twice with different values.")
    emit("Preparation: Spark-written Snappy Parquet; each arm reads the same files.")
    emit("Steady state: 2s warmup, at least 2 timed iterations and 2s timed execution per arm.")
    emit(
      "Compare the repeated fallback baseline and reported stdev before interpreting speedups.")
    emit("Use the release build; these local timings do not establish production performance.")
    emit(s"Batch size: ${CometConf.COMET_BATCH_SIZE.get(spark.sessionState.conf)}")
    emit(s"Whole-stage codegen: ${spark.conf.get(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)}")
    configs(Dispatch).foreach { case (key, value) => emit(s"$key=$value") }
    emit("Fallback: same settings, dispatcher disabled. Spark: Comet and dispatcher disabled.")
  }

  private def emit(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes(StandardCharsets.UTF_8)))
  }
}
