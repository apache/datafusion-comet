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
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ElementAt, Expression, GetMapValue}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.comet.{CometPlan, CometProjectExec}
import org.apache.spark.sql.execution.{CommandResultExec, ProjectExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometExplainInfo, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus
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
 * Steady-state tables warm JVM code/source/JIT state before timing but still include per-task
 * kernel setup. First/second execution times also include class loading, I/O and scheduling;
 * their difference is not an isolated compilation cost. The general cases can share compiled
 * source, while the map first-use mode below isolates each selected case in a fresh JVM.
 *
 * Before warmed timing, every case is run through all three arms and the rows are compared.
 * First-use measurements precede validation to avoid populating the source cache; discard their
 * results if the subsequent checks fail.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCodegenDispatchBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCodegenDispatchBenchmark-**results.txt".
 *
 * Map lookups have a separate, strict-checked matrix. Pass `--map-lookups` for warmed tables,
 * optionally `--case=get_map_value-double-4-lookup` to select one case or `--check-only` to
 * validate without timing. For first use, launch a fresh JVM per case and arm with, for example,
 * `--map-lookups --case=get_map_value-double-4-lookup --first-use=dispatch`. The other arms are
 * `fallback` and `spark`. This measures first and second query execution on 1024 rows before
 * answer/route checks; it does not run the warmed tables. Never combine cold cases in one JVM:
 * map sizes and native sibling expressions can share the same compiled lookup source.
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
    // `encode`, and the `utf-8` form of `to_binary`, reach the dispatcher on every supported
    // version, but by two different routes. On 4.0+ `Encode` is `RuntimeReplaceable` and lowers
    // to `StaticInvoke(Encode, "encode", ...)`, which the catch-all picks up. On 3.x it stays an
    // ordinary `Encode` expression and `CometEncode` routes it. Both are worth timing: they are
    // the same Spark implementation behind the same bridge, so a large gap between the versions
    // would say something about the routing rather than about `encode`.
    DispatchCase("encode(utf-8)", "select encode(c_str, 'utf-8') from parquetV1Table"),
    DispatchCase("to_binary(utf-8)", "select to_binary(c_str, 'utf-8') from parquetV1Table"),
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
    if (mainArgs.headOption.contains("--map-lookups")) {
      runMapLookups(mainArgs.drop(1))
      return
    }
    require(mainArgs.isEmpty, s"Unknown arguments: ${mainArgs.mkString(" ")}")
    val selected = cases
    runBenchmark("Codegen dispatch: environment") {
      emitEnvironment(selected)
    }
    if (selected.isEmpty) {
      return
    }

    // Run before answer checks populate the JVM-wide source cache. Some general cases still
    // share sources with each other; the map-specific first-use mode avoids that by selection.
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
    runMapLookups(Array.empty)
  }

  // Bound the nested-column corpus size while retaining multiple Comet batches per task.
  private val MapLargeRows = 64 * 1024

  private case class MapCase(lookup: String, keyType: String, entries: Int, mixed: Boolean) {
    val name: String = s"$lookup-$keyType-$entries-${if (mixed) "mixed" else "lookup"}"
    val c: DispatchCase = {
      val value = if (lookup == "get_map_value") "m[k]" else "element_at(m, k)"
      val siblings = if (mixed) ", length(c_str), c_long + 1, substring(c_str, 1, 4)" else ""
      DispatchCase(
        name,
        s"SELECT $value AS v$siblings FROM parquetV1Table",
        extraConfigs = Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"))
    }

    def isLookup(e: Expression): Boolean = e match {
      case _: GetMapValue => lookup == "get_map_value"
      case _: ElementAt => lookup == "element_at"
      case _ => false
    }
  }

  private def mapCases: Seq[MapCase] = for {
    keyType <- Seq("double", "struct")
    entries <- Seq(4, 64)
    lookup <- Seq("get_map_value", "element_at")
    mixed <- Seq(false, true)
  } yield MapCase(lookup, keyType, entries, mixed)

  private def mapArms(c: DispatchCase): Seq[(String, Seq[(String, String)])] = Seq(
    "dispatch" -> dispatchConfigs(c),
    "fallback" -> fallbackConfigs(c),
    "spark" -> sparkConfigs(c))

  private def runMapLookups(args: Array[String]): Unit = {
    require(
      args.forall(a =>
        a.startsWith("--case=") || a.startsWith("--first-use=") ||
          a == "--check-only"),
      s"Unknown map arguments: ${args.mkString(" ")}")
    def option(prefix: String): Option[String] = {
      val values = args.filter(_.startsWith(prefix))
      require(values.length <= 1, s"Repeated option: $prefix")
      values.headOption.map(_.stripPrefix(prefix))
    }
    val filter = option("--case=")
    val firstUse = option("--first-use=")
    val checkOnly = args.contains("--check-only")
    val selected = mapCases.filter(c => filter.forall(_ == c.name))
    require(
      selected.nonEmpty,
      s"Unknown case; choose from ${mapCases.map(_.name).mkString(", ")}")
    require(
      firstUse.isEmpty || (selected.size == 1 && !checkOnly),
      "First use requires exactly one --case and no --check-only; launch a fresh JVM per arm.")
    require(firstUse.forall(Set("dispatch", "fallback", "spark")), "Unknown first-use arm")
    require(
      spark.sparkContext.isLocal,
      "Map route counters and codegen metrics require local mode")

    runBenchmark("Map lookup dispatch: environment") {
      emitEnvironment(selected.map(_.c), MapLargeRows)
      emit("First-use mode measures only the sub-batch corpus; it does not run warmed tables.")
      emit("Keys: DOUBLE or STRUCT<k:INT,tag:INT>; values: BIGINT; 4 or 64 entries.")
      emit("Column lookups: 25% last-entry hit, 25% miss, 25% NULL value, 25% first-entry hit.")
      emit("DOUBLE first-entry lookups use -0.0 against +0.0. Map construction is not timed.")
      emit("Timings include planning, Parquet scan and noop sink; no Spark data cache is used.")
      emit("Warmed queries still create tasks/kernels; only JVM code/source/JIT state is warmed.")
    }

    firstUse match {
      case Some(arm) =>
        val c = selected.head
        withMapCorpus(c, SmallRows) {
          runMapFirstUse(c, arm)
          verifyMapCase(c, SmallRows)
        }
      case None =>
        for (rows <- Seq(SmallRows, MapLargeRows);
          keyType <- Seq("double", "struct"); entries <- Seq(4, 64)) {
          val group = selected.filter(c => c.keyType == keyType && c.entries == entries)
          group.headOption.foreach { first =>
            withMapCorpus(first, rows) {
              group.foreach { c =>
                verifyMapCase(c, rows)
                if (!checkOnly) runSteadyState(c.c, rows)
              }
            }
          }
        }
    }
  }

  private def withMapCorpus(c: MapCase, rows: Int)(f: => Unit): Unit = {
    def key(ordinal: String): String = if (c.keyType == "double") {
      s"CAST($ordinal AS DOUBLE)"
    } else {
      s"named_struct('k', CAST($ordinal AS INT), 'tag', CAST(pmod(id, 7) AS INT))"
    }
    val lookupKey = if (c.keyType == "double") {
      "IF(j = 0, CAST('-0.0' AS DOUBLE), CAST(j AS DOUBLE))"
    } else key("j")
    val query = s"""
       |SELECT map_from_arrays(
       |  transform(sequence(0, ${c.entries - 1}), x -> ${key("x")}),
       |  transform(sequence(0, ${c.entries - 1}),
       |    x -> IF(x = 1, CAST(NULL AS BIGINT), id * 128 + x))) AS m,
       |  $lookupKey AS k, id AS c_long, repeat(CAST(id AS STRING), 4) AS c_str
       |FROM (SELECT id, CASE pmod(id, 4)
       |  WHEN 0 THEN ${c.entries - 1} WHEN 1 THEN ${c.entries}
       |  WHEN 2 THEN 1 ELSE 0 END AS j FROM $tbl)
       |""".stripMargin
    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark.range(rows).createOrReplaceTempView(tbl)
          prepareTable(dir, spark.sql(query))
        }
        f
      }
    }
  }

  private def runMapFirstUse(c: MapCase, arm: String): Unit = {
    // Warm common machinery without executing any lookup. Neither answer checks nor another
    // map case may precede the selected first use: CodeGenerator's source cache is JVM-wide.
    val warmup = DispatchCase("warmup", "SELECT c_str rlike '[0-9]+' FROM parquetV1Table")
    mapArms(warmup).foreach { case (_, configs) =>
      (0 until 5).foreach(_ => runQuery(warmup.query, configs))
    }
    val configs = mapArms(c.c).find(_._1 == arm).get._2
    runBenchmark(s"${c.name}: first use ($arm), $SmallRows rows") {
      (1 to 2).foreach { iteration =>
        CometScalaUDFCodegen.resetStats()
        val count = CodegenMetrics.METRIC_COMPILATION_TIME.getCount
        val nanos = CodeGenerator.compileTime
        val elapsed = timeMillis(runQuery(c.c.query, configs))
        val compilationMs = (CodeGenerator.compileTime - nanos) / 1e6
        val compilations = CodegenMetrics.METRIC_COMPILATION_TIME.getCount - count
        val stats = CometScalaUDFCodegen.stats()
        emit(f"FIRST_USE case=${c.name} arm=$arm run=$iteration wall_ms=$elapsed%.3f " +
          f"jvm_compilations=$compilations jvm_compile_ms=$compilationMs%.3f " +
          s"task_kernel_initializations=${stats.compileCount} batch_cache_hits=${stats.cacheHitCount}")
      }
      emit("JVM compilation metrics include all Spark-generated classes, not only Comet kernels.")
      emit("First-minus-second is not compilation time: class loading, JIT and I/O also differ.")
    }
  }

  private def verifyMapCase(c: MapCase, rows: Int): Unit = {
    verifyArmsAgree(c.c, rows)
    val explain = new ExtendedExplainInfo
    mapArms(c.c).foreach { case (arm, configs) =>
      withSQLConf(configs: _*) {
        val df = spark.sql(c.c.query)
        val lookups = df.queryExecution.optimizedPlan.flatMap(_.expressions.flatMap(_.collect {
          case e if c.isLookup(e) => e
        }))
        assert(lookups.size == 1, s"${c.name}: lookup was optimized away or duplicated")
        assert(
          lookups.head.children.forall(_.isInstanceOf[AttributeReference]),
          s"${c.name}: lookup arguments must remain persisted columns")
        val name = CometExplainInfo.exprDisplayName(lookups.head)
        CometScalaUDFCodegen.resetStats()
        val plan = noopInputPlan(df)
        val stats = CometScalaUDFCodegen.stats()
        val dispatched = explain.getCodegenDispatchExpressions(plan).toSet
        val native = explain.getNativeExpressions(plan).toSet
        val activity = stats.compileCount + stats.cacheHitCount
        if (arm == "dispatch") {
          assert(plan.exists(_.isInstanceOf[CometProjectExec]), s"Missing CometProject: $plan")
          assert(!plan.exists(_.isInstanceOf[ProjectExec]), s"Spark projection remained: $plan")
          assert(findFirstNonCometOperator(plan).isEmpty, s"Unexpected operator fallback: $plan")
          assert(
            dispatched == Set(name) && !native.contains(name),
            s"${c.name}: dispatched=$dispatched, native=$native")
          assert(activity > 0, s"${c.name}: dispatcher did not execute")
          if (c.mixed) {
            assert(
              Set("length", "add", "substring").subsetOf(native),
              s"${c.name}: mixed siblings must be native, found $native")
          }
        } else {
          assert(
            plan.exists {
              case p: ProjectExec => p.projectList.exists(_.exists(c.isLookup))
              case _ => false
            },
            s"${c.name}: missing Spark lookup projection: $plan")
          assert(activity == 0 && dispatched.isEmpty, s"$arm unexpectedly dispatched")
          if (arm == "spark") {
            assert(!plan.exists(_.isInstanceOf[CometPlan]), s"Pure Spark contained Comet: $plan")
          } else {
            assert(plan.exists(_.isInstanceOf[CometPlan]), s"Fallback arm lost Comet scan: $plan")
          }
        }
        // This separate collect also verifies that the corpus is consumed in its entirety.
        val answers = df.collect()
        assert(
          answers.length == rows && answers.count(!_.isNullAt(0)) == rows / 2,
          s"${c.name}: unexpected corpus cardinality or hit/NULL distribution")
        emit(
          s"PASS map answers/routes: ${c.name}, rows=$rows, arm=$arm, " +
            s"dispatched=${dispatched.toSeq.sorted.mkString(",")}, " +
            s"native=${native.toSeq.sorted.mkString(",")}, " +
            s"task_kernel_initializations=${stats.compileCount}, batch_cache_hits=${stats.cacheHitCount}")
      }
    }
  }

  /** Validate the actual noop write's input, which may differ from a standalone SELECT plan. */
  private def noopInputPlan(df: DataFrame): SparkPlan = {
    val captured = new AtomicReference[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        captured.set(qe.executedPlan)
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }
    // Listener synchronization is for validation only, never part of a timed benchmark case.
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    spark.listenerManager.register(listener)
    try {
      df.noop()
      spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    assert(captured.get() != null, "No executed noop query was captured")
    val executed = captured.get() match {
      case c: CommandResultExec => c.commandPhysicalPlan
      case other => other
    }
    val inputs = collect(executed) { case w: V2TableWriteExec => w.query }
    assert(inputs.size == 1, s"Expected one noop write input, found: $executed")
    stripAQEPlan(inputs.head)
  }

  /**
   * The reader of a results file cannot see the confs the numbers were produced under, and for
   * this benchmark the batch size and the dispatcher conf are the whole point.
   */
  private def emitEnvironment(selected: Seq[DispatchCase], largeRows: Int = LargeRows): Unit = {
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
    emit(s"Row counts: $SmallRows (sub-batch) and $largeRows (multi-batch).")
    val skipped =
      Seq("to_time(fmt)" -> isSpark41Plus).collect { case (name, false) => name }
    if (skipped.nonEmpty) {
      emit(
        "Not lowered to StaticInvoke/Invoke on this Spark version, skipped: " +
          skipped.mkString(", "))
    }
    emit(s"Cases: ${selected.map(_.name).mkString(", ")}")
  }

  /**
   * Observed first and second executions after unrelated dispatcher warmup. General cases may
   * share compiled sources, and the timings include work other than compilation. Use the
   * selected-case map mode for isolated first-use measurements and actual compilation metrics.
   */
  private def runFirstUse(selected: Seq[DispatchCase]): Unit = {
    // `rlike` routes through the same dispatcher and is not one of the cases below.
    val warmup = DispatchCase("warmup", "select c_str rlike '[0-9]+' from parquetV1Table")
    (0 until 5).foreach(_ => runQuery(warmup.query, dispatchConfigs(warmup)))

    emit(
      f"${"case"}%-24s  ${"1st run (ms)"}%14s  ${"2nd run (ms)"}%14s  " +
        f"${"1st - 2nd (ms)"}%14s  ${"compiles"}%9s")
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
    emit(
      "that cache. First-minus-second can also reflect JIT, I/O, scheduling and class loading.")
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
