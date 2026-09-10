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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Matched benchmark for the two routes Spark 4.x can take for `MapSort` shapes that Comet cannot
 * sort natively:
 *
 *   - with the JVM codegen dispatcher disabled, the enclosing projection or shuffle falls back to
 *     Spark; and
 *   - with it enabled, Spark's `MapSort.doGenCode` executes inside the Comet pipeline.
 *
 * Every pair reads the same Parquet data and differs only in
 * `spark.comet.exec.scalaUDF.codegen.enabled`. Array and struct cases vary map size independently
 * from nested-key width; strict floating-point cases include NaN and signed zero. Input maps are
 * written in reverse key order and one row in 64 has a NULL map.
 *
 * Spark 4.0 and 4.1 only insert `MapSort` for grouping and repartition expressions;
 * `try_element_at` itself does not insert one. To measure a projection without also timing an
 * aggregate, `mapSortProjection` asks Spark's grouping optimizer to construct its real
 * `Project(MapSort(m))`, then executes that logical Project on its own. This avoids importing the
 * Spark-4.x-only `MapSort` class and keeps this common benchmark source compilable on Spark 3.x.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *     benchmark-org.apache.spark.sql.benchmark.CometMapSortBenchmark
 * }}}
 *
 * Formal steady-state results require at least five fresh JVM invocations, alternating
 * `-Dcomet.mapSortBenchmark.caseOrder=fallback-first` and `dispatcher-first`, and reporting the
 * median/min/max across processes rather than only Benchmark's within-process best time.
 *
 * First-action latency is a separate invocation mode. Each process measures one action only, so
 * callers should run every schema/route/workload tuple at least three times in fresh JVMs:
 * {{{
 *   -Dcomet.mapSortBenchmark.mode=first-action \
 *   -Dcomet.mapSortBenchmark.shape=array-small \
 *   -Dcomet.mapSortBenchmark.route=dispatcher \
 *   -Dcomet.mapSortBenchmark.workload=projection \
 *   -Dcomet.mapSortBenchmark.repetition=1
 * }}}
 */
object CometMapSortBenchmark extends CometBenchmarkBase {

  private val DefaultProjectionRows = 1000000
  private val DefaultShuffleRows = 250000
  private val DefaultInputPartitions = 4
  private val DefaultShufflePartitions = 16
  private val DefaultVerificationRows = 2048
  private val DefaultFirstActionRows = 1024
  private val NullMapEvery = 64

  private val ModeProperty = "comet.mapSortBenchmark.mode"
  private val CaseOrderProperty = "comet.mapSortBenchmark.caseOrder"
  private val ShapeProperty = "comet.mapSortBenchmark.shape"
  private val RouteProperty = "comet.mapSortBenchmark.route"
  private val WorkloadProperty = "comet.mapSortBenchmark.workload"
  private val RepetitionProperty = "comet.mapSortBenchmark.repetition"

  private val Mode = sys.props.getOrElse(ModeProperty, "steady")
  private val CaseOrder = sys.props.getOrElse(CaseOrderProperty, "fallback-first")

  // These overrides make plan/routing validation practical on a development machine. The normal
  // microbenchmark runner supplies none of them and therefore always uses the values above. The
  // effective values are emitted into the results file.
  private val ProjectionRows =
    intProperty("comet.mapSortBenchmark.projectionRows", DefaultProjectionRows)
  private val ShuffleRows = intProperty("comet.mapSortBenchmark.shuffleRows", DefaultShuffleRows)
  private val InputPartitions =
    intProperty("comet.mapSortBenchmark.inputPartitions", DefaultInputPartitions)
  private val ShufflePartitions =
    intProperty("comet.mapSortBenchmark.shufflePartitions", DefaultShufflePartitions)
  private val VerificationRows =
    intProperty("comet.mapSortBenchmark.verificationRows", DefaultVerificationRows)
  private val FirstActionRows =
    intProperty("comet.mapSortBenchmark.firstActionRows", DefaultFirstActionRows)

  private sealed trait KeyFamily {
    def label: String
    def mapType(width: Int): String
    def key(entry: String, width: Int): String
  }

  private case object ArrayKey extends KeyFamily {
    override val label: String = "array"

    override def mapType(width: Int): String = "MAP<ARRAY<INT>, INT>"

    override def key(entry: String, width: Int): String =
      s"""transform(
         |  sequence(0, ${width - 1}),
         |  j -> CAST(pmod(id, 1000003) * 4096 + CAST($entry AS BIGINT) * $width + j AS INT))
         |""".stripMargin.replace('\n', ' ')
  }

  private case object StructKey extends KeyFamily {
    override val label: String = "struct"

    override def mapType(width: Int): String = {
      val fields = (0 until width).map(i => s"f$i: INT").mkString(", ")
      s"MAP<STRUCT<$fields>, INT>"
    }

    override def key(entry: String, width: Int): String = {
      val fields = (0 until width).flatMap { i =>
        Seq(
          s"'f$i'",
          s"CAST(pmod(id, 1000003) * 4096 + CAST($entry AS BIGINT) * $width + $i AS INT)")
      }
      s"named_struct(${fields.mkString(", ")})"
    }
  }

  private case object StrictDoubleKey extends KeyFamily {
    override val label: String = "strict-double"

    override def mapType(width: Int): String = "MAP<DOUBLE, INT>"

    override def key(entry: String, width: Int): String =
      s"""CASE
         |  WHEN $entry = 0 THEN CAST('NaN' AS DOUBLE)
         |  WHEN $entry = 1 THEN CAST('-0.0' AS DOUBLE)
         |  ELSE CAST(id * 128 + CAST($entry AS BIGINT) + 1 AS DOUBLE)
         |END""".stripMargin.replace('\n', ' ')
  }

  private case class Shape(name: String, family: KeyFamily, mapSize: Int, keyWidth: Int) {
    require(mapSize >= 2, "mapSize must leave room for NaN and -0.0")
    require(keyWidth > 0, "keyWidth must be positive")

    def description: String =
      if (family == StrictDoubleKey) {
        s"${family.label}, map-size=$mapSize"
      } else {
        s"${family.label}, map-size=$mapSize, key-width=$keyWidth"
      }
  }

  private val ArraySmall = Shape("array-small", ArrayKey, mapSize = 4, keyWidth = 2)
  private val StructNarrow = Shape("struct-small", StructKey, mapSize = 4, keyWidth = 2)
  private val StructWide = Shape("struct-wide", StructKey, mapSize = 4, keyWidth = 8)

  private val Shapes = Seq(
    ArraySmall,
    Shape("array-large-map", ArrayKey, mapSize = 32, keyWidth = 2),
    Shape("array-wide", ArrayKey, mapSize = 4, keyWidth = 8),
    StructNarrow,
    Shape("struct-large-map", StructKey, mapSize = 32, keyWidth = 2),
    StructWide,
    Shape("double-small", StrictDoubleKey, mapSize = 4, keyWidth = 1),
    Shape("double-large-map", StrictDoubleKey, mapSize = 32, keyWidth = 1))

  private val FallbackCaseName = "Comet / Spark fallback"
  private val DispatcherCaseName = "Comet / MapSort dispatcher"

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmark("MapSort dispatcher: environment") {
      emitEnvironment()
    }

    if (!isSpark40Plus) {
      emit(s"SKIPPED: Spark ${spark.version} does not define or insert MapSort (requires 4.0+).")
      return
    }

    Mode match {
      case "first-action" =>
        runFirstAction()
        return
      case "steady" =>
      case other =>
        throw new IllegalArgumentException(
          s"invalid $ModeProperty=$other (expected steady or first-action)")
    }

    Shapes.foreach(runProjectionBenchmark)
    Shapes.foreach(runShuffleBenchmark)
  }

  private def runProjectionBenchmark(shape: Shape): Unit = {
    withCorpus(shape, ProjectionRows) {
      verifyMatchedPair(shape, "projection", () => mapSortProjection(VerificationRows))
      // Plan the dispatcher arm first and use distinct logical trees. Comet records planning
      // diagnostics in TreeNode tags; reusing (or first fallback-tagging) the same Catalyst tree
      // can otherwise make the second arm appear to have inherited the first arm's route.
      val dispatcher = prepareQuery(
        shape,
        dispatch = true,
        shuffle = false,
        mapSortProjection().queryExecution.logical)
      val fallback = prepareQuery(
        shape,
        dispatch = false,
        shuffle = false,
        mapSortProjection().queryExecution.logical)
      assertRoute(
        shape,
        "projection",
        dispatch = true,
        stripAQEPlan(dispatcher.queryExecution.executedPlan))
      assertRoute(
        shape,
        "projection",
        dispatch = false,
        stripAQEPlan(fallback.queryExecution.executedPlan))
      runBenchmark(s"MapSort projection -- ${shape.description}") {
        val benchmark = new Benchmark(
          s"MapSort projection -- ${shape.description}",
          ProjectionRows,
          output = output)
        addMatchedCases(benchmark, shape, shuffle = false, fallback, dispatcher)
        benchmark.run()
      }
    }
  }

  private def runShuffleBenchmark(shape: Shape): Unit = {
    withCorpus(shape, ShuffleRows) {
      verifyMatchedPair(shape, "shuffle", () => shuffleQuery(VerificationRows))
      val dispatcher = prepareQuery(
        shape,
        dispatch = true,
        shuffle = true,
        shuffleQuery().queryExecution.logical)
      val fallback = prepareQuery(
        shape,
        dispatch = false,
        shuffle = true,
        shuffleQuery().queryExecution.logical)
      assertRoute(
        shape,
        "shuffle",
        dispatch = true,
        stripAQEPlan(dispatcher.queryExecution.executedPlan))
      assertRoute(
        shape,
        "shuffle",
        dispatch = false,
        stripAQEPlan(fallback.queryExecution.executedPlan))
      runBenchmark(s"MapSort native shuffle -- ${shape.description}") {
        val benchmark = new Benchmark(
          s"MapSort native shuffle -- ${shape.description}",
          ShuffleRows,
          output = output)
        addMatchedCases(benchmark, shape, shuffle = true, fallback, dispatcher)
        benchmark.run()
      }
    }
  }

  /**
   * Builds the exact MapSort expression inserted by Spark's grouping optimizer, but returns only
   * that projection. The aggregate is a construction device and is never part of the returned
   * DataFrame or the timed execution.
   */
  private def mapSortProjection(maxRows: Int = Int.MaxValue): DataFrame = {
    val input = limitedInput(maxRows)
    val optimizedGrouping = input.groupBy(col("m")).count().queryExecution.optimizedPlan
    val mapSortProject = optimizedGrouping
      .collectFirst {
        case plan
            if plan.output.exists(_.name == "_groupingmapsort") &&
              plan.expressions.exists(containsMapSort) =>
          plan
      }
      .getOrElse {
        throw new IllegalStateException(
          "Spark did not insert the expected MapSort grouping projection:\n" +
            optimizedGrouping.treeString)
      }

    val projected = dataFrameOfRows(mapSortProject)
      .select(col("_groupingmapsort").as("sorted_m"))
    assertMapSortInOptimizedPlan(projected)
    projected
  }

  /**
   * Spark 4.0 moved the Dataset implementation and its `ofRows` factory to `sql.classic`, while
   * Spark 3.x keeps it in `sql`. Reflection across that packaging-only difference lets the common
   * source compile on every supported Spark line.
   */
  private def dataFrameOfRows(plan: LogicalPlan): DataFrame = {
    val companionClass =
      Seq("org.apache.spark.sql.classic.Dataset$", "org.apache.spark.sql.Dataset$").iterator
        .map(name => scala.util.Try(Class.forName(name)).toOption)
        .collectFirst { case Some(clazz) => clazz }
        .getOrElse(throw new IllegalStateException("could not locate Spark Dataset companion"))
    val module = companionClass.getField("MODULE$").get(null)
    val ofRows = companionClass.getMethods
      .find(method => method.getName == "ofRows" && method.getParameterCount == 2)
      .getOrElse(throw new IllegalStateException("could not locate Spark Dataset.ofRows"))
    ofRows.invoke(module, spark, plan).asInstanceOf[DataFrame]
  }

  private def shuffleQuery(maxRows: Int = Int.MaxValue): DataFrame = {
    val shuffled = limitedInput(maxRows).repartition(ShufflePartitions, col("m"))
    assertMapSortInOptimizedPlan(shuffled)
    shuffled
  }

  private def limitedInput(maxRows: Int): DataFrame = {
    val input = spark.table("parquetV1Table")
    if (maxRows == Int.MaxValue) input else input.where(col("id") < maxRows)
  }

  private def containsMapSort(
      expression: org.apache.spark.sql.catalyst.expressions.Expression): Boolean =
    expression.exists(_.prettyName == "mapsort")

  private def assertMapSortInOptimizedPlan(df: DataFrame): Unit = {
    val plan = df.queryExecution.optimizedPlan
    assert(
      plan.exists(_.expressions.exists(containsMapSort)),
      s"expected MapSort in optimized plan:\n${plan.treeString}")
  }

  /**
   * Executes both routes on a small prefix of the same Parquet corpus and checks results/plans.
   */
  private def verifyMatchedPair(shape: Shape, workload: String, query: () => DataFrame): Unit = {
    // See runProjectionBenchmark: route the dispatcher tree before adding any fallback tags.
    val dispatcher = captureRun(shape, dispatch = true, workload, query)
    val fallback = captureRun(shape, dispatch = false, workload, query)

    assert(
      fallback.rows.sameElements(dispatcher.rows),
      s"${shape.description} $workload routes produced different rows")
    val explain = new ExtendedExplainInfo()
    assert(
      !explain.getCodegenDispatchExpressions(fallback.plan).contains("mapsort"),
      s"MapSort was unexpectedly annotated as dispatched in $FallbackCaseName:\n" +
        fallback.plan.treeString)
    assert(
      explain.getCodegenDispatchExpressions(dispatcher.plan).contains("mapsort"),
      s"MapSort was not annotated as dispatched for ${shape.description} $workload:\n" +
        dispatcher.plan.treeString)

    workload match {
      case "projection" =>
        assert(
          dispatcher.firstNonComet.isEmpty,
          s"dispatcher projection was not fully Comet: ${dispatcher.firstNonComet}\n" +
            dispatcher.plan.treeString)
        assert(
          fallback.plan.exists(_.isInstanceOf[ProjectExec]),
          s"fallback route did not contain a Spark ProjectExec:\n${fallback.plan.treeString}")

      case "shuffle" =>
        assert(
          dispatcher.plan.exists(_.isInstanceOf[CometShuffleExchangeExec]),
          s"dispatcher route did not retain Comet native shuffle:\n${dispatcher.plan.treeString}")
        assert(
          !fallback.plan.exists(_.isInstanceOf[CometShuffleExchangeExec]) &&
            fallback.plan.exists(_.isInstanceOf[ShuffleExchangeExec]),
          s"fallback route did not use Spark shuffle exclusively:\n${fallback.plan.treeString}")

      case other => throw new IllegalArgumentException(s"unknown workload: $other")
    }

    val equality =
      if (workload == "shuffle") "equal rows and spark_partition_id assignments"
      else "equal results"
    emit(
      s"Verified ${shape.description} $workload on ${dispatcher.rows.length} rows: " +
        s"$equality; $FallbackCaseName used Spark; $DispatcherCaseName dispatched mapsort.")
    emit(s"  fallback executed plan: ${oneLine(fallback.plan.treeString)}")
    emit(s"  dispatcher executed plan: ${oneLine(dispatcher.plan.treeString)}")
    emit(
      "  dispatcher codegen expressions: " +
        new ExtendedExplainInfo().getCodegenDispatchExpressions(dispatcher.plan))
  }

  private case class CapturedRun(
      rows: Array[String],
      plan: org.apache.spark.sql.execution.SparkPlan,
      firstNonComet: Option[String])

  private def assertRoute(
      shape: Shape,
      workload: String,
      dispatch: Boolean,
      plan: org.apache.spark.sql.execution.SparkPlan): Unit = {
    val mapSortDispatched =
      new ExtendedExplainInfo().getCodegenDispatchExpressions(plan).contains("mapsort")
    assert(
      mapSortDispatched == dispatch,
      s"unexpected MapSort dispatch annotation for ${shape.description} $workload, " +
        s"dispatch=$dispatch:\n${plan.treeString}")
    (workload, dispatch) match {
      case ("projection", true) =>
        assert(
          findFirstNonCometOperator(plan).isEmpty,
          s"dispatcher projection was not fully Comet:\n${plan.treeString}")
      case ("projection", false) =>
        assert(
          plan.exists(_.isInstanceOf[ProjectExec]),
          s"fallback route did not contain a Spark ProjectExec:\n${plan.treeString}")
      case ("shuffle", true) =>
        assert(
          plan.exists(_.isInstanceOf[CometShuffleExchangeExec]),
          s"dispatcher route did not retain Comet native shuffle:\n${plan.treeString}")
      case ("shuffle", false) =>
        assert(
          !plan.exists(_.isInstanceOf[CometShuffleExchangeExec]) &&
            plan.exists(_.isInstanceOf[ShuffleExchangeExec]),
          s"fallback route did not use Spark shuffle exclusively:\n${plan.treeString}")
      case _ =>
        throw new IllegalArgumentException(s"unknown workload: $workload")
    }
  }

  private def captureRun(
      shape: Shape,
      dispatch: Boolean,
      workload: String,
      query: () => DataFrame): CapturedRun = {
    var result: CapturedRun = null
    withSQLConf(configs(shape, dispatch, workload == "shuffle"): _*) {
      val df = query()
      val checked =
        if (workload == "shuffle") {
          // Row equality alone cannot detect a different hash-partition assignment. Preserve the
          // entire row and append the partition id so the comparison validates both.
          df.select(col("*"), spark_partition_id().as("_partition_id"))
        } else {
          df
        }
      val rows = checked.collect().map(renderRow).sorted
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      result = CapturedRun(rows, plan, findFirstNonCometOperator(plan).map(_.nodeName))
    }
    result
  }

  private def renderRow(row: Row): String = row.toSeq.map(String.valueOf).mkString("|")

  private def oneLine(value: String): String =
    value.split("\\n").iterator.map(_.trim).mkString(" | ")

  /** Materializes physical planning outside Benchmark's timed closure. */
  private def prepareQuery(
      shape: Shape,
      dispatch: Boolean,
      shuffle: Boolean,
      logicalPlan: LogicalPlan): DataFrame = {
    var prepared: DataFrame = null
    withSQLConf(configs(shape, dispatch, shuffle): _*) {
      prepared = dataFrameOfRows(logicalPlan)
      prepared.queryExecution.executedPlan
    }
    prepared
  }

  private def runPreparedQuery(
      shape: Shape,
      dispatch: Boolean,
      shuffle: Boolean,
      df: DataFrame): Unit =
    withSQLConf(configs(shape, dispatch, shuffle): _*) {
      df.noop()
    }

  private def addMatchedCases(
      benchmark: Benchmark,
      shape: Shape,
      shuffle: Boolean,
      fallback: DataFrame,
      dispatcher: DataFrame): Unit = {
    def addFallback(): Unit = benchmark.addCase(FallbackCaseName) { _ =>
      runPreparedQuery(shape, dispatch = false, shuffle = shuffle, df = fallback)
    }
    def addDispatcher(): Unit = benchmark.addCase(DispatcherCaseName) { _ =>
      runPreparedQuery(shape, dispatch = true, shuffle = shuffle, df = dispatcher)
    }

    CaseOrder match {
      case "fallback-first" =>
        addFallback()
        addDispatcher()
      case "dispatcher-first" =>
        addDispatcher()
        addFallback()
      case other =>
        throw new IllegalArgumentException(
          s"invalid $CaseOrderProperty=$other (expected fallback-first or dispatcher-first)")
    }
  }

  private def configs(shape: Shape, dispatch: Boolean, shuffle: Boolean): Seq[(String, String)] =
    Seq(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString,
      CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key ->
        (shape.family == StrictDoubleKey).toString,
      CometConf.getExprAllowIncompatConfigKey("MapSort") -> "false",
      CometConf.COMET_SHUFFLE_ENABLED.key -> shuffle.toString,
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_NESTED_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> ShufflePartitions.toString)

  /**
   * Measures exactly one first action for one route/workload/schema. A valid first-action study
   * invokes this mode in a fresh JVM for every sample; the benchmark deliberately does not call a
   * second action or claim that the dispatcher counters prove how much Java compilation occurred.
   */
  private def runFirstAction(): Unit = {
    val shapeName = requiredProperty(ShapeProperty)
    val shape = Shapes
      .find(_.name == shapeName)
      .getOrElse(throw new IllegalArgumentException(
        s"invalid $ShapeProperty=$shapeName; expected one of ${Shapes.map(_.name).mkString(",")}"))
    val workload = requiredProperty(WorkloadProperty)
    require(
      workload == "projection" || workload == "shuffle",
      s"$WorkloadProperty must be projection or shuffle")
    val route = requiredProperty(RouteProperty)
    require(route == "fallback" || route == "dispatcher", s"$RouteProperty is invalid: $route")
    val dispatch = route == "dispatcher"
    val shuffle = workload == "shuffle"

    withCorpus(shape, FirstActionRows) {
      val logicalPlan =
        if (shuffle) shuffleQuery().queryExecution.logical
        else mapSortProjection().queryExecution.logical
      val df = prepareQuery(shape, dispatch, shuffle, logicalPlan)
      CometScalaUDFCodegen.resetStats()
      val elapsed =
        timeMillis(runPreparedQuery(shape, dispatch = dispatch, shuffle = shuffle, df = df))
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val stats = CometScalaUDFCodegen.stats()
      assertRoute(shape, workload, dispatch, plan)
      val repetition = sys.props.getOrElse(RepetitionProperty, "<unset>")
      emit(
        f"MAPSORT_FIRST_ACTION shape=${shape.name} route=$route workload=$workload " +
          f"repetition=$repetition rows=$FirstActionRows elapsed_ms=$elapsed%.1f " +
          s"dispatcher_compile_count=${stats.compileCount} " +
          s"dispatcher_cache_hit_count=${stats.cacheHitCount}")
      emit(s"  executed plan: ${oneLine(plan.treeString)}")
      emit(
        "This is first-action latency from one process. Dispatcher counters are routing/cache " +
          "observations, not a measurement of Java compiler time.")
    }
  }

  private def timeMillis(f: => Unit): Double = {
    val start = System.nanoTime()
    f
    (System.nanoTime() - start) / 1e6
  }

  private def withCorpus(shape: Shape, rows: Int)(f: => Unit): Unit = {
    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        spark
          .range(0L, rows.toLong, 1L, InputPartitions)
          .createOrReplaceTempView(tbl)
        prepareTable(dir, spark.sql(corpusQuery(shape)))
        f
      }
    }
  }

  private def corpusQuery(shape: Shape): String = {
    val entrySequence = s"sequence(${shape.mapSize - 1}, 0, -1)"
    val keys =
      s"transform($entrySequence, i -> ${shape.family.key("i", shape.keyWidth)})"
    val values =
      s"transform($entrySequence, i -> CAST(pmod(id * 17 + i, 2147483647) AS INT))"
    val map = s"map_from_arrays($keys, $values)"
    val lookupEntry = s"CAST(pmod(id, ${shape.mapSize}) AS INT)"
    val lookupKey = shape.family.key(lookupEntry, shape.keyWidth)

    s"""
       |SELECT
       |  id,
       |  CASE WHEN pmod(id, $NullMapEvery) = 0
       |    THEN CAST(NULL AS ${shape.family.mapType(shape.keyWidth)})
       |    ELSE $map
       |  END AS m,
       |  $lookupKey AS lookup_key
       |FROM $tbl
       |""".stripMargin
  }

  private def emitEnvironment(): Unit = {
    emit(s"Spark version: ${spark.version}")
    emit(
      s"Java version: ${System.getProperty("java.version")} " +
        s"(${System.getProperty("java.vm.name")})")
    emit(s"Scala version: ${scala.util.Properties.versionNumberString}")
    emit(s"spark.master: ${spark.conf.get("spark.master", "<unset>")}")
    emit(
      s"${CometConf.COMET_BATCH_SIZE.key}: " +
        CometConf.COMET_BATCH_SIZE.get(spark.sessionState.conf))
    emit(s"Projection rows: $ProjectionRows; shuffle rows: $ShuffleRows")
    emit(s"Input partitions: $InputPartitions; shuffle partitions: $ShufflePartitions")
    emit(s"Correctness/routing prefix rows: $VerificationRows; NULL map density: 1/$NullMapEvery")
    emit(s"Mode: $Mode; steady-state case order: $CaseOrder")
    emit("Shuffle mode: native; AQE: disabled; nested hash partitioning: enabled")
    emit(s"Only matched-arm difference: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}")
    emit(s"Pinned: ${CometConf.getExprAllowIncompatConfigKey("MapSort")}=false")
    emit("Projection note: Spark 4.0/4.1 do not insert MapSort for try_element_at. This suite")
    emit("  executes the optimizer-inserted grouping MapSort Project alone; no aggregate is run.")
    emit(s"Shapes: ${Shapes.map(_.description).mkString("; ")}")
  }

  /**
   * [[Benchmark]] tees its output; custom environment/first-use tables need the same behaviour.
   */
  private def emit(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes(StandardCharsets.UTF_8)))
  }

  private def intProperty(name: String, default: Int): Int = {
    val value = sys.props.get(name).map(_.toInt).getOrElse(default)
    require(value > 0, s"$name must be positive")
    value
  }

  private def requiredProperty(name: String): String =
    sys.props.getOrElse(name, throw new IllegalArgumentException(s"missing required -D$name"))
}
