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

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.collection.mutable

import org.apache.parquet.crypto.DecryptionPropertiesFactory
import org.apache.parquet.crypto.keytools.{KeyToolkit, PropertiesDrivenCryptoFactory}
import org.apache.parquet.crypto.keytools.mocks.InMemoryKMS
import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.comet.CometPlanChecker
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, LeafExecNode, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions

trait CometBenchmarkBase
    extends SqlBasedBenchmark
    with AdaptiveSparkPlanHelper
    with CometPlanChecker {
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      // Use Comet's shuffle manager so operators that require Comet shuffle can
      // run natively, notably aggregates planned as ObjectHashAggregate such as
      // percentile and approx_percentile. `spark.shuffle.manager` is static and
      // must be set before the context starts. CometShuffleManager falls back to
      // Spark's shuffle when Comet is disabled, so the Spark baseline cases are
      // unaffected.
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    // Benchmarks use invalid input values that should produce NULL, not exceptions
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")

    sparkSession
  }

  def runCometBenchmark(args: Array[String]): Unit

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runCometBenchmark(mainArgs)
  }

  protected val tbl = "comet_table"

  protected def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f
    finally tableNames.foreach(spark.catalog.dropTempView)
  }

  protected def runBenchmarkWithTable(
      benchmarkName: String,
      values: Int,
      useDictionary: Boolean = false)(f: Int => Any): Unit = {
    withTempTable(tbl) {
      import spark.implicits._
      // Derive the values from the row id with a pure function rather than a random number
      // generator, so that results are comparable across runs. Seeding a driver-side `Random`
      // would not work here: the closure runs per row on the executor.
      spark
        .range(values)
        .map(i =>
          if (useDictionary) CometBenchmarkBase.mix64(i) % 5 else CometBenchmarkBase.mix64(i))
        .createOrReplaceTempView(tbl)
      runBenchmark(benchmarkName)(f(values))
    }
  }

  /**
   * Runs an expression benchmark with standard cases: Spark, Comet. This provides a consistent
   * benchmark structure for expression evaluation.
   *
   * @param name
   *   Benchmark name
   * @param cardinality
   *   Number of rows being processed
   * @param query
   *   SQL query to benchmark
   * @param extraCometConfigs
   *   Additional configurations to apply for the Comet case (optional)
   */
  final def runExpressionBenchmark(
      name: String,
      cardinality: Long,
      query: String,
      extraCometConfigs: Map[String, String] = Map.empty): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    // Constant folding is excluded so that expressions over literal arguments are still evaluated
    // per row. It must be excluded for both arms: if only Comet excludes it, Spark folds the
    // expression away and does no per-row work, and the comparison is meaningless.
    val noConstantFolding =
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRulesWith(ConstantFolding.ruleName)

    val sparkConfigs = Seq(noConstantFolding, CometConf.COMET_ENABLED.key -> "false")

    val cometConfigs = Seq(
      noConstantFolding,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") ++ extraCometConfigs

    // Derive the scan-plus-row-conversion floor, and check that the benchmarked expression
    // survives optimization into the Spark baseline plan. The query is not executed: before
    // execution `stripAQEPlan` yields the initial physical plan, which already carries the
    // projections this check inspects.
    var baselineQuery: Option[String] = None
    withSQLConf(sparkConfigs: _*) {
      val df = spark.sql(query)
      baselineQuery = deriveBaselineQuery(df)
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      if (isTrivialPlan(plan)) {
        warn(
          benchmark,
          """WARNING: the Spark baseline plan does no work beyond scanning and projecting
            |columns, so this case is not measuring an expression. The expression was most
            |likely removed by the optimizer.
            |Spark plan:""".stripMargin + "\n" + plan.treeString)
      }
      val rowInvariant = rowInvariantProjections(plan)
      if (rowInvariant.nonEmpty) {
        warn(
          benchmark,
          s"""WARNING: these projections reference no input column, so their value is the same
             |for every row: ${rowInvariant.mkString(", ")}
             |An engine may evaluate them once per batch and broadcast the result rather than
             |per row, so the two arms are not necessarily doing comparable work. Benchmark the
             |expression over a column instead.""".stripMargin)
      }
    }

    // Check that the plan is fully Comet native before running the benchmark. Unlike the check
    // above this one does execute the query, because AQE can re-plan a join after the first
    // stage completes and the Comet operators are what we are checking for.
    withSQLConf(cometConfigs: _*) {
      val df = spark.sql(query)
      df.noop()
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      findFirstNonCometOperator(plan).foreach { op =>
        warn(
          benchmark,
          s"""WARNING: the Comet plan is NOT fully Comet native, so the Comet case below is
             |partly or wholly measuring Spark.
             |First non-Comet operator: ${op.nodeName}
             |Comet plan:""".stripMargin + "\n" + plan.treeString)
      }
    }

    val rows = Seq("Spark" -> sparkConfigs, "Comet" -> cometConfigs).map { case (arm, configs) =>
      val total = measure(benchmark, cardinality, configs)(spark.sql(query).noop())
      val baseline = baselineQuery.map { baseline =>
        baselineResults.getOrElseUpdate(
          (baseline, configs),
          measure(benchmark, cardinality, configs)(spark.sql(baseline).noop()))
      }
      BenchmarkRow(arm, total, baseline)
    }

    // `Benchmark.run` is deliberately not used: it renders from its own case list, so a cached
    // baseline cannot appear as a row and there is nowhere to put a derived column.
    benchmark.out.println(BenchmarkTable.render(name, cardinality, rows, baselineQuery))
  }

  /**
   * The scan-plus-row-conversion floor is the same for every expression measured over the same
   * columns of the same table, so it is measured once per (query, config) pair rather than once
   * per benchmark. Without this, adding a baseline would roughly double total benchmark runtime.
   */
  private val baselineResults =
    mutable.Map.empty[(String, Seq[(String, String)]), Benchmark.Result]

  /**
   * Runs `body` under `configs` and returns Spark's measurement of it, reusing `Benchmark`'s
   * warmup, iteration and standard deviation logic.
   */
  private def measure(benchmark: Benchmark, cardinality: Long, configs: Seq[(String, String)])(
      body: => Unit): Benchmark.Result = {
    // `withSQLConf` returns Unit on Spark 3.5, so the result has to come back through a var.
    var result: Benchmark.Result = null
    withSQLConf(configs: _*) {
      result = benchmark.measure(cardinality, 0) { timer =>
        timer.startTiming()
        body
        timer.stopTiming()
      }
    }
    result
  }

  /**
   * The query measuring the floor for `df`: the same scan, projecting the columns it reads and
   * doing no expression work. The optimized plan's leaf output is already column-pruned, so it is
   * exactly what the real query scans.
   *
   * Defined only when the plan reads a single table. A join has two leaves and no single
   * meaningful floor, so those benchmarks report totals instead.
   */
  private def deriveBaselineQuery(df: DataFrame): Option[String] = {
    val leaves = df.queryExecution.optimizedPlan.collectLeaves
    val aliases = df.queryExecution.analyzed.collect { case s: SubqueryAlias =>
      s.identifier.name
    }.distinct
    val columns = leaves.headOption.map(_.output.map(name => s"`${name.name}`")).getOrElse(Nil)
    if (leaves.size == 1 && aliases.size == 1 && columns.nonEmpty) {
      Some(s"SELECT ${columns.mkString(", ")} FROM ${aliases.head}")
    } else {
      None
    }
  }

  /**
   * Returns the value of `spark.sql.optimizer.excludedRules` with `rule` appended, so that
   * benchmark-specific exclusions do not clobber exclusions already configured by the caller.
   */
  private def excludedRulesWith(rule: String): String =
    (Utils.stringToSeq(spark.conf.get(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, "")) :+ rule).distinct
      .mkString(",")

  /**
   * Returns true if the plan does nothing but scan and emit columns, which means the benchmarked
   * expression was removed by the optimizer and the case measures nothing.
   *
   * Any node other than a scan, projection or row conversion counts as real work, so joins,
   * aggregates and filters are never reported as trivial even when they project bare attributes.
   */
  private def isTrivialPlan(plan: SparkPlan): Boolean = !plan.exists {
    case p: ProjectExec => p.projectList.exists(expr => !isTrivialExpr(expr))
    case _: ColumnarToRowExec | _: WholeStageCodegenExec | _: InputAdapter | _: LeafExecNode =>
      false
    case _ => true
  }

  /** A projection that copies a column or emits a constant, doing no per-row work. */
  private def isTrivialExpr(expr: Expression): Boolean = expr match {
    case _: AttributeReference | _: Literal => true
    case Alias(_: AttributeReference | _: Literal, _) => true
    case _ => false
  }

  /**
   * Projections whose value is the same for every row, because they reference no input column.
   *
   * An engine is free to evaluate these once per batch and broadcast the result, and engines
   * differ on whether they do. Comet's native `space` takes a `ColumnarValue::Scalar` and builds
   * one string per batch, while Spark's `StringSpace` calls `UTF8String.blankString` per row. The
   * two plans look identical, so nothing else here can tell them apart.
   *
   * Nondeterministic expressions are excluded: `rand()` also references no column but genuinely
   * evaluates per row.
   */
  private def rowInvariantProjections(plan: SparkPlan): Seq[Expression] =
    plan
      .collect { case p: ProjectExec => p }
      .flatMap(_.projectList)
      .filter(expr => expr.references.isEmpty && expr.deterministic && !isTrivialExpr(expr))

  /**
   * Writes a warning to the benchmark results file as well as the console. `Benchmark.out` tees
   * the two, and writing through it keeps the warning ordered against the results table that
   * `Benchmark.run` writes to the same stream.
   */
  private def warn(benchmark: Benchmark, message: String): Unit = {
    val border = "=" * 80
    benchmark.out.println(s"\n$border\n$message\n$border")
  }

  protected def addParquetScanCases(
      benchmark: Benchmark,
      query: String,
      caseSuffix: String = "",
      extraConf: Map[String, String] = Map.empty): Unit = {
    val suffix = if (caseSuffix.nonEmpty) s" ($caseSuffix)" else ""

    benchmark.addCase(s"SQL Parquet - Spark$suffix") { _ =>
      withSQLConf(extraConf.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase(s"SQL Parquet - Comet$suffix") { _ =>
      withSQLConf(
        (extraConf ++ Map(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true")).toSeq: _*) {
        spark.sql(query).noop()
      }
    }
  }

  protected def prepareTable(dir: File, df: DataFrame, partition: Option[String] = None): Unit = {
    val testDf = if (partition.isDefined) {
      df.write.partitionBy(partition.get)
    } else {
      df.write
    }

    saveAsParquetV1Table(testDf, dir.getCanonicalPath + "/parquetV1")
  }

  protected def saveAsParquetV1Table(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetV1Table")
  }

  protected def prepareEncryptedTable(
      dir: File,
      df: DataFrame,
      partition: Option[String] = None): Unit = {
    val testDf = if (partition.isDefined) {
      df.write.partitionBy(partition.get)
    } else {
      df.write
    }

    saveAsEncryptedParquetV1Table(testDf, dir.getCanonicalPath + "/parquetV1")
  }

  protected def prepareIcebergTable(
      dir: File,
      df: DataFrame,
      tableName: String = "icebergTable",
      partition: Option[String] = None): Unit = {
    val warehouseDir = new File(dir, "iceberg-warehouse")

    // Configure Hadoop catalog (same pattern as CometIcebergNativeSuite)
    spark.conf.set("spark.sql.catalog.benchmark_cat", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.benchmark_cat.type", "hadoop")
    spark.conf.set("spark.sql.catalog.benchmark_cat.warehouse", warehouseDir.getAbsolutePath)

    val fullTableName = s"benchmark_cat.db.$tableName"

    // Drop table if exists
    spark.sql(s"DROP TABLE IF EXISTS $fullTableName")

    // Create a temp view from the DataFrame
    df.createOrReplaceTempView("temp_df_for_iceberg")

    // Create Iceberg table from temp view
    val partitionClause = partition.map(p => s"PARTITIONED BY ($p)").getOrElse("")
    spark.sql(s"""
      CREATE TABLE $fullTableName
      USING iceberg
      TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec' = 'snappy')
      $partitionClause
      AS SELECT * FROM temp_df_for_iceberg
    """)

    // Create temp view for benchmarking
    spark.table(fullTableName).createOrReplaceTempView(tableName)

    spark.catalog.dropTempView("temp_df_for_iceberg")
  }

  protected def saveAsEncryptedParquetV1Table(df: DataFrameWriter[Row], dir: String): Unit = {
    val encoder = Base64.getEncoder
    val footerKey =
      encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
    val key1 = encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8))
    val cryptoFactoryClass =
      "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"
    withSQLConf(
      DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
      KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
        "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
      InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
        s"footerKey: ${footerKey}, key1: ${key1}") {
      df.mode("overwrite")
        .option("compression", "snappy")
        .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: id")
        .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
        .parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("parquetV1Table")
    }
  }

  protected def makeDecimalDataFrame(
      values: Int,
      decimal: DecimalType,
      useDictionary: Boolean): DataFrame = {
    import spark.implicits._

    val div = if (useDictionary) 5 else values
    spark
      .range(values)
      .map(_ % div)
      .select((($"value" - 500) / 100.0) cast decimal as Symbol("dec"))
  }
}

object CometBenchmarkBase {

  /**
   * SplitMix64 finalizer, used to turn a row id into a well distributed pseudo-random value.
   *
   * This lives in the companion object rather than the trait because referencing a trait method
   * from a Dataset closure captures `this`, which is not serializable.
   */
  def mix64(value: Long): Long = {
    var z = value + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)
  }
}
