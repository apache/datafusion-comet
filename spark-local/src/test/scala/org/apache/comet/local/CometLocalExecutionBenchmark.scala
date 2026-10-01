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

package org.apache.comet.local

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession, TPCHTables}
import org.apache.spark.sql.benchmark.TPCDSSchemaHelper
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/** Manual benchmark; run with dev/bench-local-execution.py, never as a CI timing test. */
object CometLocalExecutionBenchmark {
  private val enabled = CometConf.COMET_EXEC_LOCAL_ENABLED.key

  def main(args: Array[String]): Unit = {
    require(args.length == 6, "mode data-directory output-directory rows repetitions schema-mode")
    val Array(mode, dataArg, outputArg, rowsArg, repetitionsArg, schemaMode) = args
    require(Set("infer", "explicit").contains(schemaMode))
    require(Set("prepare", "coverage", "spark", "comet", "local").contains(mode))
    val data = Paths.get(dataArg)
    val output = Paths.get(outputArg)
    Files.createDirectories(output)
    val rows = rowsArg.toLong
    val repetitions = repetitionsArg.toInt
    require(rows >= 10000 && repetitions >= 3)
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName(s"CometLocalBenchmark-$mode")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[CometSparkSessionExtensions].getName)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "512m")
      .config("spark.comet.exec.memoryPool", "fair_unified")
      .config("spark.comet.exec.local.memoryLimit", "512m")
      .config("spark.comet.batchSize", "8192")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .config("spark.comet.shuffle.enabled", (mode == "comet").toString)
      .config("spark.comet.enabled", (mode == "comet" || mode == "local").toString)
      .config("spark.comet.exec.enabled", "true")
      .config(enabled, (mode == "local").toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      mode match {
        case "prepare" => prepare(spark, data, rows)
        case "coverage" => coverage(spark, data, output)
        case _ => measure(spark, mode, data, output, repetitions, schemaMode)
      }
    } finally spark.stop()
  }

  private def prepare(spark: SparkSession, data: Path, rows: Long): Unit = {
    require(!Files.exists(data), "Use a new data directory; benchmark never overwrites input")
    spark
      .range(0, rows, 1, 8)
      .selectExpr(
        "id",
        "id % 4096 AS k",
        "CAST(id / 10 AS DECIMAL(18, 2)) AS amount",
        "CASE WHEN id % 17 = 0 THEN NULL ELSE CAST(id AS STRING) END AS text")
      .write
      .parquet(data.resolve("fact").toString)
    spark
      .range(rows / 4, rows / 4 + rows / 100, 1, 4)
      .selectExpr("id AS rid", "id * 3 AS value")
      .write
      .parquet(data.resolve("dimension").toString)
  }

  private def queries(
      spark: SparkSession,
      data: Path,
      schemaMode: String): Seq[(String, () => DataFrame)] = {
    // These schemas describe only the synthetic fixtures created by prepare.
    def read(name: String, schema: StructType): DataFrame = {
      val reader = spark.read
      if (schemaMode == "explicit") reader.schema(schema)
      reader.parquet(data.resolve(name).toString)
    }
    def fact = read(
      "fact",
      new StructType()
        .add("id", LongType)
        .add("k", LongType)
        .add("amount", DecimalType(18, 2))
        .add("text", StringType))
    def dimension =
      read("dimension", new StructType().add("rid", LongType).add("value", LongType))
    Seq(
      "scan-filter-project" -> (() =>
        fact
          .filter("id % 100 = 0")
          .selectExpr("id + 1 AS value", "amount", "text")),
      "grouped-count-min-max" -> (() =>
        fact
          .groupBy("k")
          .agg(
            org.apache.spark.sql.functions.count("text"),
            org.apache.spark.sql.functions.min("amount"),
            org.apache.spark.sql.functions.max("id"))),
      "partitioned-join" -> (() => {
        val left = fact.alias("f")
        val right = dimension
          .hint("SHUFFLE_HASH")
          .alias("d")
        left.join(right, left("id") === right("rid")).selectExpr("f.id", "d.value")
      }),
      "top-k" -> (() =>
        fact
          .selectExpr("id", "(id * 37) % 1000003 AS rank")
          .orderBy("rank", "id")
          .limit(1000)),
      "full-sort" -> (() =>
        fact
          .selectExpr("id", "(id * 37) % 1000003 AS rank")
          .orderBy("rank", "id")))
  }

  private def measure(
      spark: SparkSession,
      mode: String,
      data: Path,
      output: Path,
      repetitions: Int,
      schemaMode: String): Unit = {
    val cpu = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    val writer = Files.newBufferedWriter(output.resolve(s"$mode.csv"), UTF_8)
    writer.write(
      "query,iteration,planning_ms,execution_ms,total_ms,cpu_ms,rows,sha256,local_nodes,comet_nodes,dataframe_ms,physical_planning_ms\n")
    try {
      val cases = queries(spark, data, schemaMode)
      for (iteration <- -2 until repetitions;
        (name, make) <- cases.drop(Math.floorMod(iteration, cases.size)) ++
          cases.take(Math.floorMod(iteration, cases.size))) {
        Files.write(output.resolve(s"$mode.phase"), s"$name,$iteration".getBytes(UTF_8))
        val start = System.nanoTime()
        val cpuStart = cpu.getProcessCpuTime
        val query = make()
        val constructed = System.nanoTime()
        val plan = query.queryExecution.executedPlan
        val local = plan.collect { case p: CometLocalExec => p }.size
        val comet = plan.collect { case p if p.nodeName.startsWith("Comet") => p }.size
        require(
          (mode == "local" && local == 1) || (mode != "local" && local == 0),
          s"Unexpected execution path: $mode/$name\n$plan")
        if (mode == "comet") require(comet > 0, s"Comet fell back completely: $name")
        val planned = System.nanoTime()
        val result = query.collect()
        val end = System.nanoTime()
        val cpuEnd = cpu.getProcessCpuTime
        Files.write(output.resolve(s"$mode.phase"), "idle".getBytes(UTF_8))
        // Outside the timing interval. Sorted cases validate order; others validate a multiset.
        val strings = result.map(_.toString)
        val canonical = if (name == "top-k" || name == "full-sort") strings else strings.sorted
        val digest = java.security.MessageDigest.getInstance("SHA-256")
        canonical.foreach { row => digest.update(row.getBytes(UTF_8)); digest.update(10.toByte) }
        val hash = digest.digest().map(b => f"${b & 0xff}%02x").mkString
        def ms(nanos: Long): Double = nanos.toDouble / 1000000.0
        writer.write(
          s"$name,$iteration,${ms(planned - start)},${ms(end - planned)}," +
            s"${ms(end - start)},${ms(cpuEnd - cpuStart)},${result.length},$hash,$local,$comet," +
            s"${ms(constructed - start)},${ms(planned - constructed)}\n")
        writer.flush()
        if (iteration == 0)
          Files.write(output.resolve(s"$mode-$name.plan.txt"), plan.toString.getBytes(UTF_8))
        if (mode == "local") require(new NativeLocal().activeQueries() == 0)
      }
      // An idle sample after results become unreachable; no explicit GC is requested.
      Files.write(output.resolve(s"$mode.phase"), "retained".getBytes(UTF_8))
      Thread.sleep(1000)
    } finally writer.close()
  }

  private def coverage(spark: SparkSession, data: Path, output: Path): Unit = {
    // One-row Parquet fixtures preserve scan nodes. This checks admission, not TPC results.
    val tpch = new TPCHTables(spark.sqlContext, "unused", "1").tables
      .map(t => t.name -> t.schema)
      .toMap
    val schemas = Seq("tpch" -> tpch, "tpcds" -> TPCDSSchemaHelper.getTableColumns)
    val writer = Files.newBufferedWriter(output.resolve("coverage.csv"), UTF_8)
    writer.write("suite,query,status,root\n")
    try
      schemas.foreach { case (suite, tables) =>
        tables.toSeq.sortBy(_._1).foreach { case (name, declaredSchema) =>
          // Parquet stores CHAR/VARCHAR physically as strings (as do generated TPC files).
          val schema = StructType(declaredSchema.fields.map { field =>
            field.dataType match {
              case _: CharType | _: VarcharType => field.copy(dataType = StringType)
              case _ => field
            }
          })
          def sample(t: DataType): Any = t match {
            case StringType => "seed"
            case LongType => 1L
            case IntegerType => 1
            case DoubleType => 1.0
            case FloatType => 1.0f
            case _: DecimalType => new java.math.BigDecimal("1.00")
            case DateType => java.sql.Date.valueOf("2000-01-01")
            case other => throw new IllegalArgumentException(s"Unsupported fixture type $other")
          }
          val path = data.resolve(s"coverage-$suite-$name")
          val row = Row.fromSeq(schema.fields.toSeq.map(f => sample(f.dataType)))
          spark
            .createDataFrame(spark.sparkContext.parallelize(Seq(row), 1), schema)
            .write
            .parquet(path.toString)
          spark.read.schema(schema).parquet(path.toString).createOrReplaceTempView(name)
        }
        spark.conf.set("spark.comet.enabled", "true")
        spark.conf.set(enabled, "true")
        val directory = Paths.get("benchmarks", "tpc", "queries", suite)
        val stream = Files.list(directory)
        val files =
          try
            stream
              .iterator()
              .asScala
              .filter(_.toString.endsWith(".sql"))
              .toSeq
              .sortBy(_.getFileName.toString)
          finally stream.close()
        files.foreach { file =>
          val name = file.getFileName.toString
          try {
            val sql = new String(Files.readAllBytes(file), UTF_8)
            // TPC-H q15 is the repository's create-view/select/drop-view script.
            val statements = sql.split(";").map(_.trim).filter(_.nonEmpty)
            statements.zipWithIndex.foreach { case (statement, index) =>
              if (suite == "tpch" && name == "q15.sql" && index != 1) {
                spark.sql(statement.replace("create view", "create temporary view")).collect()
              } else {
                val queryName = if (suite == "tpcds" && statements.length > 1) {
                  s"$name-${index + 1}"
                } else name
                val plan = spark.sql(statement).queryExecution.executedPlan
                val local = plan.collectWithSubqueries { case p: CometLocalExec => p }.size
                val status = if (local == 1) "admitted" else "fallback"
                writer.write(s"$suite,$queryName,$status,${plan.nodeName}\n")
                Files.write(
                  output.resolve(s"coverage-$suite-$queryName.plan.txt"),
                  plan.toString.getBytes(UTF_8))
              }
            }
          } catch {
            case error: Exception =>
              writer.write(s"$suite,$name,planning-error,${error.getClass.getSimpleName}\n")
              Files.write(
                output.resolve(s"coverage-$suite-$name.error.txt"),
                error.toString.getBytes(UTF_8))
          }
          writer.flush()
        }
        tables.keys.foreach(spark.catalog.dropTempView)
        spark.conf.set(enabled, "false")
        spark.conf.set("spark.comet.enabled", "false")
      }
    finally writer.close()
  }
}
