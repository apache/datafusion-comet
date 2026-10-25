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

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.comet.CometIcebergWriteExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ExtendedExplainInfo}

/**
 * End-to-end AssertNotNull benchmark. Run with:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometAssertNotNullBenchmark
 * }}}
 * Add `-- --iceberg` for required-field Iceberg writes, `--rows=N` to change the default 1M rows,
 * or `--reverse-modes` for an independent run with the engine order reversed. The projection
 * cases include a shuffle and collection of every output field; the shuffle is a full-consumption
 * boundary that permits native assertions. These timings are not isolated assertion kernel costs.
 * Nullable inputs guard the assertion with IF, as writes of optional structs do. Invalid inputs
 * belong in correctness tests: timing an early failure does not measure the same amount of work.
 * Iceberg timings include the write and commit, with setup and output verification untimed.
 */
object CometAssertNotNullBenchmark extends CometBenchmarkBase {

  private val function = "benchmark_assert_not_null"
  private val source = "assert_not_null_source"
  private val assertionKey = CometConf.getExprEnabledConfigKey("AssertNotNull")

  private def configs(comet: Boolean): Seq[(String, String)] = Seq(
    CometConf.COMET_ENABLED.key -> comet.toString,
    CometConf.COMET_EXEC_ENABLED.key -> comet.toString,
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
    assertionKey -> "true",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.SHUFFLE_PARTITIONS.key -> "2")

  private def benchmark(title: String, rows: Long): Benchmark = new Benchmark(
    title,
    rows,
    minNumIters = 5,
    warmupTime = 2.seconds,
    minTime = 3.seconds,
    output = output)

  private def verifyRoute(plans: Seq[SparkPlan], comet: Boolean): Unit = {
    val info = new ExtendedExplainInfo()
    val native = plans.exists(info.getNativeExpressions(_).contains("assertnotnull"))
    val dispatched = plans.exists(info.getCodegenDispatchExpressions(_).contains("assertnotnull"))
    require(
      native == comet && !dispatched,
      s"Unexpected assertion route:\n${plans.mkString("\n")}")
    if (!comet) {
      require(collectAcross(plans) { case p if isComet(p) => p }.isEmpty)
      require(
        collectAcross(plans) {
          case p if p.expressions.exists(_.exists(_.isInstanceOf[AssertNotNull])) => p
        }.nonEmpty,
        s"Spark's assertion was optimized away:\n${plans.mkString("\n")}")
    }
  }

  // A commutative checksum consumes every field without assuming identical shuffle ordering.
  private def consume(df: DataFrame): (Long, Long) = {
    val result = df.collect()
    (result.length.toLong, result.iterator.map(_.hashCode().toLong).sum)
  }

  private def hasDictionary(path: File): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val files = new Path(path.toURI).getFileSystem(conf).listStatus(new Path(path.toURI))
    files.filter(_.getPath.getName.endsWith(".parquet")).exists { file =>
      val reader = ParquetFileReader.open(HadoopInputFile.fromPath(file.getPath, conf))
      try {
        reader.getFooter.getBlocks.asScala.exists(_.getColumns.asScala.exists { column =>
          column.getEncodings.contains(Encoding.RLE_DICTIONARY)
        })
      } finally reader.close()
    }
  }

  private def projections(rows: Long, modes: Seq[Boolean]): Unit = {
    val shapes = Seq(
      ("primitive", "id", false),
      ("dictionary string", "CAST(id % 8 AS STRING)", true),
      (
        "struct with nullable children",
        "named_struct('i', id, 's', IF(id % 8 = 0, NULL, 'x'))",
        false))
    for ((name, value, dictionary) <- shapes; nullPercent <- Seq(0, 12, 50)) {
      withTempPath { path =>
        withTempTable(source) {
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark
              .range(rows)
              .selectExpr(s"IF(id % 100 < $nullPercent, NULL, $value) AS c")
              .coalesce(1)
              .write
              .option("parquet.enable.dictionary", dictionary.toString)
              .parquet(path.getCanonicalPath)
          }
          val input = spark.read.parquet(path.getCanonicalPath)
          require(input.schema("c").nullable, "The assertion needs a nullable input schema")
          input.createOrReplaceTempView(source)
          if (dictionary) require(hasDictionary(path), "Expected dictionary-encoded Parquet")
          val checked =
            if (nullPercent == 0) s"$function(c)"
            else
              s"IF(c IS NULL, NULL, $function(c))"
          val bench = benchmark(s"AssertNotNull: $name, $nullPercent% parent NULL", rows)
          var expected: Option[(Long, Long)] = None
          modes.foreach { comet =>
            val label = if (comet) "Comet native" else "Spark"
            def run(verify: Boolean): (Long, Long) = withSQLConf(configs(comet): _*) {
              val df = spark.sql(s"SELECT $checked AS c FROM $source").repartition(2)
              val result = consume(df)
              if (verify) verifyRoute(Seq(df.queryExecution.executedPlan), comet)
              result
            }
            val result = run(verify = true)
            require(result._1 == rows)
            expected.foreach(value => require(value == result, s"$label: $result != $value"))
            expected = Some(result)
            bench.out.println(
              s"Verified $label: rows=${result._1}, checksum=${result._2}, " +
                s"native assertion=$comet, codegen dispatch=false")
            bench.addCase(label) { _ => require(run(verify = false) == result) }
          }
          bench.run()
        }
      }
    }
  }

  private def icebergWrites(rows: Long, modes: Seq[Boolean]): Unit = {
    require(icebergOnClasspath, "--iceberg requires an Iceberg-enabled Spark profile")
    withTempPath { warehouse =>
      configureIcebergHadoopCatalog(warehouse)
      val target = s"$defaultIcebergCatalog.db.assert_not_null_target"
      val shapes = Seq(
        ("required primitive", "BIGINT NOT NULL", "id"),
        (
          "required struct",
          "STRUCT<i: BIGINT, s: STRING> NOT NULL",
          "named_struct('i', id, 's', IF(id % 8 = 0, NULL, CAST(id % 8 AS STRING)))"))
      shapes.foreach { case (name, dataType, value) =>
        withTempPath { path =>
          withTempTable(source) {
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              spark
                .range(rows)
                .selectExpr(s"$value AS c")
                .coalesce(1)
                .write
                .parquet(path.getCanonicalPath)
            }
            val input = spark.read.parquet(path.getCanonicalPath)
            require(input.schema("c").nullable)
            input.createOrReplaceTempView(source)
            val expected = consume(input)
            def reset(): Unit = {
              spark.sql(s"DROP TABLE IF EXISTS $target")
              spark.sql(s"""CREATE TABLE $target (c $dataType) USING iceberg
                |TBLPROPERTIES ('write.distribution-mode'='none',
                |'write.parquet.compression-codec'='snappy')""".stripMargin)
            }
            def verifyOutput(): Unit = withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              require(consume(spark.table(target)) == expected, "Required write changed output")
            }
            val bench = benchmark(s"AssertNotNull: Iceberg $name", rows)
            try {
              modes.foreach { comet =>
                val conf = configs(comet) ++ Seq(
                  CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> comet.toString,
                  CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> comet.toString)
                def insert(): Unit = withSQLConf(conf: _*) {
                  spark.sql(s"INSERT INTO $target SELECT * FROM $source")
                }
                reset()
                val plans = capturePlans(insert())
                verifyRoute(plans, comet)
                require(
                  collectAcross(plans) { case w: CometIcebergWriteExec => w }.nonEmpty == comet,
                  s"Unexpected writer route:\n${plans.mkString("\n")}")
                verifyOutput()
                val label = if (comet) "Comet native assertion + writer" else "Spark"
                bench.out.println(s"Verified $label: rows=${expected._1}, " +
                  s"checksum=${expected._2}, native assertion=$comet, native writer=$comet")
                bench.addTimerCase(label) { timer =>
                  reset()
                  timer.startTiming()
                  insert()
                  timer.stopTiming()
                  verifyOutput()
                }
              }
              bench.run()
            } finally spark.sql(s"DROP TABLE IF EXISTS $target")
          }
        }
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = mainArgs
      .find(_.startsWith("--rows="))
      .map(_.stripPrefix("--rows=").toLong)
      .getOrElse(1024L * 1024)
    require(rows > 0)
    val modes = if (mainArgs.contains("--reverse-modes")) Seq(true, false) else Seq(false, true)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction(
      function,
      (children: Seq[Expression]) => AssertNotNull(children.head, Seq("c")),
      "built-in")
    try {
      projections(rows, modes)
      if (mainArgs.contains("--iceberg")) icebergWrites(rows, modes)
    } finally spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS $function")
  }
}
