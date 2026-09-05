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
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.CometRowViewWriteFilesExec
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark for `spark.comet.exec.write.rowView.enabled`, which replaces Spark's `WriteFilesExec`
 * with a Comet node that drives Spark's own `OutputWriter` from Arrow batches.
 *
 * Three arms per case:
 *   - Spark's own vectorized read and write
 *   - Comet scan plus `CometColumnarToRowExec`, which materializes an `UnsafeRow` per row
 *   - Comet scan plus `CometRowViewWriteFilesExec`, which materializes none
 *
 * Every case is run both unpartitioned and partitioned, because that is the comparison this
 * benchmark exists to make. An unpartitioned write pays for one `UnsafeProjection` per row, in
 * the columnar-to-row transition. A partitioned write pays for a second one inside
 * `BaseDynamicPartitionDataWriter.writeRecord`, which projects the row again purely to strip the
 * partition columns, so removing the transition alone would not help it. The row-view node
 * removes both.
 *
 * The whole `scan -> write -> file` pipeline is timed, so the projections are only part of what
 * is measured; the point is how much of a real write they cost. Cases run at two compression
 * settings because parquet-mr encoding is what that cost has to be measured against, and a flat
 * schema is included as a noise floor: the rule declines it, so both Comet arms run the identical
 * plan there.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometWriteRowViewBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometWriteRowViewBenchmark-**results.txt".
 */
object CometWriteRowViewBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometWriteRowViewBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      // Required: `isCometLoaded` disables Comet entirely when `spark.comet.shuffle.enabled`
      // (default true) is set without Comet's shuffle manager, which would silently turn every
      // Comet case below into another Spark run. `spark.shuffle.manager` is static and must be
      // set before the context starts. CometShuffleManager falls back to Spark's shuffle when
      // Comet is disabled, so the Spark baseline case is unaffected.
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    // Comet's scan rejects tinyint/smallint unless this check is off, which would drop the
    // fixed-width case back to Spark's scan in every arm.
    sparkSession.conf.set(CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key, "false")

    sparkSession
  }

  private val sparkConfigs = Seq(CometConf.COMET_ENABLED.key -> "false")

  private val unsafeRowConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_EXEC_WRITE_ROW_VIEW_ENABLED.key -> "false")

  private val rowViewConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_EXEC_WRITE_ROW_VIEW_ENABLED.key -> "true")

  private val diagnostics = sys.env.contains("COMET_BENCH_DIAG")

  private def addWriteCases(
      benchmark: Benchmark,
      outputDir: File,
      codec: String,
      partitioned: Boolean): Unit = {

    def write(target: String, configs: Seq[(String, String)]): Unit =
      withSQLConf(configs :+ (SQLConf.PARQUET_COMPRESSION.key -> codec): _*) {
        val writer = spark.sql("SELECT * FROM parquetV1Table").write.mode("overwrite")
        val partitionedWriter = if (partitioned) writer.partitionBy("part") else writer
        partitionedWriter.parquet(new File(outputDir, target).getCanonicalPath)
      }

    // The arms below differ only in which write node feeds parquet-mr, so the comparison is
    // meaningless unless each arm actually plans the node it names. Run every arm once up front
    // and warn loudly into the results file when it does not.
    def verify(
        name: String,
        target: String,
        configs: Seq[(String, String)],
        expected: String): Unit = {
      val (writeNode, tree) = captureWriteNode(write(target, configs))
      // scalastyle:off println
      println(s"  [plan check] $name -> ${writeNode.getOrElse("<no write node in plan>")}")
      if (diagnostics) println(tree.getOrElse("<no plan>"))
      // scalastyle:on println
      if (!writeNode.contains(expected)) {
        val border = "=" * 80
        benchmark.out.println(s"""
             |$border
             |WARNING: the "$name" case did not plan $expected but
             |${writeNode.getOrElse("no write node at all")}, so it is not measuring what its
             |name says. Treat this row as invalid.
             |$border""".stripMargin)
      }
    }

    benchmark.addCase("Spark") { _ =>
      write("spark", sparkConfigs)
    }

    benchmark.addCase("Comet write via UnsafeRow") { _ =>
      write("comet-unsaferow", unsafeRowConfigs)
    }

    benchmark.addCase("Comet write via row view") { _ =>
      write("comet-rowview", rowViewConfigs)
    }

    verify("Spark", "spark", sparkConfigs, "WriteFiles")
    verify("Comet write via UnsafeRow", "comet-unsaferow", unsafeRowConfigs, "WriteFiles")
    // The rule declines a schema whose data columns are all flat, so the row-view arm is expected
    // to plan the ordinary write node there. Deriving the expectation from the schema also asserts
    // the gate, and makes the flat cases an explicit noise floor rather than a silent no-op.
    verify(
      "Comet write via row view",
      "comet-rowview",
      rowViewConfigs,
      if (expectRowView(partitioned)) "CometRowViewWriteFiles" else "WriteFiles")
  }

  /**
   * Mirrors the gate in `EliminateRedundantTransitions.columnarChildForWrite`: a partitioned
   * write always qualifies, because it removes the writer's own projection too, while an
   * unpartitioned one needs a complex data column to be worth it.
   */
  private def expectRowView(partitioned: Boolean): Boolean = partitioned || hasComplexDataColumn()

  /** Whether any data column is a struct, array or map. */
  private def hasComplexDataColumn(): Boolean =
    spark
      .table("parquetV1Table")
      .schema
      .fields
      .filterNot(_.name == "part")
      .exists(f =>
        f.dataType.typeName match {
          case "struct" | "array" | "map" => true
          case _ => false
        })

  /** Names the write node in the executed plan of a write. */
  private def captureWriteNode(writeOp: => Unit): (Option[String], Option[String]) = {
    @volatile var captured: Option[QueryExecution] = None
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured = Some(qe)
      override def onFailure(funcName: String, qe: QueryExecution, e: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      writeOp
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
      while (captured.isEmpty && System.nanoTime() < deadline) Thread.sleep(50)
      val names = captured.map { qe =>
        def flatten(p: SparkPlan): Seq[SparkPlan] = p match {
          case a: AdaptiveSparkPlanExec => a +: flatten(a.executedPlan)
          case other => other +: other.children.flatMap(flatten)
        }
        flatten(qe.executedPlan)
          .collect {
            case p: CometRowViewWriteFilesExec => p.nodeName
            case p: WriteFilesExec => p.nodeName
          }
          .mkString(", ")
      }
      val tree = captured.map { qe =>
        s"    comet.enabled=${spark.conf.get(CometConf.COMET_ENABLED.key, "unset")} " +
          s"rowView=${spark.conf.get(CometConf.COMET_EXEC_WRITE_ROW_VIEW_ENABLED.key, "unset")}\n" +
          qe.executedPlan.treeString
      }
      (names, tree)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  private def writeBenchmark(name: String, values: Int, codec: String, partitioned: Boolean)(
      columns: Seq[String]): Unit = {
    val label = if (partitioned) "partitioned" else "unpartitioned"
    val benchmark = new Benchmark(s"$name ($label, $codec)", values, output = output)
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // A low-cardinality partition column, which is the shape a partitioned write is normally
        // given: long runs of equal partition values, so `DynamicPartitionDataSingleWriter` opens
        // few files and the per-row cost dominates the per-file cost.
        val partitionColumn = if (partitioned) {
          Seq("cast(id % 8 as string) as part")
        } else {
          Seq.empty
        }
        prepareTable(dir, spark.range(values).selectExpr(columns ++ partitionColumn: _*))
        withTempPath { outputDir =>
          outputDir.mkdirs()
          addWriteCases(benchmark, outputDir, codec, partitioned)
          if (!diagnostics) benchmark.run()
        }
      }
    }
  }

  /** A flat schema, which the rule declines. Both Comet arms run the same plan: a noise floor. */
  private val fixedWidth = Seq(
    "id as long_col",
    "cast(id as int) as int_col",
    "cast(id as short) as short_col",
    "cast(id as byte) as byte_col",
    "cast(id % 2 as boolean) as bool_col",
    "cast(id as float) as float_col",
    "cast(id as double) as double_col",
    "date_add(to_date('2024-01-01'), cast(id % 365 as int)) as date_col",
    "cast(id * 2 as long) as long_col2",
    "cast(id * 3 as int) as int_col2")

  private val nested = Seq(
    "id",
    "named_struct('a', cast(id as int), 'b', cast(id as string)) as simple_struct",
    "array(cast(id as int), cast(id + 1 as int), cast(id + 2 as int)) as int_array",
    "map('k1', cast(id as string), 'k2', cast(id + 1 as string)) as str_map")

  /** The shape that motivates this: several levels mixing struct, array and map. */
  private val deeplyNested = Seq(
    "id",
    """named_struct(
         'l1', named_struct(
           'l2', named_struct(
             'l3', named_struct('v', cast(id as int), 'n', concat('x_', cast(id as string))),
             'arr', array(cast(id as int), cast(id + 1 as int))),
           'c', cast(id % 100 as int)),
         'id', id) as deep_struct""",
    """array(
         named_struct('id', cast(id as int), 'tags', array(concat('t_', cast(id as string)))),
         named_struct('id', cast(id + 1 as int), 'tags', array('t_x', 't_y'))
       ) as arr_of_structs""",
    """map('k', array(named_struct('a', cast(id as int),
                                  'b', cast(id as string)))) as map_of_arr_structs""")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val numRows = 1024 * 1024
    // Codecs can be narrowed from the command line, e.g. `-Dexec.args="uncompressed"`, to keep a
    // targeted run short.
    val codecs = if (mainArgs.nonEmpty) mainArgs.toSeq else Seq("uncompressed", "snappy")

    val schemas =
      Seq(("Fixed width", fixedWidth), ("Nested", nested), ("Deeply nested", deeplyNested))

    codecs.foreach { codec =>
      Seq(false, true).foreach { partitioned =>
        val label = if (partitioned) "partitioned" else "unpartitioned"
        schemas.foreach { case (name, columns) =>
          runBenchmark(s"Parquet write - $name ($label, $codec)") {
            writeBenchmark(s"Parquet write - $name", numRows, codec, partitioned)(columns)
          }
        }
      }
    }
  }
}
