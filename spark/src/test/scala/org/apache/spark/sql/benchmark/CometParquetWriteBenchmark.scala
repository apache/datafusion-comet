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
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometColumnarToRowViewExec, CometNativeColumnarToRowExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark for the columnar-to-row transition that sits below a Parquet write:
 *   - Spark's own vectorized read plus `ColumnarToRowExec`
 *   - Comet scan plus the JVM `CometColumnarToRowExec` (materializes an UnsafeRow per row)
 *   - Comet scan plus the native `CometNativeColumnarToRowExec`
 *   - Comet scan plus `CometColumnarToRowViewExec` (zero-copy row views over the Arrow batch)
 *
 * Each case measures `scan -> transition -> ParquetWriteSupport -> file`, so the transition is
 * only part of what is timed; the point of the comparison is how much of a real write the
 * UnsafeRow materialization costs. Cases are run at two compression settings because the encoding
 * cost is what the transition cost has to be measured against.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometParquetWriteBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometParquetWriteBenchmark-**results.txt".
 */
object CometParquetWriteBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometParquetWriteBenchmark")
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

  private def addWriteCases(benchmark: Benchmark, outputDir: File, codec: String): Unit = {
    val query = "SELECT * FROM parquetV1Table"

    def write(target: String, configs: Seq[(String, String)]): Unit =
      withSQLConf(configs :+ (SQLConf.PARQUET_COMPRESSION.key -> codec): _*) {
        spark
          .sql(query)
          .write
          .mode("overwrite")
          .parquet(new File(outputDir, target).getCanonicalPath)
      }

    // The four cases below differ only in which columnar-to-row transition feeds the writer, so
    // the comparison is meaningless unless each arm actually plans the transition it names. Run
    // every arm once up front and fail loudly when it does not.
    def verify(
        name: String,
        target: String,
        configs: Seq[(String, String)],
        expected: String): Unit = {
      val (transition, tree) = captureTransition(write(target, configs))
      // scalastyle:off println
      println(s"  [plan check] $name -> ${transition.getOrElse("<no transition in plan>")}")
      if (diagnostics) println(tree.getOrElse("<no plan>"))
      // scalastyle:on println
      if (!transition.contains(expected)) {
        val border = "=" * 80
        benchmark.out.println(s"""\n$border
             |WARNING: the "$name" case did not plan $expected but
             |${transition.getOrElse("no transition at all")}, so it is not measuring what its
             |name says. Treat this row as invalid.
             |$border""".stripMargin)
      }
    }

    benchmark.addCase("Spark") { _ =>
      write("spark", sparkConfigs)
    }

    benchmark.addCase("Comet JVM C2R (UnsafeRow)") { _ =>
      write("comet-jvm", jvmC2RConfigs)
    }

    benchmark.addCase("Comet native C2R (UnsafeRow)") { _ =>
      write("comet-native", nativeC2RConfigs)
    }

    benchmark.addCase("Comet row view (zero-copy)") { _ =>
      write("comet-rowview", rowViewConfigs)
    }

    verify("Spark", "spark", sparkConfigs, "ColumnarToRow")
    verify("Comet JVM C2R (UnsafeRow)", "comet-jvm", jvmC2RConfigs, "CometColumnarToRow")
    verify(
      "Comet native C2R (UnsafeRow)",
      "comet-native",
      nativeC2RConfigs,
      "CometNativeColumnarToRow")
    // The rule declines a schema of only flat types, so the row-view arm is expected to plan the
    // ordinary transition there. Deriving the expectation from the schema also asserts the gate.
    verify(
      "Comet row view (zero-copy)",
      "comet-rowview",
      rowViewConfigs,
      if (hasComplexType) "CometColumnarToRowView" else "CometColumnarToRow")
  }

  /** Mirrors the complex-type gate in `EliminateRedundantTransitions.rowView`. */
  private def hasComplexType: Boolean =
    spark.table("parquetV1Table").schema.fields.exists { f =>
      f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType] ||
      f.dataType.isInstanceOf[MapType]
    }

  private val sparkConfigs = Seq(CometConf.COMET_ENABLED.key -> "false")

  private val jvmC2RConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false",
    CometConf.COMET_WRITE_ROW_VIEW_ENABLED.key -> "false")

  private val nativeC2RConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true",
    CometConf.COMET_WRITE_ROW_VIEW_ENABLED.key -> "false")

  private val rowViewConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false",
    CometConf.COMET_WRITE_ROW_VIEW_ENABLED.key -> "true")

  /** Names the columnar-to-row transition in the executed plan of a write. */
  private def captureTransition(writeOp: => Unit): (Option[String], Option[String]) = {
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
            case p: ColumnarToRowExec => p.nodeName
            case p: CometColumnarToRowExec => p.nodeName
            case p: CometNativeColumnarToRowExec => p.nodeName
            case p: CometColumnarToRowViewExec => p.nodeName
          }
          .mkString(", ")
      }
      val tree = captured.map { qe =>
        s"    comet.enabled=${spark.conf.get(CometConf.COMET_ENABLED.key, "unset")} " +
          s"exec.enabled=${spark.conf.get(CometConf.COMET_EXEC_ENABLED.key, "unset")} " +
          s"rowView=${spark.conf.get(CometConf.COMET_WRITE_ROW_VIEW_ENABLED.key, "unset")}\n" +
          qe.executedPlan.treeString
      }
      (names, tree)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  private val diagnostics = sys.env.contains("COMET_BENCH_DIAG")

  private def writeBenchmark(name: String, values: Int, codec: String)(
      columns: Seq[String]): Unit = {
    val benchmark = new Benchmark(s"$name ($codec)", values, output = output)
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.range(values).selectExpr(columns: _*))
        withTempPath { outputDir =>
          outputDir.mkdirs()
          addWriteCases(benchmark, outputDir, codec)
          if (!diagnostics) benchmark.run()
        }
      }
    }
  }

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

  private val strings = Seq(
    "id",
    "concat('short_', cast(id % 100 as string)) as short_str",
    "concat('medium_string_value_', cast(id as string), '_with_more_content') as medium_str",
    "repeat(concat('long_', cast(id as string)), 10) as long_str")

  private val wide = (0 until 50).map { i =>
    i % 5 match {
      case 0 => s"cast(id + $i as int) as int_col_$i"
      case 1 => s"cast(id + $i as long) as long_col_$i"
      case 2 => s"cast(id + $i as double) as double_col_$i"
      case 3 => s"concat('str_${i}_', cast(id as string)) as str_col_$i"
      case 4 => s"cast((id + $i) % 2 as boolean) as bool_col_$i"
    }
  }

  private val nested = Seq(
    "id",
    "named_struct('a', cast(id as int), 'b', cast(id as string)) as simple_struct",
    "array(cast(id as int), cast(id + 1 as int), cast(id + 2 as int)) as int_array",
    "map('k1', cast(id as string), 'k2', cast(id + 1 as string)) as str_map")

  /** One struct, nested `depth` levels deep, to show how the saving scales with depth. */
  private def structOfDepth(depth: Int): Seq[String] = {
    def build(level: Int): String =
      if (level == depth) {
        s"named_struct('v', cast(id as int), 'n', concat('x_', cast(id as string)))"
      } else {
        s"named_struct('c', cast(id % 100 as int), 'inner', ${build(level + 1)})"
      }
    Seq("id", s"${build(1)} as deep_struct")
  }

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
    val codecs = if (mainArgs.nonEmpty) mainArgs.toSeq else Seq("uncompressed", "snappy", "zstd")

    codecs.foreach { codec =>
      runBenchmark(s"Parquet write - Fixed width ($codec)") {
        writeBenchmark("Parquet write - Fixed width", numRows, codec)(fixedWidth)
      }
      runBenchmark(s"Parquet write - Strings ($codec)") {
        writeBenchmark("Parquet write - Strings", numRows, codec)(strings)
      }
      runBenchmark(s"Parquet write - Wide 50 columns ($codec)") {
        writeBenchmark("Parquet write - Wide 50 columns", numRows, codec)(wide)
      }
      runBenchmark(s"Parquet write - Nested ($codec)") {
        writeBenchmark("Parquet write - Nested", numRows, codec)(nested)
      }
      runBenchmark(s"Parquet write - Deeply nested ($codec)") {
        writeBenchmark("Parquet write - Deeply nested", numRows, codec)(deeplyNested)
      }
      Seq(1, 2, 4, 8).foreach { depth =>
        runBenchmark(s"Parquet write - Struct depth $depth ($codec)") {
          writeBenchmark(s"Parquet write - Struct depth $depth", numRows, codec)(
            structOfDepth(depth))
        }
      }
    }
  }
}
