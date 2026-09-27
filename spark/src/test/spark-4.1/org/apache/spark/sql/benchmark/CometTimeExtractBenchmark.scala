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

import org.apache.arrow.vector.{TimeNanoVector, VarBinaryVector}
import org.apache.spark.SparkEnv
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, BoundReference, Literal, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.comet.{CometProjectExec, SerializedPlan}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.types.{DecimalType, TimeType}

import org.apache.comet.{CometArrowAllocator, CometConf}
import org.apache.comet.serde.CometScalaUDF
import org.apache.comet.udf.codegen.CometScalaUDFCodegen
import org.apache.comet.vector.CometVector

/**
 * Spark 4.1 EXTRACT(SECOND FROM TIME), including the previous JVM dispatcher path. Run with `make
 * benchmark-org.apache.spark.sql.benchmark.CometTimeExtractBenchmark PROFILES=-Pspark-4.1`.
 * Optional argument: SQL row count (default 1M). Direct kernels use 10000 rows and checksum every
 * output; the corresponding Rust cases run with `cargo bench -p datafusion-comet-spark-expr
 * --bench to_time -- seconds_of_time` from native/. The direct dispatcher excludes JNI overhead.
 * SQL inputs are materialized before timing. SQL timings include scanning, parsing, projection,
 * row conversion and Spark task overhead, but exclude query planning. Its 0/3/6-digit strings all
 * parse to TIME(6); the direct kernels exercise TIME(0), TIME(3) and TIME(6).
 */
object CometTimeExtractBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(args: Array[String]): Unit = {
    require(spark.version.startsWith("4.1."), "This benchmark targets Spark 4.1 TIME semantics")
    val rows = args.headOption.map(_.toInt).getOrElse(1024 * 1024)
    withSQLConf(
      "spark.sql.timeType.enabled" -> "true",
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      // Run the small kernels before allocating the million-row SQL inputs.
      Seq(0, 3, 6).foreach(p => Seq(false, true).foreach(runKernels(p, _)))
      Seq(0, 3, 6).foreach { precision =>
        Seq(false, true).foreach { nulls =>
          withTempTable("timeInput") {
            val time = "concat('12:34:', lpad(cast(id % 60 as string), 2, '0'), '.', " +
              "lpad(cast((id * 7919) % 1000000 as string), 6, '0'))"
            val trimmed = s"substring($time, 1, ${if (precision == 0) 8 else 9 + precision})"
            val nullable = if (nulls) s"if(id % 4 = 0, null, $trimmed)" else trimmed
            val input = spark.sql(s"select $nullable as s from range($rows)")
            val data = input.queryExecution.executedPlan.executeCollect().toSeq
            Dataset
              .ofRows(
                spark.asInstanceOf[SparkSession],
                LocalRelation(input.queryExecution.analyzed.output, data))
              .createOrReplaceTempView("timeInput")
            runCase(s"to_time + extract, digits=$precision, nulls=$nulls", rows)
          }
        }
      }
    }
  }

  // Comet's standard scan paths cannot ingest TIME columns yet. Exercise the exact Spark and
  // dispatcher kernels directly; benches/to_time.rs supplies the corresponding native cases.
  private def runKernels(precision: Int, nulls: Boolean): Unit = {
    val size = 10000
    val time = new TimeNanoVector("t", CometArrowAllocator)
    val serialized = new VarBinaryVector("expr", CometArrowAllocator)
    try {
      time.allocateNew(size)
      val divisor = math.pow(10, 9 - precision).toLong
      val inputProjection =
        UnsafeProjection.create(Seq(BoundReference(0, TimeType(precision), nullable = true)))
      val rows = (0 until size)
        .map { i =>
          if (nulls && i % 4 == 0) {
            time.setNull(i)
            InternalRow(null)
          } else {
            val nanos = (45240000000000L + (i % 60) * 1000000000L +
              (i * 7919L % 1000000) * 1000) / divisor * divisor
            time.setSafe(i, nanos)
            InternalRow(nanos)
          }
        }
        .map(row => inputProjection(row).copy())
      time.setValueCount(size)
      val expr = StaticInvoke(
        classOf[DateTimeUtils.type],
        DecimalType(8, 6),
        "getSecondsOfTimeWithFraction",
        Seq(BoundReference(0, TimeType(precision), nullable = true), Literal(precision)))
      val projection = UnsafeProjection.create(Seq(expr))
      val buffer = SparkEnv.get.closureSerializer.newInstance().serialize(expr)
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      serialized.allocateNew()
      serialized.setSafe(0, bytes)
      serialized.setValueCount(1)
      val dispatcher = new CometScalaUDFCodegen()
      val inputs = Array[org.apache.arrow.vector.ValueVector](serialized, time)
      val result = dispatcher.evaluate(inputs, size)
      try {
        val vector =
          CometVector.getVector(result.asInstanceOf[org.apache.arrow.vector.FieldVector], null)
        rows.indices.foreach { i =>
          val expected = projection(rows(i))
          require(vector.isNullAt(i) == expected.isNullAt(0))
          if (!expected.isNullAt(0))
            require(vector.getDecimal(i, 8, 6) == expected.getDecimal(0, 8, 6))
        }
      } finally result.close()
      runBenchmark(s"seconds_of_time p=$precision nulls=$nulls (10000 rows per batch)") {
        val benchmark =
          new Benchmark(s"seconds_of_time p=$precision nulls=$nulls", size, output = output)
        // Consume every result and publish one checksum per batch to prevent dead-code removal.
        benchmark.addCase("Spark generated projection") { _ =>
          var sum = 0L
          var i = 0
          while (i < size) {
            val result = projection(rows(i))
            if (!result.isNullAt(0)) sum += result.getLong(0)
            i += 1
          }
          sink = sum
        }
        benchmark.addCase("JVM dispatcher (direct)") { _ =>
          val result = dispatcher
            .evaluate(inputs, size)
            .asInstanceOf[org.apache.arrow.vector.DecimalVector]
          try {
            var sum = 0L
            var i = 0
            while (i < size) {
              if (!result.isNull(i)) sum += result.getDataBuffer.getLong(i * 16L)
              i += 1
            }
            sink = sum
          } finally result.close()
        }
        val cases = benchmark.benchmarks.toSeq :+
          benchmark.benchmarks.head.copy(name = "Spark generated projection (repeat)")
        cases.foreach { c =>
          val result = benchmark.measure(size, c.numIters)(c.fn)
          benchmark.out.println(
            f"seconds_of_time p=$precision nulls=$nulls, ${c.name}: " +
              f"mean=${result.avgMs * 1e6 / size}%.2f ns/row, " +
              f"best=${result.bestMs * 1e6 / size}%.2f ns/row, " +
              f"stdev=${result.stdevMs * 1e6 / size}%.2f ns/row")
        }
      }
    } finally {
      serialized.close()
      time.close()
    }
  }

  @volatile private var sink: Long = 0L

  private def plan(query: String, arm: String): SparkPlan = {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> (arm != "Spark").toString,
      CometConf.COMET_EXEC_ENABLED.key -> (arm != "Spark").toString,
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> (arm == "JVM dispatcher").toString) {
      val original = spark.sql(query).queryExecution.executedPlan
      if (arm == "Spark") {
        require(original.find(_.isInstanceOf[ProjectExec]).isDefined, original.toString)
        require(original.find(_.isInstanceOf[CometProjectExec]).isEmpty, original.toString)
        original
      } else {
        val projects = original.collect { case p: CometProjectExec => p }
        require(projects.size == 1, original.toString)
        require(findFirstNonCometOperator(original).isEmpty, original.toString)
        require(projects.head.nativeOp.toString.contains("seconds_of_time"))
        if (arm == "Native") original
        else
          original.transformDown { case p: CometProjectExec =>
            // Reproduce the old catch-all: serialize the entire original expression, including
            // to_time in the composed case, through the existing JVM codegen dispatcher.
            val expr = p.projectList.head.asInstanceOf[Alias].child
            val dispatch = CometScalaUDF.emitJvmCodegenDispatch(expr, p.child.output, true).get
            require(dispatch.hasJvmScalarUdf)
            val projection = p.nativeOp.getProjection.toBuilder
              .clearProjectList()
              .addProjectList(dispatch)
            val op = p.nativeOp.toBuilder.setProjection(projection).build()
            p.copy(nativeOp = op, serializedPlanOpt = SerializedPlan(None)).convertBlock()
          }
      }
    }
  }

  private def consume(plan: SparkPlan): Unit = {
    sink = plan
      .execute()
      .mapPartitions { rows =>
        var sum = 0L
        rows.foreach { row =>
          if (!row.isNullAt(0)) sum += row.getDecimal(0, 8, 6).toUnscaledLong
        }
        Iterator.single(sum)
      }
      .collect()
      .sum
  }

  private def runCase(name: String, rows: Int): Unit = {
    runBenchmark(name) {
      val query = "select extract(second from to_time(s)) as seconds from timeInput"
      val arms = Seq("Spark", "JVM dispatcher", "Native").map(arm => arm -> plan(query, arm))
      val expected = arms.head._2.executeCollect().map(_.copy())
      arms.foreach { case (arm, physical) =>
        CometScalaUDFCodegen.resetStats()
        val actual = physical.executeCollect()
        require(actual.sameElements(expected), s"$name: $arm differs from Spark")
        val stats = CometScalaUDFCodegen.stats()
        require(
          (stats.compileCount + stats.cacheHitCount > 0) == (arm == "JVM dispatcher"),
          s"$name: $arm unexpected dispatcher activity: $stats")
      }
      val benchmark = new Benchmark(name, rows, minNumIters = 5, output = output)
      benchmark.out.println(
        s"Spark ${spark.version}; Java ${System.getProperty("java.version")}; " +
          s"rows=$rows; local[1]; batchSize=${CometConf.COMET_BATCH_SIZE.get()}")
      arms.foreach { case (arm, physical) =>
        benchmark.out.println(s"Verified $arm: $physical")
        benchmark.addCase(arm)(_ => consume(physical))
      }
      benchmark.addCase("Spark (repeat)")(_ => consume(arms.head._2))
      benchmark.run()
    }
  }
}
