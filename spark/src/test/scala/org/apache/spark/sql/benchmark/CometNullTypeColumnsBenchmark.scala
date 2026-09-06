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

import java.lang.management.{ManagementFactory, MemoryType}
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometPlan}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.{CometArrowAllocator, CometConf}

import CometBenchmarkBase.BenchmarkArm

/**
 * Benchmark for NullType-bearing columns on the two paths the NullType work touches: projections
 * of NullType maps / structs consumed by the JVM codegen dispatcher, and broadcast joins whose
 * build side carries a NullType column (a plain `array<null>` coalesces through
 * `Utils.coalesceBroadcastBatches`; a NullType directly under a struct or map entry cannot be
 * coalesced, so the exchange and join stay on Spark).
 *
 * Every case is timed on five arms under the same session, warmup and iteration policy: Spark,
 * Comet, Comet on the prior path (codegen dispatcher off, so the NullType projection falls back
 * and takes its operator with it), and the two Comet arms again with a small
 * `spark.comet.batchSize`. The build side is sized for few and for many broadcast buffers.
 *
 * After timing, each case checks that every arm returns the same rows and profiles one execution
 * per arm: the executed plan (native or where it falls back), JVM allocation, peak and retained
 * heap, Spark's peak task execution memory, the native operators' own memory metrics, retained
 * Arrow memory, and for the broadcast cases the number of broadcast buffers, whether they were
 * coalesced, the number of consuming tasks, and the resulting count of Arrow IPC streams opened
 * (every consuming task decodes every buffer, see `CometBatchRDD.compute`). To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometNullTypeColumnsBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometNullTypeColumnsBenchmark-**results.txt".
 */
object CometNullTypeColumnsBenchmark extends CometBenchmarkBase {

  private val smallBatchSize = 1024

  private val dispatchOff = CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false"
  private val smallBatch = CometConf.COMET_BATCH_SIZE.key -> smallBatchSize.toString

  // The prior path: with the JVM codegen dispatcher off, a NullType-bearing projection falls
  // back to Spark and takes its enclosing operator with it. Each Comet arm is also run with a
  // small `spark.comet.batchSize`, the batch size of the native operators and of the JVM-side
  // conversions. The native Parquet scan emits 8192-row batches whatever this is set to, so the
  // broadcast buffer count is varied through the build size instead (see the profile columns).
  private val extraArms: Seq[BenchmarkArm] = Seq(
    BenchmarkArm("Comet (dispatch off, prior path)", Seq(dispatchOff), expectNative = false),
    BenchmarkArm(s"Comet (batch $smallBatchSize)", Seq(smallBatch), expectNative = true),
    BenchmarkArm(
      s"Comet (dispatch off, prior path, batch $smallBatchSize)",
      Seq(dispatchOff, smallBatch),
      expectNative = false))

  /**
   * Times the case on every arm, then checks result equality and profiles each arm.
   * `expectNative` is false for a case whose Comet plan is meant to fall back.
   */
  private def runNullTypeCase(
      name: String,
      cardinality: Long,
      query: String,
      expectNative: Boolean = true): Unit = {
    val arms = if (expectNative) extraArms else extraArms.map(_.copy(expectNative = false))
    runExpressionBenchmark(
      name,
      cardinality,
      query,
      extraArms = arms,
      expectNative = expectNative)
    verifyAndProfile(name, query, expressionBenchmarkArms(Map.empty, arms))
  }

  private def projectionBenchmarks(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT value AS c1 FROM $tbl"))

        runBenchmark("NullType projection consumed") {
          runNullTypeCase(
            "map(c1, NULL) projected and consumed",
            values,
            "SELECT size(map_keys(map(c1, NULL))) FROM parquetV1Table")
          // Control: a plain list<null>, the shape that stays on every fast path.
          runNullTypeCase(
            "transform to array<null> projected and consumed (control)",
            values,
            "SELECT size(transform(array(c1), x -> NULL)) FROM parquetV1Table")
        }
      }
    }
  }

  private def broadcastBuildBenchmarks(): Unit = {
    val probeRows = 1024 * 1024

    // Few and many broadcast buffers: the native scan emits 8192-row batches and the broadcast
    // collects one buffer per batch, so the build size sets the buffer count. Files are split and
    // packed by Spark's defaults; the hint forces the broadcast join.
    for ((label, buildRows) <- Seq(("2 buffers", 2 * 8192), ("64 buffers", 64 * 8192))) {
      withTempPath { dir =>
        withTempTable("probe", "build") {
          spark
            .range(probeRows)
            .selectExpr("id AS k", "id % 100 AS v")
            .write
            .parquet(s"${dir.getAbsolutePath}/probe")
          // Parquet cannot store NullType, so the build table holds a plain column and each case
          // projects its collection column in the join subquery.
          spark
            .range(buildRows)
            .selectExpr("id AS k")
            .write
            .parquet(s"${dir.getAbsolutePath}/build")

          spark.read.parquet(s"${dir.getAbsolutePath}/probe").createOrReplaceTempView("probe")
          spark.read.parquet(s"${dir.getAbsolutePath}/build").createOrReplaceTempView("build")

          runBenchmark(s"BroadcastHashJoin build with NullType column ($label)") {
            def joinQuery(buildProjection: String, consumed: String): String =
              s"""SELECT /*+ BROADCAST(b) */ count(p.v + $consumed)
                 |FROM probe p JOIN (SELECT k, $buildProjection FROM build) b ON p.k = b.k
                 |""".stripMargin

            // Control: a plain list<null> coalesces, since a NullVector under a list is safe to
            // append.
            runNullTypeCase(
              "array<null> column (control, coalesced)",
              probeRows,
              joinQuery("transform(array(k), x -> NULL) AS null_list", "size(b.null_list)"))
            // A NullType directly under a struct or map entry cannot be coalesced (Arrow's
            // appender cannot grow such a NullVector), so `CometBroadcastExchangeExec` refuses
            // the build side and the exchange and join stay on Spark.
            runNullTypeCase(
              "map<bigint, null> column (Spark broadcast)",
              probeRows,
              joinQuery("map(k, NULL) AS null_map", "size(map_keys(b.null_map))"),
              expectNative = false)
            // Consumed whole: a field access like `b.null_struct.a` is simplified to `b.k` by
            // SimplifyExtractValueOps and the struct would never reach the broadcast.
            runNullTypeCase(
              "struct with NULL field column (Spark broadcast)",
              probeRows,
              joinQuery(
                "named_struct('a', k, 'b', NULL) AS null_struct",
                "size(array(b.null_struct))"),
              expectNative = false)
          }
        }
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("NullType projections", 1024 * 1024 * 10) { values =>
      projectionBenchmarks(values)
    }
    broadcastBuildBenchmarks()
  }

  // ---------------------------------------------------------------------------------------------
  // Result equality and per-arm profiling
  // ---------------------------------------------------------------------------------------------

  /** An order-independent digest of a result set: row count and the sum / xor of row hashes. */
  private case class Digest(rows: Long, sum: Long, xor: Long)

  private case class BroadcastProfile(buffers: Int, coalescedBatches: Long, consumerTasks: Int) {
    def ipcStreams: Long = buffers.toLong * consumerTasks
  }

  private case class Profile(
      arm: String,
      plan: String,
      wallMs: Long,
      jvmAllocatedBytes: Long,
      heapPeakBytes: Long,
      heapRetainedBytes: Long,
      sparkPeakExecutionMemoryBytes: Long,
      nativeMemoryBytes: Long,
      arrowRetainedBytes: Long,
      broadcast: Either[String, BroadcastProfile])

  /** Records task and stage metrics of the jobs run while it is registered. */
  private class ProfileListener extends SparkListener {
    var taskPeakExecutionMemory = 0L
    var broadcastConsumerTasks = 0

    override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
      Option(event.taskMetrics).foreach { metrics =>
        taskPeakExecutionMemory = math.max(taskPeakExecutionMemory, metrics.peakExecutionMemory)
      }
    }

    // A stage that consumes a Comet broadcast has the broadcast's `CometBatchRDD` in its lineage,
    // and every one of its tasks decodes every broadcast buffer.
    override def onStageCompleted(event: SparkListenerStageCompleted): Unit = synchronized {
      if (event.stageInfo.rddInfos.exists(_.name == "CometBatchRDD")) {
        broadcastConsumerTasks += event.stageInfo.numTasks
      }
    }
  }

  private def verifyAndProfile(name: String, query: String, arms: Seq[BenchmarkArm]): Unit = {
    val digests = arms.map(arm => arm.name -> underConf(arm.configs)(digest(query)))
    val (_, reference) = digests.head
    val disagreeing = digests.filter(_._2 != reference)
    if (disagreeing.nonEmpty) {
      val detail = digests.map { case (arm, d) => s"  $arm: $d" }.mkString("\n")
      throw new IllegalStateException(
        s"Result mismatch in '$name': arms disagree with ${digests.head._1}\n$detail")
    }
    emit(s"\n$name: results identical across ${arms.size} arms " +
      s"(${reference.rows} rows, digest ${reference.sum.toHexString}/${reference.xor.toHexString})")

    val profiles = arms.map(arm => underConf(arm.configs)(profile(arm, query)))
    emitProfiles(profiles)
  }

  /** `withSQLConf` returns Unit; this variant returns the block's value. */
  private def underConf[T](configs: Seq[(String, String)])(f: => T): T = {
    var result: Option[T] = None
    withSQLConf(configs: _*) { result = Some(f) }
    result.get
  }

  private def digest(query: String): Digest = {
    val (rows, sum, xor) = spark
      .sql(query)
      .rdd
      .mapPartitions(NullTypeRowDigest.digestPartition)
      .fold((0L, 0L, 0L)) { case ((r1, s1, x1), (r2, s2, x2)) =>
        (r1 + r2, s1 + s2, x1 ^ x2)
      }
    Digest(rows, sum, xor)
  }

  private def profile(arm: BenchmarkArm, query: String): Profile = {
    val heapPools =
      ManagementFactory.getMemoryPoolMXBeans.asScala.filter(_.getType == MemoryType.HEAP)
    val threads = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    def allocatedByAllThreads(): Long =
      threads.getThreadAllocatedBytes(threads.getAllThreadIds).filter(_ > 0).sum
    def usedHeapAfterGc(): Long = {
      System.gc()
      System.gc()
      val runtime = Runtime.getRuntime
      runtime.totalMemory - runtime.freeMemory
    }

    val listener = new ProfileListener
    spark.sparkContext.addSparkListener(listener)
    try {
      val heapBefore = usedHeapAfterGc()
      heapPools.foreach(_.resetPeakUsage())
      val arrowBefore = CometArrowAllocator.getAllocatedMemory
      val allocatedBefore = allocatedByAllThreads()
      val start = System.nanoTime()

      // Execute the dataset's own physical plan rather than `noop()`: a write plans a separate
      // query, whose operators would be the ones carrying the metrics and the final AQE plan.
      val qe = spark.sql(query).queryExecution
      SQLExecution.withNewExecutionId(qe) {
        qe.toRdd.foreach(_ => ())
      }

      val wallMs = (System.nanoTime() - start) / 1000000
      val allocated = allocatedByAllThreads() - allocatedBefore
      val heapPeak = heapPools.map(_.getPeakUsage.getUsed).sum
      spark.sparkContext.listenerBus.waitUntilEmpty()
      val arrowRetained = CometArrowAllocator.getAllocatedMemory - arrowBefore
      val heapRetained = usedHeapAfterGc() - heapBefore

      val plan = stripAQEPlan(qe.executedPlan)
      Profile(
        arm.name,
        describePlan(arm, plan),
        wallMs,
        allocated,
        heapPeak,
        heapRetained,
        listener.taskPeakExecutionMemory,
        nativeMemory(plan),
        arrowRetained,
        broadcastProfile(plan, listener.broadcastConsumerTasks))
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  /**
   * Memory reported by the native operators' own metrics: the hash join's `build_mem_used` and
   * the aggregate's `peak_mem_used`, summed over the plan and over tasks. Spark's task-level
   * `peakExecutionMemory` only sees Spark operators, so the two columns are complementary.
   */
  private def nativeMemory(plan: SparkPlan): Long =
    operators(plan)
      .collect { case p: CometPlan => p }
      .flatMap { p =>
        p.metrics.collect {
          case (name, metric) if name == "build_mem_used" || name == "peak_mem_used" =>
            metric.value
        }
      }
      .sum

  /**
   * Every operator of the executed plan in pre-order, descending into AQE query stages and reused
   * exchanges, whose operators are only reachable through `plan` / `child` after execution. The
   * original instances are returned (never a transformed copy: a copy carries fresh, unregistered
   * metrics), each once, since a reused exchange is shared.
   */
  private def operators(plan: SparkPlan): Seq[SparkPlan] = {
    val seen = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[SparkPlan, java.lang.Boolean])
    def walk(p: SparkPlan): Seq[SparkPlan] =
      if (!seen.add(p)) {
        Nil
      } else {
        p match {
          case s: QueryStageExec => s +: walk(s.plan)
          case r: ReusedExchangeExec => r +: walk(r.child)
          case _ => p +: p.children.flatMap(walk)
        }
      }
    walk(plan)
  }

  private def describePlan(arm: BenchmarkArm, plan: SparkPlan): String =
    if (arm.name == "Spark") {
      "Spark"
    } else {
      // `findFirstNonCometOperator` walks a subtree and stops at query stage leaves, so ask it
      // about each operator of the expanded plan in turn; a node is non-Comet when it is the
      // first one reported for its own subtree. Stage wrappers, reused exchanges and AQE shuffle
      // reads are plumbing Comet consumes natively.
      val plumbing =
        Seq(classOf[QueryStageExec], classOf[ReusedExchangeExec], classOf[AQEShuffleReadExec])
      operators(plan).find(op =>
        findFirstNonCometOperator(op, plumbing: _*).exists(_ eq op)) match {
        case None => "fully Comet native"
        case Some(op) => s"falls back at ${op.nodeName}"
      }
    }

  private def broadcastProfile(
      plan: SparkPlan,
      consumerTasks: Int): Either[String, BroadcastProfile] = {
    val all = operators(plan)
    all.collect { case b: CometBroadcastExchangeExec => b } match {
      case Seq(b) =>
        val buffers = b.executeBroadcast[Array[ChunkedByteBuffer]]().value.length
        Right(BroadcastProfile(buffers, b.metrics("numCoalescedBatches").value, consumerTasks))
      case Seq() =>
        val sparkBroadcast = all.exists(_.isInstanceOf[BroadcastExchangeExec])
        Left(if (sparkBroadcast) "Spark broadcast" else "no broadcast")
      case many => Left(s"${many.size} Comet broadcasts")
    }
  }

  private def emitProfiles(profiles: Seq[Profile]): Unit = {
    def mb(bytes: Long): String = f"${bytes / (1024.0 * 1024.0)}%.1f"
    val header = Seq(
      "arm",
      "plan",
      "wall ms",
      "JVM alloc MB",
      "heap peak MB",
      "heap retained MB",
      "Spark peak exec MB",
      "native mem MB",
      "Arrow retained MB",
      "bcast buffers",
      "coalesced",
      "consumer tasks",
      "IPC streams")
    val rows = profiles.map { p =>
      val (buffers, coalesced, consumers, ipc) = p.broadcast match {
        case Right(b) =>
          (
            b.buffers.toString,
            b.coalescedBatches.toString,
            b.consumerTasks.toString,
            b.ipcStreams.toString)
        case Left(reason) => (reason, "-", "-", "-")
      }
      Seq(
        p.arm,
        p.plan,
        p.wallMs.toString,
        mb(p.jvmAllocatedBytes),
        mb(p.heapPeakBytes),
        mb(p.heapRetainedBytes),
        mb(p.sparkPeakExecutionMemoryBytes),
        mb(p.nativeMemoryBytes),
        mb(p.arrowRetainedBytes),
        buffers,
        coalesced,
        consumers,
        ipc)
    }
    val widths = (header +: rows).transpose.map(_.map(_.length).max)
    def line(cells: Seq[String]): String =
      cells
        .zip(widths)
        .zipWithIndex
        .map { case ((cell, w), i) =>
          if (i < 2) cell.padTo(w, ' ') else cell.reverse.padTo(w, ' ').reverse
        }
        .mkString("  ")
    emit(
      "Per-arm profile of one execution after warmup (JVM alloc: bytes allocated by all live " +
        "threads; Spark peak exec: max peakExecutionMemory over tasks, reported by Spark " +
        "operators only; native mem: the native join build / aggregate peak memory metrics " +
        "summed over operators and tasks; IPC streams = broadcast buffers x consumer tasks):")
    emit(line(header))
    emit("-" * (widths.sum + 2 * (widths.size - 1)))
    rows.foreach(row => emit(line(row)))
    emit("")
  }

  /** Writes a line to the console and to the results file, like the timing tables. */
  private def emit(text: String): Unit = {
    // scalastyle:off println
    println(text)
    // scalastyle:on println
    output.foreach(_.write((text + "\n").getBytes(StandardCharsets.UTF_8)))
  }
}

/**
 * Row digest used from executor closures; kept outside the benchmark object so it captures
 * nothing.
 */
private[benchmark] object NullTypeRowDigest {
  def digestPartition(rows: Iterator[Row]): Iterator[(Long, Long, Long)] = {
    var count = 0L
    var sum = 0L
    var xor = 0L
    rows.foreach { row =>
      val h = row.mkString("\u0001").hashCode.toLong
      count += 1
      sum += h
      xor ^= h
    }
    Iterator((count, sum, xor))
  }
}
