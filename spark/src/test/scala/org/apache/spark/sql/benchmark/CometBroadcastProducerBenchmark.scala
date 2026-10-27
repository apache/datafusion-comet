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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.comet.execution.shuffle.NativeBatchDecoderIterator
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.comet.Native
import org.apache.comet.vector.NativeUtil

/**
 * Measures native shuffle decoding, Arrow export and broadcast serialization together. Input
 * frames are prepared before timing; output validation and driver decoding are also untimed. Both
 * arms use the same artifact and differ only in producer batching. This is a producer
 * microbenchmark, not a complete Spark query or a shuffle transport benchmark.
 *
 * Pass --on-first to reverse the case order and an optional scenario-name substring to filter.
 *
 * To run:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *   benchmark-org.apache.spark.sql.benchmark.CometBroadcastProducerBenchmark BENCH_HEAP=4g
 * }}}
 */
object CometBroadcastProducerBenchmark extends CometBenchmarkBase {

  private val targetRows = 8192
  private val wideValue = "x" * (6 * 1024)

  private case class Scenario(name: String, rows: Int, rowsPerFrame: Int, wide: Boolean = false)
  private case class Result(rows: Long, streams: Long, bytes: Long)

  private val scenarios = Seq(
    Scenario("tiny: 1 row per frame", 8192, 1),
    Scenario("tiny: 8 rows per frame", 8192, 8),
    Scenario("full: 8192 rows per frame", 8192, 8192),
    Scenario("byte cap: 64 rows of 6 KiB strings", 8192, 64, wide = true))

  private def frames(scenario: Scenario): Array[Byte] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val ids = new BigIntVector("id", allocator)
    val values = new IntVector("value", allocator)
    val strings = new VarCharVector("payload", allocator)
    val root =
      if (scenario.wide) VectorSchemaRoot.of(ids, values, strings)
      else VectorSchemaRoot.of(ids, values)
    val output = new ByteArrayOutputStream()
    val payload = wideValue.getBytes(StandardCharsets.UTF_8)
    try {
      ids.allocateNew(scenario.rowsPerFrame)
      values.allocateNew(scenario.rowsPerFrame)
      if (scenario.wide) strings.allocateNew()
      var start = 0
      while (start < scenario.rows) {
        val count = math.min(scenario.rowsPerFrame, scenario.rows - start)
        var row = 0
        while (row < count) {
          val id = start + row
          ids.setSafe(row, id.toLong)
          if (id % 7 == 0) values.setNull(row) else values.setSafe(row, id * 3)
          if (scenario.wide) strings.setSafe(row, payload)
          row += 1
        }
        root.setRowCount(count)
        val ipc = new ByteArrayOutputStream()
        val writer = new ArrowStreamWriter(root, null, Channels.newChannel(ipc))
        try {
          writer.start()
          writer.writeBatch()
          writer.end()
        } finally writer.close()
        val bytes = ipc.toByteArray
        val header = ByteBuffer
          .allocate(20)
          .order(ByteOrder.LITTLE_ENDIAN)
          .putLong(12L + bytes.length.toLong)
          .putLong(root.getFieldVectors.size().toLong)
          .put(Array[Byte](78, 79, 78, 69)) // NONE codec, followed by Arrow IPC.
        output.write(header.array())
        output.write(bytes)
        start += count
      }
      output.toByteArray
    } finally {
      root.close()
      if (!scenario.wide) strings.close()
      allocator.close()
    }
  }

  private def produce(
      input: Array[Byte],
      scenario: Scenario,
      enabled: Boolean,
      verify: Boolean): Result = {
    val util = new NativeUtil
    val decoder = NativeBatchDecoderIterator(
      new ByteArrayInputStream(input),
      new SQLMetric("nsTiming", 0L),
      new Native,
      util,
      tracingEnabled = false,
      coalescing = if (enabled) Some(targetRows) else None)
    var rows = 0L
    var streams = 0L
    var bytes = 0L
    var checkedRows = 0L
    try {
      Utils.serializeBatches(decoder).foreach { case (count, buffer) =>
        try {
          rows += count
          streams += 1L
          bytes += buffer.size
          if (verify) {
            val batches = Utils.decodeBatches(buffer, "broadcast-producer-benchmark")
            while (batches.hasNext) {
              val batch = batches.next()
              var row = 0
              while (row < batch.numRows()) {
                assert(batch.column(0).getLong(row) == checkedRows)
                assert(batch.column(1).isNullAt(row) == (checkedRows % 7 == 0))
                if (checkedRows % 7 != 0) {
                  assert(batch.column(1).getInt(row) == checkedRows.toInt * 3)
                }
                if (scenario.wide) {
                  assert(batch.column(2).getUTF8String(row).toString == wideValue)
                }
                checkedRows += 1L
                row += 1
              }
            }
          }
        } finally buffer.dispose()
      }
      assert(rows == scenario.rows.toLong)
      if (verify) assert(checkedRows == rows)
      Result(rows, streams, bytes)
    } finally {
      try decoder.close()
      finally util.close()
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    // Initialize SparkEnv, whose compression codec serializeBatches uses.
    spark.sparkContext.setLogLevel("WARN")
    val scenarioFilters = mainArgs.filterNot(_ == "--on-first")
    val modes = if (mainArgs.contains("--on-first")) {
      Seq("ON" -> true, "OFF" -> false, "ON (repeat)" -> true)
    } else {
      Seq("OFF" -> false, "ON" -> true, "OFF (repeat)" -> false)
    }
    scenarios.filter(s => scenarioFilters.forall(s.name.contains)).foreach { scenario =>
      val input = frames(scenario)
      runBenchmark(scenario.name) {
        val benchmark =
          new Benchmark(scenario.name, scenario.rows.toLong, minNumIters = 5, output = output)
        val off = produce(input, scenario, enabled = false, verify = true)
        val on = produce(input, scenario, enabled = true, verify = true)
        benchmark.out.println(s"OFF: $off; ON: $on")
        if (scenario.rowsPerFrame == targetRows) {
          assert(on.streams == off.streams)
        } else {
          assert(on.streams < off.streams)
        }
        modes.foreach { case (name, enabled) =>
          benchmark.addCase(name) { _ =>
            val result = produce(input, scenario, enabled, verify = false)
            assert(result == (if (enabled) on else off))
          }
        }
        benchmark.run()
      }
    }
  }
}
