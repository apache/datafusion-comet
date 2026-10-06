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

package org.apache.spark.sql.comet.execution.arrow

import java.lang.management.ManagementFactory
import java.util.Arrays

import scala.concurrent.duration._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecializedGetters}
import org.apache.spark.unsafe.types.UTF8String

import com.sun.management.ThreadMXBean

/**
 * Compare the original ByteBuffer string writer with direct copying into Arrow-owned memory.
 * Inputs, including explicitly off-heap UTF8Strings, are prepared outside measurement. Both
 * writers use identical fresh destinations: either fully reserved, or small enough to force both
 * data and offset/validity growth. Initial destination allocation, validation and cleanup are
 * excluded; growth during writing is included. JVM allocation excludes Arrow's native bytes.
 *
 * To run this benchmark (append `-- --smoke` for a short run):
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.comet.execution.arrow.CometStringWriterBenchmark
 * }}}
 */
object CometStringWriterBenchmark extends BenchmarkBase {

  // Accumulate results after each measured batch without allocating arrays in the measured region.
  @volatile private var consumed: Long = 0L

  private class ByteBufferStringWriter(vector: VarCharVector) extends StringWriter(vector) {
    override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
      val utf8 = input.getUTF8String(ordinal)
      val buffer = utf8.getByteBuffer
      valueVector.setSafe(count, buffer, buffer.position(), utf8.numBytes())
    }
  }

  private case class Input(rows: Array[GenericInternalRow], expected: Array[Array[Byte]]) {
    val bytes: Long = expected.map(_.length.toLong).sum
  }

  private def withInput(allocator: RootAllocator, backing: String, sizes: Seq[Int])(
      f: Input => Unit): Unit = {
    val numRows = math.max(32, math.min(8192, (8 * 1024 * 1024) / sizes.max))
    val expected = Array.tabulate(numRows) { row =>
      Array.tabulate[Byte](sizes(row % sizes.length))(i => ('a' + (row + i) % 26).toByte)
    }
    // Padding gives off-heap and heap slices non-zero starting offsets.
    val padding = 11
    val source = if (backing == "off-heap") {
      allocator.buffer(expected.map(_.length.toLong + padding).sum)
    } else {
      null
    }
    try {
      var offset = 0L
      val rows = expected.map { bytes =>
        val utf8 = backing match {
          case "off-heap" =>
            offset += padding
            source.setBytes(offset, bytes)
            val value =
              UTF8String.fromAddress(null, source.memoryAddress() + offset, bytes.length)
            offset += bytes.length
            value
          case "heap slice" =>
            val array = new Array[Byte](bytes.length + 2 * padding)
            System.arraycopy(bytes, 0, array, padding, bytes.length)
            UTF8String.fromBytes(array, padding, bytes.length)
          case "heap array" => UTF8String.fromBytes(bytes)
        }
        new GenericInternalRow(Array[Any](utf8))
      }
      f(Input(rows, expected))
    } finally {
      if (source != null) source.close()
    }
  }

  private def withDestination(
      allocator: RootAllocator,
      input: Input,
      grow: Boolean,
      baseline: Boolean)(f: (ArrowFieldWriter, VarCharVector) => Unit): Unit = {
    val vector = new VarCharVector("s", allocator)
    try {
      if (grow) {
        vector.allocateNew(8L, 1)
        require(vector.getByteCapacity < input.bytes)
        require(vector.getValueCapacity < input.rows.length)
      } else {
        vector.allocateNew(input.bytes, input.rows.length)
      }
      val writer = if (baseline) new ByteBufferStringWriter(vector) else new StringWriter(vector)
      f(writer, vector)
    } finally {
      vector.close()
    }
  }

  private def write(writer: ArrowFieldWriter, input: Input): Unit = {
    var row = 0
    while (row < input.rows.length) {
      writer.write(input.rows(row), 0)
      row += 1
    }
    writer.finish()
  }

  private def validate(vector: VarCharVector, input: Input): Unit = {
    require(vector.getValueCount == input.rows.length)
    var row = 0
    while (row < input.rows.length) {
      require(!vector.isNull(row) && Arrays.equals(vector.get(row), input.expected(row)))
      row += 1
    }
  }

  private def consume(vector: VarCharVector, input: Input): Unit = {
    consumed += vector.getValueCount.toLong + vector.getDataBuffer.getByte(input.bytes - 1)
  }

  override def runBenchmarkSuite(args: Array[String]): Unit = {
    require(args.forall(_ == "--smoke"), "Supported argument: --smoke")
    val smoke = args.contains("--smoke")
    val duration = if (smoke) 100.millis else 1.second
    val shapes = if (smoke) {
      Seq("16 B" -> Seq(16), "1 MiB" -> Seq(1024 * 1024))
    } else {
      Seq(
        "16 B" -> Seq(16),
        "1 KiB" -> Seq(1024),
        "64 KiB" -> Seq(64 * 1024),
        "1 MiB" -> Seq(1024 * 1024),
        "varied" -> Seq(0, 16, 1024, 64 * 1024, 1024 * 1024))
    }
    val allocationBean = ManagementFactory.getThreadMXBean match {
      case bean: ThreadMXBean if bean.isThreadAllocatedMemorySupported =>
        if (!bean.isThreadAllocatedMemoryEnabled) bean.setThreadAllocatedMemoryEnabled(true)
        Some(bean)
      case _ => None
    }
    val threadId = Thread.currentThread().getId
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      shapes.foreach { case (shape, sizes) =>
        Seq("off-heap", "heap array", "heap slice").foreach { backing =>
          withInput(allocator, backing, sizes) { input =>
            Seq(false, true).foreach { grow =>
              val destination = if (grow) "growing" else "preallocated"
              val benchmark = new Benchmark(
                s"String to Arrow: $backing, $shape, $destination",
                input.rows.length.toLong,
                warmupTime = duration,
                minTime = duration,
                output = output)
              val cases = Seq("ByteBuffer baseline" -> true, "direct copy" -> false)
              cases.foreach { case (name, baseline) =>
                // Validate every byte once per case, without introducing temporary arrays into
                // every timing iteration and distorting its subsequent GC pressure.
                withDestination(allocator, input, grow, baseline) { (writer, vector) =>
                  write(writer, input)
                  validate(vector, input)
                }
                benchmark.addTimerCase(name) { timer =>
                  withDestination(allocator, input, grow, baseline) { (writer, vector) =>
                    timer.startTiming()
                    write(writer, input)
                    timer.stopTiming()
                    consume(vector, input)
                  }
                }
              }
              benchmark.run()
              // Sample allocation separately after the throughput warmup, so reading the
              // management bean cannot affect short-string throughput measurements.
              allocationBean match {
                case Some(bean) =>
                  cases.foreach { case (name, baseline) =>
                    val samples = if (smoke) 2 else 10
                    var allocated = 0L
                    var sample = 0
                    while (sample < samples) {
                      withDestination(allocator, input, grow, baseline) { (writer, vector) =>
                        val before = bean.getThreadAllocatedBytes(threadId)
                        write(writer, input)
                        allocated += bean.getThreadAllocatedBytes(threadId) - before
                        consume(vector, input)
                      }
                      sample += 1
                    }
                    val bytesPerRow = allocated.toDouble / samples / input.rows.length
                    benchmark.out.println(f"$name JVM allocation: $bytesPerRow%.1f bytes/row")
                  }
                case None =>
                  benchmark.out.println(
                    "JVM allocation unavailable: no thread allocation counter")
              }
              benchmark.out.println()
            }
          }
        }
      }
    } finally {
      allocator.close()
    }
  }
}
