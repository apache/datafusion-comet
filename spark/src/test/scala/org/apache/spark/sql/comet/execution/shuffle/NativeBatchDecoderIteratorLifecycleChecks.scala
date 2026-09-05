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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.{ByteArrayInputStream, EOFException, IOException}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.memory.OutOfMemoryException
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.{CometShuffleReadFailureHandler, Native}
import org.apache.comet.serde.{OperatorOuterClass, QueryPlanSerde}
import org.apache.comet.vector.NativeUtil

/** Exercises the decoder's ownership independently of the native codec implementation. */
private[shuffle] object NativeBatchDecoderIteratorLifecycleChecks {

  private final class TrackingBatch(failure: Option[Throwable] = None)
      extends ColumnarBatch(Array.empty[ColumnVector], 1) {
    var closeCalls = 0

    override def close(): Unit = {
      closeCalls += 1
      super.close()
      failure.foreach(throw _)
    }
  }

  private val frame = ByteBuffer
    .allocate(20)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putLong(12L)
    .putLong(0L)
    .putInt(0)
    .array()

  private def withDecoder(batch: TrackingBatch, closeFailure: Option[Throwable] = None)(
      check: (NativeBatchDecoderIterator, () => Int) => Unit): Unit = {
    var closeCalls = 0
    val input = new ByteArrayInputStream(frame) {
      override def close(): Unit = {
        closeCalls += 1
        super.close()
        closeFailure.foreach(throw _)
      }
    }
    val util = new NativeUtil {
      override def getNextBatch(
          numOutputCols: Int,
          decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = Some(batch)
    }
    try {
      val decoder = NativeBatchDecoderIterator(
        input,
        new SQLMetric("nsTiming", 0L),
        null,
        util,
        tracingEnabled = false)
      check(decoder, () => closeCalls)
    } finally {
      util.close()
    }
  }

  def closesPrefetchedBatch(): Unit = {
    val batch = new TrackingBatch()
    withDecoder(batch) { (decoder, inputCloseCalls) =>
      assert(decoder.hasNext)
      decoder.close()
      decoder.close()
      assert(batch.closeCalls == 1)
      assert(inputCloseCalls() == 1)
      assert(!decoder.hasNext)
    }
  }

  def closesDeliveredBatch(): Unit = {
    val batch = new TrackingBatch()
    withDecoder(batch) { (decoder, inputCloseCalls) =>
      assert(decoder.next() eq batch)
      decoder.close()
      decoder.close()
      assert(batch.closeCalls == 1)
      assert(inputCloseCalls() == 1)
      assert(!decoder.hasNext)
    }
  }

  def preservesCleanupFailures(): Unit = {
    Seq(false, true).foreach { delivered =>
      val batchFailure = new IOException("batch release failed")
      val streamFailure = new IOException("stream release failed")
      val batch = new TrackingBatch(Some(batchFailure))
      withDecoder(batch, Some(streamFailure)) { (decoder, inputCloseCalls) =>
        assert(decoder.hasNext)
        if (delivered) assert(decoder.next() eq batch)
        val caught =
          try {
            decoder.close()
            throw new AssertionError("Expected the batch release failure")
          } catch {
            case failure: IOException => failure
          }
        assert(caught eq batchFailure)
        assert(caught.getSuppressed.toSeq == Seq(streamFailure))
        decoder.close()
        assert(batch.closeCalls == 1)
        assert(inputCloseCalls() == 1)
        assert(!decoder.hasNext)
      }
    }
  }

  def forwardsDecodeFailuresButNotCleanup(): Unit = {
    val decodeFailure = new IOException("native decode failed")
    val reportedFailure = new IOException("remote fetch failed", decodeFailure)
    val closeFailure = new IOException("stream close failed")
    var reports = 0
    val input = new ByteArrayInputStream(frame) with CometShuffleReadFailureHandler {
      override def onShuffleReadFailure(failure: Throwable): Unit = {
        assert(failure eq decodeFailure)
        reports += 1
        throw reportedFailure
      }

      override def close(): Unit = throw closeFailure
    }
    val util = new NativeUtil {
      override def getNextBatch(
          numOutputCols: Int,
          decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = {
        decode(Array.empty[Long], Array.empty[Long])
        throw new AssertionError("Expected native decoding to fail")
      }
    }
    val nativeLib = new Native {
      override def decodeShuffleBlockWithValidation(
          block: ByteBuffer,
          length: Int,
          arrays: Array[Long],
          schemas: Array[Long],
          tracing: Boolean,
          expectedSchema: Array[Byte]): Long = throw decodeFailure
    }
    val decoder = NativeBatchDecoderIterator(
      input,
      new SQLMetric("nsTiming", 0L),
      nativeLib,
      util,
      tracingEnabled = false,
      expectedSchema = Some(Array.empty[Byte]))
    try {
      val readError =
        try {
          decoder.hasNext
          throw new AssertionError("Expected the remote fetch failure")
        } catch {
          case failure: IOException => failure
        }
      assert(readError eq reportedFailure)
      val closeError =
        try {
          decoder.close()
          throw new AssertionError("Expected the stream close failure")
        } catch {
          case failure: IOException => failure
        }
      assert(closeError eq closeFailure)
      assert(reports == 1)
    } finally {
      util.close()
    }
  }

  def selectsValidationOnlyForRemoteStreams(): Unit = {
    val intSchema = OperatorOuterClass.ShuffleScan
      .newBuilder()
      .addFields(QueryPlanSerde.serializeDataType(IntegerType).get)
      .build()
      .toByteArray
    Seq(None, Some(Array.empty[Byte]), Some(intSchema)).foreach { expectedSchema =>
      val remote = expectedSchema.isDefined
      var localCalls = 0
      var validatedCalls = 0
      val input =
        if (remote) {
          new ByteArrayInputStream(frame) with CometShuffleReadFailureHandler {
            override def onShuffleReadFailure(failure: Throwable): Unit = throw failure
          }
        } else {
          new ByteArrayInputStream(frame)
        }
      val batch = new TrackingBatch()
      val util = new NativeUtil {
        override def getNextBatch(
            numOutputCols: Int,
            decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = {
          assert(decode(Array.empty[Long], Array.empty[Long]) == 1L)
          Some(batch)
        }
      }
      val nativeLib = new Native {
        override def decodeShuffleBlock(
            block: ByteBuffer,
            length: Int,
            arrays: Array[Long],
            schemas: Array[Long],
            tracing: Boolean): Long = {
          localCalls += 1
          1L
        }

        override def decodeShuffleBlockWithValidation(
            block: ByteBuffer,
            length: Int,
            arrays: Array[Long],
            schemas: Array[Long],
            tracing: Boolean,
            schema: Array[Byte]): Long = {
          assert(expectedSchema.exists(_.sameElements(schema)))
          validatedCalls += 1
          1L
        }
      }
      val decoder = NativeBatchDecoderIterator(
        input,
        new SQLMetric("nsTiming", 0L),
        nativeLib,
        util,
        tracingEnabled = false,
        expectedSchema = expectedSchema)
      try {
        assert(decoder.hasNext)
        assert(localCalls == (if (remote) 0 else 1))
        assert(validatedCalls == (if (remote) 1 else 0))
      } finally {
        decoder.close()
        util.close()
      }
      assert(batch.closeCalls == 1)
    }
  }

  def rejectsRemoteStreamsWithoutExpectedSchema(): Unit = {
    Seq(None, Some(null)).foreach { expectedSchema =>
      var reads = 0
      var reports = 0
      val input = new ByteArrayInputStream(frame) with CometShuffleReadFailureHandler {
        override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
          reads += 1
          super.read(buffer, offset, length)
        }

        override def onShuffleReadFailure(failure: Throwable): Unit = { reports += 1 }
      }
      try {
        val failure =
          try {
            NativeBatchDecoderIterator(
              input,
              new SQLMetric("nsTiming", 0L),
              null,
              null,
              tracingEnabled = false,
              expectedSchema = expectedSchema)
            throw new AssertionError("Expected the missing remote schema to fail")
          } catch {
            case failure: IllegalArgumentException => failure
          }
        assert(failure.getMessage.contains("expected Spark schema"))
        assert(reads == 0)
        assert(reports == 0)
      } finally {
        input.close()
      }
    }
  }

  def doesNotReportAllocationOrImportFailures(): Unit = {
    Seq(
      new OutOfMemoryException("Arrow allocator exhausted"),
      new IllegalArgumentException("Arrow import failed")).foreach { expected =>
      var reports = 0
      val input = new ByteArrayInputStream(frame) with CometShuffleReadFailureHandler {
        override def onShuffleReadFailure(failure: Throwable): Unit = { reports += 1 }
      }
      val util = new NativeUtil {
        override def getNextBatch(
            numOutputCols: Int,
            decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = throw expected
      }
      val decoder = NativeBatchDecoderIterator(
        input,
        new SQLMetric("nsTiming", 0L),
        null,
        util,
        tracingEnabled = false,
        expectedSchema = Some(Array.empty[Byte]))
      try {
        val actual =
          try {
            decoder.hasNext
            throw new AssertionError("Expected the allocation/import failure")
          } catch {
            case failure: RuntimeException => failure
          }
        assert(actual eq expected)
        assert(reports == 0)
      } finally {
        decoder.close()
        util.close()
      }
    }
  }

  def propagatesHeaderAndEofCleanupFailures(): Unit = {
    Seq(false, true).foreach { closeAtEof =>
      val expected =
        if (closeAtEof) new IOException("EOF stream close failed")
        else new EOFException("remote reducer ended during the frame header")
      var reports = 0
      val input = new ByteArrayInputStream(Array.empty[Byte])
        with CometShuffleReadFailureHandler {
        override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
          if (closeAtEof) -1 else throw expected
        }

        override def close(): Unit = {
          if (closeAtEof) throw expected
        }

        override def onShuffleReadFailure(failure: Throwable): Unit = { reports += 1 }
      }
      val decoder = NativeBatchDecoderIterator(
        input,
        new SQLMetric("nsTiming", 0L),
        null,
        null,
        tracingEnabled = false,
        expectedSchema = Some(Array.empty[Byte]))
      try {
        val actual =
          try {
            decoder.hasNext
            throw new AssertionError("Expected the header/cleanup failure")
          } catch {
            case failure: IOException => failure
          }
        assert(actual eq expected)
        assert(reports == 0)
      } finally {
        decoder.close()
      }
    }
  }
}
