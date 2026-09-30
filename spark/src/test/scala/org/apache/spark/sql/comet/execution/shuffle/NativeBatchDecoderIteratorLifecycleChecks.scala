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

  private class TrackingNative extends Native {
    val handle = 42L
    var createCalls = 0
    var releaseCalls = 0

    override def createRemoteShuffleDecoder(expectedSchema: Array[Byte]): Long = {
      createCalls += 1
      handle
    }

    override def releaseRemoteShuffleDecoder(decoderHandle: Long): Unit = {
      assert(decoderHandle == handle)
      releaseCalls += 1
    }
  }

  private final class TrackingRemoteInput(
      bytes: Array[Byte],
      closeFailure: Option[Throwable] = None)
      extends ByteArrayInputStream(bytes)
      with CometShuffleReadFailureHandler {
    var closeCalls = 0
    var reports = 0

    override def close(): Unit = {
      closeCalls += 1
      super.close()
      closeFailure.foreach(throw _)
    }

    override def onShuffleReadFailure(failure: Throwable): Unit = {
      reports += 1
      throw failure
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
    val nativeLib = new TrackingNative {
      override def decodeShuffleBlockWithValidation(
          block: ByteBuffer,
          length: Int,
          arrays: Array[Long],
          schemas: Array[Long],
          tracing: Boolean,
          decoderHandle: Long): Long = {
        assert(decoderHandle == handle)
        throw decodeFailure
      }
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
      decoder.close()
      assert(nativeLib.createCalls == 1)
      assert(nativeLib.releaseCalls == 1)
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
    Seq(
      false -> None,
      false -> Some(intSchema),
      true -> Some(Array.empty[Byte]),
      true -> Some(intSchema)).foreach { case (remote, expectedSchema) =>
      var localCalls = 0
      var validatedCalls = 0
      val input =
        if (remote) {
          new ByteArrayInputStream(frame ++ frame) with CometShuffleReadFailureHandler {
            override def onShuffleReadFailure(failure: Throwable): Unit = throw failure
          }
        } else {
          new ByteArrayInputStream(frame ++ frame)
        }
      val batches = Seq.fill(2)(new TrackingBatch())
      val pending = batches.iterator
      val util = new NativeUtil {
        override def getNextBatch(
            numOutputCols: Int,
            decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = {
          assert(decode(Array.empty[Long], Array.empty[Long]) == 1L)
          Some(pending.next())
        }
      }
      val nativeLib = new TrackingNative {
        override def createRemoteShuffleDecoder(schema: Array[Byte]): Long = {
          assert(expectedSchema.exists(_.sameElements(schema)))
          super.createRemoteShuffleDecoder(schema)
        }

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
            decoderHandle: Long): Long = {
          assert(decoderHandle == handle)
          assert(createCalls == 1)
          assert(releaseCalls == 0)
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
        assert(nativeLib.createCalls == 0)
        batches.foreach { batch =>
          assert(decoder.hasNext)
          assert(decoder.hasNext)
          assert(decoder.next() eq batch)
        }
        assert(!decoder.hasNext)
        assert(localCalls == (if (remote) 0 else 2))
        assert(validatedCalls == (if (remote) 2 else 0))
      } finally {
        decoder.close()
        util.close()
      }
      assert(batches.forall(_.closeCalls == 1))
      assert(nativeLib.createCalls == (if (remote) 1 else 0))
      assert(nativeLib.releaseCalls == (if (remote) 1 else 0))
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

  def doesNotAllocateUnusedRemoteDecoder(): Unit = {
    Seq(false, true).foreach { closeBeforeReading =>
      val input = new TrackingRemoteInput(if (closeBeforeReading) frame else Array.empty[Byte])
      val nativeLib = new TrackingNative
      val decoder = NativeBatchDecoderIterator(
        input,
        new SQLMetric("nsTiming", 0L),
        nativeLib,
        null,
        tracingEnabled = false,
        expectedSchema = Some(Array.empty[Byte]))
      if (!closeBeforeReading) assert(!decoder.hasNext)
      decoder.close()
      decoder.close()
      assert(!decoder.hasNext)
      assert(nativeLib.createCalls == 0)
      assert(nativeLib.releaseCalls == 0)
      assert(input.closeCalls == 1)
      assert(input.reports == 0)
    }
  }

  def preservesRemoteDecoderCleanupFailures(): Unit = {
    for (delivered <- Seq(false, true); failBatch <- Seq(false, true)) {
      val batchFailure = new IOException("batch release failed")
      val nativeFailure = new IOException("native decoder release failed")
      val streamFailure = new IOException("stream release failed")
      val batch = new TrackingBatch(if (failBatch) Some(batchFailure) else None)
      val input = new TrackingRemoteInput(frame, Some(streamFailure))
      val nativeLib = new TrackingNative {
        override def releaseRemoteShuffleDecoder(decoderHandle: Long): Unit = {
          super.releaseRemoteShuffleDecoder(decoderHandle)
          throw nativeFailure
        }
      }
      val util = new NativeUtil {
        override def getNextBatch(
            numOutputCols: Int,
            decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = Some(batch)
      }
      val decoder = NativeBatchDecoderIterator(
        input,
        new SQLMetric("nsTiming", 0L),
        nativeLib,
        util,
        tracingEnabled = false,
        expectedSchema = Some(Array.empty[Byte]))
      try {
        assert(decoder.hasNext)
        if (delivered) assert(decoder.next() eq batch)
        val caught =
          try {
            decoder.close()
            throw new AssertionError("Expected a resource release failure")
          } catch {
            case failure: IOException => failure
          }
        assert(caught eq (if (failBatch) batchFailure else nativeFailure))
        val suppressed =
          if (failBatch) Seq(nativeFailure, streamFailure) else Seq(streamFailure)
        assert(caught.getSuppressed.toSeq == suppressed)
      } finally {
        decoder.close()
        util.close()
      }
      assert(batch.closeCalls == 1)
      assert(nativeLib.createCalls == 1)
      assert(nativeLib.releaseCalls == 1)
      assert(input.closeCalls == 1)
      assert(input.reports == 0)
      assert(!decoder.hasNext)
    }
  }

  def doesNotReportRemoteDecoderCreationFailures(): Unit = {
    val expected = new IllegalArgumentException("invalid expected Spark schema")
    val input = new TrackingRemoteInput(frame)
    val nativeLib = new TrackingNative {
      override def createRemoteShuffleDecoder(schema: Array[Byte]): Long = {
        super.createRemoteShuffleDecoder(schema)
        throw expected
      }
    }
    val decoder = NativeBatchDecoderIterator(
      input,
      new SQLMetric("nsTiming", 0L),
      nativeLib,
      null,
      tracingEnabled = false,
      expectedSchema = Some(Array.empty[Byte]))
    try {
      val caught =
        try {
          decoder.hasNext
          throw new AssertionError("Expected the native decoder creation failure")
        } catch {
          case failure: IllegalArgumentException => failure
        }
      assert(caught eq expected)
    } finally {
      decoder.close()
    }
    assert(nativeLib.createCalls == 1)
    assert(nativeLib.releaseCalls == 0)
    assert(input.closeCalls == 1)
    assert(input.reports == 0)
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
      val nativeLib = new TrackingNative
      val decoder = NativeBatchDecoderIterator(
        input,
        new SQLMetric("nsTiming", 0L),
        nativeLib,
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
      assert(nativeLib.createCalls == 1)
      assert(nativeLib.releaseCalls == 1)
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
