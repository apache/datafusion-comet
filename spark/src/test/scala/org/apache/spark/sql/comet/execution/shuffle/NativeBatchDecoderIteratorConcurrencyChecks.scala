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

import java.io.{ByteArrayInputStream, IOException}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.{CometShuffleReadFailureHandler, Native}
import org.apache.comet.vector.NativeUtil

/** Exercises one consuming thread racing with task cleanup, without calling native code. */
private[shuffle] object NativeBatchDecoderIteratorConcurrencyChecks {

  private val timeoutSeconds = 10L

  private def await(latch: CountDownLatch): Unit = {
    assert(latch.await(timeoutSeconds, TimeUnit.SECONDS), "Timed out waiting for worker")
  }

  private final class Worker(name: String)(run: => Unit) {
    val finished = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(
      () => {
        try run
        catch {
          case caught: Throwable => failure.set(caught)
        } finally {
          finished.countDown()
        }
      },
      name)
    thread.setDaemon(true)
    thread.start()
  }

  private def join(workers: Seq[Worker]): Unit = {
    // Join every worker before propagating errors so one failure cannot skip another join.
    workers.foreach(_.thread.join(TimeUnit.SECONDS.toMillis(timeoutSeconds)))
    workers.foreach(worker => assert(!worker.thread.isAlive, s"${worker.thread.getName} hung"))
    workers.foreach { worker =>
      Option(worker.failure.get()).foreach(throw _)
    }
  }

  private def awaitBlocked(worker: Worker): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
    while (worker.thread.getState != Thread.State.BLOCKED &&
      worker.finished.getCount != 0 && System.nanoTime() < deadline) {
      Thread.`yield`()
    }
    assert(
      worker.thread.getState == Thread.State.BLOCKED,
      "Cleanup must wait for the consuming thread to leave the decoder monitor")
  }

  private final class TrackingBatch extends ColumnarBatch(Array.empty[ColumnVector], 1) {
    val closeCalls = new AtomicInteger()

    override def close(): Unit = {
      closeCalls.incrementAndGet()
      super.close()
    }
  }

  private class TrackingNative extends Native {
    val handle = 42L
    val createCalls = new AtomicInteger()
    val decodeCalls = new AtomicInteger()
    val releaseCalls = new AtomicInteger()

    override def createRemoteShuffleDecoder(expectedSchema: Array[Byte]): Long = {
      createCalls.incrementAndGet()
      handle
    }

    override def decodeShuffleBlockWithValidation(
        block: ByteBuffer,
        length: Int,
        arrays: Array[Long],
        schemas: Array[Long],
        tracing: Boolean,
        decoderHandle: Long): Long = {
      assert(decoderHandle == handle)
      assert(releaseCalls.get() == 0)
      decodeCalls.incrementAndGet()
      1L
    }

    override def releaseRemoteShuffleDecoder(decoderHandle: Long): Unit = {
      assert(decoderHandle == handle)
      releaseCalls.incrementAndGet()
    }
  }

  private val frame = ByteBuffer
    .allocate(20)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putLong(12L)
    .putLong(0L)
    .putInt(0)
    .array()

  private sealed trait DecodeStage
  private case object CreateDecoder extends DecodeStage
  private case object DecodeBlock extends DecodeStage
  private case object ImportBatch extends DecodeStage

  private def closeWaitsFor(stage: DecodeStage, interruptClose: Boolean = false): Unit = {
    val entered = new CountDownLatch(1)
    val proceed = new CountDownLatch(1)
    def pauseAt(current: DecodeStage): Unit = {
      if (stage == current) {
        entered.countDown()
        await(proceed)
      }
    }

    val batch = new TrackingBatch
    val inputCloseCalls = new AtomicInteger()
    val input = new ByteArrayInputStream(frame) with CometShuffleReadFailureHandler {
      override def close(): Unit = {
        inputCloseCalls.incrementAndGet()
        super.close()
      }

      override def onShuffleReadFailure(failure: Throwable): Unit = throw failure
    }
    val nativeLib = new TrackingNative {
      override def createRemoteShuffleDecoder(expectedSchema: Array[Byte]): Long = {
        val created = super.createRemoteShuffleDecoder(expectedSchema)
        pauseAt(CreateDecoder)
        assert(releaseCalls.get() == 0)
        created
      }

      override def decodeShuffleBlockWithValidation(
          block: ByteBuffer,
          length: Int,
          arrays: Array[Long],
          schemas: Array[Long],
          tracing: Boolean,
          decoderHandle: Long): Long = {
        val rows = super.decodeShuffleBlockWithValidation(
          block,
          length,
          arrays,
          schemas,
          tracing,
          decoderHandle)
        pauseAt(DecodeBlock)
        assert(releaseCalls.get() == 0, "Cleanup released a decoder that is still in use")
        rows
      }
    }
    val util = new NativeUtil {
      override def getNextBatch(
          numOutputCols: Int,
          decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = {
        assert(decode(Array.empty[Long], Array.empty[Long]) == 1L)
        pauseAt(ImportBatch)
        assert(batch.closeCalls.get() == 0)
        Some(batch)
      }
    }
    val decoder = NativeBatchDecoderIterator(
      input,
      new SQLMetric("nsTiming", 0L),
      nativeLib,
      util,
      tracingEnabled = false,
      expectedSchema = Some(Array.empty[Byte]))
    val consumer = new Worker("shuffle decoder")({
      decoder.hasNext
      ()
    })
    var closer: Option[Worker] = None
    try {
      await(entered)
      val cleanup = new Worker("shuffle cleanup")({
        decoder.close()
        if (interruptClose) assert(Thread.currentThread().isInterrupted)
      })
      closer = Some(cleanup)
      awaitBlocked(cleanup)
      if (interruptClose) {
        cleanup.thread.interrupt()
        awaitBlocked(cleanup)
      }
      assert(nativeLib.releaseCalls.get() == 0)
      assert(batch.closeCalls.get() == 0)
      assert(inputCloseCalls.get() == 0)
    } finally {
      proceed.countDown()
      try join(Seq(consumer) ++ closer)
      finally {
        decoder.close()
        util.close()
      }
    }
    assert(nativeLib.createCalls.get() == 1)
    assert(nativeLib.decodeCalls.get() == 1)
    assert(nativeLib.releaseCalls.get() == 1)
    assert(batch.closeCalls.get() == 1, "Cleanup missed the batch produced by the active read")
    assert(inputCloseCalls.get() == 1)
    assert(!decoder.hasNext)
  }

  def closeWaitsForDecoderCreation(): Unit = closeWaitsFor(CreateDecoder)

  def closeWaitsForNativeDecoding(): Unit = {
    closeWaitsFor(DecodeBlock)
    closeWaitsFor(DecodeBlock, interruptClose = true)
  }

  def closeWaitsForBatchImportAndPublication(): Unit = closeWaitsFor(ImportBatch)

  def closeProceedsDuringFailureReporting(): Unit = {
    val reporting = new CountDownLatch(1)
    val finishReporting = new CountDownLatch(1)
    val importCleanedUp = new AtomicBoolean()
    val cleanupBeforeReporting = new AtomicBoolean()
    val decodeFailure = new IOException("native decode failed")
    val reportedFailure = new IOException("remote fetch failed", decodeFailure)
    val inputCloseCalls = new AtomicInteger()
    val input = new ByteArrayInputStream(frame) with CometShuffleReadFailureHandler {
      override def onShuffleReadFailure(failure: Throwable): Unit = {
        assert(failure eq decodeFailure)
        cleanupBeforeReporting.set(importCleanedUp.get())
        reporting.countDown()
        await(finishReporting)
        throw reportedFailure
      }

      override def close(): Unit = {
        inputCloseCalls.incrementAndGet()
        super.close()
      }
    }
    val nativeLib = new TrackingNative {
      override def decodeShuffleBlockWithValidation(
          block: ByteBuffer,
          length: Int,
          arrays: Array[Long],
          schemas: Array[Long],
          tracing: Boolean,
          decoderHandle: Long): Long = throw decodeFailure
    }
    val util = new NativeUtil {
      override def getNextBatch(
          numOutputCols: Int,
          decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = {
        try {
          decode(Array.empty[Long], Array.empty[Long])
          None
        } finally {
          importCleanedUp.set(true)
        }
      }
    }
    val decoder = NativeBatchDecoderIterator(
      input,
      new SQLMetric("nsTiming", 0L),
      nativeLib,
      util,
      tracingEnabled = false,
      expectedSchema = Some(Array.empty[Byte]))
    val consumer = new Worker("shuffle failure reporting")({
      try {
        decoder.hasNext
        throw new AssertionError("Expected the remote fetch failure")
      } catch {
        case caught: IOException => assert(caught eq reportedFailure)
      }
    })
    var closer: Option[Worker] = None
    try {
      await(reporting)
      assert(
        cleanupBeforeReporting.get(),
        "Arrow import must unwind before reporting data errors")
      val cleanup = new Worker("shuffle failure cleanup")({ decoder.close() })
      closer = Some(cleanup)
      await(cleanup.finished)
      assert(nativeLib.releaseCalls.get() == 1)
      assert(inputCloseCalls.get() == 1)
    } finally {
      finishReporting.countDown()
      try join(Seq(consumer) ++ closer)
      finally {
        decoder.close()
        util.close()
      }
    }
    assert(nativeLib.releaseCalls.get() == 1)
    assert(inputCloseCalls.get() == 1)
    assert(!decoder.hasNext)
  }

  def closeUnblocksTransportAndPreventsDecoding(): Unit = {
    val readingBody = new CountDownLatch(1)
    val streamClosed = new CountDownLatch(1)
    val inputCloseCalls = new AtomicInteger()
    val input = new ByteArrayInputStream(frame) with CometShuffleReadFailureHandler {
      override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
        if (available() == 4) {
          readingBody.countDown()
          await(streamClosed)
        }
        // A transport read can finish successfully even after cleanup has closed the stream.
        super.read(bytes, offset, length)
      }

      override def close(): Unit = {
        inputCloseCalls.incrementAndGet()
        streamClosed.countDown()
        super.close()
      }

      override def onShuffleReadFailure(failure: Throwable): Unit = throw failure
    }
    val nativeLib = new TrackingNative
    val importCalls = new AtomicInteger()
    val util = new NativeUtil {
      override def getNextBatch(
          numOutputCols: Int,
          decode: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = {
        importCalls.incrementAndGet()
        None
      }
    }
    val decoder = NativeBatchDecoderIterator(
      input,
      new SQLMetric("nsTiming", 0L),
      nativeLib,
      util,
      tracingEnabled = false,
      expectedSchema = Some(Array.empty[Byte]))
    val consumer = new Worker("shuffle transport read")({ assert(!decoder.hasNext) })
    var closer: Option[Worker] = None
    try {
      await(readingBody)
      val cleanup = new Worker("shuffle transport cleanup")({ decoder.close() })
      closer = Some(cleanup)
      await(cleanup.finished)
      await(consumer.finished)
    } finally {
      streamClosed.countDown()
      try join(Seq(consumer) ++ closer)
      finally {
        decoder.close()
        util.close()
      }
    }
    assert(inputCloseCalls.get() == 1)
    assert(nativeLib.createCalls.get() == 0)
    assert(nativeLib.decodeCalls.get() == 0)
    assert(nativeLib.releaseCalls.get() == 0)
    assert(importCalls.get() == 0)
    assert(!decoder.hasNext)
  }
}
