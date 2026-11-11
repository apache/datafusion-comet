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

package org.apache.spark.sql.comet

import java.io.DataInputStream
import java.nio.channels.Channels
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.NativeUtil

/**
 * The value of a Comet broadcast: the Arrow IPC streams that [[Utils.serializeBatches]] wrote,
 * compressed with Spark's codec. It lets the tasks of an executor that read the broadcast at the
 * same time share one decoded copy of it.
 *
 * Without this, every task that reads the broadcast decompresses and decodes all of it. Spark
 * keeps one deserialized copy of a broadcast's value per executor, so the tasks of an executor
 * get the same instance of this class, and the state that they share lives on it. It is not
 * serialized, so each executor's instance starts with no decode.
 *
 * @param chunks
 *   the compressed Arrow IPC streams
 */
private[comet] final class CometBroadcastValue(val chunks: Array[ChunkedByteBuffer])
    extends Serializable {

  // A lazy val rather than a field initializer, because deserializing this class, with Java
  // serialization or with Kryo, does not run its constructor.
  @transient private lazy val shared = new SharedDecode(chunks, SharedDecode.decode)

  /** See [[SharedDecode.batches]]. */
  def batches(context: TaskContext, source: String): Iterator[ColumnarBatch] =
    shared.batches(context, source)

  /** See [[SharedDecode.holders]]. */
  def holders: Int = shared.holders
}

/**
 * One broadcast value's decode, shared by the tasks that read it. The first task to read the
 * value decodes it into buffers that this object owns. Tasks that come while it is decoding wait
 * for it. Each task then reads vectors of its own that hold references on those buffers, so a
 * consumer can close what it reads, as
 * [[org.apache.spark.sql.comet.execution.arrow.ColumnarBatchArrowReader]] does, without affecting
 * another task.
 *
 * The decode is freed when the last task holding it completes, and a task that reads the value
 * after that decodes it again. A native join keeps a build side of one batch, which is what
 * coalescing makes of a broadcast, for as long as its task runs, so for such a broadcast the
 * decode holds no memory that the tasks would not hold anyway. A native join copies a build side
 * of several batches, so the decode keeps one copy of such a broadcast that the tasks would have
 * freed once they built their tables.
 *
 * @param chunks
 *   the compressed Arrow IPC streams of the broadcast value
 * @param decode
 *   decodes the streams; `None` makes each task decode them for itself
 */
private[comet] final class SharedDecode(
    chunks: Array[ChunkedByteBuffer],
    decode: (Array[ChunkedByteBuffer], String) => Option[SharedDecode.Decoded]) {
  import SharedDecode.{Decoded, KillCheckIntervalMs}

  // The decode that running tasks hold, if there are any. Guarded by `this`.
  private var current: Entry = _

  /**
   * The batches of the broadcast value for the task of `context`. As with
   * [[Utils.decodeBatches]], each batch is valid until the next call to `hasNext` or `next`, or
   * until the task completes. A task must read them on its own thread, because that is where its
   * completion listener runs and gives back the buffers.
   */
  def batches(context: TaskContext, source: String): Iterator[ColumnarBatch] =
    new SharedBatchIterator(context, source)

  /** The number of holds on the decode, one for each read by a running task. */
  def holders: Int = synchronized(if (current == null) 0 else current.holders)

  /** Hands out the entry that tasks hold, creating it if none does. True if it did. */
  private def acquire(): (Entry, Boolean) = synchronized {
    val loading = current == null
    if (loading) {
      current = new Entry
    }
    current.holders += 1
    (current, loading)
  }

  /** Gives back a task's hold on `entry`, and frees the entry once no task holds it. */
  private def release(entry: Entry): Unit = {
    val unheld = synchronized {
      entry.holders -= 1
      val unheld = entry.holders == 0
      if (unheld) {
        forget(entry)
      }
      unheld
    }
    if (unheld) {
      entry.close()
    }
  }

  /** Stops handing `entry` out, unless a newer entry has replaced it. Callers hold `this`. */
  private def forget(entry: Entry): Unit = {
    if (current eq entry) {
      current = null
    }
  }

  private final class Entry {
    // One hold for each read of the broadcast by a running task. Guarded by the outer `this`.
    var holders = 0
    private val done = new CountDownLatch(1)
    // Written by the task that loads the entry before `done` opens, and read by others after.
    @volatile private var decoded: Option[Decoded] = None

    /** Decodes the broadcast for every task that holds the entry. Runs in the first of them. */
    def load(source: String): Option[Decoded] = {
      try {
        decoded = decode(chunks, source)
        decoded
      } catch {
        case t: Throwable =>
          // The task may have failed only because it was killed, so let a later task try again.
          // The tasks that are waiting decode the broadcast for themselves.
          SharedDecode.this.synchronized(forget(this))
          throw t
      } finally {
        done.countDown()
      }
    }

    /** Waits for the task that loads the entry, stopping if this task is killed. */
    def await(context: TaskContext): Option[Decoded] = {
      while (!done.await(KillCheckIntervalMs, TimeUnit.MILLISECONDS)) {
        context.killTaskIfInterrupted()
      }
      decoded
    }

    /** Frees the decoded batches. Called once no task holds the entry. */
    def close(): Unit = decoded.foreach(_.close())
  }

  /**
   * One task's read of the broadcast: views of the shared batches when the entry has them, or the
   * task's own decode when it does not.
   */
  private final class SharedBatchIterator(context: TaskContext, source: String)
      extends Iterator[ColumnarBatch] {

    // The task takes its hold on the first call to hasNext, which is also when reading the
    // broadcast without sharing starts to decode it.
    private var started = false
    private var failure: Throwable = _
    private var batches: Iterator[ColumnarBatch] = _
    // The root of the view that `next` returned last, closed by the next call to `hasNext`.
    private var current: VectorSchemaRoot = _

    override def hasNext: Boolean = {
      if (failure != null) {
        throw failure
      }
      if (!started) {
        start()
      }
      closeCurrent()
      batches.hasNext
    }

    override def next(): ColumnarBatch = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      batches.next()
    }

    private def start(): Unit = {
      started = true
      val (entry, loading) = acquire()
      context.addTaskCompletionListener[Unit] { _ =>
        try closeCurrent()
        finally release(entry)
      }
      try {
        batches = (if (loading) entry.load(source) else entry.await(context)) match {
          case Some(decoded) =>
            Iterator.tabulate(decoded.numBatches) { i =>
              current = decoded.view(i)
              NativeUtil.rootAsBatch(current)
            }
          case None => chunks.iterator.flatMap(Utils.decodeBatches(_, source))
        }
      } catch {
        case t: Throwable =>
          failure = t
          throw t
      }
    }

    private def closeCurrent(): Unit = {
      if (current != null) {
        current.close()
        current = null
      }
    }
  }
}

private[comet] object SharedDecode {

  /** How often a task waiting for another task's decode checks whether it has been killed. */
  private val KillCheckIntervalMs = 100L

  /**
   * Decodes the Arrow IPC streams in `chunks`, which [[Utils.serializeBatches]] wrote through
   * Spark's codec, into buffers of an allocator of their own. Returns `None`, having kept
   * nothing, if a stream has dictionaries: [[Utils.coalesceBroadcastBatches]] leaves such streams
   * as it found them, and a stream can redefine a dictionary from one batch to the next, so its
   * batches cannot be read apart from its reader.
   */
  def decode(chunks: Array[ChunkedByteBuffer], source: String): Option[Decoded] = {
    val allocator = CometArrowAllocator.newChildAllocator(s"$source (shared)", 0, Long.MaxValue)
    val batches = ArrayBuffer.empty[(Schema, ArrowRecordBatch)]
    var result: Option[Decoded] = None
    try {
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val streams = chunks.iterator.filter(_.size > 0)
      var supported = true
      while (supported && streams.hasNext) {
        val in = new DataInputStream(codec.compressedInputStream(streams.next().toInputStream()))
        val reader = new ArrowStreamReader(Channels.newChannel(in), allocator)
        try {
          val root = reader.getVectorSchemaRoot
          supported = reader.getDictionaryVectors.isEmpty
          while (supported && reader.loadNextBatch()) {
            // The record batch holds its own references on the buffers that the root drops when
            // it loads the next batch.
            batches += ((root.getSchema, new VectorUnloader(root).getRecordBatch))
          }
        } finally {
          reader.close()
        }
      }
      if (supported) {
        result = Some(new Decoded(allocator, batches.toArray))
      }
      result
    } finally {
      if (result.isEmpty) {
        new Decoded(allocator, batches.toArray).close()
      }
    }
  }

  /**
   * The batches of a broadcast. Each is kept as an `ArrowRecordBatch`, with its schema, and holds
   * references on its buffers, which `allocator` allocated, until it is closed.
   */
  final class Decoded private[SharedDecode] (
      allocator: BufferAllocator,
      batches: Array[(Schema, ArrowRecordBatch)]) {

    def numBatches: Int = batches.length

    /**
     * A root of the caller's own over the buffers of batch `i`. Its vectors take references of
     * their own on those buffers, which closing the root gives back.
     */
    def view(i: Int): VectorSchemaRoot = {
      val (schema, batch) = batches(i)
      val root = VectorSchemaRoot.create(schema, CometArrowAllocator)
      try {
        new VectorLoader(root).load(batch)
        root
      } catch {
        case NonFatal(e) =>
          root.close()
          throw e
      }
    }

    def close(): Unit = {
      try {
        batches.foreach(_._2.close())
      } finally {
        allocator.close()
      }
    }
  }
}
