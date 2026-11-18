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
import org.apache.spark.util.{ThreadUtils, Utils => SparkUtils}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.NativeUtil

/**
 * The value of a Comet broadcast: the Arrow IPC streams that [[Utils.serializeBatches]] wrote,
 * compressed with Spark's codec. It lets the tasks of an executor that read the broadcast at the
 * same time, or soon after one another, share one decoded copy of it.
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
  def batches(
      context: TaskContext,
      source: String,
      retention: SharedDecode.Retention): Iterator[ColumnarBatch] =
    shared.batches(context, source, retention)

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
 * When the last task holding the decode completes, the decode is kept for the idle timeout of
 * that task's read, so that a task that comes by then, such as one of the next wave of tasks of
 * the stage, reads it too. The decode is freed once the timeout passes with no task taking it, or
 * at once if the timeout is 0, and a task that reads the value after that decodes it again. The
 * idle decodes of all broadcasts on the executor are capped in bytes: a decode larger than the
 * cap is freed at once, and keeping a decode frees the decodes that have been idle longest, until
 * the idle decodes fit the cap again. A native join keeps a build side of one batch, which is
 * what coalescing makes of a broadcast, for as long as its task runs, so for such a broadcast the
 * decode holds no memory that running tasks would not hold anyway, apart from while it is idle. A
 * native join copies a build side of several batches, so the decode keeps one copy of such a
 * broadcast that the tasks would have freed once they built their tables.
 *
 * @param chunks
 *   the compressed Arrow IPC streams of the broadcast value
 * @param decode
 *   decodes the streams; `None` makes each task decode them for itself
 * @param schedule
 *   runs a function on a thread of its own once a delay in milliseconds has passed
 * @param idleDecodes
 *   the idle decodes that count against the cap, shared with the other broadcasts
 */
private[comet] final class SharedDecode(
    chunks: Array[ChunkedByteBuffer],
    decode: (Array[ChunkedByteBuffer], String) => Option[SharedDecode.Decoded],
    schedule: (Long, () => Unit) => Unit = SharedDecode.schedule,
    idleDecodes: SharedDecode.IdleDecodes = SharedDecode.idleDecodes) {
  import SharedDecode.{Decoded, Idle, KillCheckIntervalMs, Retention}

  // The decode that running tasks hold, or that waits out its idle timeout. Guarded by `this`.
  // An entry is freed only once it is no longer `current`, so an expiry that finds another entry
  // here, or none, frees nothing.
  private var current: Entry = _

  /**
   * The batches of the broadcast value for the task of `context`. As with
   * [[Utils.decodeBatches]], each batch is valid until the next call to `hasNext` or `next`, or
   * until the task completes. A task must read them on its own thread, because that is where its
   * completion listener runs and gives back the buffers. If the task is the last to hold the
   * decode, the decode is kept as `retention` says after the task completes.
   */
  def batches(
      context: TaskContext,
      source: String,
      retention: Retention): Iterator[ColumnarBatch] =
    new SharedBatchIterator(context, source, retention)

  /** The number of holds on the decode, one for each read by a running task. */
  def holders: Int = synchronized(if (current == null) 0 else current.holders)

  /** Hands out the entry, held or idle, creating it if there is none. True if it did. */
  private def acquire(): (Entry, Boolean) = synchronized {
    val loading = current == null
    if (loading) {
      current = new Entry
    } else if (current.holders == 0) {
      idleDecodes.remove(current, current.idlePeriods)
    }
    current.holders += 1
    (current, loading)
  }

  /**
   * Gives back a task's hold on `entry`. Once no task holds the entry, it is kept as `retention`
   * says, or freed at once if it may not be kept.
   */
  private def release(entry: Entry, retention: Retention): Unit = {
    var free = false
    var evicted = Seq.empty[Idle]
    synchronized {
      entry.holders -= 1
      if (entry.holders == 0) {
        if (retention.idleTimeoutMs > 0 && entry.bytes <= retention.maxIdleBytes &&
          (current eq entry)) {
          entry.idlePeriods += 1
          val idlePeriod = entry.idlePeriods
          evicted = idleDecodes.add(
            Idle(entry, idlePeriod, entry.bytes, () => expire(entry, idlePeriod)),
            retention.maxIdleBytes)
          schedule(retention.idleTimeoutMs, () => expire(entry, idlePeriod))
        } else {
          forget(entry)
          free = true
        }
      }
    }
    if (free) {
      entry.close()
    }
    // Outside this object's lock, because each takes the lock of the decode that it frees.
    evicted.foreach(_.expire())
  }

  /** Frees `entry` if no task has taken it since the release that began `idlePeriod`. */
  private def expire(entry: Entry, idlePeriod: Long): Unit = {
    val free = synchronized {
      idleDecodes.remove(entry, idlePeriod)
      val idle = entry.holders == 0 && entry.idlePeriods == idlePeriod && (current eq entry)
      if (idle) {
        current = null
      }
      idle
    }
    if (free) {
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
    // How many times the last holder has given the entry back. An expiry frees the entry only if
    // no task has taken it since the release that scheduled the expiry. Guarded by the outer
    // `this`.
    var idlePeriods = 0L
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

    /** The bytes of the decoded batches, 0 until the entry loads or if it has none. */
    def bytes: Long = decoded.fold(0L)(_.bytes)

    /** Frees the decoded batches. Called once the entry is no longer handed out. */
    def close(): Unit = decoded.foreach(_.close())
  }

  /**
   * One task's read of the broadcast: views of the shared batches when the entry has them, or the
   * task's own decode when it does not.
   */
  private final class SharedBatchIterator(
      context: TaskContext,
      source: String,
      retention: Retention)
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
        finally release(entry, retention)
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
   * How long the last task to hold a decode has it kept, and the cap on the bytes of the idle
   * decodes of the executor that its keeping it may leave.
   */
  final case class Retention(idleTimeoutMs: Long, maxIdleBytes: Long)

  /** Frees a decode as soon as no task holds it. */
  val NoRetention: Retention = Retention(0, 0)

  /**
   * An idle decode, `key` being its entry, kept since the release that began `idlePeriod`.
   * `expire` frees it unless a task has taken it since.
   */
  final case class Idle(key: AnyRef, idlePeriod: Long, bytes: Long, expire: () => Unit)

  /**
   * The idle decodes that count against the cap, oldest first. It takes no other lock, so the
   * decodes may call it while they hold their own.
   */
  final class IdleDecodes {
    private val idle = new java.util.LinkedHashMap[AnyRef, Idle]
    private var idleBytes = 0L

    /** The bytes of the idle decodes. */
    def bytes: Long = synchronized(idleBytes)

    /**
     * Adds `decode`, and removes and returns the decodes that have been idle longest, until the
     * rest fit in `maxBytes`. The caller expires what it returns.
     */
    def add(decode: Idle, maxBytes: Long): Seq[Idle] = synchronized {
      idle.put(decode.key, decode)
      idleBytes += decode.bytes
      val evicted = ArrayBuffer.empty[Idle]
      val oldestFirst = idle.values.iterator
      while (idleBytes > maxBytes && oldestFirst.hasNext) {
        val oldest = oldestFirst.next()
        if (oldest ne decode) {
          oldestFirst.remove()
          idleBytes -= oldest.bytes
          evicted += oldest
        }
      }
      evicted.toSeq
    }

    /** Removes the idle decode of `key`, if it is idle since the release that began `period`. */
    def remove(key: AnyRef, idlePeriod: Long): Unit = synchronized {
      val decode = idle.get(key)
      if (decode != null && decode.idlePeriod == idlePeriod) {
        idle.remove(key)
        idleBytes -= decode.bytes
      }
    }
  }

  /** The idle decodes of the executor. */
  private lazy val idleDecodes = new IdleDecodes

  /** The thread that frees the decodes that no task has taken for their idle timeouts. */
  private lazy val expiries =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("comet-broadcast-decode-expiry")

  /** Runs `f` on the thread of [[expiries]] once `delayMs` has passed, logging what it throws. */
  private def schedule(delayMs: Long, f: () => Unit): Unit = {
    val _ = expiries.schedule(
      new Runnable {
        override def run(): Unit = SparkUtils.tryLogNonFatalError(f())
      },
      delayMs,
      TimeUnit.MILLISECONDS)
  }

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
        result = Some(new Decoded(allocator, batches.toArray, allocator.getAllocatedMemory))
      }
      result
    } finally {
      if (result.isEmpty) {
        new Decoded(allocator, batches.toArray, 0).close()
      }
    }
  }

  /**
   * The batches of a broadcast. Each is kept as an `ArrowRecordBatch`, with its schema, and holds
   * references on its buffers, which `allocator` allocated, until it is closed. `bytes` is what
   * the allocator held once it had decoded them.
   */
  final class Decoded private[SharedDecode] (
      allocator: BufferAllocator,
      batches: Array[(Schema, ArrowRecordBatch)],
      val bytes: Long) {

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
