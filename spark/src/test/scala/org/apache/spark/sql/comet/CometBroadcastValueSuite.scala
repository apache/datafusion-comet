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

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

import org.apache.arrow.vector.{IntVector, VarCharVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.apache.spark.{SparkConf, TaskContextImpl, TaskKilledException}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.{CometArrowAllocator, CometKryoRegistrator}
import org.apache.comet.vector.CometVector

class CometBroadcastValueSuite extends CometTestBase {

  private val Timeout = 30.seconds

  test("tasks that read a broadcast at the same time decode it once") {
    val value = broadcastValue(rows(1000))
    val (shared, decodes) = countingDecode(value)
    val before = CometArrowAllocator.getAllocatedMemory
    val tasks = (1 to 4).map(i => newTask(i.toLong))

    assert(readAll(shared.batches(tasks.head, "test")) == rows(1000))
    val sharedCopy = CometArrowAllocator.getAllocatedMemory - before
    assert(sharedCopy > 0)
    tasks.tail.init.foreach { task =>
      assert(readAll(shared.batches(task, "test")) == rows(1000))
    }
    assert(decodes.get == 1)
    assert(shared.holders == 3)
    // Each task closed what it read, the way the native input reader does, so the one shared
    // copy is all that the three tasks hold.
    assert(CometArrowAllocator.getAllocatedMemory - before == sharedCopy)

    // A consumer that leaves its batches open, as the JVM columnar-to-row path does, has them
    // closed when it moves on and when its task completes.
    assert(readAll(shared.batches(tasks.last, "test"), close = false) == rows(1000))
    assert(decodes.get == 1)

    // The last task to complete frees the decode, and a task that comes later decodes again.
    tasks.init.foreach(_.markTaskCompleted(None))
    assert(shared.holders == 1)
    assert(CometArrowAllocator.getAllocatedMemory > before)
    tasks.last.markTaskCompleted(None)
    assert(shared.holders == 0)
    assert(CometArrowAllocator.getAllocatedMemory == before)

    val later = newTask(5)
    assert(readAll(shared.batches(later, "test")) == rows(1000))
    assert(decodes.get == 2)
    later.markTaskCompleted(None)
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("a task that stops reading part way frees what it read when it completes") {
    // Without coalescing, so the broadcast has a batch after the one the task stops at.
    val value =
      new CometBroadcastValue((serialize(rows(100)) ++ serialize(rows(200).drop(100))).toArray)
    val before = CometArrowAllocator.getAllocatedMemory
    val task = newTask(1)
    val first = value.batches(task, "test").next()
    assert(first.numRows() == 100)
    task.markTaskCompleted(None)
    assert(value.holders == 0)
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("a task that comes while another task decodes the broadcast waits for that decode") {
    val gated = new GatedDecode(broadcastValue(rows(1000)))
    val before = CometArrowAllocator.getAllocatedMemory
    val loader = newTask(1)
    val waiter = newTask(2)

    val loaded = gated.startLoad(loader)
    val waited = inThread(readAll(gated.shared.batches(waiter, "test")))
    gated.awaitHolders(2)
    Thread.sleep(200)
    assert(!waited.isCompleted)

    gated.finish.countDown()
    assert(Await.result(loaded, Timeout) == rows(1000))
    assert(Await.result(waited, Timeout) == rows(1000))
    assert(gated.decodes.get == 1)

    loader.markTaskCompleted(None)
    waiter.markTaskCompleted(None)
    assert(gated.shared.holders == 0)
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("a task killed while it waits for another task's decode stops waiting") {
    val gated = new GatedDecode(broadcastValue(rows(100)))
    val before = CometArrowAllocator.getAllocatedMemory
    val loader = newTask(1)
    val waiter = newTask(2)

    val loaded = gated.startLoad(loader)
    val waited = inThread(gated.shared.batches(waiter, "test").hasNext)
    gated.awaitHolders(2)
    waiter.markInterrupted("killed by the test")
    val killed = intercept[TaskKilledException](Await.result(waited, Timeout))
    assert(killed.reason == "killed by the test")
    waiter.markTaskCompleted(Some(killed))

    gated.finish.countDown()
    assert(Await.result(loaded, Timeout) == rows(100))
    loader.markTaskCompleted(None)
    assert(gated.shared.holders == 0)
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("tasks waiting for a decode that fails decode the broadcast for themselves") {
    val gated = new GatedDecode(broadcastValue(rows(1000)), failFirst = true)
    val before = CometArrowAllocator.getAllocatedMemory
    val loader = newTask(1)
    val waiter = newTask(2)

    val loaded = gated.startLoad(loader)
    val waited = inThread(readAll(gated.shared.batches(waiter, "test")))
    gated.awaitHolders(2)
    gated.finish.countDown()
    val failure = intercept[IllegalStateException](Await.result(loaded, Timeout))
    assert(failure.getMessage == "decode failed")
    assert(Await.result(waited, Timeout) == rows(1000))
    assert(gated.decodes.get == 1, "the waiting task decoded the broadcast through the entry")

    // The failed entry is no longer handed out, so the next task decodes the broadcast again.
    val later = newTask(3)
    assert(readAll(gated.shared.batches(later, "test")) == rows(1000))
    assert(gated.decodes.get == 2)

    loader.markTaskCompleted(Some(failure))
    waiter.markTaskCompleted(None)
    later.markTaskCompleted(None)
    assert(gated.shared.holders == 0)
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("each task decodes a broadcast with dictionaries for itself") {
    val values = Seq("a", "b", null, "a", "c", "b")
    val value = dictionaryBroadcastValue(values)
    val before = CometArrowAllocator.getAllocatedMemory
    assert(SharedDecode.decode(value, "test").isEmpty)
    assert(CometArrowAllocator.getAllocatedMemory == before)

    val (shared, decodes) = countingDecode(value)
    val tasks = (1 to 2).map(i => newTask(i.toLong))
    tasks.foreach { task =>
      val strings = shared.batches(task, "test").flatMap { batch =>
        (0 until batch.numRows()).map { i =>
          if (batch.column(0).isNullAt(i)) null else batch.column(0).getUTF8String(i).toString
        }
      }
      assert(strings.toList == values)
    }
    assert(decodes.get == 1)
    tasks.foreach(_.markTaskCompleted(None))
    assert(shared.holders == 0)
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("a broadcast of several streams reads every batch of each") {
    val batches = Seq(rows(10), rows(300).drop(10), Seq.empty, rows(1000).drop(300))
    // Without coalescing, as for the streams that coalescing leaves as it found them.
    val value = batches.flatMap(serialize).toArray
    val (shared, decodes) = countingDecode(value)
    val before = CometArrowAllocator.getAllocatedMemory
    val tasks = (1 to 2).map(i => newTask(i.toLong))
    // The second task leaves each batch open, so moving on to the next one has to close it.
    tasks.zip(Seq(true, false)).foreach { case (task, close) =>
      assert(readAll(shared.batches(task, "test"), close) == rows(1000))
    }
    assert(decodes.get == 1)
    tasks.foreach(_.markTaskCompleted(None))
    assert(shared.holders == 0)
    assert(CometArrowAllocator.getAllocatedMemory == before)
  }

  test("zero-column batches keep their row counts, and an empty broadcast has no batches") {
    val empty = (1 to 3).map(_ => new ColumnarBatch(Array.empty[ColumnVector], 5))
    val serialized = Utils.serializeBatches(empty.iterator).map(_._2).toList
    val value = new CometBroadcastValue(Utils.coalesceBroadcastBatches(serialized.iterator)._1)
    val task = newTask(1)
    assert(value.batches(task, "test").map(_.numRows()).sum == 15)
    assert(!new CometBroadcastValue(Array.empty).batches(task, "test").hasNext)
    task.markTaskCompleted(None)
    assert(value.holders == 0)
  }

  test("a broadcast value survives Java and Kryo serialization with no decode") {
    val value = new CometBroadcastValue(broadcastValue(rows(100)))
    // Decoding first gives the instance state that must not travel with it.
    val task = newTask(1)
    assert(readAll(value.batches(task, "test")) == rows(100))
    val serializers = Seq(
      new JavaSerializer(new SparkConf()),
      new KryoSerializer(
        new SparkConf()
          .set("spark.kryo.registrationRequired", "true")
          .set("spark.kryo.registrator", CometKryoRegistrator.CLASS_NAME)))
    try {
      serializers.foreach { serializer =>
        val instance = serializer.newInstance()
        val copy = instance.deserialize[CometBroadcastValue](instance.serialize(value))
        assert(copy.holders == 0)
        val copyTask = newTask(2)
        assert(readAll(copy.batches(copyTask, "test")) == rows(100))
        copyTask.markTaskCompleted(None)
        assert(copy.holders == 0)
      }
    } finally {
      task.markTaskCompleted(None)
    }
  }

  test("CometBatchRDD reads through the shared decode only when asked to") {
    val broadcast =
      spark.sparkContext.broadcast(new CometBroadcastValue(broadcastValue(rows(100))))
    try {
      Seq(true, false).foreach { share =>
        val rdd = new CometBatchRDD(spark.sparkContext, 2, broadcast, share)
        val tasks = (1 to 2).map(i => newTask(i.toLong))
        rdd.partitions.zip(tasks).foreach { case (partition, task) =>
          assert(readAll(rdd.compute(partition, task)) == rows(100))
        }
        assert(broadcast.value.holders == (if (share) 2 else 0))
        tasks.foreach(_.markTaskCompleted(None))
        assert(broadcast.value.holders == 0)
      }
    } finally {
      broadcast.destroy()
    }
  }

  test("broadcast joins over nested columns release the shared decode") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 4000).map(i => (i % 400, s"fact-$i")), "fact") {
        val dim = (0 until 300).map { i =>
          (i, if (i % 9 == 0) null else s"dim-$i", (i, s"s$i"), Seq(i, i + 1))
        }
        withParquetTable(dim, "dim") {
          // The repartition gives the probe side eight tasks, which read the broadcast at once.
          val fact = "(SELECT /*+ REPARTITION(8) */ * FROM fact) f"
          Seq(
            (
              s"SELECT /*+ BROADCAST(d) */ * FROM $fact JOIN dim d ON f._1 = d._1",
              classOf[CometBroadcastHashJoinExec]),
            (
              s"SELECT /*+ BROADCAST(d) */ * FROM $fact JOIN dim d ON f._1 < d._1 AND d._1 < 5",
              classOf[CometBroadcastNestedLoopJoinExec])).foreach { case (query, join) =>
            val before = CometArrowAllocator.getAllocatedMemory
            checkSparkAnswerAndOperator(
              sql(query),
              Seq(classOf[CometBroadcastExchangeExec], join))
            assert(
              CometArrowAllocator.getAllocatedMemory == before,
              "a completed task still holds a broadcast")
          }
        }
      }
    }
  }

  /** `count` rows of an int and a string column, both null in every seventh row. */
  private def rows(count: Int): Seq[(Any, Any)] =
    (0 until count).map(i => if (i % 7 == 0) (null, null) else (i, s"value-$i"))

  /** Serializes `rows` as one batch, the way a Comet broadcast collects each of its batches. */
  private def serialize(rows: Seq[(Any, Any)]): Seq[ChunkedByteBuffer] = {
    val ints = new IntVector("i", CometArrowAllocator)
    val strings = new VarCharVector("s", CometArrowAllocator)
    ints.allocateNew(rows.length)
    strings.allocateNew()
    rows.zipWithIndex.foreach {
      case ((null, null), i) =>
        ints.setNull(i)
        strings.setNull(i)
      case ((n: Int, s: String), i) =>
        ints.set(i, n)
        strings.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
      case (row, _) => fail(s"unexpected row $row")
    }
    ints.setValueCount(rows.length)
    strings.setValueCount(rows.length)
    val columns = Array[ColumnVector](
      CometVector.getVector(ints, null: DictionaryProvider),
      CometVector.getVector(strings, null: DictionaryProvider))
    val batch = new ColumnarBatch(columns, rows.length)
    try Utils.serializeBatches(Iterator(batch)).map(_._2).filter(_.size > 0).toList
    finally batch.close()
  }

  /** `rows` as the value of a Comet broadcast, coalesced into one stream. */
  private def broadcastValue(rows: Seq[(Any, Any)]): Array[ChunkedByteBuffer] =
    Utils.coalesceBroadcastBatches(serialize(rows).iterator)._1

  /** A broadcast of one dictionary-encoded string column, which coalescing leaves as it is. */
  private def dictionaryBroadcastValue(values: Seq[String]): Array[ChunkedByteBuffer] = {
    val distinct = values.filter(_ != null).distinct
    val dictionaryValues = new VarCharVector("dictionary", CometArrowAllocator)
    dictionaryValues.allocateNew()
    distinct.zipWithIndex.foreach { case (s, i) =>
      dictionaryValues.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
    }
    dictionaryValues.setValueCount(distinct.length)
    val indexType = new ArrowType.Int(32, true)
    val encoding = new DictionaryEncoding(0L, false, indexType)
    val provider =
      new DictionaryProvider.MapDictionaryProvider(new Dictionary(dictionaryValues, encoding))
    val indices =
      new IntVector("s", new FieldType(true, indexType, encoding), CometArrowAllocator)
    indices.allocateNew(values.length)
    values.zipWithIndex.foreach {
      case (null, i) => indices.setNull(i)
      case (s, i) => indices.set(i, distinct.indexOf(s))
    }
    indices.setValueCount(values.length)
    val batch =
      new ColumnarBatch(
        Array[ColumnVector](CometVector.getVector(indices, provider)),
        values.length)
    try {
      val serialized = Utils.serializeBatches(Iterator(batch)).map(_._2).toList
      val (value, coalesced, _) = Utils.coalesceBroadcastBatches(serialized.iterator)
      assert(coalesced == 0, "expected coalescing to leave the dictionary stream as it is")
      value
    } finally {
      batch.close()
      dictionaryValues.close()
    }
  }

  /**
   * Reads every row. By default it closes each batch after reading it, as the native input reader
   * does.
   */
  private def readAll(batches: Iterator[ColumnarBatch], close: Boolean = true): Seq[(Any, Any)] =
    batches.flatMap { batch =>
      try {
        (0 until batch.numRows()).map { i =>
          val n = if (batch.column(0).isNullAt(i)) null else batch.column(0).getInt(i)
          val s =
            if (batch.column(1).isNullAt(i)) null else batch.column(1).getUTF8String(i).toString
          (n, s)
        }
      } finally {
        if (close) {
          batch.close()
        }
      }
    }.toList

  private def countingDecode(chunks: Array[ChunkedByteBuffer]): (SharedDecode, AtomicInteger) = {
    val decodes = new AtomicInteger
    val shared = new SharedDecode(
      chunks,
      (chunks, source) => {
        decodes.incrementAndGet()
        SharedDecode.decode(chunks, source)
      })
    (shared, decodes)
  }

  private def newTask(taskAttemptId: Long): TaskContextImpl =
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = taskAttemptId.toInt,
      numPartitions = 8,
      taskAttemptId = taskAttemptId,
      attemptNumber = 0,
      taskMemoryManager =
        new TaskMemoryManager(new TestMemoryManager(new SparkConf()), taskAttemptId),
      localProperties = new Properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty)

  /** Runs `f` on a thread of its own, as a task running beside the test's would. */
  private def inThread[T](f: => T): Future[T] = {
    val result = Promise[T]()
    val thread = new Thread(() => result.complete(Try(f)))
    thread.setDaemon(true)
    thread.start()
    result.future
  }

  /**
   * A shared decode of `chunks` whose first decode signals `decoding` and waits for `finish`,
   * then fails if `failFirst`. Later decodes go straight through, and `decodes` counts all of
   * them.
   */
  private class GatedDecode(chunks: Array[ChunkedByteBuffer], failFirst: Boolean = false) {
    private val decoding = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val decodes = new AtomicInteger
    val shared = new SharedDecode(
      chunks,
      (chunks, source) => {
        if (decodes.incrementAndGet() == 1) {
          decoding.countDown()
          finish.await()
          if (failFirst) {
            throw new IllegalStateException("decode failed")
          }
        }
        SharedDecode.decode(chunks, source)
      })

    /**
     * Starts `task`'s read of the broadcast on a thread of its own and waits until it decodes.
     */
    def startLoad(task: TaskContextImpl): Future[Seq[(Any, Any)]] = {
      val loaded = inThread(readAll(shared.batches(task, "test")))
      assert(decoding.await(Timeout.toSeconds, TimeUnit.SECONDS))
      loaded
    }

    def awaitHolders(holders: Int): Unit =
      eventually(timeout(Timeout), interval(10.millis)) {
        assert(shared.holders == holders)
      }
  }
}
