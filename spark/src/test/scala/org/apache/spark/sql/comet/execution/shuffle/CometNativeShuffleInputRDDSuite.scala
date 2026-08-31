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

import org.apache.spark.{HashPartitioner, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometMetricNode, NativeExecContext}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Ensure the serialized shuffle-map-stage task binary does not grow with the number of
 * partitions.
 *
 * `DAGScheduler.submitMissingTasks` broadcasts the serialized `(stage.rdd, stage.shuffleDep)`
 * pair, which must fit in ~2GB. On the native-shuffle path the scan plan data is one serialized
 * blob per map partition; the leak lived under
 * `CometShuffleDependency.nativeShuffleSpec.execContext`, not on the thin RDD. So this builds
 * both carriers and serializes the pair, which catches a regression in either the RDD or the
 * dependency guards without needing a real 2GB allocation.
 *
 * Lives in the `execution.shuffle` package so it can construct the `private[shuffle]`
 * [[CometNativeShuffleInputRDD]] and the `private[comet]` [[NativeExecContext]] directly.
 */
class CometNativeShuffleInputRDDSuite extends CometTestBase {

  test("spill reporting is registered before native shuffle input producers") {
    Seq(None, Some(new IllegalStateException("failed native shuffle"))).foreach { failure =>
      val writerDisk = new SQLMetric("writerDisk")
      val writerMemory = new SQLMetric("writerMemory")
      val childDisk = new SQLMetric("childDisk")
      val childMemory = new SQLMetric("childMemory")
      val childMetrics =
        CometMetricNode(Map("spilled_bytes" -> childDisk, "memory_spilled_bytes" -> childMemory))
      val taskContext = TaskContext.empty()
      val nestedInput = new RDD[AnyRef](spark.sparkContext, Nil) {
        override protected def getPartitions: Array[Partition] = Array(new Partition {
          override def index: Int = 0
        })

        override def compute(split: Partition, context: TaskContext): Iterator[AnyRef] = {
          context.addTaskCompletionListener[Unit] { _ =>
            childDisk.set(19L)
            childMemory.set(37L)
          }
          Iterator.single(null)
        }
      }
      val writerMetrics =
        Map("spilled_bytes" -> writerDisk, "memory_spilled_bytes" -> writerMemory)
      val inputRDD = new CometNativeShuffleInputRDD(
        spark.sparkContext,
        Seq(nestedInput),
        1,
        Set.empty,
        CometMetricNode(writerMetrics, Seq(childMetrics)))

      inputRDD.iterator(inputRDD.partitions.head, taskContext)
      new CometNativeShuffleWriter[Int, Any](
        NativeShuffleSpec(null, childMetrics, null),
        null,
        Nil,
        writerMetrics,
        1,
        0,
        0L,
        taskContext,
        null)
      taskContext.addTaskCompletionListener[Unit] { _ =>
        writerDisk.set(23L)
        writerMemory.set(41L)
      }
      taskContext.markTaskCompleted(failure)

      assert(taskContext.taskMetrics.diskBytesSpilled == 42L)
      assert(taskContext.taskMetrics.memoryBytesSpilled == 78L)
    }
  }

  test("serialized (rdd, dep) task binary size is independent of partition count") {
    val sc = spark.sparkContext
    val ser = new JavaSerializer(sc.getConf).newInstance()

    // One scan key with a 1KB blob per map partition -- the per-partition plan data. Build both
    // objects the DAGScheduler serializes for a shuffle-map stage: the thin RDD, and the
    // CometShuffleDependency whose NativeShuffleSpec holds a NativeExecContext with this map.
    def build(numPartitions: Int): (
        CometNativeShuffleInputRDD,
        CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch]) = {
      val perPartitionByKey =
        Map("scan-0" -> Array.fill(numPartitions)(new Array[Byte](1024)))
      val childMetricNode = CometMetricNode(Map.empty)
      val writerMetrics = Map(
        "spilled_bytes" -> SQLMetrics.createSizeMetric(sc, "disk spilled bytes"),
        "memory_spilled_bytes" -> SQLMetrics.createSizeMetric(sc, "memory spilled bytes"))
      val rdd = new CometNativeShuffleInputRDD(
        sc,
        inputRDDs = Seq.empty,
        numPartitionsParam = numPartitions,
        blockScanIndices = Set.empty,
        spillMetricNode = CometMetricNode(writerMetrics, Seq(childMetricNode)),
        perPartitionByKey = perPartitionByKey)
      val execContext = NativeExecContext(
        inputs = Seq.empty,
        numPartitions = numPartitions,
        subqueries = Seq.empty,
        broadcastedHadoopConfForEncryption = None,
        encryptedFilePaths = Seq.empty,
        commonByKey = Map.empty,
        perPartitionByKey = perPartitionByKey,
        blockScanIndices = Set.empty,
        hasScanInput = false)
      val spec = NativeShuffleSpec(Operator.getDefaultInstance, childMetricNode, execContext)
      val dep = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        _rdd = rdd,
        partitioner = new HashPartitioner(numPartitions),
        decodeTime = SQLMetrics.createMetric(sc, "decode time"),
        shuffleWriteMetrics = writerMetrics,
        nativeShuffleSpec = Some(spec))
      (rdd, dep)
    }

    // Pre-fix this pair grew from ~13KB to ~10MB between 10 and 10000 partitions. With the map held
    // @transient on both the RDD and the NativeExecContext, the pair stays roughly constant.
    val (_, smallDep) = build(10)
    val (largeRdd, largeDep) = build(10000)
    val smallPair = ser.serialize((smallDep.rdd, smallDep)).limit()
    val largePair = ser.serialize((largeDep.rdd, largeDep)).limit()
    assert(
      math.abs(largePair - smallPair) < 100 * 1024,
      s"serialized (rdd, dep) grew with partition count (small=$smallPair, large=$largePair); " +
        "the per-partition plan-data map is leaking into the broadcast task binary")

    // Each task's Partition object still carries its own slice so the writer can inject plan data.
    val part = largeRdd.partitions(7).asInstanceOf[CometNativeShuffleInputPartition]
    assert(part.planDataByKey.keySet == Set("scan-0"))
    assert(part.planDataByKey("scan-0").length == 1024)
  }
}
