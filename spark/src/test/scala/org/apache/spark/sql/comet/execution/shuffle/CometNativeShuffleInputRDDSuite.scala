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

import org.apache.spark.HashPartitioner
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometMetricNode, NativeExecContext}
import org.apache.spark.sql.execution.metric.SQLMetrics
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
      val rdd = new CometNativeShuffleInputRDD(
        sc,
        inputRDDs = Seq.empty,
        numPartitionsParam = numPartitions,
        shuffleScanIndices = Set.empty,
        perPartitionByKey = perPartitionByKey)
      val execContext = NativeExecContext(
        inputs = Seq.empty,
        numPartitions = numPartitions,
        subqueries = Seq.empty,
        broadcastedHadoopConfForEncryption = None,
        encryptedFilePaths = Seq.empty,
        commonByKey = Map.empty,
        perPartitionByKey = perPartitionByKey,
        shuffleScanIndices = Set.empty,
        hasScanInput = false)
      val spec =
        NativeShuffleSpec(Operator.getDefaultInstance, CometMetricNode(Map.empty), execContext)
      val dep = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        _rdd = rdd,
        partitioner = new HashPartitioner(numPartitions),
        decodeTime = SQLMetrics.createMetric(sc, "decode time"),
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
