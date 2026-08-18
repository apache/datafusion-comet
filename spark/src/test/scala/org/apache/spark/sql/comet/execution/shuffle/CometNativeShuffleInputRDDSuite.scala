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

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.CometTestBase

/**
 * Ensure that serialized RDD does not overflow in size with a very large number of partitions
 * Lives in the `execution.shuffle` package so it can construct the `private[shuffle]`
 * [[CometNativeShuffleInputRDD]] directly.
 */
class CometNativeShuffleInputRDDSuite extends CometTestBase {

  test("serialized RDD size is independent of partition count") {
    val sc = spark.sparkContext
    val ser = new JavaSerializer(sc.getConf).newInstance()

    // One scan key with a 1KB blob per map partition. Pre-fix the whole array serializes into the
    // RDD, so 10000 partitions add ~10MB versus 10.
    def buildRDD(numPartitions: Int): CometNativeShuffleInputRDD = {
      val perPartitionByKey =
        Map("scan-0" -> Array.fill(numPartitions)(new Array[Byte](1024)))
      new CometNativeShuffleInputRDD(
        sc,
        inputRDDs = Seq.empty,
        numPartitionsParam = numPartitions,
        shuffleScanIndices = Set.empty,
        perPartitionByKey = perPartitionByKey)
    }

    val smallSize = ser.serialize(buildRDD(10)).limit()
    val large = buildRDD(10000)
    val largeSize = ser.serialize(large).limit()
    assert(
      math.abs(largeSize - smallSize) < 100 * 1024,
      s"serialized RDD grew with partition count (small=$smallSize, large=$largeSize); " +
        "perPartitionByKey is leaking into the broadcast task binary")

    // Each task's Partition object still carries its own slice so the writer can inject plan data.
    val part = large.partitions(7).asInstanceOf[CometNativeShuffleInputPartition]
    assert(part.planDataByKey.keySet == Set("scan-0"))
    assert(part.planDataByKey("scan-0").length == 1024)
    val partSize = ser.serialize(part).limit()
    assert(
      partSize < 100 * 1024,
      s"partition slice unexpectedly large ($partSize); it must hold one blob, not the full array")
  }
}
