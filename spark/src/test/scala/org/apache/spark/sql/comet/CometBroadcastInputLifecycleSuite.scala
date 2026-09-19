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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{CometBroadcastMemoryManager, SparkConf, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometExecIterator

class CometBroadcastInputLifecycleSuite extends CometTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.plugins", "org.apache.spark.CometPlugin")

  test("lazy broadcast inputs are polled on the driving Spark task thread") {
    val results = spark.sparkContext
      .parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val task = TaskContext.get()
        val drivingThread = Thread.currentThread()
        val readThread = new AtomicReference[Thread]()
        val broadcast = new Broadcast[Array[ChunkedByteBuffer]](73L) {
          override protected def getValue(): Array[ChunkedByteBuffer] = {
            readThread.set(Thread.currentThread())
            Array.empty[ChunkedByteBuffer]
          }
          override protected def doUnpersist(blocking: Boolean): Unit = ()
          override protected def doDestroy(blocking: Boolean): Unit = ()
        }
        val owner = CometBroadcastMemoryManager.getOrCreate(1024L * 1024 * 1024)
        Predef.require(owner != null, "test requires a broadcast storage owner")
        val input = new CometBroadcastInput(
          broadcast,
          StructType(Seq(StructField("key", LongType))),
          "broadcast-lifecycle-test",
          task,
          owner)
        val plan =
          CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("key", LongType)), 1).get
        val iterator = new CometExecIterator(
          id = task.taskAttemptId(),
          inputObjects = Array(input.asInstanceOf[Object]),
          numOutputCols = 1,
          protobufQueryPlan = plan.toByteArray,
          nativeMetrics = CometMetricNode(Map.empty),
          numParts = 1,
          partitionIndex = 0)
        try {
          Predef.require(!iterator.hasNext, "the empty broadcast must produce no rows")
          // A lazy broadcast is absent from the ordinary scan list. It must still use the
          // synchronous JNI path so native readers close before task-owned JVM allocators.
          Predef.require(
            readThread.get() eq drivingThread,
            "lazy broadcast was polled outside the driving Spark task thread")
        } finally {
          iterator.close()
        }
        Iterator.single(1)
      }
      .collect()
    assert(results.toSeq == Seq(1))
  }
}
