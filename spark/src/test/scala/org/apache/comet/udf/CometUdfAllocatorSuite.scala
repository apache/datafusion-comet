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

package org.apache.comet.udf

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector
import org.apache.spark.TaskContext
import org.apache.spark.comet.CometTaskContextShim
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

class CometUdfAllocatorSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("CometUdfAllocatorSuite")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "64m")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    super.afterAll()
  }

  test("acquire registers a MemoryConsumer that charges the task") {
    val sc = spark.sparkContext
    val taskMemBefore: LongAccumulator = sc.longAccumulator("taskMemBefore")
    val taskMemDuring: LongAccumulator = sc.longAccumulator("taskMemDuring")
    val taskMemAfter: LongAccumulator = sc.longAccumulator("taskMemAfter")

    sc.parallelize(Seq(0), 1).foreachPartition { _: Iterator[Int] =>
      val ctx = TaskContext.get()
      // TaskContext.taskMemoryManager() is private[spark]; use the shim from a non-spark package.
      val tmm = CometTaskContextShim.taskMemoryManager(ctx)
      taskMemBefore.add(tmm.getMemoryConsumptionForThisTask)

      val allocator: BufferAllocator = CometUdfAllocator.acquire(ctx)
      val vec = new IntVector("test", allocator)
      try {
        vec.allocateNew(1024)
        vec.setValueCount(1024)
        taskMemDuring.add(tmm.getMemoryConsumptionForThisTask)
      } finally {
        vec.close()
      }
      taskMemAfter.add(tmm.getMemoryConsumptionForThisTask)
    }

    assert(
      taskMemDuring.value > taskMemBefore.value,
      "task memory should grow while UDF allocator holds buffers; " +
        s"before=${taskMemBefore.value} during=${taskMemDuring.value}")
    assert(
      taskMemAfter.value == taskMemBefore.value,
      "task memory should be released after the allocation closes; " +
        s"before=${taskMemBefore.value} after=${taskMemAfter.value}")
  }

  test("child allocator is closed and uncached on task completion") {
    val sc = spark.sparkContext
    val cacheSizeDuring: LongAccumulator = sc.longAccumulator("cacheSizeDuring")

    sc.parallelize(Seq(0), 1).foreachPartition { _: Iterator[Int] =>
      val ctx = TaskContext.get()
      val _ = CometUdfAllocator.acquire(ctx)
      cacheSizeDuring.add(CometUdfAllocator.cacheSize().toLong)
    }

    assert(cacheSizeDuring.value >= 1L, "allocator should be cached during the task")
    assert(
      CometUdfAllocator.cacheSize() == 0,
      "TaskCompletionListener should remove the entry once the task ends")
  }
}
