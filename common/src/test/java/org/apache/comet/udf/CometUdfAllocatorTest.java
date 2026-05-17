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

package org.apache.comet.udf;

import java.util.Collections;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.comet.CometTaskContextShim;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that {@link CometUdfAllocator} both charges the executing Spark task's {@link
 * TaskMemoryManager} for allocations and tears down the per-task allocator on task completion.
 * Lives in the {@code common} module to stay on the unshaded side of the Arrow relocation boundary;
 * the spark module sees {@code BufferAllocator} as the shaded type.
 */
public class CometUdfAllocatorTest {

  private static SparkSession spark;
  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("CometUdfAllocatorTest")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "67108864")
            .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void acquireRegistersMemoryConsumerThatChargesTask() {
    LongAccumulator before = jsc.sc().longAccumulator("before");
    LongAccumulator during = jsc.sc().longAccumulator("during");
    LongAccumulator after = jsc.sc().longAccumulator("after");

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                it -> {
                  TaskContext ctx = TaskContext.get();
                  TaskMemoryManager tmm = CometTaskContextShim.taskMemoryManager(ctx);
                  before.add(tmm.getMemoryConsumptionForThisTask());

                  BufferAllocator allocator = CometUdfAllocator.acquire(ctx);
                  IntVector vec = new IntVector("test", allocator);
                  try {
                    vec.allocateNew(1024);
                    vec.setValueCount(1024);
                    during.add(tmm.getMemoryConsumptionForThisTask());
                  } finally {
                    vec.close();
                  }
                  after.add(tmm.getMemoryConsumptionForThisTask());
                });

    assertTrue(
        "task memory should grow while allocator holds buffers; before="
            + before.value()
            + " during="
            + during.value(),
        during.value() > before.value());
    assertEquals(
        "task memory should be released after vector closes", before.value(), after.value());
  }

  @Test
  public void childAllocatorClosedAndUncachedOnTaskCompletion() {
    LongAccumulator cacheSizeDuring = jsc.sc().longAccumulator("cacheSizeDuring");

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                it -> {
                  TaskContext ctx = TaskContext.get();
                  CometUdfAllocator.acquire(ctx);
                  cacheSizeDuring.add((long) CometUdfAllocator.cacheSize());
                });

    assertTrue("allocator should be cached during the task", cacheSizeDuring.value() >= 1L);
    assertEquals(
        "TaskCompletionListener should remove the entry once the task ends",
        0,
        CometUdfAllocator.cacheSize());
  }
}
