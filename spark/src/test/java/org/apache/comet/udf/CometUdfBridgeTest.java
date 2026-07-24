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
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CometUdfBridgeTest {
  private static SparkSession spark;
  private static JavaSparkContext jsc;
  private static final AtomicReference<ArrowArray> DEFERRED_ARRAY = new AtomicReference<>();

  @BeforeClass
  public static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("CometUdfBridgeTest")
            .config("spark.ui.enabled", "false")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "67108864")
            .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
  }

  @AfterClass
  public static void tearDown() {
    ArrowArray deferred = DEFERRED_ARRAY.getAndSet(null);
    if (deferred != null) {
      deferred.release();
      deferred.close();
    }
    if (spark != null) {
      spark.stop();
      spark = null;
      jsc = null;
    }
  }

  @Test
  public void outputAllocationIsChargedUntilFfiRelease() {
    LongAccumulator before = jsc.sc().longAccumulator("udf-memory-before");
    LongAccumulator during = jsc.sc().longAccumulator("udf-memory-during");
    LongAccumulator retained = jsc.sc().longAccumulator("udf-memory-retained");
    LongAccumulator released = jsc.sc().longAccumulator("udf-memory-released");
    LongAccumulator stateCount = jsc.sc().longAccumulator("udf-state-count");

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  TaskMemoryManager taskMemoryManager =
                      CometTaskContextShim.taskMemoryManager(context);
                  before.add(taskMemoryManager.getMemoryConsumptionForThisTask());

                  BufferAllocator allocator = CometUdfBridge.taskAllocator(context);
                  try (ArrowArray array =
                      ArrowArray.allocateNew(
                          org.apache.comet.package$.MODULE$.CometArrowAllocator())) {
                    try (IntVector vector = new IntVector("result", allocator)) {
                      vector.allocateNew(1024);
                      vector.setValueCount(1024);
                      during.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                      Data.exportVector(allocator, vector, null, array);
                    }
                    retained.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                    array.release();
                    released.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                  }

                  ArrowArray deferred =
                      ArrowArray.allocateNew(
                          org.apache.comet.package$.MODULE$.CometArrowAllocator());
                  try (IntVector vector = new IntVector("deferred", allocator)) {
                    vector.allocateNew(1024);
                    vector.setValueCount(1024);
                    Data.exportVector(allocator, vector, null, deferred);
                  }
                  DEFERRED_ARRAY.set(deferred);
                  stateCount.add(CometUdfBridge.taskStateCount());
                });

    assertTrue("Arrow output should be charged to the Spark task", during.value() > before.value());
    assertTrue(
        "FFI should retain the task charge after the Java vector closes",
        retained.value() > before.value());
    assertEquals("FFI release should return to baseline", before.value(), released.value());
    assertTrue("task state should exist while the task is running", stateCount.value() >= 1L);
    assertEquals(
        "task state should retain an exported buffer after completion",
        1,
        CometUdfBridge.taskStateCount());

    ArrowArray deferred = DEFERRED_ARRAY.getAndSet(null);
    assertNotNull("task should export a deferred FFI array", deferred);
    deferred.release();
    deferred.close();
    assertEquals(
        "FFI release should remove completed task state", 0, CometUdfBridge.taskStateCount());
  }
}
