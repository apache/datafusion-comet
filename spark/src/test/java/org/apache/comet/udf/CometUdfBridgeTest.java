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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.IntVector;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.comet.CometTaskContextShim;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
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
    startSpark();
  }

  private static void startSpark() {
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
  public void bufferReleaseDoesNotWaitForMemoryAcquisition() {
    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>) CometUdfBridgeTest::runBlockingReleaseRegression);
  }

  private static void runBlockingReleaseRegression(Iterator<Integer> ignored) throws Exception {
    TaskContext context = TaskContext.get();
    CometUdfBridge.registerTask(context);
    BufferAllocator allocator = CometUdfBridge.taskAllocator(context);
    TaskMemoryManager taskMemoryManager = CometTaskContextShim.taskMemoryManager(context);
    long allocationSize = 1L << 20;
    ArrowBuf previous = allocator.buffer(allocationSize);
    CountDownLatch spillStarted = new CountDownLatch(1);
    CountDownLatch finishSpill = new CountDownLatch(1);
    MemoryConsumer holder =
        new MemoryConsumer(taskMemoryManager, 0L, MemoryMode.OFF_HEAP) {
          @Override
          public long spill(long size, MemoryConsumer trigger) throws IOException {
            spillStarted.countDown();
            try {
              if (!finishSpill.await(10, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting to finish test spill");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
            }
            return 0L;
          }
        };
    long held = holder.acquireMemory(64L << 20);
    ExecutorService threads = Executors.newFixedThreadPool(2);
    Future<ArrowBuf> pending = threads.submit(() -> allocator.buffer(allocationSize));
    Future<?> release = null;
    boolean releasedWhileAcquireBlocked = false;
    try {
      assertTrue("allocation should block in spill", spillStarted.await(10, TimeUnit.SECONDS));
      release = threads.submit(previous::close);
      try {
        release.get(2, TimeUnit.SECONDS);
        releasedWhileAcquireBlocked = true;
      } catch (TimeoutException ignoredTimeout) {
        // The assertion below reports the monitor regression after cleanup unblocks.
      }
    } finally {
      finishSpill.countDown();
      if (release != null) {
        release.get(10, TimeUnit.SECONDS);
      } else {
        previous.close();
      }
      try {
        ArrowBuf next = pending.get(10, TimeUnit.SECONDS);
        next.close();
      } catch (ExecutionException expected) {
        assertTrue(expected.getCause() instanceof OutOfMemoryException);
      }
      holder.freeMemory(held);
      threads.shutdownNow();
    }
    assertTrue(
        "buffer release must not wait for a blocking memory acquire", releasedWhileAcquireBlocked);
  }

  @Test
  public void taskCompletionWaitsForInFlightEvaluation() {
    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CountDownLatch taskCompleted = new CountDownLatch(1);
                  CountDownLatch evaluationCompleted = new CountDownLatch(1);
                  AtomicReference<Throwable> failure = new AtomicReference<>();
                  context.addTaskCompletionListener(
                      ignoredContext -> {
                        taskCompleted.countDown();
                        assertTrue("evaluation should finish", await(evaluationCompleted));
                        if (failure.get() != null) {
                          throw new AssertionError(failure.get());
                        }
                      });
                  CountDownLatch evaluationStarted = new CountDownLatch(1);
                  Thread evaluation =
                      new Thread(
                          () -> {
                            Runnable finishEvaluation = null;
                            try {
                              finishEvaluation = CometUdfBridge.beginTaskEvaluation(context);
                              evaluationStarted.countDown();
                              assertTrue(
                                  "task should complete while evaluation is in flight",
                                  await(taskCompleted));
                              assertEquals(
                                  "task state should remain until UDF evaluation finishes",
                                  1,
                                  CometUdfBridge.taskStateCount());
                            } catch (Throwable t) {
                              failure.set(t);
                            } finally {
                              if (finishEvaluation != null) {
                                finishEvaluation.run();
                              }
                              evaluationCompleted.countDown();
                            }
                          });
                  evaluation.setDaemon(true);
                  evaluation.start();
                  assertTrue(
                      "evaluation should start before task completion", await(evaluationStarted));
                });
    assertEquals(
        "finished evaluation should remove task state", 0, CometUdfBridge.taskStateCount());
  }

  private static boolean await(CountDownLatch latch) {
    try {
      return latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  public void outputAllocationIsChargedUntilFfiRelease() {
    // This and the restart below make each job the first task in a fresh SparkContext, forcing
    // taskAttemptId reuse while the first task's exported buffer is still retained.
    spark.stop();
    spark = null;
    jsc = null;
    startSpark();

    LongAccumulator before = jsc.sc().longAccumulator("udf-memory-before");
    LongAccumulator during = jsc.sc().longAccumulator("udf-memory-during");
    LongAccumulator retained = jsc.sc().longAccumulator("udf-memory-retained");
    LongAccumulator released = jsc.sc().longAccumulator("udf-memory-released");
    LongAccumulator stateCount = jsc.sc().longAccumulator("udf-state-count");
    LongAccumulator firstTaskAttemptId = jsc.sc().longAccumulator("first-task-attempt-id");

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  firstTaskAttemptId.add(context.taskAttemptId());
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

    spark.stop();
    spark = null;
    jsc = null;
    startSpark();

    LongAccumulator secondTaskAttemptId = jsc.sc().longAccumulator("second-task-attempt-id");
    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  secondTaskAttemptId.add(context.taskAttemptId());
                  CometUdfBridge.registerTask(context);
                  CometUdfBridge.taskAllocator(context);
                });
    assertEquals(
        "separate SparkContexts should reuse the task attempt ID in this regression",
        firstTaskAttemptId.value(),
        secondTaskAttemptId.value());

    ArrowArray deferred = DEFERRED_ARRAY.getAndSet(null);
    assertNotNull("task should export a deferred FFI array", deferred);
    deferred.release();
    deferred.close();
    assertEquals(
        "FFI release should remove completed task state", 0, CometUdfBridge.taskStateCount());
  }
}
