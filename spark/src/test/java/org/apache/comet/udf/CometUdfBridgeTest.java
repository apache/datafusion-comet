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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
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
import static org.junit.Assert.fail;

public class CometUdfBridgeTest {
  private static SparkSession spark;
  private static JavaSparkContext jsc;
  private static final AtomicReference<ArrowArray> DEFERRED_ARRAY = new AtomicReference<>();
  private static final AtomicReference<TaskContext> COMPLETED_CONTEXT = new AtomicReference<>();

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
  public void stragglerEvaluationsAroundTaskCompletionAreSafe() {
    LongAccumulator duringTeardown = jsc.sc().longAccumulator("straggler-during-teardown");
    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  COMPLETED_CONTEXT.set(context);
                  // Registered before registerTask, so Spark's LIFO listener order runs this
                  // after the bridge's own completion listener: the misordered-registration
                  // scenario, where a straggler evaluation arrives mid-teardown after task
                  // state was already completed and removed.
                  context.addTaskCompletionListener(
                      ignoredContext -> {
                        Runnable finishEvaluation = null;
                        try {
                          finishEvaluation = CometUdfBridge.beginTaskEvaluation(context);
                        } catch (IllegalStateException rejectedAsCompleted) {
                          // Also safe: the recreated state completed before the evaluation began.
                        } finally {
                          if (finishEvaluation != null) {
                            finishEvaluation.run();
                          }
                          duringTeardown.add(1L);
                        }
                      });
                  CometUdfBridge.registerTask(context);
                  CometUdfBridge.taskAllocator(context);
                });
    assertEquals(
        "the mid-teardown straggler must run or be rejected without failing the task",
        1L,
        duringTeardown.value().longValue());
    assertEquals(
        "a mid-teardown straggler must not leak task state", 0, CometUdfBridge.taskStateCount());

    // A straggler arriving after the task fully finished: registering the completion listener on
    // the finished task completes the recreated state immediately, so evaluation is rejected.
    TaskContext completedContext = COMPLETED_CONTEXT.getAndSet(null);
    assertNotNull("task should publish its TaskContext", completedContext);
    try {
      CometUdfBridge.beginTaskEvaluation(completedContext);
      fail("evaluation for a finished task must be rejected");
    } catch (IllegalStateException expected) {
      // expected
    }
    assertEquals(
        "a post-completion straggler must not leak task state", 0, CometUdfBridge.taskStateCount());
  }

  @Test
  public void outputChargeMovesToNativeOwnershipAtExport() {
    LongAccumulator before = jsc.sc().longAccumulator("udf-memory-before");
    LongAccumulator during = jsc.sc().longAccumulator("udf-memory-during");
    LongAccumulator afterTransfer = jsc.sc().longAccumulator("udf-memory-after-transfer");
    LongAccumulator released = jsc.sc().longAccumulator("udf-memory-released");
    LongAccumulator taskAllocatedAfterTransfer =
        jsc.sc().longAccumulator("task-allocator-after-transfer");
    LongAccumulator transferredValue = jsc.sc().longAccumulator("transferred-first-value");
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

                  BufferAllocator rootAllocator =
                      org.apache.comet.package$.MODULE$.CometArrowAllocator();
                  BufferAllocator allocator = CometUdfBridge.taskAllocator(context);
                  try (ArrowArray array = ArrowArray.allocateNew(rootAllocator)) {
                    FieldVector exported;
                    try (IntVector vector = new IntVector("result", allocator)) {
                      vector.allocateNew(1024);
                      vector.setSafe(0, 42);
                      vector.setValueCount(1024);
                      during.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                      exported = CometUdfBridge.transferOutputForExport(context, vector);
                    }
                    try {
                      // The Spark charge moves to native ownership at export, before the FFI
                      // release, while the buffers are still alive and readable.
                      afterTransfer.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                      taskAllocatedAfterTransfer.add(allocator.getAllocatedMemory());
                      transferredValue.add(((IntVector) exported).get(0));
                      Data.exportVector(rootAllocator, exported, null, array);
                    } finally {
                      exported.close();
                    }
                    array.release();
                    released.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                  }

                  ArrowArray deferred = ArrowArray.allocateNew(rootAllocator);
                  FieldVector deferredExported;
                  try (IntVector vector = new IntVector("deferred", allocator)) {
                    vector.allocateNew(1024);
                    vector.setValueCount(1024);
                    deferredExported = CometUdfBridge.transferOutputForExport(context, vector);
                  }
                  try {
                    Data.exportVector(rootAllocator, deferredExported, null, deferred);
                  } finally {
                    deferredExported.close();
                  }
                  DEFERRED_ARRAY.set(deferred);
                  stateCount.add(CometUdfBridge.taskStateCount());
                });

    assertTrue(
        "Arrow output should be charged to the Spark task while the UDF holds it",
        during.value() > before.value());
    assertEquals(
        "the task charge should move to native ownership at export",
        before.value(),
        afterTransfer.value());
    assertEquals(
        "exported buffers should leave the task allocator",
        0L,
        taskAllocatedAfterTransfer.value().longValue());
    assertEquals(
        "transferred buffers should stay readable", 42L, transferredValue.value().longValue());
    assertEquals(
        "the FFI release should not free the task charge a second time",
        before.value(),
        released.value());
    assertTrue("task state should exist while the task is running", stateCount.value() >= 1L);
    assertEquals(
        "exported buffers should not retain task state after completion",
        0,
        CometUdfBridge.taskStateCount());

    ArrowArray deferred = DEFERRED_ARRAY.getAndSet(null);
    assertNotNull("task should export a deferred FFI array", deferred);
    deferred.release();
    deferred.close();
    assertEquals(
        "a deferred FFI release should not involve task state", 0, CometUdfBridge.taskStateCount());
  }

  @Test
  public void emptyChildAllocationsReleaseSparkChargeAtExport() {
    LongAccumulator before = jsc.sc().longAccumulator("empty-child-before");
    LongAccumulator during = jsc.sc().longAccumulator("empty-child-during");
    LongAccumulator afterTransfer = jsc.sc().longAccumulator("empty-child-after-transfer");
    LongAccumulator released = jsc.sc().longAccumulator("empty-child-released");

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  TaskMemoryManager taskMemoryManager =
                      CometTaskContextShim.taskMemoryManager(context);
                  before.add(taskMemoryManager.getMemoryConsumptionForThisTask());

                  BufferAllocator rootAllocator =
                      org.apache.comet.package$.MODULE$.CometArrowAllocator();
                  BufferAllocator allocator = CometUdfBridge.taskAllocator(context);
                  try (ArrowArray array = ArrowArray.allocateNew(rootAllocator)) {
                    FieldVector exported;
                    try (ListVector list = ListVector.empty("result", allocator)) {
                      list.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));
                      list.setInitialCapacity(1024);
                      list.allocateNew();
                      // All-empty lists: the child data vector keeps its allocated capacity but
                      // reports a zero buffer size, the case getBuffers(false) omits.
                      list.setValueCount(1024);
                      during.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                      exported = CometUdfBridge.transferOutputForExport(context, list);
                    }
                    try {
                      afterTransfer.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                      Data.exportVector(rootAllocator, exported, null, array);
                    } finally {
                      exported.close();
                    }
                    array.release();
                    released.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                  }
                });

    assertTrue(
        "allocated empty children should be charged to the Spark task",
        during.value() > before.value());
    assertEquals(
        "the full charge, including allocated empty children, should move to native "
            + "ownership at export",
        before.value(),
        afterTransfer.value());
    assertEquals("no charge should remain after the FFI release", before.value(), released.value());
  }

  @Test
  public void sharedScratchChunkChargeIsReleasedExactlyOnce() {
    LongAccumulator before = jsc.sc().longAccumulator("scratch-before");
    LongAccumulator afterScratch = jsc.sc().longAccumulator("scratch-allocated");
    LongAccumulator afterTransfer = jsc.sc().longAccumulator("scratch-after-transfer");
    LongAccumulator afterFfiRelease = jsc.sc().longAccumulator("scratch-after-ffi-release");
    LongAccumulator sliceValue = jsc.sc().longAccumulator("scratch-slice-value");
    LongAccumulator afterScratchClose = jsc.sc().longAccumulator("scratch-after-close");
    LongAccumulator unrelatedCharge = jsc.sc().longAccumulator("scratch-unrelated-charge");
    LongAccumulator end = jsc.sc().longAccumulator("scratch-end");

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  TaskMemoryManager taskMemoryManager =
                      CometTaskContextShim.taskMemoryManager(context);
                  long beforeCharge = taskMemoryManager.getMemoryConsumptionForThisTask();
                  before.add(beforeCharge);

                  BufferAllocator rootAllocator =
                      org.apache.comet.package$.MODULE$.CometArrowAllocator();
                  BufferAllocator allocator = CometUdfBridge.taskAllocator(context);
                  IntVector scratch = new IntVector("scratch", allocator);
                  scratch.allocateNew(2048);
                  for (int i = 0; i < 2048; i++) {
                    scratch.set(i, i);
                  }
                  scratch.setValueCount(2048);
                  afterScratch.add(taskMemoryManager.getMemoryConsumptionForThisTask());

                  // Aligned sub-range slice: shares the scratch chunks without copying, the
                  // documented custom-CometUDF scratch-buffer pattern.
                  TransferPair slicePair = scratch.getTransferPair(allocator);
                  slicePair.splitAndTransfer(1024, 512);
                  FieldVector slice = (FieldVector) slicePair.getTo();
                  FieldVector exported;
                  try {
                    exported = CometUdfBridge.transferOutputForExport(context, slice);
                  } finally {
                    slice.close();
                  }
                  try (ArrowArray array = ArrowArray.allocateNew(rootAllocator)) {
                    try {
                      afterTransfer.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                      Data.exportVector(rootAllocator, exported, null, array);
                    } finally {
                      exported.close();
                    }
                    // Native releases the FFI result: Arrow silently returns chunk ownership
                    // to the retained scratch ledger with no listener callback.
                    array.release();
                  }
                  long afterFfiReleaseCharge = taskMemoryManager.getMemoryConsumptionForThisTask();
                  afterFfiRelease.add(afterFfiReleaseCharge);
                  sliceValue.add(scratch.get(1024));

                  ArrowBuf unrelated = allocator.buffer(8192);
                  unrelatedCharge.add(
                      taskMemoryManager.getMemoryConsumptionForThisTask() - afterFfiReleaseCharge);
                  scratch.close();
                  afterScratchClose.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                  unrelated.close();
                  end.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                });

    assertTrue(
        "the scratch allocation should be charged to the Spark task",
        afterScratch.value() > before.value());
    assertEquals(
        "a chunk shared with retained scratch must keep its Spark charge at export",
        afterScratch.value(),
        afterTransfer.value());
    assertEquals(
        "the FFI release returns ownership to scratch without changing the charge",
        afterScratch.value(),
        afterFfiRelease.value());
    assertEquals(
        "scratch buffers should stay readable after the FFI release returns ownership",
        1024L,
        sliceValue.value().longValue());
    assertTrue("an unrelated allocation should add its own charge", unrelatedCharge.value() > 0L);
    assertEquals(
        "closing scratch must release the shared-chunk charge exactly once",
        before.value() + unrelatedCharge.value(),
        afterScratchClose.value().longValue());
    assertEquals(
        "no over-release: the unrelated charge must survive the scratch close and be "
            + "returned by its own close",
        before.value(),
        end.value());
  }
}
