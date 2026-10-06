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
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.spark.CometTaskMemoryManager;
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
  private static final long OFF_HEAP_BYTES = 64L << 20;
  // What the memory-pressure tests leave free in the pool: less than one UDF output needs.
  private static final long FREE_BYTES = 4096L;
  private static final int NUM_ROWS = 1024;
  private static final long SCRATCH_BYTES = 8192L;

  private static SparkSession spark;
  private static JavaSparkContext jsc;
  private static final AtomicReference<ArrowArray> DEFERRED_ARRAY = new AtomicReference<>();
  private static final AtomicReference<TaskContext> COMPLETED_CONTEXT = new AtomicReference<>();
  private static final Queue<ArrowBuf> RETAINED_BUFFERS = new ConcurrentLinkedQueue<>();
  private static final Queue<WeakReference<Object>> TASK_OBJECTS = new ConcurrentLinkedQueue<>();

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
            .config("spark.memory.offHeap.size", Long.toString(OFF_HEAP_BYTES))
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
    closeRetainedBuffers();
    ScratchUdf.releaseUnclosed();
    LeakingScratchUdf.releaseLeaked();
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
      // Recorded, not refused: Spark's spill loop does not retry the acquire after a spill that
      // freed nothing, so the pending allocation may be granted nothing, and it must still
      // succeed.
      ArrowBuf next = pending.get(10, TimeUnit.SECONDS);
      next.close();
      holder.freeMemory(held);
      threads.shutdownNow();
    }
    assertTrue(
        "buffer release must not wait for a blocking memory acquire", releasedWhileAcquireBlocked);
    assertEquals(
        "every charge should be handed back once the buffers are closed",
        0L,
        taskMemoryManager.getMemoryConsumptionForThisTask());
  }

  /**
   * The memory-pressure regression. A native consumer ({@code CometTaskMemoryManager}, whose {@code
   * spill} returns 0) holds all but {@link #FREE_BYTES} of the pool, as it does once native
   * operators have reserved until their own {@code try_grow} failed. A UDF evaluation that needs
   * more Arrow memory than Spark can grant must still succeed, so that the native operator it feeds
   * gets the chance to spill, and the export must hand Spark back exactly what it granted.
   */
  @Test
  public void udfEvaluationSucceedsWhenSparkGrantsLessThanRequested() {
    LongAccumulator held = jsc.sc().longAccumulator("pressure-held");
    LongAccumulator afterEvaluate = jsc.sc().longAccumulator("pressure-after-evaluate");
    LongAccumulator resultSum = jsc.sc().longAccumulator("pressure-result-sum");
    LongAccumulator afterFfiRelease = jsc.sc().longAccumulator("pressure-after-ffi-release");
    LongAccumulator end = jsc.sc().longAccumulator("pressure-end");
    AllocatingUdf.ALLOCATED.set(-1L);
    AllocatingUdf.TASK_CHARGE.set(-1L);

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  TaskMemoryManager taskMemoryManager =
                      CometTaskContextShim.taskMemoryManager(context);
                  CometTaskMemoryManager nativeConsumer =
                      new CometTaskMemoryManager(0L, context.taskAttemptId());
                  long nativeHeld = nativeConsumer.acquireMemory(OFF_HEAP_BYTES - FREE_BYTES);
                  held.add(nativeHeld);
                  BufferAllocator rootAllocator =
                      org.apache.comet.package$.MODULE$.CometArrowAllocator();
                  try (ArrowArray outArray = ArrowArray.allocateNew(rootAllocator);
                      ArrowSchema outSchema = ArrowSchema.allocateNew(rootAllocator)) {
                    CometUdfBridge.evaluate(
                        AllocatingUdf.class.getName(),
                        new long[0],
                        new long[0],
                        outArray.memoryAddress(),
                        outSchema.memoryAddress(),
                        NUM_ROWS,
                        context,
                        Thread.currentThread().getContextClassLoader());
                    afterEvaluate.add(
                        taskMemoryManager.getMemoryConsumptionForThisTask() - nativeHeld);
                    try (FieldVector result =
                        Data.importVector(rootAllocator, outArray, outSchema, null)) {
                      IntVector ints = (IntVector) result;
                      for (int i = 0; i < ints.getValueCount(); i++) {
                        resultSum.add(ints.get(i));
                      }
                    }
                    afterFfiRelease.add(
                        taskMemoryManager.getMemoryConsumptionForThisTask() - nativeHeld);
                  } finally {
                    nativeConsumer.releaseMemory(nativeHeld);
                  }
                  end.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                });

    assertEquals(
        "the native consumer should leave only FREE_BYTES of the pool",
        OFF_HEAP_BYTES - FREE_BYTES,
        held.value().longValue());
    assertTrue(
        "the UDF must need more Arrow memory than Spark can grant",
        AllocatingUdf.ALLOCATED.get() > FREE_BYTES);
    assertEquals(
        "Spark should be charged what it could grant rather than refuse the allocation",
        FREE_BYTES,
        AllocatingUdf.TASK_CHARGE.get() - held.value());
    assertEquals(
        "the export must hand back exactly the grant, leaving native's reservation intact",
        0L,
        afterEvaluate.value().longValue());
    assertEquals(
        "the result should arrive intact",
        (long) NUM_ROWS * (NUM_ROWS - 1) / 2,
        resultSum.value().longValue());
    assertEquals(
        "the FFI release should not touch Spark's accounting",
        0L,
        afterFfiRelease.value().longValue());
    assertEquals("nothing should remain charged", 0L, end.value().longValue());
    assertEquals("the task state should be cleaned up", 0, CometUdfBridge.taskStateCount());
  }

  /**
   * Short grants settle without over-releasing on the paths that do not go through export. A
   * release repays the shortfall before handing Spark anything back, so the grant keeps backing
   * memory that is still outstanding, and task completion hands back exactly what Spark granted
   * while buffers recorded beyond the grant are still live.
   */
  @Test
  public void shortGrantsAreRepaidBeforeSparkIsHandedBack() {
    LongAccumulator afterAllocations = jsc.sc().longAccumulator("short-grant-after-allocations");
    LongAccumulator afterRelease = jsc.sc().longAccumulator("short-grant-after-release");
    LongAccumulator afterCompletion = jsc.sc().longAccumulator("short-grant-after-completion");
    LongAccumulator end = jsc.sc().longAccumulator("short-grant-end");
    Set<BufferAllocator> allocatorsBefore = taskAllocators();

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  TaskMemoryManager taskMemoryManager =
                      CometTaskContextShim.taskMemoryManager(context);
                  CometTaskMemoryManager nativeConsumer =
                      new CometTaskMemoryManager(0L, context.taskAttemptId());
                  long nativeHeld = nativeConsumer.acquireMemory(OFF_HEAP_BYTES - FREE_BYTES);
                  // Registered before registerTask, so Spark's LIFO order runs it after the
                  // bridge's own completion listener has settled the task's accounting.
                  context.addTaskCompletionListener(
                      ignoredContext -> {
                        afterCompletion.add(
                            taskMemoryManager.getMemoryConsumptionForThisTask() - nativeHeld);
                        nativeConsumer.releaseMemory(nativeHeld);
                        end.add(taskMemoryManager.getMemoryConsumptionForThisTask());
                      });
                  CometUdfBridge.registerTask(context);
                  BufferAllocator allocator = CometUdfBridge.taskAllocator(context);

                  // Spark grants FREE_BYTES of the first buffer and nothing of the second.
                  ArrowBuf first = allocator.buffer(2 * FREE_BYTES);
                  ArrowBuf second = allocator.buffer(2 * FREE_BYTES);
                  afterAllocations.add(
                      taskMemoryManager.getMemoryConsumptionForThisTask() - nativeHeld);
                  // Repaid from the shortfall: the grant keeps backing `second`.
                  first.close();
                  afterRelease.add(
                      taskMemoryManager.getMemoryConsumptionForThisTask() - nativeHeld);
                  // Both outlive the task, one of them recorded without any grant at all.
                  RETAINED_BUFFERS.add(second);
                  RETAINED_BUFFERS.add(allocator.buffer(2 * FREE_BYTES));
                });

    assertEquals(
        "Spark should be charged what it could grant",
        FREE_BYTES,
        afterAllocations.value().longValue());
    assertEquals(
        "a release should repay the shortfall before handing back a grant that still backs "
            + "outstanding memory",
        FREE_BYTES,
        afterRelease.value().longValue());
    assertEquals(
        "task completion must hand back exactly what Spark granted",
        0L,
        afterCompletion.value().longValue());
    assertEquals("nothing should remain charged", 0L, end.value().longValue());
    try {
      assertEquals(
          "the completed task should be dropped even while its buffers live on",
          0,
          CometUdfBridge.taskStateCount());
      assertEquals(
          "buffers outliving the task should keep its allocator open",
          1,
          newTaskAllocators(allocatorsBefore).size());
    } finally {
      closeRetainedBuffers();
    }
    assertEquals(
        "closing the last buffer should close the allocator",
        Collections.emptySet(),
        newTaskAllocators(allocatorsBefore));
  }

  private static void closeRetainedBuffers() {
    ArrowBuf buffer;
    while ((buffer = RETAINED_BUFFERS.poll()) != null) {
      buffer.close();
    }
  }

  /** Allocates its result from the allocator it is given and records what that cost the task. */
  public static final class AllocatingUdf implements CometUDF {
    static final AtomicLong ALLOCATED = new AtomicLong(-1L);
    static final AtomicLong TASK_CHARGE = new AtomicLong(-1L);

    @Override
    public ValueVector evaluate(BufferAllocator allocator, ValueVector[] inputs, int numRows) {
      IntVector out = new IntVector("out", allocator);
      try {
        out.allocateNew(numRows);
        for (int i = 0; i < numRows; i++) {
          out.set(i, i);
        }
        out.setValueCount(numRows);
      } catch (RuntimeException e) {
        out.close();
        throw e;
      }
      ALLOCATED.set(allocator.getAllocatedMemory());
      TASK_CHARGE.set(
          CometTaskContextShim.taskMemoryManager(TaskContext.get())
              .getMemoryConsumptionForThisTask());
      return out;
    }
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

  /**
   * Output from a child of the task allocator carries a Spark charge through the inherited
   * listener. Export releases it.
   */
  @Test
  public void childAllocatorChargeIsReleasedAtExport() {
    LongAccumulator charged = jsc.sc().longAccumulator("child-allocator-charged");
    LongAccumulator afterExports = jsc.sc().longAccumulator("child-allocator-after-exports");
    ChildAllocatorUdf.TASK_CHARGE.set(-1L);
    ChildAllocatorUdf.CLOSED.set(0);

    jsc.parallelize(Collections.singletonList(0), 1)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  TaskMemoryManager taskMemoryManager =
                      CometTaskContextShim.taskMemoryManager(context);
                  long before = taskMemoryManager.getMemoryConsumptionForThisTask();
                  for (int batch = 0; batch < 3; batch++) {
                    evaluateThroughBridge(ChildAllocatorUdf.class.getName(), context);
                  }
                  charged.add(ChildAllocatorUdf.TASK_CHARGE.get() - before);
                  afterExports.add(taskMemoryManager.getMemoryConsumptionForThisTask() - before);
                });

    assertTrue(
        "the child allocator's output should be charged to the Spark task", charged.value() > 0L);
    assertEquals(
        "each export should release the charge of the child allocator's chunks",
        0L,
        afterExports.value().longValue());
    assertEquals("close() should run once", 1, ChildAllocatorUdf.CLOSED.get());
    assertEquals("no task state should outlive its task", 0, CometUdfBridge.taskStateCount());
  }

  /**
   * A UDF that keeps a scratch buffer from the task allocator across calls releases it in {@code
   * close()}, which the bridge calls once the task has completed. Over three sequential tasks
   * nothing is left behind: no task state, and no task allocator, which Arrow would otherwise keep
   * registered with the root for the life of the executor.
   */
  @Test
  public void scratchReleasedInCloseLeavesNothingBehindAcrossTasks() {
    Set<BufferAllocator> allocatorsBefore = taskAllocators();
    ScratchUdf.CLOSED.set(0);

    jsc.parallelize(Arrays.asList(0, 1, 2), 3)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  // Twice, so the second call reuses the scratch buffer the first one kept.
                  evaluateThroughBridge(ScratchUdf.class.getName(), context);
                  evaluateThroughBridge(ScratchUdf.class.getName(), context);
                });

    try {
      assertEquals("close() should run once per task", 3, ScratchUdf.CLOSED.get());
      assertEquals("no task state should outlive its task", 0, CometUdfBridge.taskStateCount());
      assertEquals(
          "no task allocator should outlive its task",
          Collections.emptySet(),
          newTaskAllocators(allocatorsBefore));
    } finally {
      ScratchUdf.releaseUnclosed();
    }
  }

  /**
   * A UDF that never releases the scratch buffer it keeps leaks the buffer's bytes, as it would on
   * the root allocator, but must not keep its completed tasks alive. Arrow keeps the task allocator
   * it cannot close registered with the root for good, so anything the allocator's listener still
   * refers to would be pinned for the life of the executor.
   */
  @Test
  public void unreleasedScratchDoesNotPinCompletedTasks() throws InterruptedException {
    Set<BufferAllocator> allocatorsBefore = taskAllocators();
    TASK_OBJECTS.clear();

    jsc.parallelize(Arrays.asList(0, 1, 2), 3)
        .foreachPartition(
            (VoidFunction<Iterator<Integer>>)
                ignored -> {
                  TaskContext context = TaskContext.get();
                  CometUdfBridge.registerTask(context);
                  TASK_OBJECTS.add(new WeakReference<>(context));
                  TASK_OBJECTS.add(
                      new WeakReference<>(CometTaskContextShim.taskMemoryManager(context)));
                  evaluateThroughBridge(LeakingScratchUdf.class.getName(), context);
                });

    try {
      assertEquals(
          "each task should record its context and memory manager", 6, TASK_OBJECTS.size());
      assertTrue(
          "a completed task's TaskContext and TaskMemoryManager should not be retained",
          awaitCollected(TASK_OBJECTS));
      assertEquals("no task state should outlive its task", 0, CometUdfBridge.taskStateCount());
      Set<BufferAllocator> leaking = newTaskAllocators(allocatorsBefore);
      assertEquals("each task allocator should stay open for its leaked buffer", 3, leaking.size());
      for (BufferAllocator allocator : leaking) {
        assertEquals(
            "only the leaked scratch buffer should remain",
            SCRATCH_BYTES,
            allocator.getAllocatedMemory());
      }
    } finally {
      LeakingScratchUdf.releaseLeaked();
    }
    assertEquals(
        "releasing the leaked buffers should close their allocators",
        Collections.emptySet(),
        newTaskAllocators(allocatorsBefore));
  }

  /** Calls the UDF through the bridge as native execution does, then releases the result. */
  private static void evaluateThroughBridge(String udfClassName, TaskContext context) {
    BufferAllocator rootAllocator = org.apache.comet.package$.MODULE$.CometArrowAllocator();
    try (ArrowArray outArray = ArrowArray.allocateNew(rootAllocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(rootAllocator)) {
      CometUdfBridge.evaluate(
          udfClassName,
          new long[0],
          new long[0],
          outArray.memoryAddress(),
          outSchema.memoryAddress(),
          NUM_ROWS,
          context,
          Thread.currentThread().getContextClassLoader());
      Data.importVector(rootAllocator, outArray, outSchema, null).close();
    }
  }

  /** The task allocators currently registered with the root allocator. */
  private static Set<BufferAllocator> taskAllocators() {
    Set<BufferAllocator> allocators = Collections.newSetFromMap(new IdentityHashMap<>());
    for (BufferAllocator child :
        org.apache.comet.package$.MODULE$.CometArrowAllocator().getChildAllocators()) {
      if (child.getName().startsWith("comet-udf-task-")) {
        allocators.add(child);
      }
    }
    return allocators;
  }

  private static Set<BufferAllocator> newTaskAllocators(Set<BufferAllocator> before) {
    Set<BufferAllocator> allocators = taskAllocators();
    allocators.removeAll(before);
    return allocators;
  }

  /** Whether every referent is collected within a few garbage collections. */
  private static boolean awaitCollected(Collection<WeakReference<Object>> references)
      throws InterruptedException {
    for (int attempt = 0; attempt < 20; attempt++) {
      if (references.stream().allMatch(reference -> reference.get() == null)) {
        return true;
      }
      System.gc();
      Thread.sleep(100);
    }
    return references.stream().allMatch(reference -> reference.get() == null);
  }

  /** Keeps a scratch buffer from the task allocator across calls and releases it in close(). */
  public static final class ScratchUdf implements CometUDF {
    static final AtomicInteger CLOSED = new AtomicInteger();
    // Scratch buffers close() has not released, so that a failing test does not leave them behind.
    private static final Queue<ArrowBuf> UNCLOSED = new ConcurrentLinkedQueue<>();
    private ArrowBuf scratch;

    @Override
    public ValueVector evaluate(BufferAllocator allocator, ValueVector[] inputs, int numRows) {
      if (scratch == null) {
        scratch = allocator.buffer(SCRATCH_BYTES);
        UNCLOSED.add(scratch);
      }
      return nullResult(allocator, numRows);
    }

    @Override
    public void close() {
      if (scratch != null) {
        UNCLOSED.remove(scratch);
        scratch.close();
      }
      CLOSED.incrementAndGet();
    }

    static void releaseUnclosed() {
      ArrowBuf buffer;
      while ((buffer = UNCLOSED.poll()) != null) {
        buffer.close();
      }
    }
  }

  /**
   * Keeps a scratch buffer from the task allocator across calls and never releases it. The buffer
   * is also queued here so the test can release it once it has checked what the leak retains.
   */
  public static final class LeakingScratchUdf implements CometUDF {
    private static final Queue<ArrowBuf> LEAKED = new ConcurrentLinkedQueue<>();
    private ArrowBuf scratch;

    @Override
    public ValueVector evaluate(BufferAllocator allocator, ValueVector[] inputs, int numRows) {
      if (scratch == null) {
        scratch = allocator.buffer(SCRATCH_BYTES);
        LEAKED.add(scratch);
      }
      return nullResult(allocator, numRows);
    }

    static void releaseLeaked() {
      ArrowBuf buffer;
      while ((buffer = LEAKED.poll()) != null) {
        buffer.close();
      }
    }
  }

  /** Allocates its result from a child of the task allocator, and closes the child in close(). */
  public static final class ChildAllocatorUdf implements CometUDF {
    static final AtomicLong TASK_CHARGE = new AtomicLong(-1L);
    static final AtomicInteger CLOSED = new AtomicInteger();
    private BufferAllocator child;

    @Override
    public ValueVector evaluate(BufferAllocator allocator, ValueVector[] inputs, int numRows) {
      if (child == null) {
        child = allocator.newChildAllocator("udf-child", 0L, Long.MAX_VALUE);
      }
      IntVector out = nullResult(child, numRows);
      TASK_CHARGE.set(
          CometTaskContextShim.taskMemoryManager(TaskContext.get())
              .getMemoryConsumptionForThisTask());
      return out;
    }

    @Override
    public void close() {
      if (child != null) {
        child.close();
      }
      CLOSED.incrementAndGet();
    }
  }

  /** A result of {@code numRows} null values. */
  private static IntVector nullResult(BufferAllocator allocator, int numRows) {
    IntVector out = new IntVector("out", allocator);
    try {
      out.allocateNew(numRows);
      out.setValueCount(numRows);
    } catch (RuntimeException e) {
      out.close();
      throw e;
    }
    return out;
  }
}
