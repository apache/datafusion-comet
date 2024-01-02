package org.apache.spark;

import java.io.IOException;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

/**
 * A adapter class that is used by Comet native to acquire & release memory through Spark's unified
 * memory manager. This assumes Spark's off-heap memory mode is enabled.
 */
public class CometTaskMemoryManager {
  private final TaskMemoryManager internal;
  private final NativeMemoryConsumer nativeMemoryConsumer;

  public CometTaskMemoryManager() {
    this.internal = TaskContext$.MODULE$.get().taskMemoryManager();
    this.nativeMemoryConsumer = new NativeMemoryConsumer();
  }

  // Called by Comet native through JNI.
  // Returns the actual amount of memory (in bytes) granted.
  public long acquireMemory(long size) {
    return internal.acquireExecutionMemory(size, nativeMemoryConsumer);
  }

  // Called by Comet native through JNI
  public void releaseMemory(long size) {
    internal.releaseExecutionMemory(size, nativeMemoryConsumer);
  }

  /**
   * A dummy memory consumer that does nothing when spilling. At the moment, Comet native doesn't
   * share the same API as Spark and cannot trigger spill when acquire memory. Therefore, when
   * acquiring memory from native or JVM, spilling can only be triggered from JVM operators.
   */
  private class NativeMemoryConsumer extends MemoryConsumer {
    protected NativeMemoryConsumer() {
      super(CometTaskMemoryManager.this.internal, 0, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      // No spilling
      return 0;
    }
  }
}
