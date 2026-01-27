<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg Partition Task Optimization: Technical Explanation

## Overview

This document explains how the Iceberg native scan optimization ensures that **each executor only receives the partition and file task information it needs**, rather than broadcasting all partition data to every executor.

## The Problem: Broadcasting Waste

### Old Approach (Before Optimization)
In a traditional distributed query execution:

1. **Driver serializes ALL partition tasks** into a protobuf message
2. **Broadcast to ALL executors** via Spark's serialization mechanism
3. **Each executor receives N×task_size bytes** where N is the total number of partitions
4. **Each executor only uses 1/N of the data** it receives (its own partition tasks)
5. **Result: 99% waste for large N**

### Example
- Table with **1000 partitions**
- Each partition has **100KB of task data** (file paths, partition values, schemas, etc.)
- Total task data: **100MB**
- **Problem**: EVERY executor receives all 100MB, but only uses ~100KB

For a cluster with 100 executors:
- **Total network transfer**: 100 executors × 100MB = **10GB**
- **Useful data**: 100 executors × 100KB = **10MB**
- **Waste**: 99% of transferred data is discarded!

---

## The Solution: Partition-Specific Task Distribution

The optimization implements a **three-stage approach** to ensure executors only receive their required partition data:

### Stage 1: Driver-Side Partitioning (Query Planning)

**File**: `CometIcebergNativeScan.scala:740-1004`

```scala
// Map to store serialized task bytes per partition
val partitionTasksMap = mutable.HashMap[Int, Array[Byte]]()

scan.wrapped.inputRDD match {
  case rdd: org.apache.spark.sql.execution.datasources.v2.DataSourceRDD =>
    val partitions = rdd.partitions
    partitions.zipWithIndex.foreach { case (partition, partitionIndex) =>
      val partitionBuilder = OperatorOuterClass.IcebergFilePartition.newBuilder()

      // Extract FileScanTasks for THIS partition only
      inputPartitions.foreach { inputPartition =>
        // ... extract file scan tasks ...
        partitionBuilder.addFileScanTasks(taskBuilder.build())
      }

      // Serialize THIS partition's tasks to bytes
      val builtPartition = partitionBuilder.build()
      val partitionBytes = builtPartition.toByteArray

      // Store in map: partition index -> task bytes
      partitionTasksMap.put(partitionIndex, partitionBytes)
    }
}
```

**What happens here:**
1. During query planning on the **driver**, the code iterates through each Spark partition
2. For each partition `i`, it extracts **only the FileScanTasks that belong to that partition**
3. These tasks are serialized to protobuf bytes: `IcebergFilePartition` → `Array[Byte]`
4. Stored in a map: `Map[Int, Array[Byte]]` where key = partition index

**Result**: Instead of one big blob of all tasks, we have N separate blobs, one per partition.

### Stage 2: Custom RDD for Partition-Specific Distribution

**File**: `IcebergScanRDD.scala:29-99`

```scala
class IcebergScanPartition(override val index: Int, val taskBytes: Array[Byte])
    extends Partition

class IcebergScanRDD(
    @transient private val sc: SparkContext,
    numPartitions: Int,
    partitionTasks: Map[Int, Array[Byte]],
    createIterator: (Int, Array[Byte]) => CometExecIterator)
    extends RDD[ColumnarBatch](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { i =>
      val taskBytes = partitionTasks.getOrElse(i, ...)
      new IcebergScanPartition(i, taskBytes)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[IcebergScanPartition]
    // Each partition carries only its own task bytes!
    createIterator(partition.index, partition.taskBytes)
  }
}
```

**What happens here:**
1. **Custom Partition class**: `IcebergScanPartition` carries its own `taskBytes: Array[Byte]`
2. **getPartitions()**: Creates N partition objects, each with only its own task data
3. **Spark's RDD serialization**: When Spark schedules tasks, it serializes the `Partition` object and sends it to the executor
4. **Key insight**: Spark's task serialization **only sends the specific Partition object to the executor that will compute it**

**Result**: Executor processing partition 5 only receives `IcebergScanPartition(5, taskBytesFor5)`, NOT all partition data.

#### Why This Works: Spark's Task Serialization

Spark's task scheduling works as follows:
1. **Driver** calls `getPartitions()` → creates array of Partition objects
2. **Scheduler** assigns tasks to executors: "Executor A: compute partition 5", "Executor B: compute partition 8", etc.
3. **Task serialization**: When sending the task to an executor, Spark serializes:
   - The task function (`compute()` closure)
   - The **specific Partition object** for that task
   - Any captured variables (closures)

Since each `IcebergScanPartition` object contains only its own `taskBytes`, the executor receives **O(1) task data** instead of **O(N)**.

### Stage 3: Executor-Side Task Retrieval (JNI Bridge)

**File**: `CometIcebergNativeScanExec.scala:160-212`

```scala
if (useJniTaskRetrieval) {
  def createIterator(partitionIndex: Int, taskBytes: Array[Byte]): CometExecIterator = {
    // Step 1: Store task bytes in thread-local storage
    Native.setIcebergPartitionTasks(taskBytes)

    try {
      // Step 2: Create native execution plan
      // The native side will call back to get task bytes via JNI
      new CometExecIterator(
        CometExec.newIterId,
        Seq.empty,
        outputLength,
        serializedPlanCopy,
        nativeMetrics,
        numPartitions,
        partitionIndex,
        None,
        Seq.empty
      )
    } finally {
      // Step 3: Clean up thread-local to prevent memory leaks
      Native.clearIcebergPartitionTasks()
    }
  }

  new IcebergScanRDD(sparkContext, numPartitions, partitionTasks, createIterator)
}
```

**What happens here:**

#### On the Executor (JVM side):
1. **Receive**: Executor receives `IcebergScanPartition(5, taskBytes)` from Spark
2. **Thread-local storage**: Task bytes stored in `ThreadLocal[Array[Byte]]` via `Native.setIcebergPartitionTasks(taskBytes)`
3. **Create iterator**: Native execution plan is initialized
4. **Cleanup**: After execution, `clearIcebergPartitionTasks()` removes data from thread-local

**File**: `Native.scala:224-263`

```scala
object Native {
  // Thread-local ensures task isolation between concurrent tasks
  private val icebergPartitionTasks = new ThreadLocal[Array[Byte]]()

  def setIcebergPartitionTasks(taskBytes: Array[Byte]): Unit = {
    icebergPartitionTasks.set(taskBytes)
  }

  def getIcebergPartitionTasksInternal(): Array[Byte] = {
    icebergPartitionTasks.get()
  }

  def clearIcebergPartitionTasks(): Unit = {
    icebergPartitionTasks.remove()
  }
}
```

**Why Thread-local?**
- Multiple tasks may run concurrently on the same executor JVM
- Each task runs in its own thread
- Thread-local storage ensures each task only accesses **its own partition data**
- Prevents cross-task contamination

#### On the Native side (Rust):

**File**: `jni_api.rs:833-866`

```rust
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_getIcebergPartitionTasks<'local>(
    mut e: JNIEnv<'local>,
    _class: JClass,
) -> jni::sys::jobject {
    // Call back to JVM to retrieve task bytes from thread-local
    let result = e.call_static_method_unchecked(
        &jvm_classes.native.class,
        jvm_classes.native.method_get_iceberg_partition_tasks_internal,
        ReturnType::Array,
        &[],
    );

    match result {
        Ok(value) => value.l().unwrap().as_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}
```

**What happens here:**
1. Native Iceberg planner calls `getIcebergPartitionTasks()` via JNI
2. This calls back to `Native.getIcebergPartitionTasksInternal()` on JVM side
3. Retrieves the `Array[Byte]` from thread-local storage
4. Returns to native code for deserialization and execution

**Result**: Native code gets **only the tasks for the current partition**, not all partition data.

---

## Why is the JNI Callback Necessary?

A natural question arises: **Why do we need a JNI callback at all? Why not just pass the task bytes directly to native code?**

The answer lies in **Comet's execution architecture** and how it separates plan structure from partition-specific data.

### The Two Data Paths

Comet's execution model uses **two separate data paths**:

1. **Shared Plan Path**: One serialized operator DAG for all partitions
2. **Partition-Specific Path**: Task data that varies per partition

#### Path 1: The Shared Serialized Plan

**File**: `CometIcebergNativeScanExec.scala:260-270`

```scala
override def convertBlock(): CometNativeExec = {
  // Serialize the plan to protobuf - ONCE on the driver
  val size = nativeOp.getSerializedSize
  val bytes = new Array[Byte](size)
  val codedOutput = com.google.protobuf.CodedOutputStream.newInstance(bytes)
  nativeOp.writeTo(codedOutput)

  // This serialized plan is SHARED by all executors/partitions
  copy(
    serializedPlanOpt = SerializedPlan(Some(bytes)),
    partitionTasks = partitionTasks  // @transient - doesn't serialize!
  )
}
```

The `serializedPlanOpt` contains the **operator DAG structure**:
- Scan → Filter → Project, etc.
- Schema definitions
- Filter predicates
- Projection columns

But it does **NOT** contain partition-specific FileScanTasks because:
1. It's created **once** on the driver
2. It's **shared** by all executors
3. It's the same for partition 0, partition 5, partition 1000, etc.

#### Path 2: Partition-Specific Task Data

**File**: `CometIcebergNativeScanExec.scala:173-188`

```scala
def createIterator(partitionIndex: Int, taskBytes: Array[Byte]): CometExecIterator = {
  // taskBytes contains THIS partition's FileScanTasks
  Native.setIcebergPartitionTasks(taskBytes)

  try {
    // Create native plan from SHARED serialized plan
    new CometExecIterator(
      CometExec.newIterId,
      Seq.empty,
      outputLength,
      serializedPlanCopy,  // ← SHARED across all partitions
      nativeMetrics,
      numPartitions,
      partitionIndex,
      None,
      Seq.empty
    )
  }
}
```

The `taskBytes` are **partition-specific** and arrive via the RDD partition object (our optimization).

### The Timeline Problem

Here's what happens during execution:

```
Executor Timeline:
┌─────────────────────────────────────────────────────────────┐
│ 1. Receive task from Spark                                  │
│    → Gets: IcebergScanPartition(5, taskBytes_5)             │
│                                                               │
│ 2. Store in thread-local                                     │
│    → Native.setIcebergPartitionTasks(taskBytes_5)           │
│                                                               │
│ 3. Create native execution plan                              │
│    → new CometExecIterator(..., serializedPlanCopy, ...)   │
│    → Passes SHARED plan protobuf to native code             │
│                                                               │
│ 4. Native deserializes protobuf                              │
│    → Creates operator instances from protobuf                │
│    → Creates IcebergScanExec operator                        │
│                                                               │
│ 5. IcebergScanExec.init() needs FileScanTasks ◄─ PROBLEM!  │
│    → Protobuf doesn't have partition-specific tasks         │
│    → Must retrieve from JVM side                             │
│                                                               │
│ 6. JNI callback to retrieve tasks                            │
│    → getIcebergPartitionTasks() via JNI                     │
│    → Returns taskBytes_5 from thread-local                   │
│                                                               │
│ 7. Deserialize and execute                                   │
│    → Parse IcebergFileScanTask protobuf messages            │
│    → Execute scan for partition 5                            │
└─────────────────────────────────────────────────────────────┘
```

**The problem**: Native code receives the shared protobuf plan (step 3) but needs partition-specific tasks (step 5). The JNI callback (step 6) bridges this gap.

### Why Not Embed Tasks in Protobuf?

You might ask: **"Why not include partition-specific data in the protobuf?"**

**Answer**: Because Comet's architecture assumes **one serialized plan for all partitions**.

If we embedded partition-specific data in protobuf, we'd need:

| Approach | Implications |
|----------|--------------|
| **Current: JNI Callback** | ✓ One shared plan protobuf<br>✓ Leverages existing Comet architecture<br>✓ Partition data via RDD (our optimization)<br>⚠ Extra JNI roundtrip (minimal overhead) |
| **Alternative: Embed in Protobuf** | ✗ Would need N different protobuf plans (one per partition)<br>✗ Each executor receives different protobuf<br>✗ Breaks Comet's shared plan model<br>✗ Major architectural restructuring required |

### The JNI Callback as a Bridge

The JNI callback serves as a **bridge** between the two data paths:

```
┌──────────────────────────────────────────────────────┐
│           SHARED PLAN PATH                           │
│   (One serialized plan for all partitions)           │
│                                                       │
│  Driver: serializedPlanCopy → All Executors          │
│           (operator DAG)                              │
└───────────────────────┬──────────────────────────────┘
                        │
                        ▼
                ┌───────────────┐
                │  Native Code  │
                │  Deserializes │
                │     Plan      │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ IcebergScan   │
                │ Operator Init │
                │               │
                │ Needs Tasks!  │◄──── Where are the FileScanTasks?
                └───────┬───────┘
                        │
                    (JNI Callback)
                        │
                        ▼
┌──────────────────────────────────────────────────────┐
│        PARTITION-SPECIFIC DATA PATH                  │
│   (Each partition carries its own data)              │
│                                                       │
│  Driver: IcebergScanRDD → Executor N                 │
│          (taskBytes_N)     (thread-local)            │
└──────────────────────────────────────────────────────┘
```

### Performance Impact of JNI Callback

The JNI callback overhead is **minimal** compared to the optimization benefits:

- **JNI call overhead**: ~1-10 microseconds (one-time per partition)
- **Data transfer savings**: 100-200× reduction in network transfer (ongoing)
- **Memory savings**: 100-200× reduction in executor memory (ongoing)

For a table with 10,000 partitions:
- JNI overhead: 10,000 partitions × 10μs = **0.1 seconds total**
- Network savings: 100GB → 500MB = **99.5 GB saved**
- Memory savings: 100GB → 500MB executor memory = **199.5 GB saved**

The JNI callback cost is **negligible** compared to the massive savings from partition-specific distribution.

### Summary: Why JNI Callback is Required

The JNI callback is necessary because:

1. **Comet's architecture**: Uses one shared serialized plan for all partitions (performance optimization)
2. **Partition data arrives separately**: Via RDD partition objects (our network optimization)
3. **Native code needs both**: Plan structure (from protobuf) + partition tasks (from RDD)
4. **JNI callback bridges them**: Allows native code to retrieve partition data during operator initialization
5. **Minimal overhead**: One-time microsecond cost vs. gigabyte-scale savings

Without the JNI callback, we would have to fundamentally restructure how Comet serializes and distributes execution plans—a much larger, riskier change that would affect all Comet operators, not just Iceberg scans.

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                            DRIVER (Planning Phase)                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  1. CometIcebergNativeScan.convert()                                 │
│     ├─ Iterate partitions: for i in 0..N                             │
│     ├─ Extract FileScanTasks for partition i                         │
│     ├─ Serialize to protobuf: tasks_i → bytes_i                      │
│     └─ Store: partitionTasksMap[i] = bytes_i                         │
│                                                                       │
│  2. CometIcebergNativeScanExec.apply()                               │
│     └─ Create exec with partitionTasks: Map[Int, Array[Byte]]       │
│                                                                       │
│  3. IcebergScanRDD created                                           │
│     └─ getPartitions() creates:                                      │
│        ├─ IcebergScanPartition(0, bytes_0)                           │
│        ├─ IcebergScanPartition(1, bytes_1)                           │
│        └─ ...                                                         │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Spark Task Scheduling
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    EXECUTORS (Execution Phase)                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐        │
│  │  Executor A    │  │  Executor B    │  │  Executor C    │        │
│  │                │  │                │  │                │        │
│  │  Receives:     │  │  Receives:     │  │  Receives:     │        │
│  │  Partition(0)  │  │  Partition(5)  │  │  Partition(8)  │        │
│  │  + bytes_0     │  │  + bytes_5     │  │  + bytes_8     │        │
│  │                │  │                │  │                │        │
│  │  100 KB        │  │  100 KB        │  │  100 KB        │        │
│  └────────────────┘  └────────────────┘  └────────────────┘        │
│         │                    │                    │                  │
│         ▼                    ▼                    ▼                  │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐        │
│  │ Thread-local   │  │ Thread-local   │  │ Thread-local   │        │
│  │ bytes_0        │  │ bytes_5        │  │ bytes_8        │        │
│  └────────────────┘  └────────────────┘  └────────────────┘        │
│         │                    │                    │                  │
│         ▼                    ▼                    ▼                  │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐        │
│  │ Native Exec    │  │ Native Exec    │  │ Native Exec    │        │
│  │ (JNI callback) │  │ (JNI callback) │  │ (JNI callback) │        │
│  │ ├─ getTasks()  │  │ ├─ getTasks()  │  │ ├─ getTasks()  │        │
│  │ ├─ Parse       │  │ ├─ Parse       │  │ ├─ Parse       │        │
│  │ └─ Execute     │  │ └─ Execute     │  │ └─ Execute     │        │
│  └────────────────┘  └────────────────┘  └────────────────┘        │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Performance Impact Analysis

### Network Transfer Savings

**Before optimization:**
- Total data per executor = N × avg_task_size
- Total cluster network = num_executors × N × avg_task_size

**After optimization:**
- Total data per executor = avg_tasks_per_executor × avg_task_size
- Total cluster network = num_executors × avg_tasks_per_executor × avg_task_size

**Savings ratio:**
```
savings = 1 - (avg_tasks_per_executor / N)
```

For evenly distributed data: `avg_tasks_per_executor ≈ N / num_executors`

### Example: Large Table Scan

**Scenario:**
- Table with 10,000 partitions
- 200 executors
- 50KB average task data per partition
- Total task metadata: 10,000 × 50KB = **500MB**

**Before optimization:**
- Each executor receives: **500MB** (all partition data)
- Total network transfer: 200 × 500MB = **100GB**
- Each executor uses: ~50 partitions × 50KB = **2.5MB** (0.5%)
- Wasted transfer: **99.5%**

**After optimization:**
- Each executor receives: ~50 × 50KB = **2.5MB** (only its partitions)
- Total network transfer: 200 × 2.5MB = **500MB**
- Each executor uses: **2.5MB** (100%)
- Network savings: **199.5× reduction** (100GB → 500MB)

### Memory Pressure Reduction

**Before:**
- Driver memory: 500MB (serialize all tasks)
- Executor memory: 500MB × 200 = **100GB** across cluster
- GC pressure: High (500MB objects per executor)

**After:**
- Driver memory: 500MB (same, but partitioned)
- Executor memory: 2.5MB × 200 = **500MB** across cluster
- GC pressure: Low (2.5MB objects per executor)
- **200× memory reduction** on executors

---

## Key Design Decisions

### 1. Why Not Use Broadcast Variables?

Spark's broadcast variables would still send all data to all executors. The optimization relies on Spark's **task-level serialization**, which only sends partition-specific data.

### 2. Why Thread-Local Storage?

**Problem**: Need to pass partition-specific data from JVM to native code during execution.

**Options considered:**
1. **Pass as function parameter**: Would require modifying the entire call chain
2. **Global state**: Unsafe with concurrent tasks
3. **Thread-local**: ✓ Safe, simple, minimal API changes

**Chosen solution**: Thread-local storage provides thread-safe isolation with minimal code changes.

### 3. Why Custom RDD Instead of MapPartitionsRDD?

`MapPartitionsRDD` would still require all partition data to be captured in the closure. By implementing a custom RDD with partition-specific data embedded in `Partition` objects, we leverage Spark's built-in partition-level serialization.

### 4. What About Protobuf Deduplication?

The code still uses deduplication pools (CometIcebergNativeScan.scala:696-705) to reduce redundancy **within each partition's task data**:
- Schema pool
- Partition spec pool
- Delete files pool
- etc.

This is **orthogonal** to the partition distribution optimization. Both work together:
- **Deduplication**: Reduces task data size within each partition
- **Partition-specific distribution**: Ensures executors only receive their partition data

---

## Code Flow Summary

### Query Planning (Driver)
1. `CometScanRule` → creates `CometBatchScanExec` with Iceberg metadata
2. `CometIcebergNativeScan.convert()` → serializes plan to protobuf
   - Extracts FileScanTasks per partition
   - Stores in `partitionTasksMap: Map[Int, Array[Byte]]`
   - Caches in `partitionTasksCache` keyed by plan ID
3. `CometIcebergNativeScan.createExec()` → creates `CometIcebergNativeScanExec`
   - Retrieves `partitionTasks` from cache
   - Passes to exec constructor
4. `CometIcebergNativeScanExec.convertBlock()` → serializes plan for executors
   - Preserves `partitionTasks` in serialized copy (@transient field)
5. `CometIcebergNativeScanExec.doExecuteColumnar()` → creates `IcebergScanRDD`
   - Passes `partitionTasks` map to RDD constructor

### Task Execution (Executors)
1. Spark schedules task for partition `i` on executor
2. Spark serializes and sends `IcebergScanPartition(i, taskBytes_i)` to executor
3. `IcebergScanRDD.compute()` called with partition object
4. `createIterator(i, taskBytes_i)` called:
   - Stores `taskBytes_i` in thread-local: `Native.setIcebergPartitionTasks(taskBytes_i)`
   - Creates native execution plan: `new CometExecIterator(...)`
5. Native planner initializes Iceberg scan operator
6. Native code calls `getIcebergPartitionTasks()` via JNI
7. JNI calls back to `Native.getIcebergPartitionTasksInternal()`
8. Returns task bytes from thread-local
9. Native code deserializes and executes tasks
10. After execution: `Native.clearIcebergPartitionTasks()`

---

## Testing & Verification

To verify the optimization is working:

1. **Check logs for partition data distribution:**
   ```
   INFO CometIcebergNativeScan: Cached N partitions (avg X bytes/partition)
   ```

2. **Monitor network transfer:**
   - Use Spark UI → Stages → Task Metrics → "Shuffle Read Size"
   - Compare total serialized task size before/after optimization

3. **Memory profiling:**
   - Executor heap dumps before/after
   - Should see significant reduction in task metadata size per executor

4. **Correctness:**
   - Run full Iceberg test suite
   - Verify query results match expected output
   - Check partition pruning still works correctly

---

## Future Enhancements

1. **Dynamic partition balancing**: Adjust partition assignments based on data skew
2. **Lazy task loading**: Load task data on-demand instead of upfront
3. **Compression**: Compress task bytes before serialization
4. **Metadata caching**: Cache common metadata (schemas, specs) separately from tasks

---

## Conclusion

This optimization achieves **O(1) task data per executor** instead of **O(N)** by:

1. **Partitioning task data** during planning (driver-side)
2. **Embedding partition-specific data** in custom RDD partitions
3. **Leveraging Spark's task serialization** to send only relevant data to each executor
4. **Using thread-local storage** to bridge JVM/native boundary safely

**Result**: 100-200× reduction in network transfer and executor memory usage for large tables with many partitions.
