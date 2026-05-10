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

import java.nio.ByteBuffer
import java.util.{Collections, LinkedHashMap}
import java.util.concurrent.atomic.AtomicLong

import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, ValueVector, VarBinaryVector, VarCharVector}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import org.apache.comet.udf.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}

/**
 * Arrow-direct codegen dispatcher. For each (bound Spark `Expression`, input Arrow schema) pair,
 * compiles a specialized [[CometBatchKernel]] on first encounter and caches the compile.
 * Subsequent batches with the same expression and schema reuse the cached compile.
 *
 * Arg 0 is a `VarBinaryVector` scalar carrying the serialized Expression bytes (produced on the
 * driver by Spark's closure serializer). Args 1..N are the data columns the `BoundReference`s
 * refer to, in ordinal order. The bytes self-describe the expression so the path works in cluster
 * mode without executor-side state.
 *
 * Three caches compose at different scopes: the JVM-wide compile cache on the companion
 * (`kernelCache`); a per-thread UDF instance map in `CometUdfBridge.INSTANCES`; and per-partition
 * kernel instance state on this object (`activeKernel`, `activeKey`, `activePartition`) managed
 * by [[ensureKernel]]. See `docs/source/contributor-guide/jvm_udf_dispatch.md` for the rationale
 * and why none of the layers can be collapsed.
 */
class CometCodegenDispatchUDF extends CometUDF {

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(
      inputs.length >= 1,
      "CometCodegenDispatchUDF requires at least 1 input (serialized expression), " +
        s"got ${inputs.length}")
    val exprVec = inputs(0).asInstanceOf[VarBinaryVector]
    require(
      exprVec.getValueCount >= 1 && !exprVec.isNull(0),
      "CometCodegenDispatchUDF requires non-null serialized expression bytes at arg 0")
    val bytes = exprVec.get(0)

    // TODO(dict-encoded): kernels assume materialized inputs; dict-encoded vectors would fail the
    // cast in `specFor` below. See docs/source/contributor-guide/jvm_udf_dispatch.md#open-items.

    val numDataCols = inputs.length - 1
    val dataCols = new Array[ValueVector](numDataCols)
    val specs = new Array[ArrowColumnSpec](numDataCols)
    var di = 0
    while (di < numDataCols) {
      val v = inputs(di + 1)
      dataCols(di) = v
      specs(di) = specFor(v)
      di += 1
    }
    val n = numRows
    val specsSeq = specs.toIndexedSeq

    val key = CometCodegenDispatchUDF.CacheKey(ByteBuffer.wrap(bytes), specsSeq)
    val entry = CometCodegenDispatchUDF.lookupOrCompile(key, bytes, specsSeq)

    val partitionId = CometCodegenDispatchUDF.currentPartitionIndex()
    val kernel = ensureKernel(entry.compiled, key, partitionId)

    val out = CometBatchKernelCodegen.allocateOutput(
      entry.outputType,
      "codegen_result",
      n,
      estimatedOutputBytes(entry.outputType, dataCols))
    try {
      kernel.process(dataCols, out, n)
      out.setValueCount(n)
      out
    } catch {
      case t: Throwable =>
        try out.close()
        catch { case _: Throwable => () }
        throw t
    }
  }

  /**
   * Per-partition kernel instance cache. The dispatcher's compile cache (on the companion object)
   * is JVM-wide and stores the compiled `GeneratedClass`. The kernel '''instance''', however,
   * holds per-row mutable state for non-deterministic and stateful expressions (`Rand`'s
   * `XORShiftRandom`, `MonotonicallyIncreasingID`'s counter, etc.). That state must advance
   * across batches in one partition and reset across partitions. Allocating per batch (the prior
   * model) reset state every batch and was wrong; allocating per partition is right.
   *
   * `CometCodegenDispatchUDF` is per-thread via `CometUdfBridge.INSTANCES`, and Spark tasks are
   * single-threaded on a partition, so plain instance fields are safe without synchronisation. A
   * different partition or a different cached expression flowing through the same thread triggers
   * a fresh allocation; same partition + same expression reuses the kernel.
   */
  private var activeKernel: CometBatchKernel = _
  private var activeKey: CometCodegenDispatchUDF.CacheKey = _
  private var activePartition: Int = -1

  private def ensureKernel(
      compiled: CometBatchKernelCodegen.CompiledKernel,
      key: CometCodegenDispatchUDF.CacheKey,
      partitionId: Int): CometBatchKernel = {
    if (activeKernel == null || activePartition != partitionId || activeKey != key) {
      activeKernel = compiled.newInstance()
      activeKernel.init(partitionId)
      activeKey = key
      activePartition = partitionId
    }
    activeKernel
  }

  /**
   * Did any row in this Arrow vector set the null bit? The cache key carries this per column, so
   * a batch with no nulls and a later batch with nulls map to different keys and different
   * compiles, no correctness risk from flipping this. The tighter `nullable=false` compile lets
   * the kernel emit `return false` from its `isNullAt` switch and, once paired with the
   * BoundReference tree rewrite in `lookupOrCompile`, lets Spark's `BoundReference.genCode` skip
   * the null branch at source level rather than relying on JIT constant-folding.
   *
   * Trade-off: if real workloads flip a column's nullability frequently across batches, each
   * expression caches up to `2^numCols` variants and the bounded LRU churns. The common case is
   * stable per-column nullability per query, which keeps variance at one kernel per expression.
   */
  private def nullable(v: ValueVector): Boolean = v.getNullCount != 0

  /**
   * Build the compile-time spec for one input Arrow vector. Recurses on complex types; scalars
   * produce a [[ScalarColumnSpec]] carrying the concrete Arrow vector class and nullability.
   * Spark `DataType`s on complex children come from [[Utils.fromArrowField]] so the Arrow ->
   * Spark mapping stays in one place.
   */
  private def specFor(v: ValueVector): ArrowColumnSpec = v match {
    case map: MapVector =>
      // MapVector extends ListVector; match it first. Its data vector is a StructVector with
      // child 0 = key and child 1 = value.
      val struct = map.getDataVector.asInstanceOf[StructVector]
      val keyVec = struct.getChildByOrdinal(0).asInstanceOf[ValueVector]
      val valueVec = struct.getChildByOrdinal(1).asInstanceOf[ValueVector]
      MapColumnSpec(
        nullable = nullable(map),
        keySparkType = Utils.fromArrowField(keyVec.getField),
        valueSparkType = Utils.fromArrowField(valueVec.getField),
        key = specFor(keyVec),
        value = specFor(valueVec))
    case list: ListVector =>
      val child = list.getDataVector
      ArrayColumnSpec(nullable(list), Utils.fromArrowField(child.getField), specFor(child))
    case struct: StructVector =>
      val fieldSpecs = (0 until struct.size()).map { fi =>
        val childVec = struct.getChildByOrdinal(fi).asInstanceOf[ValueVector]
        val field = struct.getField.getChildren.get(fi)
        StructFieldSpec(
          name = field.getName,
          sparkType = Utils.fromArrowField(field),
          nullable = field.isNullable,
          child = specFor(childVec))
      }
      StructColumnSpec(nullable(struct), fieldSpecs)
    case _: BitVector | _: TinyIntVector | _: SmallIntVector | _: IntVector | _: BigIntVector |
        _: Float4Vector | _: Float8Vector | _: DecimalVector | _: VarCharVector |
        _: VarBinaryVector | _: DateDayVector | _: TimeStampMicroVector |
        _: TimeStampMicroTZVector =>
      ScalarColumnSpec(v.getClass.asInstanceOf[Class[_ <: ValueVector]], nullable(v))
    case other =>
      throw new UnsupportedOperationException(
        s"CometCodegenDispatchUDF: unsupported Arrow vector ${other.getClass.getSimpleName}")
  }

  /**
   * Estimate output byte capacity for variable-length output types. Sums the data-buffer sizes of
   * variable-length input vectors as an upper bound for typical transform expressions (replace,
   * upper, lower, substring, concat on the same inputs). Underestimates are still corrected by
   * `setSafe`; this just reduces the odds of mid-loop reallocation.
   */
  private def estimatedOutputBytes(outputType: DataType, dataCols: Array[ValueVector]): Int = {
    outputType match {
      case _: StringType | _: BinaryType =>
        var sum = 0
        var i = 0
        while (i < dataCols.length) {
          dataCols(i) match {
            case v: VarCharVector => sum += v.getDataBuffer.writerIndex().toInt
            case v: VarBinaryVector => sum += v.getDataBuffer.writerIndex().toInt
            case _ => // no size hint for fixed-width vector types
          }
          i += 1
        }
        sum
      case _ => -1
    }
  }
}

object CometCodegenDispatchUDF {

  private val CacheCapacity: Int = 128

  /**
   * Cache key: serialized expression bytes plus per-column compile-time invariants.
   *
   * `hashCode` walks `bytesKey` per lookup, so for large ScalaUDF closures it scales with closure
   * size. TODO(perf-cache-key): see
   * `docs/source/contributor-guide/jvm_udf_dispatch.md#open-items` for possible optimizations if
   * a workload makes this hot.
   */
  final case class CacheKey(bytesKey: ByteBuffer, specs: IndexedSeq[ArrowColumnSpec])

  private case class CacheEntry(
      compiled: CometBatchKernelCodegen.CompiledKernel,
      outputType: DataType)

  private val kernelCache: java.util.Map[CacheKey, CacheEntry] =
    Collections.synchronizedMap(
      new LinkedHashMap[CacheKey, CacheEntry](CacheCapacity, 0.75f, true) {
        override def removeEldestEntry(
            eldest: java.util.Map.Entry[CacheKey, CacheEntry]): Boolean =
          size() > CacheCapacity
      })

  // Observability counters. Incremented under the `kernelCache.synchronized` block in
  // `lookupOrCompile` so counter increments and cache mutations cannot interleave. Read via
  // [[stats]]; reset via [[resetStats]] for tests.
  private val compileCount = new AtomicLong(0)
  private val cacheHitCount = new AtomicLong(0)

  /**
   * Snapshot of dispatcher cache counters and current size. Intended for tests, logging, and
   * future integration with Spark SQL metrics. Not thread-synchronized across the three fields
   * (each read is atomic, but they are not read atomically together); snapshots taken during
   * concurrent activity may show a consistent individual-field view but a slightly inconsistent
   * combined view. Fine for reporting, not for assertions that require cross-field invariants.
   */
  final case class DispatcherStats(compileCount: Long, cacheHitCount: Long, cacheSize: Int) {
    def totalLookups: Long = compileCount + cacheHitCount
    def hitRate: Double =
      if (totalLookups == 0) 0.0 else cacheHitCount.toDouble / totalLookups.toDouble
  }

  /** Returns a snapshot of cache counters and current size. Cheap; safe to call anytime. */
  def stats(): DispatcherStats =
    DispatcherStats(compileCount.get(), cacheHitCount.get(), kernelCache.size())

  /** Reset counters to zero. Leaves the compile cache intact. Intended for tests. */
  def resetStats(): Unit = {
    compileCount.set(0)
    cacheHitCount.set(0)
  }

  /**
   * Test-facing snapshot of compiled kernel signatures currently in the cache. Each entry is the
   * pair `(input Arrow vector classes in ordinal order, output Spark DataType)` the kernel
   * compiled against. Lets tests assert that the dispatcher actually specialized on the types it
   * was expected to, not just that the query returned a correct result (which Spark would do
   * regardless of how the kernel was shaped).
   *
   * Drops the `ArrowColumnSpec.nullable` bit to keep assertions robust to per-batch nullability
   * variance: test data with no nulls compiles with `nullable=false` and the same expression run
   * against data with nulls would cache a second variant. Tests assert on vector class and output
   * type; both variants satisfy the same assertion.
   */
  def snapshotCompiledSignatures(): Set[(IndexedSeq[Class[_ <: ValueVector]], DataType)] = {
    kernelCache.synchronized {
      import scala.jdk.CollectionConverters._
      kernelCache
        .entrySet()
        .asScala
        .iterator
        .map { e =>
          (e.getKey.specs.map(_.vectorClass), e.getValue.outputType)
        }
        .toSet
    }
  }

  private def lookupOrCompile(
      key: CacheKey,
      bytes: Array[Byte],
      specs: IndexedSeq[ArrowColumnSpec]): CacheEntry = {
    kernelCache.synchronized {
      val existing = kernelCache.get(key)
      if (existing != null) {
        cacheHitCount.incrementAndGet()
        existing
      } else {
        // Use a classloader that can see Spark classes. The Comet native runtime calls us on a
        // Tokio worker thread where the context classloader may not be set to Spark's task
        // loader, so fall back to the loader that loaded `Expression` itself if needed.
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(classOf[Expression].getClassLoader)
        val rawExpr = SparkEnv.get.closureSerializer
          .newInstance()
          .deserialize[Expression](ByteBuffer.wrap(bytes), loader)
        // Tighten BoundReference.nullable based on the observed batch. The plan-time value is
        // conservative (the column may be null somewhere in the query's execution), but for
        // this specific batch we know. Rewriting lets Spark's `BoundReference.genCode` skip the
        // `isNull` branch at source level rather than leaving it to JIT constant-folding.
        // Correctness is preserved by the cache key: a later batch with nulls on this column has
        // a different `specs`, so it hits a different kernel compiled with nullable=true.
        val boundExpr = rewriteBoundReferences(rawExpr, specs)
        val compiled = CometBatchKernelCodegen.compile(boundExpr, specs)
        val entry = CacheEntry(compiled, boundExpr.dataType)
        kernelCache.put(key, entry)
        compileCount.incrementAndGet()
        entry
      }
    }
  }

  /**
   * Walk the bound expression tree and rewrite any `BoundReference(ord, dt, nullable=true)` to
   * `nullable=false` when the corresponding input column in `specs` is non-nullable for this
   * batch. Only tightens; never relaxes. Expressions outside the `BoundReference` leaves are
   * unchanged.
   */
  private def rewriteBoundReferences(
      expr: Expression,
      specs: IndexedSeq[ArrowColumnSpec]): Expression = {
    expr.transform {
      case BoundReference(ord, dt, true)
          if ord >= 0 && ord < specs.length && !specs(ord).nullable =>
        BoundReference(ord, dt, nullable = false)
      // Fall through unchanged: non-BoundReference nodes and BoundReferences that are already
      // non-nullable or point at a nullable column in this batch.
      case other => other
    }
  }

  /**
   * Partition index for the generated kernel's `init`. Expressions whose `doGenCode` calls
   * `addPartitionInitializationStatement` (e.g. `Rand`, `Randn`, `Uuid`) reseed mutable state
   * from this. Falls back to 0 when the dispatcher is exercised outside a Spark task (unit tests)
   * so an absent `TaskContext` does not fail the call; the result is still deterministic for that
   * fallback.
   */
  private def currentPartitionIndex(): Int =
    Option(TaskContext.get()).map(_.partitionId()).getOrElse(0)
}
