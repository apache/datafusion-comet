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

package org.apache.comet.udf.codegen

import java.nio.ByteBuffer
import java.util.{Collections, LinkedHashMap}
import java.util.concurrent.atomic.AtomicLong

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import org.apache.comet.codegen.{CometBatchKernel, CometBatchKernelCodegen}
import org.apache.comet.codegen.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}
import org.apache.comet.udf.CometUDF

/**
 * Arrow-direct codegen dispatcher. For each (bound `Expression`, input Arrow schema) pair,
 * compiles a specialized [[CometBatchKernel]] on first encounter and caches it; subsequent
 * batches with the same shape reuse the compile.
 *
 * Arg 0 is a `VarBinaryVector` scalar carrying the closure-serialized bound Expression bytes.
 * Args 1..N are the data columns the `BoundReference`s read, in ordinal order. The bytes
 * self-describe the expression so the path works in cluster mode without executor-side state.
 *
 * Three caches at different scopes: the JVM-wide compile cache (`kernelCache` on the companion);
 * the per-task UDF-instance cache in `CometUdfBridge.INSTANCES`; and per-partition kernel state
 * on this instance (`activeKernel`, `activeKey`, `activePartition`) managed by [[ensureKernel]].
 * Each layer covers a distinct lifetime: JVM (compiled bytecode, immutable), task (UDF instance,
 * isolated from worker reuse), partition (kernel mutable state for `Rand` /
 * `MonotonicallyIncreasingID` / etc.).
 */
class CometScalaUDFCodegen extends CometUDF {

  /**
   * Per-partition kernel instance cache. The compile cache stores the compiled `GeneratedClass`;
   * the kernel '''instance''' holds per-row mutable state (`Rand`'s `XORShiftRandom`,
   * `MonotonicallyIncreasingID`'s counter, etc.) that must advance across batches in one
   * partition and reset across partitions. Allocating per partition gets that right.
   *
   * Plain `var`s are safe: this dispatcher is per-task (`CometUdfBridge.INSTANCES` keys by
   * `taskAttemptId`) and Spark drives one partition per task, so [[ensureKernel]] never sees
   * concurrent access. A different partition or expression triggers a fresh allocation.
   */
  private var activeKernel: CometBatchKernel = _
  private var activeKey: CometScalaUDFCodegen.CacheKey = _
  private var activePartition: Int = -1

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(
      inputs.length >= 1,
      "CometScalaUDFCodegen requires at least 1 input (serialized expression), " +
        s"got ${inputs.length}")
    val exprVec = inputs(0).asInstanceOf[VarBinaryVector]
    require(
      exprVec.getValueCount >= 1 && !exprVec.isNull(0),
      "CometScalaUDFCodegen requires non-null serialized expression bytes at arg 0")
    val bytes = exprVec.get(0)

    // TODO(dict-encoded): kernels assume materialized inputs; dict-encoded vectors would fail the
    // cast in `specFor` below. Fix is to materialize at the dispatcher (via
    // `CDataDictionaryProvider`) or widen `emitTypedGetters` with a dict-index + lookup path.

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

    val key = CometScalaUDFCodegen.CacheKey(ByteBuffer.wrap(bytes), specsSeq)
    val entry = CometScalaUDFCodegen.lookupOrCompile(key, bytes, specsSeq)

    val partitionId = CometScalaUDFCodegen.currentPartitionIndex()
    val kernel = ensureKernel(entry.compiled, key, partitionId)

    val out = CometBatchKernelCodegen.allocateOutput(
      entry.outputField,
      n,
      estimatedOutputBytes(entry.outputType, dataCols))
    try {
      kernel.process(dataCols, out, n)
      out.setValueCount(n)
      out
    } catch {
      case t: Throwable =>
        try out.close()
        catch {
          case _: Throwable => ()
        }
        throw t
    }
  }

  private def ensureKernel(
      compiled: CometBatchKernelCodegen.CompiledKernel,
      key: CometScalaUDFCodegen.CacheKey,
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
   * Did any row in this batch set the null bit? Carried per column on the cache key, so batches
   * with different nullability map to different kernels (no correctness risk). The
   * `nullable=false` compile emits `return false` from `isNullAt` and, paired with the
   * `BoundReference` tree rewrite in `lookupOrCompile`, lets Spark skip the null branch at source
   * level rather than via JIT folding.
   *
   * Workloads that flip nullability frequently can cache up to `2^numCols` kernel variants per
   * expression; common-case stable nullability stays at one.
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
        s"CometScalaUDFCodegen: unsupported Arrow vector ${other.getClass.getSimpleName}")
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
            case v: BaseVariableWidthVector => sum += v.getDataBuffer.writerIndex().toInt
            case _ => // no size hint for fixed-width vector types
          }
          i += 1
        }
        sum
      case _ => -1
    }
  }
}

object CometScalaUDFCodegen {

  private val CacheCapacity: Int = 128
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

  /** Returns a snapshot of cache counters and current size. Cheap; safe to call anytime. */
  def stats(): DispatcherStats =
    DispatcherStats(compileCount.get(), cacheHitCount.get(), kernelCache.size())

  /** Reset counters to zero. Leaves the compile cache intact. Intended for tests. */
  def resetStats(): Unit = {
    compileCount.set(0)
    cacheHitCount.set(0)
  }

  /**
   * Test-facing snapshot of compiled kernel signatures: `(input Arrow vector classes in ordinal
   * order, output Spark DataType)` per cache entry. Lets tests assert specialization shape, not
   * just result correctness. Drops `ArrowColumnSpec.nullable` so a single assertion matches both
   * `nullable=true` and `nullable=false` variants of the same expression.
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
        val outputField =
          Utils.toArrowField("codegen_result", boundExpr.dataType, nullable = true, "UTC")
        val entry = CacheEntry(compiled, boundExpr.dataType, outputField)
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

  /**
   * Cache key: serialized expression bytes plus per-column compile-time invariants.
   *
   * `hashCode` walks `bytesKey` per lookup, so for large ScalaUDF closures it scales with closure
   * size. TODO(perf-cache-key): if this becomes hot, options are a driver-precomputed hash piggy-
   * backed through the proto, a per-instance last-key memoization, or a two-tier cache keyed on
   * the generated source string.
   */
  final case class CacheKey(bytesKey: ByteBuffer, specs: IndexedSeq[ArrowColumnSpec])

  /**
   * Snapshot of dispatcher cache counters and current size. Intended for tests, logging, and
   * future integration with Spark SQL metrics. Not thread-synchronized across the three fields
   * (each read is atomic, but they are not read atomically together); snapshots taken during
   * concurrent activity may show a consistent individual-field view but a slightly inconsistent
   * combined view. Fine for reporting, not for assertions that require cross-field invariants.
   */
  final case class DispatcherStats(compileCount: Long, cacheHitCount: Long, cacheSize: Int) {
    def hitRate: Double =
      if (totalLookups == 0) 0.0 else cacheHitCount.toDouble / totalLookups.toDouble

    def totalLookups: Long = compileCount + cacheHitCount
  }

  private case class CacheEntry(
      compiled: CometBatchKernelCodegen.CompiledKernel,
      outputType: DataType,
      outputField: Field)
}
