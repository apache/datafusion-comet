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

import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, ValueVector, VarBinaryVector, VarCharVector, ViewVarBinaryVector, ViewVarCharVector}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}

import org.apache.comet.udf.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}

/**
 * Arrow-direct codegen dispatcher. For each (bound Spark `Expression`, input Arrow schema) pair,
 * compiles a specialized [[CometBatchKernel]] on first encounter and caches the compile.
 * Subsequent batches with the same expression and the same schema reuse the cached compile.
 *
 * ==Transport==
 *
 * Arg 0 is a `VarBinaryVector` scalar carrying the serialized Expression bytes (produced on the
 * driver by [[org.apache.spark.SparkEnv SparkEnv]]'s closure serializer). Args 1..N are the data
 * columns the bound expression's `BoundReference`s refer to, in ordinal order. The bytes
 * self-describe the expression so the path works in cluster mode without executor-side state.
 *
 * ==Cache key: serialized expression plus input schema fingerprint==
 *
 * Compile-time specialization bakes the concrete Arrow vector class and the nullability of each
 * input column into the generated kernel. A batch with the same expression but a different input
 * vector class (e.g. `VarCharVector` vs `ViewVarCharVector`) is a different kernel. The cache key
 * therefore combines the expression bytes with the per-column [[ArrowColumnSpec]] list.
 *
 * ==Three cache layers==
 *
 * The dispatcher composes three caches at three different scopes. They are not redundant: each
 * holds something the others do not, and collapsing any pair would either lose correctness or pay
 * an avoidable cost. Walking from broadest to narrowest:
 *
 *   1. '''JVM-wide compile cache.''' Holds `CompiledKernel(GeneratedClass, references)` keyed by
 *      [[CometCodegenDispatchUDF.CacheKey]]. Lives on this object's companion (`kernelCache`).
 *      Bounded LRU using the `synchronizedMap(LinkedHashMap(accessOrder=true)) +
 *      removeEldestEntry` pattern from `IcebergPlanDataInjector.commonCache`. Amortizes the
 *      Janino compile cost across every thread and every query in the JVM.
 *
 * 2. '''Per-thread UDF instance cache.''' `CometUdfBridge.INSTANCES` is a `ThreadLocal<Map>` that
 * hands each task thread its own `CometCodegenDispatchUDF` object (one per UDF class). Lets
 * instance fields on this UDF (cache 3 below) stay safe without synchronisation.
 *
 * 3. '''Per-partition kernel instance cache.''' Plain mutable fields `activeKernel`, `activeKey`,
 * `activePartition` on each UDF instance, managed by [[ensureKernel]]. The compiled
 * `GeneratedClass` from cache 1 produces a kernel instance, and the kernel carries per-row
 * mutable state (`Rand`'s `XORShiftRandom`, `MonotonicallyIncreasingID`'s counter,
 * `addMutableState` fields) that must advance across batches in one partition and reset across
 * partitions. `ensureKernel` allocates a fresh kernel and calls `init(partitionIndex)` only when
 * the partition or cache key changes; otherwise the same kernel handles every batch in the
 * partition.
 *
 * Why none of the three can be collapsed:
 *
 *   - Collapse 1 + 3 (per-thread compile cache): every thread would re-run Janino for the same
 *     expression. Wasteful.
 *   - Collapse 1 + 2 (no per-thread UDF separation): every thread would share one UDF instance.
 *     Cache 3's instance fields would race; we'd need a `ConcurrentHashMap` keyed on `(thread,
 *     partition, key)` or explicit locking.
 *   - Collapse 2 + 3 (no per-partition resets): partition state would never reset, so a sequence
 *     started in partition 0 would continue into partition 1 and our results would diverge from
 *     Spark's.
 *
 * Each cache is the smallest scope that still does its job.
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

    // TODO: dictionary-encoded inputs. Comet's native scan/shuffle paths currently materialize
    // dictionaries before the UDF bridge, so we do not expect dict-encoded `FieldVector`s here.
    // If that invariant is ever relaxed upstream, `v.getField.getDictionary != null` will be
    // true on some arrivals and the cast in the pattern match below will throw ClassCast-style
    // errors. The fix at that point: materialize at the dispatcher via `CDataDictionaryProvider`
    // (see `NativeUtil.importVector`) or widen `typedInputAccessors` with a dict-index read
    // plus a lookup into the dictionary vector. Materialization is simpler; per-kernel
    // specialization is faster but adds a cache-key dimension.

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
    kernel.process(dataCols, out, n)
    out.setValueCount(n)
    out
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
   * Build the compile-time spec for one input Arrow vector. Recurses on `ListVector`'s data
   * vector to produce an [[ArrayColumnSpec]] carrying the element's concrete vector class and
   * Spark element type; scalars produce a [[ScalarColumnSpec]] directly. Unknown vector classes
   * fall through with an explicit error so the dispatcher surface is a single edit point when
   * extending to new Arrow types.
   */
  private def specFor(v: ValueVector): ArrowColumnSpec = v match {
    case list: ListVector =>
      val child = list.getDataVector
      ArrayColumnSpec(nullable(list), sparkTypeFor(child), specFor(child))
    case struct: StructVector =>
      val fieldSpecs = (0 until struct.size()).map { fi =>
        val childVec = struct.getChildByOrdinal(fi).asInstanceOf[ValueVector]
        val field = struct.getField.getChildren.get(fi)
        StructFieldSpec(
          name = field.getName,
          sparkType = sparkTypeFor(childVec),
          nullable = field.isNullable,
          child = specFor(childVec))
      }
      StructColumnSpec(nullable(struct), fieldSpecs)
    case _: BitVector | _: TinyIntVector | _: SmallIntVector | _: IntVector | _: BigIntVector |
        _: Float4Vector | _: Float8Vector | _: DecimalVector | _: VarCharVector |
        _: ViewVarCharVector | _: VarBinaryVector | _: ViewVarBinaryVector | _: DateDayVector |
        _: TimeStampMicroVector | _: TimeStampMicroTZVector =>
      ScalarColumnSpec(v.getClass.asInstanceOf[Class[_ <: ValueVector]], nullable(v))
    case other =>
      throw new UnsupportedOperationException(
        s"CometCodegenDispatchUDF: unsupported Arrow vector ${other.getClass.getSimpleName}")
  }

  /**
   * Map an Arrow vector to its Spark `DataType`. Used to populate
   * [[ArrayColumnSpec.elementSparkType]] so the codegen nested-class emitter can pick the right
   * element-getter template from the element's static Spark type (rather than re-deriving it from
   * the vector class).
   */
  private def sparkTypeFor(v: ValueVector): DataType = v match {
    case _: BitVector => BooleanType
    case _: TinyIntVector => ByteType
    case _: SmallIntVector => ShortType
    case _: IntVector => IntegerType
    case _: BigIntVector => LongType
    case _: Float4Vector => FloatType
    case _: Float8Vector => DoubleType
    case d: DecimalVector => DecimalType(d.getPrecision, d.getScale)
    case _: VarCharVector | _: ViewVarCharVector => StringType
    case _: VarBinaryVector | _: ViewVarBinaryVector => BinaryType
    case _: DateDayVector => DateType
    case _: TimeStampMicroVector => TimestampNTZType
    case _: TimeStampMicroTZVector => TimestampType
    case list: ListVector =>
      ArrayType(sparkTypeFor(list.getDataVector))
    case struct: StructVector =>
      val sparkFields = (0 until struct.size()).map { fi =>
        val childVec = struct.getChildByOrdinal(fi).asInstanceOf[ValueVector]
        val field = struct.getField.getChildren.get(fi)
        StructField(field.getName, sparkTypeFor(childVec), field.isNullable)
      }
      StructType(sparkFields.toArray)
    case other =>
      throw new UnsupportedOperationException(
        s"CometCodegenDispatchUDF: no Spark type mapping for ${other.getClass.getSimpleName}")
  }

  /**
   * Estimate output byte capacity for variable-length output types. Sums the data-buffer sizes of
   * variable-length input vectors as an upper bound for typical transform expressions (replace,
   * upper, lower, substring, concat on the same inputs). Covers both character and binary
   * variable-width vectors and their view-format counterparts so the estimate is meaningful
   * regardless of which string / binary input type the caller passed in. Underestimates are still
   * corrected by `setSafe`; this just reduces the odds of mid-loop reallocation.
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
            case v: ViewVarCharVector =>
              val bufs = v.getDataBuffers
              var j = 0
              while (j < bufs.size()) {
                sum += bufs.get(j).writerIndex().toInt
                j += 1
              }
            case v: ViewVarBinaryVector =>
              val bufs = v.getDataBuffers
              var j = 0
              while (j < bufs.size()) {
                sum += bufs.get(j).writerIndex().toInt
                j += 1
              }
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
   * Cache key: serialized expression bytes + per-column compile-time invariants.
   *
   * TODO(perf): Every batch invocation walks `bytesKey` once for `hashCode` (and again for
   * `equals` on hash collision / final confirm in `ensureKernel`), so HashMap lookup is
   * O(bytes.length) per batch. For small expressions (a few KB) this is single-digit us and
   * invisible; for large ScalaUDF closures with heavy encoders (tens to hundreds of KB) it can
   * climb to tens of us per batch, measurable at ~1-10% of hot-path time. If a workload shows
   * this on a profile, three succinct alternatives worth exploring:
   *
   *   1. Driver-side precomputed hash piggybacked through the Arrow transport as a small tag
   *      (e.g. 8 bytes). Executor uses the tag directly as the key. O(1) per batch, and the tag
   *      is tiny versus the full byte array. 2. Per-UDF-instance byte-identity fast path.
   *      `CometCodegenDispatchUDF` is per-thread; the expression is invariant for the life of one
   *      task. Memoize the last-seen `(Arrow data buffer address, offset, length)` tuple and skip
   *      the HashMap entirely when it matches. `VarBinaryVector.get(0)` allocates a fresh
   *      `byte[]` each call, so identity-on-the-array won't hit, but the underlying Arrow buffer
   *      address should be stable within a task. 3. Two-level cache with source-string outer
   *      tier. Keep bytes-based L1 as today; add an L2 keyed on `generateSource(expr).code.body`
   *      that stores only the Janino-compiled class (no references). On L1 miss + L2 hit, skip
   *      Janino compile and reuse the class with fresh per-call references. Captures the "same
   *      lambda, different closure identity" cross-query reuse case (e.g. the same `udf((i: Int)
   *      \=> i + 1)` registered across sessions produces identical source but different
   *      serialized bytes).
   *
   * None of these are worth doing until a profile shows lookup in the hot path. Today's bytes-
   * based key is correct and simple.
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
