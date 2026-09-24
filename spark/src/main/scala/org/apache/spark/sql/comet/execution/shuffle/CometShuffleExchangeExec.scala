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

package org.apache.spark.sql.comet.execution.shuffle

import java.util.function.Supplier

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression, PlanExpression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.comet.{CometFilterExec, CometMetricNode, CometNativeExec, CometNativeScanExec, CometPlan, CometProjectExec, CometSinkPlaceHolder, NativeExecContext}
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}
import org.apache.spark.util.random.XORShiftRandom

import com.google.common.base.Objects

import org.apache.comet.{CometConf, CometExplainInfo, DataTypeSupport}
import org.apache.comet.CometConf.{COMET_SHUFFLE_ENABLED, COMET_SHUFFLE_MODE}
import org.apache.comet.CometSparkSessionExtensions.{cometCelebornShuffleFallbackReason, hasFallbackReason, isCometCelebornShuffleManagerEnabled, isCometShuffleManagerEnabled, isSpark40Plus, withFallbackReasons}
import org.apache.comet.serde.{Compatible, OperatorOuterClass, QueryPlanSerde, SupportLevel, Unsupported}
import org.apache.comet.serde.operator.CometSink
import org.apache.comet.shims.{CometTypeShim, ShimCometShuffleExchangeExec}

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
case class CometShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    originalPlan: ShuffleExchangeLike,
    shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS,
    shuffleType: ShuffleType = CometNativeShuffle,
    advisoryPartitionSize: Option[Long] = None)
    extends ShuffleExchangeLike
    with CometPlan
    with ShimCometShuffleExchangeExec {

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(
      sparkContext,
      "number of partitions")) ++ readMetrics ++ writeMetrics ++ CometMetricNode.shuffleMetrics(
    sparkContext)

  override def nodeName: String = if (shuffleType == CometNativeShuffle) {
    "CometExchange"
  } else {
    "CometColumnarExchange"
  }

  // Exclude originalPlan from canonical form. It's a reference to the
  // pre-Comet Spark exchange kept for metrics, not semantic content.
  // Without this, two identical CometShuffleExchangeExec nodes with
  // different originalPlans (e.g., one scan has DPP filters, one doesn't)
  // would fail to match in AQE's stageCache, preventing exchange reuse.
  // Matches CometBroadcastExchangeExec.doCanonicalize which also nulls
  // originalPlan.
  override def doCanonicalize(): SparkPlan = {
    val base = super.doCanonicalize().asInstanceOf[CometShuffleExchangeExec]
    base.copy(originalPlan = null)
  }

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  /**
   * Single-driver native-shuffle context, computed once and shared between [[inputRDD]] and
   * [[shuffleDependency]]. `Some` only when `shuffleType == CometNativeShuffle` AND the child is
   * a [[CometNativeExec]] subtree. Otherwise the dep is built via the
   * [[CometShuffleExchangeExec.prepareShuffleDependency]] convenience overload (synthetic Scan
   * placeholder).
   */
  @transient private lazy val nativeChildContext: Option[NativeExecContext] = child match {
    case nativeChild: CometNativeExec if shuffleType == CometNativeShuffle =>
      Some(nativeChild.buildNativeContext())
    case _ => None
  }

  /**
   * Positional round-robin decision, computed once so that the RDD's determinism level and the
   * writer's placement cannot disagree. Only the native writer places positionally.
   */
  @transient private[shuffle] lazy val positionalRoundRobin: Option[PositionalRoundRobin] =
    if (shuffleType == CometNativeShuffle) {
      CometShuffleExchangeExec.positionalRoundRobinSpec(outputPartitioning, child)
    } else {
      None
    }

  /** Whether this exchange's writer places rows positionally rather than by content. */
  private[shuffle] def usesPositionalRoundRobin: Boolean = positionalRoundRobin.isDefined

  @transient private lazy val nativeChildMetricNode: CometMetricNode =
    CometMetricNode.fromCometPlan(child)

  @transient lazy val inputRDD: RDD[_] = if (shuffleType == CometNativeShuffle) {
    nativeChildContext match {
      case Some(ctx) =>
        new CometNativeShuffleInputRDD(
          sparkContext,
          ctx.inputs,
          ctx.numPartitions,
          ctx.shuffleScanIndices,
          CometMetricNode(metrics, Seq(nativeChildMetricNode)),
          ctx.perPartitionByKey,
          positionalRoundRobin.isDefined)
      case None =>
        // Non-native child (e.g. CometSparkToColumnarExec): no subtree to inline. The dep gets
        // built via the convenience overload below; we just need a real RDD of batches.
        child.executeColumnar()
    }
  } else if (shuffleType == CometColumnarShuffle) {
    // Row-based shuffle. CometNativeExec.doExecute wraps columnar output with
    // ColumnarToRowExec; non-Comet children flow through directly.
    child.execute()
  } else {
    throw new UnsupportedOperationException(
      s"Unsupported shuffle type: ${shuffleType.getClass.getName}")
  }

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient
  override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      CometCelebornShuffleMaterialization.forDependency(shuffleDependency) match {
        case Some(materialization) => materialization
        case None => sparkContext.submitMapStage(shuffleDependency)
      }
    }
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] =
    new CometShuffledBatchRDD(shuffleDependency, readMetrics, partitionSpecs)

  override def runtimeStatistics: Statistics = {
    val dataSize =
      metrics("dataSize").value * Math.max(CometConf.COMET_EXCHANGE_SIZE_MULTIPLIER.get(conf), 1)
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize.toLong, Some(rowCount))
  }

  // TODO: add `override` keyword after dropping Spark-3.x supports
  def shuffleId: Int = {
    val current = shuffleDependency match {
      case comet: CometShuffleDependency[Int @unchecked, _, _] => comet.currentShuffleDependency
      case other => other
    }
    getShuffleId(current)
  }

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on the partitioning
   * scheme defined in `newPartitioning`. Those partitions of the returned ShuffleDependency will
   * be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, _, _] =
    if (shuffleType == CometNativeShuffle) {
      val dep = nativeChildContext match {
        case Some(ctx) =>
          val nativeChild = child.asInstanceOf[CometNativeExec]
          // RangePartitioner needs real rows for sampling. Reuse the precomputed context so we
          // don't re-walk the SparkPlan tree or re-broadcast the encryption Hadoop conf.
          val samplingRDD: Option[RDD[ColumnarBatch]] = outputPartitioning match {
            case _: RangePartitioning =>
              Some(
                nativeChild.executeColumnarWithContext(
                  ctx,
                  nativeChildMetricNode.withoutAggregateMetrics(nativeChild)))
            case _ => None
          }
          CometShuffleExchangeExec.prepareNativeShuffleDependency(
            inputRDD.asInstanceOf[CometNativeShuffleInputRDD],
            samplingRDD,
            child.output,
            outputPartitioning,
            serializer,
            metrics,
            NativeShuffleSpec(
              nativeChild.nativeOp,
              nativeChildMetricNode,
              ctx,
              positionalRoundRobin))
        case None =>
          CometShuffleExchangeExec.prepareShuffleDependency(
            inputRDD.asInstanceOf[RDD[ColumnarBatch]],
            child.output,
            outputPartitioning,
            serializer,
            metrics)
      }
      metrics("numPartitions").set(dep.partitioner.numPartitions)
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sparkContext,
        executionId,
        metrics("numPartitions") :: Nil)
      dep
    } else if (shuffleType == CometColumnarShuffle) {
      val dep = CometShuffleExchangeExec.prepareJVMShuffleDependency(
        inputRDD.asInstanceOf[RDD[InternalRow]],
        child.output,
        outputPartitioning,
        serializer,
        metrics)
      metrics("numPartitions").set(dep.partitioner.numPartitions)
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sparkContext,
        executionId,
        metrics("numPartitions") :: Nil)
      dep
    } else {
      throw new UnsupportedOperationException(
        s"Unsupported shuffle type: ${shuffleType.getClass.getName}")
    }

  protected override def doExecute(): RDD[InternalRow] = {
    ColumnarToRowExec(this).doExecute()
  }

  /**
   * Comet supports columnar execution.
   */
  override val supportsColumnar: Boolean = true

  /**
   * Caches the created CometShuffledBatchRDD so we can reuse that.
   */
  private var cachedShuffleRDD: CometShuffledBatchRDD = null

  /**
   * Comet returns RDD[ColumnarBatch] for columnar execution.
   */
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Returns the same CometShuffledBatchRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new CometShuffledBatchRDD(shuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometShuffleExchangeExec =
    copy(child = newChild)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometShuffleExchangeExec =>
        this.outputPartitioning == other.outputPartitioning &&
        this.shuffleOrigin == other.shuffleOrigin && this.child == other.child &&
        this.shuffleType == other.shuffleType &&
        this.advisoryPartitionSize == other.advisoryPartitionSize
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(outputPartitioning, shuffleOrigin, shuffleType, advisoryPartitionSize, child)

  override def stringArgs: Iterator[Any] =
    Iterator(outputPartitioning, shuffleOrigin, shuffleType, child) ++ Iterator(s"[plan_id=$id]")
}

object CometShuffleExchangeExec
    extends CometSink[ShuffleExchangeExec]
    with ShimCometShuffleExchangeExec
    with CometTypeShim
    with SQLConfHelper {

  override def getSupportLevel(op: ShuffleExchangeExec): SupportLevel = {
    if (shuffleSupported(op).isDefined) Compatible() else Unsupported()
  }

  /**
   * Whether a round-robin exchange over `child` places rows positionally
   * (`RoundRobinStrategy::RowGroups` in `PhysicalPlanner::create_partitioning`), and with what
   * group size. Read once on the driver and frozen with the shuffle dependency, so that the RDD's
   * determinism level, the writer's placement and the group size cannot disagree, and so that a
   * map task re-executed after the session's batch size changed still uses the group size, and so
   * the placement, of the attempt it replaces. On an executor `CometConf.get()` resolves against
   * a `SQLConf` rebuilt from the task's local properties, which returned the default group size
   * rather than the session's.
   *
   * Only where [[replaysRowsInOrder]] holds, and not under Celeborn, whose push path has not been
   * shown to handle sliced batches or an indeterminate stage's rollback. The `numPartitions > 1`
   * guard mirrors `isRoundRobin` in `prepareJVMShuffleDependency`: with one output partition
   * there is no placement to get wrong.
   */
  def positionalRoundRobinSpec(
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[PositionalRoundRobin] = {
    val eligible = outputPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      outputPartitioning.numPartitions > 1 &&
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_POSITIONAL_ENABLED.get() &&
      !isCometCelebornShuffleManagerEnabled(conf) &&
      replaysRowsInOrder(child)
    if (eligible) {
      Some(
        PositionalRoundRobin(
          resolvePositionalGroupRows(
            CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_POSITIONAL_GROUP_ROWS.get(),
            CometConf.COMET_BATCH_SIZE.get(),
            outputPartitioning.numPartitions)))
    } else {
      None
    }
  }

  /**
   * Smallest derived group, which caps how finely a batch is cut: with far more output partitions
   * than `batchSize / 64`, `batchSize / numPartitions` would round down towards one row and turn
   * the flush back into the per-row gather positional placement exists to avoid. Not an alignment
   * guarantee: after a filter a batch starts at an arbitrary row ordinal, so its runs start off a
   * byte boundary whatever the group size.
   */
  private val MinDerivedGroupRows = 64

  /**
   * The group size positional placement uses. An explicit `configured` value is taken as given;
   * `0` derives `batchSize / numPartitions`, floored at [[MinDerivedGroupRows]] and capped at a
   * batch, so that each task wraps around the output partitions about once per batch.
   */
  private[shuffle] def resolvePositionalGroupRows(
      configured: Int,
      batchSize: Int,
      numPartitions: Int): Int = {
    if (configured > 0) {
      configured
    } else {
      val batch = math.max(batchSize, 1)
      val derived = batch / math.max(numPartitions, 1)
      math.min(math.max(derived, math.min(MinDerivedGroupRows, batch)), batch)
    }
  }

  /**
   * Output partition that map task `mapPartitionId` places its first group in. This is Spark's
   * own round-robin start, scrambled through `XORShiftRandom` because adjacent starts leave the
   * tail of the partition space empty (SPARK-21782), and a pure function of the map partition so
   * that a re-executed task reproduces its placement. The `+ 1` is Spark's pre-increment. See
   * `native_shuffle.md` for why the starts must be decorrelated rather than merely distinct.
   */
  def positionalStartPartition(mapPartitionId: Int, numPartitions: Int): Int =
    new XORShiftRandom(mapPartitionId).nextInt(math.max(numPartitions, 1)) + 1

  /**
   * Whether re-executing this subtree yields the same rows in the same order.
   *
   * Positional placement is a function of row order, so this is the only thing standing between
   * it and SPARK-23207, and it asks more than Spark's own round robin does: by default Spark
   * sorts each map partition before assigning positions
   * (`spark.sql.execution.sortBeforeRepartition`), so a retry only has to produce the same rows.
   * `CometNativeShuffleInputRDD` mirrors Spark's `isOrderSensitive` rule for the RDD graph, but
   * under this allowlist the only leaf is a native scan, which contributes no RDD input, so that
   * check cannot fire. Widening this is what would make it live.
   *
   * Deliberately a short allowlist rather than a denylist, because being wrong costs silent data
   * loss rather than a failure: a re-executed task that orders its rows differently writes a
   * different partitioning of them, and once any reducer has fetched from the attempt it
   * replaces, some rows arrive twice and others not at all. A native scan replays its partition
   * because its file splits are fixed on the driver, and deterministic projections and filters
   * are row-wise. A nondeterministic expression is out even though it is evaluated per row, since
   * nothing bounds what it does between attempts: a nondeterministic UDF can drop, keep or
   * reorder rows differently on a retry. Anything that spills is out, since it emits rows in an
   * order that depends on how often it spilled. Other leaf scans plausibly qualify, but each
   * needs that argument made for it.
   */
  private def replaysRowsInOrder(plan: SparkPlan): Boolean = plan match {
    case _: CometNativeScanExec => true
    case p: CometProjectExec =>
      p.projectList.forall(_.deterministic) && replaysRowsInOrder(p.child)
    case f: CometFilterExec => f.condition.deterministic && replaysRowsInOrder(f.child)
    case _ => false
  }

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: ShuffleExchangeExec): CometNativeExec = {
    shuffleSupported(op) match {
      case Some(CometNativeShuffle) if op.children.forall(_.isInstanceOf[CometNativeExec]) =>
        CometSinkPlaceHolder(
          nativeOp,
          op,
          CometShuffleExchangeExec(op, shuffleType = CometNativeShuffle))
      case Some(CometColumnarShuffle) =>
        CometSinkPlaceHolder(
          nativeOp,
          op,
          CometShuffleExchangeExec(op, shuffleType = CometColumnarShuffle))
      case Some(CometNativeShuffle) =>
        // Native was chosen but children are not native - fall through to columnar if possible.
        // This can happen when getSupportLevel selected native but a later pass changed the plan.
        throw new IllegalStateException(
          "shuffleSupported chose native shuffle but children are not all CometNativeExec")
      case None =>
        throw new IllegalStateException()
    }
  }

  /**
   * Decide which Comet shuffle path (if any) can handle this shuffle. Returns `None` if neither
   * native nor columnar shuffle can be used; in that case the node is tagged with the combined
   * fallback reasons via `withFallbackReasons` so subsequent passes short-circuit via
   * `hasFallbackReason`.
   *
   * This is the single coordination point: the two path-specific predicates
   * (`nativeShuffleFailureReasons` / `columnarShuffleFailureReasons`) are pure - they return
   * collected reasons but do not tag. Tagging only happens here, and only on total failure.
   */
  def shuffleSupported(s: ShuffleExchangeExec): Option[ShuffleType] = {
    // Sticky: a prior rule pass (initial planning or an earlier AQE pass) already decided this
    // shuffle falls back to Spark and tagged it. Preserve that decision - re-deriving it against
    // a possibly-reshaped subtree (e.g. AQE stage-wrapping) can flip the answer and produce
    // inconsistent plans across passes (see #3949).
    if (hasFallbackReason(s)) return None

    isCometShuffleEnabledReason(s) match {
      case Some(reason) =>
        withFallbackReasons(s, Set(reason))
        return None
      case None =>
    }

    // A Comet shuffle wrapped around a stage that still contains a Spark FileSourceScanExec
    // with DPP produces inefficient row<->columnar transitions. This only happens when the
    // scan fell back to Spark (e.g., AQE DPP on Spark 3.4, or unsupported scan type).
    // On 3.5+ with AQE DPP, the scan converts to CometNativeScanExec and
    // stageContainsDPPScan won't match (it checks FileSourceScanExec).
    if (stageContainsDPPScan(s)) {
      withFallbackReasons(s, Set("Stage contains a scan with Dynamic Partition Pruning"))
      return None
    }

    val usesCelebornShuffleManager = isCometCelebornShuffleManagerEnabled(s.conf)

    // Native createExec requires a CometNativeExec child. A previously unwrapped CometPlan is
    // not sufficient, and Celeborn cannot fall back to a Comet JVM columnar dependency.
    val nativeChild = if (usesCelebornShuffleManager) {
      s.child.isInstanceOf[CometNativeExec]
    } else {
      isCometPlan(s.child)
    }
    val nativeReasons: Seq[String] =
      if (nativeChild) nativeShuffleFailureReasons(s) else Seq.empty
    if (nativeChild && nativeReasons.isEmpty) {
      return Some(CometNativeShuffle)
    }

    if (usesCelebornShuffleManager) {
      val reasons = if (nativeChild) {
        nativeReasons
      } else {
        Seq("Celeborn native shuffle requires a CometNativeExec child")
      }
      val columnarReason =
        "Comet columnar shuffle is not supported by the Celeborn shuffle manager"
      withFallbackReasons(s, (reasons :+ columnarReason).toSet)
      return None
    }

    if (!isCometPlan(s.child) &&
      !CometConf.COMET_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.get(s.conf)) {
      withFallbackReasons(
        s,
        Set(
          s"${CometConf.COMET_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.key} is disabled " +
            "and child is not a Comet plan"))
      return None
    }

    val columnarReasons = columnarShuffleFailureReasons(s)
    if (columnarReasons.isEmpty) {
      return Some(CometColumnarShuffle)
    }

    val combined = (nativeReasons ++ columnarReasons).toSet
    if (combined.nonEmpty) withFallbackReasons(s, combined)
    None
  }

  /**
   * Reasons the native shuffle path cannot handle this shuffle. Empty means native is supported.
   * Pure: does not tag the node.
   */
  private def nativeShuffleFailureReasons(s: ShuffleExchangeExec): Seq[String] = {
    val conf = SQLConf.get

    val nestedHashPartitioningEnabled =
      CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_NESTED_ENABLED.get(conf)

    /**
     * Determine which data types are supported as partition columns in native shuffle.
     *
     * For HashPartitioning this defines the key that determines how data should be collocated for
     * operations like `groupByKey`, `reduceByKey`, or `join`.
     *
     * Nested types (struct/array/map) are supported when
     * `spark.comet.shuffle.native.partitioning.hash.nested.enabled` is enabled: the native
     * Murmur3 kernel in hash_funcs/utils.rs hashes them recursively. Nesting is checked
     * recursively, so a leaf type that cannot be hashed natively -- a collated string, or an
     * interval the hasher has no branch for -- disqualifies the whole key and the shuffle falls
     * back to Spark.
     */
    def supportedHashPartitioningDataType(dt: DataType): Boolean = dt match {
      // Collated strings require collation-aware hashing; Comet only hashes raw bytes,
      // which would misroute rows that compare equal under the collation.
      case st: StringType if isStringCollationType(st) => false
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
          _: FloatType | _: DoubleType | _: StringType | _: BinaryType | _: TimestampType |
          _: TimestampNTZType | _: DateType =>
        true
      case _: DecimalType =>
        // TODO enforce this check
        // https://github.com/apache/datafusion-comet/issues/3079
        // Decimals with precision > 18 require Java BigDecimal conversion before hashing
        // d.precision <= 18
        true
      case dt if isTimeType(dt) =>
        true
      case StructType(fields) if nestedHashPartitioningEnabled =>
        // `fields.nonEmpty` mirrors the guard on the data-column gate below. An empty struct is
        // not reachable end-to-end anyway: Parquet cannot store an empty group, and an in-memory
        // relation with one does not survive scan conversion.
        fields.nonEmpty && fields.forall(f => supportedHashPartitioningDataType(f.dataType))
      case ArrayType(elementType, _) if nestedHashPartitioningEnabled =>
        supportedHashPartitioningDataType(elementType)
      case MapType(keyType, valueType, _) if nestedHashPartitioningEnabled =>
        // Map entry order is not semantically meaningful, so two equal maps must hash alike.
        // Spark 4.0+ normalizes a map shuffle key by wrapping it in `mapsort(...)`, which is
        // gated separately by CometMapSort (scalar map keys only) and, when unsupported, fails
        // the expression check below. Earlier Spark versions insert no such normalization, so
        // Comet would hash physical entry order and could route equal maps differently.
        isSpark40Plus &&
        supportedHashPartitioningDataType(keyType) &&
        supportedHashPartitioningDataType(valueType)
      case _ =>
        false
    }

    /**
     * Determine which data types are supported as data columns in native shuffle.
     *
     * Native shuffle relies on the Arrow IPC writer to serialize batches to disk, so it should
     * support all types that Comet supports.
     */
    def supportedSerializableDataType(dt: DataType): Boolean = dt match {
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
          _: FloatType | _: DoubleType | _: StringType | _: BinaryType | _: TimestampType |
          _: TimestampNTZType | _: DecimalType | _: DateType | _: NullType |
          _: YearMonthIntervalType | _: DayTimeIntervalType | CalendarIntervalType =>
        true
      case dt if isTimeType(dt) =>
        true
      case StructType(fields) =>
        fields.nonEmpty && fields.forall(f => supportedSerializableDataType(f.dataType)) &&
        // Java Arrow keys struct children by name, so the FFI import of a decoded batch
        // fails on duplicate field names
        !DataTypeSupport.hasDuplicateFieldNames(fields)
      case ArrayType(elementType, _) =>
        supportedSerializableDataType(elementType)
      case MapType(keyType, valueType, _) =>
        supportedSerializableDataType(keyType) && supportedSerializableDataType(valueType)
      case _ =>
        false
    }

    val reasons = scala.collection.mutable.ListBuffer.empty[String]

    if (!isCometNativeShuffleMode(s.conf)) {
      reasons += "Comet native shuffle not enabled"
      return reasons.toSeq
    }

    val inputs = s.child.output

    for (input <- inputs) {
      if (!supportedSerializableDataType(input.dataType)) {
        reasons += s"unsupported shuffle data type ${input.dataType} for input $input"
        return reasons.toSeq
      }
    }

    val partitioning = s.outputPartitioning
    partitioning match {
      case HashPartitioning(expressions, _) =>
        if (!CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.get(conf)) {
          reasons +=
            s"${CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key} is disabled"
        }
        for (expr <- expressions) {
          if (QueryPlanSerde.exprToProto(expr, inputs).isEmpty) {
            reasons += s"unsupported hash partitioning expression: $expr"
          }
        }
        for (dt <- expressions.map(_.dataType).distinct) {
          if (!supportedHashPartitioningDataType(dt)) {
            reasons += s"unsupported hash partitioning data type for native shuffle: $dt"
          }
        }
      case SinglePartition =>
      // we already checked that the input types are supported
      case RangePartitioning(orderings, _) =>
        /**
         * Determine which data types are supported as partition columns in native shuffle.
         *
         * For RangePartitioning this defines the key that determines how data should be
         * collocated for operations like `orderBy`, `repartitionByRange`. Native code does not
         * support sorting complex types.
         */
        def supportedRangePartitioningDataType(dt: DataType): Boolean = dt match {
          // Collated strings require collation-aware ordering; Comet only compares raw bytes.
          case st: StringType if isStringCollationType(st) => false
          // The native range partitioner normalizes its comparison keys and its sampled boundary
          // rows the same way the native sort does, so scalar floats match Spark's ordering even
          // under spark.comet.exec.strictFloatingPoint=true.
          case _: FloatType | _: DoubleType => true
          case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
              _: StringType | _: BinaryType | _: TimestampType | _: TimestampNTZType |
              _: DecimalType | _: DateType =>
            true
          case _ =>
            false
        }

        if (!CometConf.COMET_SHUFFLE_NATIVE_RANGE_PARTITIONING_ENABLED.get(conf)) {
          reasons +=
            s"${CometConf.COMET_SHUFFLE_NATIVE_RANGE_PARTITIONING_ENABLED.key} is disabled"
          return reasons.toSeq
        }
        for (o <- orderings) {
          if (QueryPlanSerde.exprToProto(o, inputs).isEmpty) {
            reasons += s"unsupported range partitioning sort order: $o"
            // Roll up fallback reasons recorded on the sort-order expression (e.g. strict
            // floating-point sort) so they surface in the shuffle's explain output.
            o.getTagValue(CometExplainInfo.FALLBACK_REASONS).foreach(reasons ++= _)
          }
        }
        for (dt <- orderings.map(_.dataType).distinct) {
          if (!supportedRangePartitioningDataType(dt)) {
            reasons += s"unsupported range partitioning data type for native shuffle: $dt"
          }
        }
      case RoundRobinPartitioning(_) =>
        val config = CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED
        if (!config.get(conf)) {
          reasons += s"${config.key} is disabled"
        }
      case _ =>
        reasons +=
          s"unsupported Spark partitioning for native shuffle: ${partitioning.getClass.getName}"
    }
    reasons.toSeq
  }

  /**
   * Reasons the columnar shuffle path cannot handle this shuffle. Empty means columnar is
   * supported. Pure: does not tag the node.
   */
  private def columnarShuffleFailureReasons(s: ShuffleExchangeExec): Seq[String] = {

    /**
     * Determine which data types are supported as data columns in columnar shuffle.
     *
     * Comet columnar shuffle used native code to convert Spark unsafe rows to Arrow batches, see
     * shuffle/row.rs
     */
    def supportedSerializableDataType(dt: DataType): Boolean = dt match {
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
          _: FloatType | _: DoubleType | _: StringType | _: BinaryType | _: TimestampType |
          _: TimestampNTZType | _: DecimalType | _: DateType | _: NullType =>
        true
      case dt if isTimeType(dt) =>
        true
      case StructType(fields) =>
        fields.nonEmpty && fields.forall(f => supportedSerializableDataType(f.dataType)) &&
        // Java Arrow stream reader cannot work on duplicate field name
        !DataTypeSupport.hasDuplicateFieldNames(fields)
      case ArrayType(elementType, _) =>
        supportedSerializableDataType(elementType)
      case MapType(keyType, valueType, _) =>
        supportedSerializableDataType(keyType) && supportedSerializableDataType(valueType)
      case _ =>
        false
    }

    val reasons = scala.collection.mutable.ListBuffer.empty[String]

    if (!isCometJVMShuffleMode(s.conf)) {
      reasons += "Comet columnar shuffle not enabled"
      return reasons.toSeq
    }

    if (isShuffleOperator(s.child)) {
      reasons += s"Child ${s.child.getClass.getName} is a shuffle operator"
      return reasons.toSeq
    }

    if (!(!s.child.supportsColumnar || isCometPlan(s.child))) {
      reasons += s"Child ${s.child.getClass.getName} is a neither row-based or a Comet operator"
      return reasons.toSeq
    }

    val inputs = s.child.output

    for (input <- inputs) {
      if (!supportedSerializableDataType(input.dataType)) {
        reasons += s"unsupported shuffle data type ${input.dataType} for input $input"
        return reasons.toSeq
      }
    }

    val partitioning = s.outputPartitioning
    partitioning match {
      case HashPartitioning(expressions, _) =>
        for (expr <- expressions) {
          if (QueryPlanSerde.exprToProto(expr, inputs).isEmpty) {
            reasons += s"unsupported hash partitioning expression: $expr"
          }
        }
        for (dt <- expressions.map(_.dataType).distinct) {
          if (isStringCollationType(dt)) {
            reasons += s"unsupported hash partitioning data type for columnar shuffle: $dt"
          }
        }
      case SinglePartition =>
      // we already checked that the input types are supported
      case RoundRobinPartitioning(_) =>
      // we already checked that the input types are supported
      case RangePartitioning(orderings, _) =>
        for (o <- orderings) {
          if (QueryPlanSerde.exprToProto(o, inputs).isEmpty) {
            reasons += s"unsupported range partitioning sort order: $o"
          }
        }
        for (dt <- orderings.map(_.dataType).distinct) {
          if (isStringCollationType(dt)) {
            reasons += s"unsupported range partitioning data type for columnar shuffle: $dt"
          }
        }
      case _ =>
        reasons +=
          s"unsupported Spark partitioning for columnar shuffle: ${partitioning.getClass.getName}"
    }
    reasons.toSeq
  }

  private def isCometNativeShuffleMode(conf: SQLConf): Boolean = {
    COMET_SHUFFLE_MODE.get(conf) match {
      case "native" => true
      case "auto" => true
      case _ => false
    }
  }

  private def isCometJVMShuffleMode(conf: SQLConf): Boolean = {
    COMET_SHUFFLE_MODE.get(conf) match {
      case "jvm" => true
      case "auto" => true
      case _ => false
    }
  }

  private def isCometPlan(op: SparkPlan): Boolean = op.isInstanceOf[CometPlan]

  /**
   * Returns true if a given spark plan is Comet shuffle operator.
   */
  private def isShuffleOperator(op: SparkPlan): Boolean = {
    op match {
      case op: ShuffleQueryStageExec if op.plan.isInstanceOf[CometShuffleExchangeExec] => true
      case _: CometShuffleExchangeExec => true
      case op: CometSinkPlaceHolder => isShuffleOperator(op.child)
      case _ => false
    }
  }

  /**
   * Returns true if the stage (the subtree rooted at this shuffle) contains a scan with Dynamic
   * Partition Pruning (DPP). When DPP is present, the scan falls back to Spark, and wrapping the
   * stage with Comet shuffle creates inefficient row-to-columnar transitions.
   */
  private def stageContainsDPPScan(s: ShuffleExchangeExec): Boolean = {
    def isDynamicPruningFilter(e: Expression): Boolean =
      e.exists(_.isInstanceOf[PlanExpression[_]])

    s.child.exists {
      case scan: FileSourceScanExec =>
        scan.partitionFilters.exists(isDynamicPruningFilter)
      case _ => false
    }
  }

  /**
   * Reason Comet shuffle is not enabled for this node, or `None` if it is enabled. Pure: does not
   * tag the node.
   */
  private def isCometShuffleEnabledReason(op: SparkPlan): Option[String] = {
    if (!COMET_SHUFFLE_ENABLED.get(op.conf)) {
      Some(s"Comet shuffle is not enabled: ${COMET_SHUFFLE_ENABLED.key} is not enabled")
    } else if (!isCometShuffleManagerEnabled) {
      Some(
        s"spark.shuffle.manager is not set to ${classOf[CometShuffleManager].getName} or " +
          classOf[CometCelebornShuffleManager].getName)
    } else {
      cometCelebornShuffleFallbackReason(op.conf, op.outputPartitioning.numPartitions)
    }
  }

  /**
   * Build a Comet native shuffle dependency around an existing `RDD[ColumnarBatch]` of real
   * batches. Used by [[org.apache.spark.sql.comet.CometCollectLimitExec]] and
   * [[org.apache.spark.sql.comet.CometTakeOrderedAndProjectExec]] where the input is the result
   * of a local-limit / topK transform and there is no separate child native subtree to inline.
   *
   * Implemented as a thin wrapper around [[prepareNativeShuffleDependency]]: synthesizes a
   * `Scan("ShuffleWriterInput")` as the child native op (so the writer's plan is still
   * `ShuffleWriter -> Scan`, consuming JVM batches via Arrow C Stream), wraps `rdd` as the single
   * leaf input of a thin scheduling RDD, and supplies a minimal [[NativeExecContext]]. Lets the
   * writer use one code path for both this case and the [[CometShuffleExchangeExec]] case.
   */
  def prepareShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {

    val scanBuilder = OperatorOuterClass.Scan.newBuilder().setSource("ShuffleWriterInput")
    val scanTypes = outputAttributes.flatMap { attr =>
      QueryPlanSerde.serializeDataType(attr.dataType)
    }
    if (scanTypes.length != outputAttributes.length) {
      throw new UnsupportedOperationException(
        s"$outputAttributes contains unsupported data types for CometShuffleExchangeExec.")
    }
    scanBuilder.addAllFields(scanTypes.asJava)
    val scanOp = OperatorOuterClass.Operator.newBuilder().setScan(scanBuilder).build()

    // Wrap the raw batches as an RDD[ArrowArrayStream] so the leaf reaches native via the Arrow C
    // Stream Interface, matching how CometNativeExec.buildNativeContext feeds the native-child
    // path. The synthetic Scan("ShuffleWriterInput") above is the native consumer.
    val streamRDD = CometArrowStream.wrapColumnarBatchRDD(
      rdd,
      StructType(
        outputAttributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))),
      CometArrowStream.NATIVE_TIMEZONE,
      "ShuffleWriterInput")

    val childMetricNode = CometMetricNode(Map.empty)
    val thinRDD = new CometNativeShuffleInputRDD(
      rdd.sparkContext,
      Seq(streamRDD),
      rdd.getNumPartitions,
      shuffleScanIndices = Set.empty,
      spillMetricNode = CometMetricNode(metrics, Seq(childMetricNode)))

    val ctx = NativeExecContext(
      inputs = Seq(streamRDD),
      numPartitions = rdd.getNumPartitions,
      subqueries = Seq.empty,
      broadcastedHadoopConfForEncryption = None,
      encryptedFilePaths = Seq.empty,
      commonByKey = Map.empty,
      perPartitionByKey = Map.empty,
      shuffleScanIndices = Set.empty,
      hasScanInput = false)

    // The Scan placeholder has no per-operator metrics, so the metric tree for the unified plan
    // is `shuffleWriterMetrics` at the root with one empty leaf for the Scan child.
    prepareNativeShuffleDependency(
      thinRDD,
      Some(rdd),
      outputAttributes,
      outputPartitioning,
      serializer,
      metrics,
      NativeShuffleSpec(scanOp, childMetricNode, ctx))
  }

  /**
   * Build a Comet native shuffle dependency for the [[CometShuffleExchangeExec]] case where the
   * shuffle is fed by a [[CometNativeExec]] child. The writer drives the unified
   * `ShuffleWriter(child = childNativeOp)` plan in a single
   * [[org.apache.comet.CometExecIterator]] per partition. The returned dep carries the
   * [[NativeShuffleSpec]] so [[CometNativeShuffleWriter]] can reach the child's per-partition
   * execution context, root native operator, and metric node at task time.
   *
   * @param thinRDD
   *   scheduling-anchor RDD whose `compute` returns a [[CometNativeShuffleInputIterator]];
   *   produces no batches itself.
   * @param samplingRDD
   *   regular columnar execution of the child, only required for [[RangePartitioning]] (sampling
   *   needs real rows). `None` for hash / single / round-robin.
   */
  def prepareNativeShuffleDependency(
      thinRDD: CometNativeShuffleInputRDD,
      samplingRDD: Option[RDD[ColumnarBatch]],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric],
      spec: NativeShuffleSpec): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val numParts = thinRDD.getNumPartitions

    // Subqueries in the partitioning expressions (e.g. DISTRIBUTE BY over a subquery) belong to
    // this exchange, not the native child, so the child's collectSubqueries misses them. The
    // writer serializes them with their exprId, so they must be registered against the iterator or
    // the native lookup fails with "Subquery N not found". Both the native-child and
    // non-native-child native-shuffle paths funnel through here.
    //
    // Only ScalarSubquery is matched because it is the sole expression QueryPlanSerde turns into a
    // native Subquery proto (and the only id the native side looks up); this mirrors the other
    // registration sites (CometExec.collectSubqueries, CometNativeExec.prepareSubqueries). The
    // exprId is stable under reuse, so a ReusedSubqueryExec plan still resolves to the same result.
    // The `case _ => Nil` fallthrough is safe: partitionings that are not Expressions
    // (SinglePartition, RoundRobinPartitioning) carry no key expressions to hold a subquery.
    val partitioningSubqueries = outputPartitioning match {
      case e: Expression => e.collect { case s: ScalarSubquery => s }
      case _ => Nil
    }
    // Drop the per-partition plan-data map off the spec that lands on the (non-transient)
    // CometShuffleDependency.nativeShuffleSpec. Each partition's slice now rides on the thin RDD's
    // Partition objects (see CometNativeShuffleInputRDD.getPartitions), so the full
    // O(numPartitions) map is dead weight here and would blow the 2GB ByteArrayOutputStream limit
    // at stage submission on very-high-partition-count jobs. NativeExecContext.perPartitionByKey is
    // also @transient (the structural guard against any build path), but we empty it explicitly
    // here too so the map isn't retained on the driver via this dependency. commonByKey stays
    // (O(#scans), not O(#partitions), and the writer still needs it).
    val augmentedSpec = spec.copy(execContext = spec.execContext.copy(
      subqueries = spec.execContext.subqueries ++ partitioningSubqueries,
      perPartitionByKey = Map.empty))

    // The code block below is mostly brought over from
    // ShuffleExchangeExec::prepareShuffleDependency
    val (partitioner, rangePartitionBounds) = outputPartitioning match {
      case rangePartitioning: RangePartitioning =>
        // Sampling needs real rows; use the dedicated samplingRDD (a regular columnar execution
        // of the child). The thin RDD itself yields nothing.
        val samplingInput = samplingRDD.getOrElse(
          throw new IllegalStateException(
            "RangePartitioning requires a samplingRDD on the native-shuffle path"))
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = samplingInput.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(rangePartitioning.ordering.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()

          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.flatMap { batch =>
            val rowIter = batch.rowIterator().asScala
            rowIter.map { row =>
              mutablePair.update(projection(row).copy(), null)
            }
          }
        }

        // Construct ordering on extracted sort key.
        val orderingAttributes = rangePartitioning.ordering.zipWithIndex.map { case (ord, i) =>
          ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        // Use Spark's RangePartitioner to compute bounds from global samples
        val rangePartitioner = new RangePartitioner(
          rangePartitioning.numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)

        // Use reflection to access the private rangeBounds field
        val rangeBoundsField = rangePartitioner.getClass.getDeclaredField("rangeBounds")
        rangeBoundsField.setAccessible(true)
        val rangeBounds =
          rangeBoundsField.get(rangePartitioner).asInstanceOf[Array[InternalRow]].toSeq

        (rangePartitioner.asInstanceOf[Partitioner], Some(rangeBounds))

      case _ =>
        (
          new Partitioner {
            override def numPartitions: Int = outputPartitioning.numPartitions

            override def getPartition(key: Any): Int = key.asInstanceOf[Int]
          },
          None)
    }

    // The remote stage can finish some maps before an oversized row requires a complete local
    // replacement. Keep output statistics separate from the public counters so neither those
    // completed maps nor late remote updates inflate the selected shuffle's AQE statistics.
    val outputMetrics = thinRDD.context.env.shuffleManager match {
      case _: CometCelebornShuffleManager if numParts > 0 =>
        Some(CometShuffleOutputMetrics(thinRDD.context, metrics))
      case _ => None
    }
    val destinationMetrics = metrics ++ outputMetrics.toSeq.flatMap(_.metrics)

    new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      thinRDD,
      serializer = serializer,
      shuffleWriterProcessor =
        ShuffleExchangeExec.createShuffleWriteProcessor(destinationMetrics),
      shuffleType = CometNativeShuffle,
      partitioner = partitioner,
      decodeTime = metrics("decode_time"),
      outputPartitioning = Some(outputPartitioning),
      outputAttributes = outputAttributes,
      shuffleWriteMetrics = destinationMetrics,
      numParts = numParts,
      rangePartitionBounds = rangePartitionBounds,
      nativeShuffleSpec = Some(augmentedSpec),
      outputMetrics = outputMetrics)
  }

  /**
   * This is copied from Spark `ShuffleExchangeExec.needToCopyObjectsBeforeShuffle`. The only
   * difference is that we use `CometShuffleManager` instead of `SortShuffleManager`.
   */
  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[CometShuffleManager]
    val bypassMergeThreshold = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties and the number of partitions doesn't exceed the limitation. If this
        // optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, and the
        // serializer in Spark SQL always satisfy the properties, so we only need to check whether
        // the number of partitions exceeds the limitation.
        false
      } else {
        // This different to Spark `SortShuffleManager`.
        // Comet doesn't use Spark `ExternalSorter` to buffer records in memory, so we don't need to
        // copy.
        false
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on the
   * partitioning scheme defined in `newPartitioning`. Those partitions of the returned
   * ShuffleDependency will be the input of shuffle.
   */
  def prepareJVMShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric]): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        // For HashPartitioning, the partitioning key is already a valid partition ID, as we use
        // `HashPartitioning.partitionIdExpression` to produce partitioning key.
        new PartitionIdPassthrough(n)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.map(row => mutablePair.update(projection(row).copy(), null))
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map { case (ord, i) =>
          ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
      case SinglePartition => new ConstantPartitioner
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }

    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        // nextInt(numPartitions) implementation has a special case when bound is a power of 2,
        // which is basically taking several highest bits from the initial seed, with only a
        // minimal scrambling. Due to deterministic seed, using the generator only once,
        // and lack of scrambling, the position values for power-of-two numPartitions always
        // end up being almost the same regardless of the index. substantially scrambling the
        // seed by hashing will help. Refer to SPARK-21782 for more details.
        val partitionId = TaskContext.get().partitionId()
        var position = new XORShiftRandom(partitionId).nextInt(numPartitions)
        (_: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
        row => projection(row).getInt(0)
      case RangePartitioning(sortingExpressions, _) =>
        val projection =
          UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
        row => projection(row)
      case SinglePartition => identity
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      // [SPARK-23207] Have to make sure the generated RoundRobinPartitioning is deterministic,
      // otherwise a retry task may output different rows and thus lead to data loss.
      //
      // Currently we following the most straight-forward way that perform a local sort before
      // partitioning.
      //
      // Note that we don't perform local sort if the new partitioning has only 1 partition, under
      // that case all output rows go to the same partition.
      val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
        rdd.mapPartitionsInternal { iter =>
          val recordComparatorSupplier = new Supplier[RecordComparator] {
            override def get: RecordComparator = new RecordBinaryComparator()
          }
          // The comparator for comparing row hashcode, which should always be Integer.
          val prefixComparator = PrefixComparators.LONG

          // The prefix computer generates row hashcode as the prefix, so we may decrease the
          // probability that the prefixes are equal when input rows choose column values from a
          // limited range.
          val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix

            override def computePrefix(
                row: InternalRow): UnsafeExternalRowSorter.PrefixComputer.Prefix = {
              // The hashcode generated from the binary form of a [[UnsafeRow]] should not be null.
              result.isNull = false
              result.value = row.hashCode()
              result
            }
          }
          val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

          val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
            fromAttributes(outputAttributes),
            recordComparatorSupplier,
            prefixComparator,
            prefixComputer,
            pageSize,
            // We are comparing binary here, which does not support radix sort.
            // See more details in SPARK-28699.
            false)
          sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
        }
      } else {
        rdd
      }

      // round-robin function is order sensitive if we don't sort the input.
      val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition
      if (CometShuffleExchangeExec.needToCopyObjectsBeforeShuffle(part)) {
        newRdd.mapPartitionsWithIndexInternal(
          (_, iter) => {
            val getPartitionKey = getPartitionKeyExtractor()
            iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
          },
          isOrderSensitive = isOrderSensitive)
      } else {
        newRdd.mapPartitionsWithIndexInternal(
          (_, iter) => {
            val getPartitionKey = getPartitionKeyExtractor()
            val mutablePair = new MutablePair[Int, InternalRow]()
            iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
          },
          isOrderSensitive = isOrderSensitive)
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
      new CometShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
        shuffleType = CometColumnarShuffle,
        schema = Some(fromAttributes(outputAttributes)),
        decodeTime = writeMetrics("decode_time"),
        shuffleWriteMetrics = writeMetrics)

    dependency
  }
}
