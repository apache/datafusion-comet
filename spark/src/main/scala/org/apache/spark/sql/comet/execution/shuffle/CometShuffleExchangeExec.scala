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
import org.apache.spark.sql.comet.{CometMetricNode, CometNativeExec, CometPlan, CometSinkPlaceHolder}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}
import org.apache.spark.util.random.XORShiftRandom

import com.google.common.base.Objects

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometConf.{COMET_EXEC_SHUFFLE_ENABLED, COMET_SHUFFLE_MODE}
import org.apache.comet.CometSparkSessionExtensions.{hasExplainInfo, isCometShuffleManagerEnabled, withInfos}
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

  @transient lazy val inputRDD: RDD[_] = if (shuffleType == CometNativeShuffle) {
    // CometNativeShuffle assumes that the input plan is Comet plan.
    child.executeColumnar()
  } else if (shuffleType == CometColumnarShuffle) {
    // CometColumnarShuffle uses Spark's row-based execute() API. For Spark row-based plans,
    // rows flow directly. For Comet native plans, their doExecute() wraps with ColumnarToRowExec
    // to convert columnar batches to rows.
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
      sparkContext.submitMapStage(shuffleDependency)
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
  def shuffleId: Int = getShuffleId(shuffleDependency)

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on the partitioning
   * scheme defined in `newPartitioning`. Those partitions of the returned ShuffleDependency will
   * be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, _, _] =
    if (shuffleType == CometNativeShuffle) {
      val dep = CometShuffleExchangeExec.prepareShuffleDependency(
        inputRDD.asInstanceOf[RDD[ColumnarBatch]],
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

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: ShuffleExchangeExec): CometNativeExec = {
    shuffleSupported(op) match {
      case Some(CometNativeShuffle) if op.children.forall(_.isInstanceOf[CometNativeExec]) =>
        // Switch to use Decimal128 regardless of precision, since Arrow native execution
        // doesn't support Decimal32 and Decimal64 yet.
        conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
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
   * fallback reasons via `withInfos` so subsequent passes short-circuit via `hasExplainInfo`.
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
    if (hasExplainInfo(s)) return None

    isCometShuffleEnabledReason(s) match {
      case Some(reason) =>
        withInfos(s, Set(reason))
        return None
      case None =>
    }

    // A Comet shuffle wrapped around a stage that still contains a Spark FileSourceScanExec
    // with DPP produces inefficient row<->columnar transitions. This only happens when the
    // scan fell back to Spark (e.g., AQE DPP on Spark 3.4, or unsupported scan type).
    // On 3.5+ with AQE DPP, the scan converts to CometNativeScanExec and
    // stageContainsDPPScan won't match (it checks FileSourceScanExec).
    if (stageContainsDPPScan(s)) {
      withInfos(s, Set("Stage contains a scan with Dynamic Partition Pruning"))
      return None
    }

    // Native path is only eligible when the child is already a Comet plan; otherwise skip it
    // silently (no reason to surface) and let columnar take over.
    val nativeReasons: Seq[String] =
      if (isCometPlan(s.child)) nativeShuffleFailureReasons(s) else Seq.empty
    if (isCometPlan(s.child) && nativeReasons.isEmpty) {
      return Some(CometNativeShuffle)
    }

    if (!isCometPlan(s.child) &&
      !CometConf.COMET_EXEC_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.get(s.conf)) {
      withInfos(
        s,
        Set(
          s"${CometConf.COMET_EXEC_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.key} is disabled " +
            "and child is not a Comet plan"))
      return None
    }

    val columnarReasons = columnarShuffleFailureReasons(s)
    if (columnarReasons.isEmpty) {
      return Some(CometColumnarShuffle)
    }

    val combined = (nativeReasons ++ columnarReasons).toSet
    if (combined.nonEmpty) withInfos(s, combined)
    None
  }

  /**
   * Reasons the native shuffle path cannot handle this shuffle. Empty means native is supported.
   * Pure: does not tag the node.
   */
  private def nativeShuffleFailureReasons(s: ShuffleExchangeExec): Seq[String] = {
    val conf = SQLConf.get

    /**
     * Determine which data types are supported as partition columns in native shuffle.
     *
     * For HashPartitioning this defines the key that determines how data should be collocated for
     * operations like `groupByKey`, `reduceByKey`, or `join`. Native code does not support
     * hashing complex types, see hash_funcs/utils.rs
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
          _: TimestampNTZType | _: DecimalType | _: DateType =>
        true
      case StructType(fields) =>
        fields.nonEmpty && fields.forall(f => supportedSerializableDataType(f.dataType))
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
        if (!CometConf.COMET_EXEC_SHUFFLE_WITH_HASH_PARTITIONING_ENABLED.get(conf)) {
          reasons +=
            s"${CometConf.COMET_EXEC_SHUFFLE_WITH_HASH_PARTITIONING_ENABLED.key} is disabled"
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
        val strictFloatingPoint = CometConf.COMET_EXEC_STRICT_FLOATING_POINT.get(conf)

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
          case _: FloatType | _: DoubleType =>
            !strictFloatingPoint
          case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
              _: StringType | _: BinaryType | _: TimestampType | _: TimestampNTZType |
              _: DecimalType | _: DateType =>
            true
          case _ =>
            false
        }

        if (!CometConf.COMET_EXEC_SHUFFLE_WITH_RANGE_PARTITIONING_ENABLED.get(conf)) {
          reasons +=
            s"${CometConf.COMET_EXEC_SHUFFLE_WITH_RANGE_PARTITIONING_ENABLED.key} is disabled"
          return reasons.toSeq
        }
        for (o <- orderings) {
          if (QueryPlanSerde.exprToProto(o, inputs).isEmpty) {
            reasons += s"unsupported range partitioning sort order: $o"
            // Roll up fallback reasons recorded on the sort-order expression (e.g. strict
            // floating-point sort) so they surface in the shuffle's explain output.
            o.getTagValue(CometExplainInfo.EXTENSION_INFO).foreach(reasons ++= _)
          }
        }
        for (dt <- orderings.map(_.dataType).distinct) {
          if (!supportedRangePartitioningDataType(dt)) {
            val reason = dt match {
              case _: FloatType | _: DoubleType if strictFloatingPoint =>
                s"Range partitioning on $dt is not 100% compatible with Spark, and Comet is " +
                  s"running with ${CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key}=true. " +
                  s"${CometConf.COMPAT_GUIDE}"
              case _ =>
                s"unsupported range partitioning data type for native shuffle: $dt"
            }
            reasons += reason
          }
        }
      case RoundRobinPartitioning(_) =>
        val config = CometConf.COMET_EXEC_SHUFFLE_WITH_ROUND_ROBIN_PARTITIONING_ENABLED
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
          _: TimestampNTZType | _: DecimalType | _: DateType =>
        true
      case StructType(fields) =>
        fields.nonEmpty && fields.forall(f => supportedSerializableDataType(f.dataType)) &&
        // Java Arrow stream reader cannot work on duplicate field name
        fields.map(f => f.name).distinct.length == fields.length &&
        fields.nonEmpty
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
    if (!COMET_EXEC_SHUFFLE_ENABLED.get(op.conf)) {
      Some(s"Comet shuffle is not enabled: ${COMET_EXEC_SHUFFLE_ENABLED.key} is not enabled")
    } else if (!isCometShuffleManagerEnabled(op.conf)) {
      Some(s"spark.shuffle.manager is not set to ${classOf[CometShuffleManager].getName}")
    } else {
      None
    }
  }

  def prepareShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val numParts = rdd.getNumPartitions

    // The code block below is mostly brought over from
    // ShuffleExchangeExec::prepareShuffleDependency
    val (partitioner, rangePartitionBounds) = outputPartitioning match {
      case rangePartitioning: RangePartitioning =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
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

    val dependency = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rdd.map(
        (0, _)
      ), // adding fake partitionId that is always 0 because ShuffleDependency requires it
      serializer = serializer,
      shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(metrics),
      shuffleType = CometNativeShuffle,
      partitioner = partitioner,
      decodeTime = metrics("decode_time"),
      outputPartitioning = Some(outputPartitioning),
      outputAttributes = outputAttributes,
      shuffleWriteMetrics = metrics,
      numParts = numParts,
      rangePartitionBounds = rangePartitionBounds)
    dependency
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
