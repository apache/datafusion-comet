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

package org.apache.spark.sql.comet

import java.io.ByteArrayOutputStream

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Expression, ExpressionSet, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode, Final, Partial}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, TimestampNTZType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.io.ChunkedByteBuffer

import com.google.common.base.Objects

import org.apache.comet.{CometConf, CometExecIterator, CometRuntimeException, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.{isCometShuffleEnabled, withInfo}
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.{AggregateMode => CometAggregateMode, Operator}
import org.apache.comet.serde.QueryPlanSerde.{aggExprToProto, exprToProto, supportedSortType}
import org.apache.comet.serde.operator.CometSink

/**
 * A Comet physical operator
 */
abstract class CometExec extends CometPlan {

  /** The original Spark operator from which this Comet operator is converted from */
  def originalPlan: SparkPlan

  /** Comet always support columnar execution */
  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = originalPlan.output

  override def doExecute(): RDD[InternalRow] =
    ColumnarToRowExec(this).doExecute()

  override def executeCollect(): Array[InternalRow] =
    ColumnarToRowExec(this).executeCollect()

  override def outputOrdering: Seq[SortOrder] = originalPlan.outputOrdering

  // `CometExec` reuses the outputPartitioning of the original SparkPlan.
  // Note that if the outputPartitioning of the original SparkPlan depends on its children,
  // we should override this method in the specific CometExec, because Spark AQE may change the
  // outputPartitioning of SparkPlan, e.g., AQEShuffleReadExec.
  override def outputPartitioning: Partitioning = originalPlan.outputPartitioning

  protected def setSubqueries(planId: Long, sparkPlan: SparkPlan): Unit = {
    sparkPlan.children.foreach(setSubqueries(planId, _))

    sparkPlan.expressions.foreach {
      _.collect { case sub: ScalarSubquery =>
        CometScalarSubquery.setSubquery(planId, sub)
      }
    }
  }

  protected def cleanSubqueries(planId: Long, sparkPlan: SparkPlan): Unit = {
    sparkPlan.children.foreach(cleanSubqueries(planId, _))

    sparkPlan.expressions.foreach {
      _.collect { case sub: ScalarSubquery =>
        CometScalarSubquery.removeSubquery(planId, sub)
      }
    }
  }
}

object CometExec {
  // An unique id for each CometExecIterator, used to identify the native query execution.
  private val curId = new java.util.concurrent.atomic.AtomicLong()

  def newIterId: Long = curId.getAndIncrement()

  def getCometIterator(
      inputs: Seq[Iterator[ColumnarBatch]],
      numOutputCols: Int,
      nativePlan: Operator,
      numParts: Int,
      partitionIdx: Int): CometExecIterator = {
    getCometIterator(
      inputs,
      numOutputCols,
      nativePlan,
      CometMetricNode(Map.empty),
      numParts,
      partitionIdx,
      broadcastedHadoopConfForEncryption = None,
      encryptedFilePaths = Seq.empty)
  }

  def getCometIterator(
      inputs: Seq[Iterator[ColumnarBatch]],
      numOutputCols: Int,
      nativePlan: Operator,
      nativeMetrics: CometMetricNode,
      numParts: Int,
      partitionIdx: Int,
      broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]],
      encryptedFilePaths: Seq[String]): CometExecIterator = {
    val outputStream = new ByteArrayOutputStream()
    nativePlan.writeTo(outputStream)
    outputStream.close()
    val bytes = outputStream.toByteArray
    new CometExecIterator(
      newIterId,
      inputs,
      numOutputCols,
      bytes,
      nativeMetrics,
      numParts,
      partitionIdx,
      broadcastedHadoopConfForEncryption,
      encryptedFilePaths)
  }

  /**
   * Executes this Comet operator and serialized output ColumnarBatch into bytes.
   */
  def getByteArrayRdd(cometPlan: CometPlan): RDD[(Long, ChunkedByteBuffer)] = {
    cometPlan.executeColumnar().mapPartitionsInternal { iter =>
      Utils.serializeBatches(iter)
    }
  }
}

/**
 * A Comet native physical operator.
 */
abstract class CometNativeExec extends CometExec {

  /**
   * The serialized native query plan, optional. This is only defined when the current node is the
   * "boundary" node between native and Spark.
   */
  def serializedPlanOpt: SerializedPlan

  /** The Comet native operator */
  def nativeOp: Operator

  override protected def doPrepare(): Unit = prepareSubqueries(this)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.baselineMetrics(sparkContext)

  private def prepareSubqueries(sparkPlan: SparkPlan): Unit = {
    val runningSubqueries = new ArrayBuffer[ExecSubqueryExpression]

    sparkPlan.children.foreach(prepareSubqueries)

    sparkPlan.expressions.foreach {
      _.collect { case e: ScalarSubquery =>
        runningSubqueries += e
      }
    }

    // fill in the result of subqueries
    runningSubqueries.foreach { sub =>
      sub.updateResult()
    }

    runningSubqueries.clear()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    serializedPlanOpt.plan match {
      case None =>
        // This is in the middle of a native execution, it should not be executed directly.
        throw new CometRuntimeException(
          s"CometNativeExec should not be executed directly without a serialized plan: $this")
      case Some(serializedPlan) =>
        // Switch to use Decimal128 regardless of precision, since Arrow native execution
        // doesn't support Decimal32 and Decimal64 yet.
        SQLConf.get.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")

        val serializedPlanCopy = serializedPlan
        // TODO: support native metrics for all operators.
        val nativeMetrics = CometMetricNode.fromCometPlan(this)

        // For each relation in a CometNativeScan generate a hadoopConf,
        // for each file path in a relation associate with hadoopConf
        val cometNativeScans: Seq[CometNativeScanExec] = this
          .collectLeaves()
          .filter(_.isInstanceOf[CometNativeScanExec])
          .map(_.asInstanceOf[CometNativeScanExec])
        assert(
          cometNativeScans.size <= 1,
          "We expect one native scan in a Comet plan since we will broadcast one hadoopConf.")
        // If this assumption changes in the future, you can look at the commit history of #2447
        // to see how there used to be a map of relations to broadcasted confs in case multiple
        // relations in a single plan. The example that came up was UNION. See discussion at:
        // https://github.com/apache/datafusion-comet/pull/2447#discussion_r2406118264
        val (broadcastedHadoopConfForEncryption, encryptedFilePaths) =
          cometNativeScans.headOption.fold(
            (None: Option[Broadcast[SerializableConfiguration]], Seq.empty[String])) { scan =>
            // This creates a hadoopConf that brings in any SQLConf "spark.hadoop.*" configs and
            // per-relation configs since different tables might have different decryption
            // properties.
            val hadoopConf = scan.relation.sparkSession.sessionState
              .newHadoopConfWithOptions(scan.relation.options)
            val encryptionEnabled = CometParquetUtils.encryptionEnabled(hadoopConf)
            if (encryptionEnabled) {
              // hadoopConf isn't serializable, so we have to do a broadcasted config.
              val broadcastedConf =
                scan.relation.sparkSession.sparkContext
                  .broadcast(new SerializableConfiguration(hadoopConf))
              (Some(broadcastedConf), scan.relation.inputFiles.toSeq)
            } else {
              (None, Seq.empty)
            }
          }

        def createCometExecIter(
            inputs: Seq[Iterator[ColumnarBatch]],
            numParts: Int,
            partitionIndex: Int): CometExecIterator = {
          val it = new CometExecIterator(
            CometExec.newIterId,
            inputs,
            output.length,
            serializedPlanCopy,
            nativeMetrics,
            numParts,
            partitionIndex,
            broadcastedHadoopConfForEncryption,
            encryptedFilePaths)

          setSubqueries(it.id, this)

          Option(TaskContext.get()).foreach { context =>
            context.addTaskCompletionListener[Unit] { _ =>
              it.close()
              cleanSubqueries(it.id, this)
            }
          }

          it
        }

        // Collect the input ColumnarBatches from the child operators and create a CometExecIterator
        // to execute the native plan.
        val sparkPlans = ArrayBuffer.empty[SparkPlan]
        val inputs = ArrayBuffer.empty[RDD[ColumnarBatch]]

        foreachUntilCometInput(this)(sparkPlans += _)

        // Find the first non broadcast plan
        val firstNonBroadcastPlan = sparkPlans.zipWithIndex.find {
          case (_: CometBroadcastExchangeExec, _) => false
          case (BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _), _) => false
          case (BroadcastQueryStageExec(_, _: ReusedExchangeExec, _), _) => false
          case (ReusedExchangeExec(_, _: CometBroadcastExchangeExec), _) => false
          case _ => true
        }

        val containsBroadcastInput = sparkPlans.exists {
          case _: CometBroadcastExchangeExec => true
          case BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) => true
          case BroadcastQueryStageExec(_, _: ReusedExchangeExec, _) => true
          case ReusedExchangeExec(_, _: CometBroadcastExchangeExec) => true
          case _ => false
        }

        // If the first non broadcast plan is not found, it means all the plans are broadcast plans.
        // This is not expected, so throw an exception.
        if (containsBroadcastInput && firstNonBroadcastPlan.isEmpty) {
          throw new CometRuntimeException(s"Cannot find the first non broadcast plan: $this")
        }

        // If the first non broadcast plan is found, we need to adjust the partition number of
        // the broadcast plans to make sure they have the same partition number as the first non
        // broadcast plan.
        val (firstNonBroadcastPlanRDD, firstNonBroadcastPlanNumPartitions) =
          firstNonBroadcastPlan.get._1 match {
            case plan: CometNativeExec =>
              (null, plan.outputPartitioning.numPartitions)
            case plan =>
              val rdd = plan.executeColumnar()
              (rdd, rdd.getNumPartitions)
          }

        // Spark doesn't need to zip Broadcast RDDs, so it doesn't schedule Broadcast RDDs with
        // same partition number. But for Comet, we need to zip them so we need to adjust the
        // partition number of Broadcast RDDs to make sure they have the same partition number.
        sparkPlans.zipWithIndex.foreach { case (plan, idx) =>
          plan match {
            case c: CometBroadcastExchangeExec =>
              inputs += c.executeColumnar(firstNonBroadcastPlanNumPartitions)
            case BroadcastQueryStageExec(_, c: CometBroadcastExchangeExec, _) =>
              inputs += c.executeColumnar(firstNonBroadcastPlanNumPartitions)
            case ReusedExchangeExec(_, c: CometBroadcastExchangeExec) =>
              inputs += c.executeColumnar(firstNonBroadcastPlanNumPartitions)
            case BroadcastQueryStageExec(
                  _,
                  ReusedExchangeExec(_, c: CometBroadcastExchangeExec),
                  _) =>
              inputs += c.executeColumnar(firstNonBroadcastPlanNumPartitions)
            case _: CometNativeExec =>
            // no-op
            case _ if idx == firstNonBroadcastPlan.get._2 =>
              inputs += firstNonBroadcastPlanRDD
            case _ =>
              val rdd = plan.executeColumnar()
              if (rdd.getNumPartitions != firstNonBroadcastPlanNumPartitions) {
                throw new CometRuntimeException(
                  s"Partition number mismatch: ${rdd.getNumPartitions} != " +
                    s"$firstNonBroadcastPlanNumPartitions")
              } else {
                inputs += rdd
              }
          }
        }

        if (inputs.isEmpty && !sparkPlans.forall(_.isInstanceOf[CometNativeExec])) {
          throw new CometRuntimeException(s"No input for CometNativeExec:\n $this")
        }

        if (inputs.nonEmpty) {
          ZippedPartitionsRDD(sparkContext, inputs.toSeq)(createCometExecIter)
        } else {
          val partitionNum = firstNonBroadcastPlanNumPartitions
          CometExecRDD(sparkContext, partitionNum)(createCometExecIter)
        }
    }
  }

  /**
   * Traverse the tree of Comet physical operators until reaching the input sources operators and
   * apply the given function to each operator.
   *
   * The input sources include the following operators:
   *   - CometScanExec - Comet scan node
   *   - CometBatchScanExec - Comet scan node
   *   - ShuffleQueryStageExec - AQE shuffle stage node on top of Comet shuffle
   *   - AQEShuffleReadExec - AQE shuffle read node on top of Comet shuffle
   *   - CometShuffleExchangeExec - Comet shuffle exchange node
   *   - CometUnionExec, etc. which executes its children native plan and produces ColumnarBatches
   *
   * @param plan
   *   the root of the Comet physical plan tree (e.g., the root of the SparkPlan tree of a query)
   *   to traverse
   * @param func
   *   the function to apply to each Comet physical operator
   */
  def foreachUntilCometInput(plan: SparkPlan)(func: SparkPlan => Unit): Unit = {
    plan match {
      case _: CometNativeScanExec | _: CometScanExec | _: CometBatchScanExec |
          _: ShuffleQueryStageExec | _: AQEShuffleReadExec | _: CometShuffleExchangeExec |
          _: CometUnionExec | _: CometTakeOrderedAndProjectExec | _: CometCoalesceExec |
          _: ReusedExchangeExec | _: CometBroadcastExchangeExec | _: BroadcastQueryStageExec |
          _: CometSparkToColumnarExec | _: CometLocalTableScanExec =>
        func(plan)
      case _: CometPlan =>
        // Other Comet operators, continue to traverse the tree.
        plan.children.foreach(foreachUntilCometInput(_)(func))
      case _ =>
      // no op
    }
  }

  /**
   * Converts this native Comet operator and its children into a native block which can be
   * executed as a whole (i.e., in a single JNI call) from the native side.
   */
  def convertBlock(): CometNativeExec = {
    def transform(arg: Any): AnyRef = arg match {
      case serializedPlan: SerializedPlan if serializedPlan.isEmpty =>
        val out = new ByteArrayOutputStream()
        nativeOp.writeTo(out)
        out.close()
        SerializedPlan(Some(out.toByteArray))
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(transform)
    makeCopy(newArgs).asInstanceOf[CometNativeExec]
  }

  /**
   * Cleans the serialized plan from this native Comet operator. Used to canonicalize the plan.
   */
  def cleanBlock(): CometNativeExec = {
    def transform(arg: Any): AnyRef = arg match {
      case serializedPlan: SerializedPlan if serializedPlan.isDefined =>
        SerializedPlan(None)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(transform)
    makeCopy(newArgs).asInstanceOf[CometNativeExec]
  }

  override protected def doCanonicalize(): SparkPlan = {
    val canonicalizedPlan = super
      .doCanonicalize()
      .asInstanceOf[CometNativeExec]
      .canonicalizePlans()

    if (serializedPlanOpt.isDefined) {
      // If the plan is a boundary node, we should remove the serialized plan.
      canonicalizedPlan.cleanBlock()
    } else {
      canonicalizedPlan
    }
  }

  /**
   * Canonicalizes the plans of Product parameters in Comet native operators.
   */
  protected def canonicalizePlans(): CometNativeExec = {
    def transform(arg: Any): AnyRef = arg match {
      case sparkPlan: SparkPlan
          if !sparkPlan.isInstanceOf[CometNativeExec] &&
            children.forall(_ != sparkPlan) =>
        // Different to Spark, Comet native query node might have a Spark plan as Product element.
        // We need to canonicalize the Spark plan. But it cannot be another Comet native query node,
        // otherwise it will cause recursive canonicalization.
        null
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(transform)
    makeCopy(newArgs).asInstanceOf[CometNativeExec]
  }
}

abstract class CometLeafExec extends CometNativeExec with LeafExecNode

abstract class CometUnaryExec extends CometNativeExec with UnaryExecNode

abstract class CometBinaryExec extends CometNativeExec with BinaryExecNode

/**
 * Represents the serialized plan of Comet native operators. Only the first operator in a block of
 * continuous Comet native operators has defined plan bytes which contains the serialization of
 * the plan tree of the block.
 */
case class SerializedPlan(plan: Option[Array[Byte]]) {
  def isDefined: Boolean = plan.isDefined

  def isEmpty: Boolean = plan.isEmpty
}

object CometProjectExec extends CometOperatorSerde[ProjectExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_PROJECT_ENABLED)

  override def convert(
      op: ProjectExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val exprs = op.projectList.map(exprToProto(_, op.child.output))

    if (exprs.forall(_.isDefined) && childOp.nonEmpty) {
      val projectBuilder = OperatorOuterClass.Projection
        .newBuilder()
        .addAllProjectList(exprs.map(_.get).asJava)
      Some(builder.setProjection(projectBuilder).build())
    } else {
      withInfo(op, op.projectList: _*)
      None
    }
  }

  override def createExec(nativeOp: Operator, op: ProjectExec): CometNativeExec = {
    CometProjectExec(nativeOp, op, op.output, op.projectList, op.child, SerializedPlan(None))
  }
}

case class CometProjectExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec
    with PartitioningPreservingUnaryExecNode {
  override def producedAttributes: AttributeSet = outputSet

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(output, projectList, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometProjectExec =>
        this.output == other.output &&
        this.projectList == other.projectList &&
        this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, projectList, child)

  override protected def outputExpressions: Seq[NamedExpression] = projectList
}

object CometFilterExec extends CometOperatorSerde[FilterExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_FILTER_ENABLED)

  override def convert(
      op: FilterExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val cond = exprToProto(op.condition, op.child.output)

    if (cond.isDefined && childOp.nonEmpty) {
      val filterBuilder = OperatorOuterClass.Filter
        .newBuilder()
        .setPredicate(cond.get)
      Some(builder.setFilter(filterBuilder).build())
    } else {
      withInfo(op, op.condition, op.child)
      None
    }
  }

  override def createExec(nativeOp: Operator, op: FilterExec): CometNativeExec = {
    CometFilterExec(nativeOp, op, op.output, op.condition, op.child, SerializedPlan(None))
  }
}

case class CometFilterExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    condition: Expression,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(output, condition, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometFilterExec =>
        this.output == other.output &&
        this.condition == other.condition && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, condition, child)

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |Condition : ${condition}
       |""".stripMargin
  }
}

object CometSortExec extends CometOperatorSerde[SortExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_SORT_ENABLED)

  override def convert(
      op: SortExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    if (!supportedSortType(op, op.sortOrder)) {
      withInfo(op, "Unsupported data type in sort expressions")
      return None
    }

    val sortOrders = op.sortOrder.map(exprToProto(_, op.child.output))

    if (sortOrders.forall(_.isDefined) && childOp.nonEmpty) {
      val sortBuilder = OperatorOuterClass.Sort
        .newBuilder()
        .addAllSortOrders(sortOrders.map(_.get).asJava)
      Some(builder.setSort(sortBuilder).build())
    } else {
      withInfo(op, "sort order not supported", op.sortOrder: _*)
      None
    }
  }

  override def createExec(nativeOp: Operator, op: SortExec): CometNativeExec = {
    CometSortExec(
      nativeOp,
      op,
      op.output,
      op.outputOrdering,
      op.sortOrder,
      op.child,
      SerializedPlan(None))
  }
}

case class CometSortExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    override val outputOrdering: Seq[SortOrder],
    sortOrder: Seq[SortOrder],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(output, sortOrder, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometSortExec =>
        this.output == other.output &&
        this.sortOrder == other.sortOrder && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, sortOrder, child)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.baselineMetrics(sparkContext) ++
      Map(
        "spill_count" -> SQLMetrics.createMetric(sparkContext, "number of spills"),
        "spilled_bytes" -> SQLMetrics.createSizeMetric(sparkContext, "total spilled bytes"))
}

object CometLocalLimitExec extends CometOperatorSerde[LocalLimitExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_LOCAL_LIMIT_ENABLED)

  override def convert(
      op: LocalLimitExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    if (childOp.nonEmpty) {
      // LocalLimit doesn't use offset, but it shares same operator serde class.
      // Just set it to zero.
      val limitBuilder = OperatorOuterClass.Limit
        .newBuilder()
        .setLimit(op.limit)
        .setOffset(0)
      Some(builder.setLimit(limitBuilder).build())
    } else {
      withInfo(op, "No child operator")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: LocalLimitExec): CometNativeExec = {
    CometLocalLimitExec(nativeOp, op, op.limit, op.child, SerializedPlan(None))
  }
}

case class CometLocalLimitExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    limit: Int,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(limit, child)

  override lazy val metrics: Map[String, SQLMetric] = Map.empty

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometLocalLimitExec =>
        this.output == other.output &&
        this.limit == other.limit && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, limit: java.lang.Integer, child)
}

object CometGlobalLimitExec extends CometOperatorSerde[GlobalLimitExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_GLOBAL_LIMIT_ENABLED)

  override def convert(
      op: GlobalLimitExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    if (childOp.nonEmpty) {
      val limitBuilder = OperatorOuterClass.Limit.newBuilder()

      limitBuilder.setLimit(op.limit).setOffset(op.offset)

      Some(builder.setLimit(limitBuilder).build())
    } else {
      withInfo(op, "No child operator")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: GlobalLimitExec): CometNativeExec = {
    CometGlobalLimitExec(nativeOp, op, op.limit, op.offset, op.child, SerializedPlan(None))
  }
}

case class CometGlobalLimitExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    limit: Int,
    offset: Int,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(limit, offset, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometGlobalLimitExec =>
        this.output == other.output &&
        this.limit == other.limit &&
        this.offset == other.offset &&
        this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(output, limit: java.lang.Integer, offset: java.lang.Integer, child)
}

object CometExpandExec extends CometOperatorSerde[ExpandExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_EXPAND_ENABLED)

  override def convert(
      op: ExpandExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    var allProjExprs: Seq[Expression] = Seq()
    val projExprs = op.projections.flatMap(_.map(e => {
      allProjExprs = allProjExprs :+ e
      exprToProto(e, op.child.output)
    }))

    if (projExprs.forall(_.isDefined) && childOp.nonEmpty) {
      val expandBuilder = OperatorOuterClass.Expand
        .newBuilder()
        .addAllProjectList(projExprs.map(_.get).asJava)
        .setNumExprPerProject(op.projections.head.size)
      Some(builder.setExpand(expandBuilder).build())
    } else {
      withInfo(op, allProjExprs: _*)
      None
    }
  }

  override def createExec(nativeOp: Operator, op: ExpandExec): CometNativeExec = {
    CometExpandExec(nativeOp, op, op.output, op.projections, op.child, SerializedPlan(None))
  }
}

case class CometExpandExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    projections: Seq[Seq[Expression]],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def producedAttributes: AttributeSet = outputSet

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(projections, output, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometExpandExec =>
        this.output == other.output &&
        this.projections == other.projections && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, projections, child)

  // TODO: support native Expand metrics
  override lazy val metrics: Map[String, SQLMetric] = Map.empty
}

object CometUnionExec extends CometSink[UnionExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_UNION_ENABLED)

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: UnionExec): CometNativeExec = {
    CometSinkPlaceHolder(nativeOp, op, CometUnionExec(op, op.output, op.children))
  }
}

case class CometUnionExec(
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    children: Seq[SparkPlan])
    extends CometExec {

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    sparkContext.union(children.map(_.executeColumnar()))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    this.copy(children = newChildren)

  override def verboseStringWithOperatorId(): String = {
    val childrenString = children.zipWithIndex
      .map { case (child, index) =>
        s"Child $index ${ExplainUtils.generateFieldString("Input", child.output)}"
      }
      .mkString("\n")
    s"""
       |$formattedNodeName
       |$childrenString
       |""".stripMargin
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometUnionExec =>
        this.output == other.output &&
        this.children == other.children
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, children)
}

trait CometBaseAggregate {

  def doConvert(
      aggregate: BaseAggregateExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {

    val modes = aggregate.aggregateExpressions.map(_.mode).distinct
    // In distinct aggregates there can be a combination of modes
    val multiMode = modes.size > 1
    // For a final mode HashAggregate, we only need to transform the HashAggregate
    // if there is Comet partial aggregation.
    val sparkFinalMode = modes.contains(Final) && findCometPartialAgg(aggregate.child).isEmpty

    if (multiMode || sparkFinalMode) {
      return None
    }

    val groupingExpressions = aggregate.groupingExpressions
    val aggregateExpressions = aggregate.aggregateExpressions
    val aggregateAttributes = aggregate.aggregateAttributes
    val resultExpressions = aggregate.resultExpressions
    val child = aggregate.child

    if (groupingExpressions.isEmpty && aggregateExpressions.isEmpty) {
      withInfo(aggregate, "No group by or aggregation")
      return None
    }

    // Aggregate expressions with filter are not supported yet.
    if (aggregateExpressions.exists(_.filter.isDefined)) {
      withInfo(aggregate, "Aggregate expression with filter is not supported")
      return None
    }

    if (groupingExpressions.exists(expr =>
        expr.dataType match {
          case _: MapType => true
          case _ => false
        })) {
      withInfo(aggregate, "Grouping on map types is not supported")
      return None
    }

    val groupingExprsWithInput =
      groupingExpressions.map(expr => expr.name -> exprToProto(expr, child.output))

    val emptyExprs = groupingExprsWithInput.collect {
      case (expr, proto) if proto.isEmpty => expr
    }

    if (emptyExprs.nonEmpty) {
      withInfo(aggregate, s"Unsupported group expressions: ${emptyExprs.mkString(", ")}")
      return None
    }

    val groupingExprs = groupingExprsWithInput.map(_._2)

    // In some of the cases, the aggregateExpressions could be empty.
    // For example, if the aggregate functions only have group by or if the aggregate
    // functions only have distinct aggregate functions:
    //
    // SELECT COUNT(distinct col2), col1 FROM test group by col1
    //  +- HashAggregate (keys =[col1# 6], functions =[count (distinct col2#7)] )
    //    +- Exchange hashpartitioning (col1#6, 10), ENSURE_REQUIREMENTS, [plan_id = 36]
    //      +- HashAggregate (keys =[col1#6], functions =[partial_count (distinct col2#7)] )
    //        +- HashAggregate (keys =[col1#6, col2#7], functions =[] )
    //          +- Exchange hashpartitioning (col1#6, col2#7, 10), ENSURE_REQUIREMENTS, ...
    //            +- HashAggregate (keys =[col1#6, col2#7], functions =[] )
    //              +- FileScan parquet spark_catalog.default.test[col1#6, col2#7] ......
    // If the aggregateExpressions is empty, we only want to build groupingExpressions,
    // and skip processing of aggregateExpressions.
    if (aggregateExpressions.isEmpty) {
      val hashAggBuilder = OperatorOuterClass.HashAggregate.newBuilder()
      hashAggBuilder.addAllGroupingExprs(groupingExprs.map(_.get).asJava)
      val attributes = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
      val resultExprs = resultExpressions.map(exprToProto(_, attributes))
      if (resultExprs.exists(_.isEmpty)) {
        withInfo(
          aggregate,
          s"Unsupported result expressions found in: $resultExpressions",
          resultExpressions: _*)
        return None
      }
      hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
      Some(builder.setHashAgg(hashAggBuilder).build())
    } else {
      val modes = aggregateExpressions.map(_.mode).distinct

      if (modes.size != 1) {
        // This shouldn't happen as all aggregation expressions should share the same mode.
        // Fallback to Spark nevertheless here.
        withInfo(aggregate, "All aggregate expressions do not have the same mode")
        return None
      }

      val mode = modes.head match {
        case Partial => CometAggregateMode.Partial
        case Final => CometAggregateMode.Final
        case _ =>
          withInfo(aggregate, s"Unsupported aggregation mode ${modes.head}")
          return None
      }

      // In final mode, the aggregate expressions are bound to the output of the
      // child and partial aggregate expressions buffer attributes produced by partial
      // aggregation. This is done in Spark `HashAggregateExec` internally. In Comet,
      // we don't have to do this because we don't use the merging expression.
      val binding = mode != CometAggregateMode.Final
      // `output` is only used when `binding` is true (i.e., non-Final)
      val output = child.output

      val aggExprs =
        aggregateExpressions.map(aggExprToProto(_, output, binding, aggregate.conf))
      if (childOp.nonEmpty && groupingExprs.forall(_.isDefined) &&
        aggExprs.forall(_.isDefined)) {
        val hashAggBuilder = OperatorOuterClass.HashAggregate.newBuilder()
        hashAggBuilder.addAllGroupingExprs(groupingExprs.map(_.get).asJava)
        hashAggBuilder.addAllAggExprs(aggExprs.map(_.get).asJava)
        if (mode == CometAggregateMode.Final) {
          val attributes = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
          val resultExprs = resultExpressions.map(exprToProto(_, attributes))
          if (resultExprs.exists(_.isEmpty)) {
            withInfo(
              aggregate,
              s"Unsupported result expressions found in: $resultExpressions",
              resultExpressions: _*)
            return None
          }
          hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
        }
        hashAggBuilder.setModeValue(mode.getNumber)
        Some(builder.setHashAgg(hashAggBuilder).build())
      } else {
        val allChildren: Seq[Expression] =
          groupingExpressions ++ aggregateExpressions ++ aggregateAttributes
        withInfo(aggregate, allChildren: _*)
        None
      }
    }

  }

  /**
   * Find the first Comet partial aggregate in the plan. If it reaches a Spark HashAggregate with
   * partial mode, it will return None.
   */
  private def findCometPartialAgg(plan: SparkPlan): Option[CometHashAggregateExec] = {
    plan.collectFirst {
      case agg: CometHashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
        Some(agg)
      case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) => None
      case agg: ObjectHashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
        None
      case a: AQEShuffleReadExec => findCometPartialAgg(a.child)
      case s: ShuffleQueryStageExec => findCometPartialAgg(s.plan)
    }.flatten
  }

}

object CometHashAggregateExec
    extends CometOperatorSerde[HashAggregateExec]
    with CometBaseAggregate {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_AGGREGATE_ENABLED)

  override def convert(
      aggregate: HashAggregateExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    doConvert(aggregate, builder, childOp: _*)
  }

  override def createExec(nativeOp: Operator, op: HashAggregateExec): CometNativeExec = {
    CometHashAggregateExec(
      nativeOp,
      op,
      op.output,
      op.groupingExpressions,
      op.aggregateExpressions,
      op.resultExpressions,
      op.child.output,
      op.child,
      SerializedPlan(None))
  }
}

object CometObjectHashAggregateExec
    extends CometOperatorSerde[ObjectHashAggregateExec]
    with CometBaseAggregate {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_AGGREGATE_ENABLED)

  override def convert(
      aggregate: ObjectHashAggregateExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {

    if (!isCometShuffleEnabled(aggregate.conf)) {
      // When Comet shuffle is disabled, we don't want to transform the HashAggregate
      // to CometHashAggregate. Otherwise, we probably get partial Comet aggregation
      // and final Spark aggregation.
      return None
    }

    doConvert(aggregate, builder, childOp: _*)
  }

  override def createExec(nativeOp: Operator, op: ObjectHashAggregateExec): CometNativeExec = {
    CometHashAggregateExec(
      nativeOp,
      op,
      op.output,
      op.groupingExpressions,
      op.aggregateExpressions,
      op.resultExpressions,
      op.child.output,
      op.child,
      SerializedPlan(None))
  }
}

case class CometHashAggregateExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression],
    input: Seq[Attribute],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec
    with PartitioningPreservingUnaryExecNode {

  // The aggExprs could be empty. For example, if the aggregate functions only have
  // distinct aggregate functions or only have group by, the aggExprs is empty and
  // modes is empty too. If aggExprs is not empty, we need to verify all the
  // aggregates have the same mode.
  val modes: Seq[AggregateMode] = aggregateExpressions.map(_.mode).distinct
  assert(modes.length == 1 || modes.isEmpty)
  val mode = modes.headOption

  override def producedAttributes: AttributeSet = outputSet ++ AttributeSet(resultExpressions)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("Keys", groupingExpressions)}
       |${ExplainUtils.generateFieldString("Functions", aggregateExpressions)}
       |""".stripMargin
  }

  override def stringArgs: Iterator[Any] =
    Iterator(input, mode, groupingExpressions, aggregateExpressions, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometHashAggregateExec =>
        this.output == other.output &&
        this.groupingExpressions == other.groupingExpressions &&
        this.aggregateExpressions == other.aggregateExpressions &&
        this.input == other.input &&
        this.mode == other.mode &&
        this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(output, groupingExpressions, aggregateExpressions, input, mode, child)

  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions
}

trait CometHashJoin {

  def doConvert(
      join: HashJoin,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // `HashJoin` has only two implementations in Spark, but we check the type of the join to
    // make sure we are handling the correct join type.
    if (!(CometConf.COMET_EXEC_HASH_JOIN_ENABLED.get(join.conf) &&
        join.isInstanceOf[ShuffledHashJoinExec]) &&
      !(CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.get(join.conf) &&
        join.isInstanceOf[BroadcastHashJoinExec])) {
      withInfo(join, s"Invalid hash join type ${join.nodeName}")
      return None
    }

    if (join.buildSide == BuildRight && join.joinType == LeftAnti) {
      // https://github.com/apache/datafusion-comet/issues/457
      withInfo(join, "BuildRight with LeftAnti is not supported")
      return None
    }

    val condition = join.condition.map { cond =>
      val condProto = exprToProto(cond, join.left.output ++ join.right.output)
      if (condProto.isEmpty) {
        withInfo(join, cond)
        return None
      }
      condProto.get
    }

    val joinType = {
      import OperatorOuterClass.JoinType
      join.joinType match {
        case Inner => JoinType.Inner
        case LeftOuter => JoinType.LeftOuter
        case RightOuter => JoinType.RightOuter
        case FullOuter => JoinType.FullOuter
        case LeftSemi => JoinType.LeftSemi
        case LeftAnti => JoinType.LeftAnti
        case _ =>
          // Spark doesn't support other join types
          withInfo(join, s"Unsupported join type ${join.joinType}")
          return None
      }
    }

    val leftKeys = join.leftKeys.map(exprToProto(_, join.left.output))
    val rightKeys = join.rightKeys.map(exprToProto(_, join.right.output))

    if (leftKeys.forall(_.isDefined) &&
      rightKeys.forall(_.isDefined) &&
      childOp.nonEmpty) {
      val joinBuilder = OperatorOuterClass.HashJoin
        .newBuilder()
        .setJoinType(joinType)
        .addAllLeftJoinKeys(leftKeys.map(_.get).asJava)
        .addAllRightJoinKeys(rightKeys.map(_.get).asJava)
        .setBuildSide(if (join.buildSide == BuildLeft) OperatorOuterClass.BuildSide.BuildLeft
        else OperatorOuterClass.BuildSide.BuildRight)
      condition.foreach(joinBuilder.setCondition)
      Some(builder.setHashJoin(joinBuilder).build())
    } else {
      val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
      withInfo(join, allExprs: _*)
      None
    }
  }
}

object CometBroadcastHashJoinExec extends CometOperatorSerde[HashJoin] with CometHashJoin {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_HASH_JOIN_ENABLED)

  override def convert(
      join: HashJoin,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] =
    doConvert(join, builder, childOp: _*)

  override def createExec(nativeOp: Operator, op: HashJoin): CometNativeExec = {
    CometBroadcastHashJoinExec(
      nativeOp,
      op,
      op.output,
      op.outputOrdering,
      op.leftKeys,
      op.rightKeys,
      op.joinType,
      op.condition,
      op.buildSide,
      op.left,
      op.right,
      SerializedPlan(None))
  }
}

object CometHashJoinExec extends CometOperatorSerde[HashJoin] with CometHashJoin {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_HASH_JOIN_ENABLED)

  override def convert(
      join: HashJoin,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] =
    doConvert(join, builder, childOp: _*)

  override def createExec(nativeOp: Operator, op: HashJoin): CometNativeExec = {
    CometHashJoinExec(
      nativeOp,
      op,
      op.output,
      op.outputOrdering,
      op.leftKeys,
      op.rightKeys,
      op.joinType,
      op.condition,
      op.buildSide,
      op.left,
      op.right,
      SerializedPlan(None))
  }
}

case class CometHashJoinExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    override val outputOrdering: Seq[SortOrder],
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    buildSide: BuildSide,
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometBinaryExec {

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(s"ShuffledJoin should not take $x as the JoinType")
  }

  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    this.copy(left = newLeft, right = newRight)

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, buildSide, condition, left, right)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometHashJoinExec =>
        this.output == other.output &&
        this.leftKeys == other.leftKeys &&
        this.rightKeys == other.rightKeys &&
        this.condition == other.condition &&
        this.buildSide == other.buildSide &&
        this.left == other.left &&
        this.right == other.right &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(output, leftKeys, rightKeys, condition, buildSide, left, right)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.hashJoinMetrics(sparkContext)
}

case class CometBroadcastHashJoinExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    override val outputOrdering: Seq[SortOrder],
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    buildSide: BuildSide,
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometBinaryExec {

  // The following logic of `outputPartitioning` is copied from Spark `BroadcastHashJoinExec`.
  protected lazy val streamedPlan: SparkPlan = buildSide match {
    case BuildLeft => right
    case BuildRight => left
  }

  override lazy val outputPartitioning: Partitioning = {
    joinType match {
      case _: InnerLike if conf.broadcastHashJoinOutputPartitioningExpandLimit > 0 =>
        streamedPlan.outputPartitioning match {
          case h: HashPartitioning => expandOutputPartitioning(h)
          case h: Expression if h.getClass.getName.contains("CoalescedHashPartitioning") =>
            expandOutputPartitioning(h)
          case c: PartitioningCollection => expandOutputPartitioning(c)
          case other => other
        }
      case _ => streamedPlan.outputPartitioning
    }
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(
      leftKeys.length == rightKeys.length &&
        leftKeys
          .map(_.dataType)
          .zip(rightKeys.map(_.dataType))
          .forall(types => types._1.sameType(types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  // An one-to-many mapping from a streamed key to build keys.
  private lazy val streamedKeyToBuildKeyMapping = {
    val mapping = mutable.Map.empty[Expression, Seq[Expression]]
    streamedKeys.zip(buildKeys).foreach { case (streamedKey, buildKey) =>
      val key = streamedKey.canonicalized
      mapping.get(key) match {
        case Some(v) => mapping.put(key, v :+ buildKey)
        case None => mapping.put(key, Seq(buildKey))
      }
    }
    mapping.toMap
  }

  // Expands the given partitioning collection recursively.
  private def expandOutputPartitioning(
      partitioning: PartitioningCollection): PartitioningCollection = {
    PartitioningCollection(partitioning.partitionings.flatMap {
      case h: HashPartitioning => expandOutputPartitioning(h).partitionings
      case h: Expression if h.getClass.getName.contains("CoalescedHashPartitioning") =>
        expandOutputPartitioning(h).partitionings
      case c: PartitioningCollection => Seq(expandOutputPartitioning(c))
      case other => Seq(other)
    })
  }

  // Expands the given hash partitioning by substituting streamed keys with build keys.
  // For example, if the expressions for the given partitioning are Seq("a", "b", "c")
  // where the streamed keys are Seq("b", "c") and the build keys are Seq("x", "y"),
  // the expanded partitioning will have the following expressions:
  // Seq("a", "b", "c"), Seq("a", "b", "y"), Seq("a", "x", "c"), Seq("a", "x", "y").
  // The expanded expressions are returned as PartitioningCollection.
  private def expandOutputPartitioning(
      partitioning: Partitioning with Expression): PartitioningCollection = {
    val maxNumCombinations = conf.broadcastHashJoinOutputPartitioningExpandLimit
    var currentNumCombinations = 0

    def generateExprCombinations(
        current: Seq[Expression],
        accumulated: Seq[Expression]): Seq[Seq[Expression]] = {
      if (currentNumCombinations >= maxNumCombinations) {
        Nil
      } else if (current.isEmpty) {
        currentNumCombinations += 1
        Seq(accumulated)
      } else {
        val buildKeysOpt = streamedKeyToBuildKeyMapping.get(current.head.canonicalized)
        generateExprCombinations(current.tail, accumulated :+ current.head) ++
          buildKeysOpt
            .map(_.flatMap(b => generateExprCombinations(current.tail, accumulated :+ b)))
            .getOrElse(Nil)
      }
    }

    val hashPartitioningLikeExpressions =
      partitioning match {
        case p: HashPartitioningLike => p.expressions
        case _ => Seq()
      }
    PartitioningCollection(
      generateExprCombinations(hashPartitioningLikeExpressions, Nil)
        .map(exprs => partitioning.withNewChildren(exprs).asInstanceOf[Partitioning]))
  }

  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    this.copy(left = newLeft, right = newRight)

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, condition, buildSide, left, right)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometBroadcastHashJoinExec =>
        this.output == other.output &&
        this.leftKeys == other.leftKeys &&
        this.rightKeys == other.rightKeys &&
        this.condition == other.condition &&
        this.buildSide == other.buildSide &&
        this.left == other.left &&
        this.right == other.right &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(output, leftKeys, rightKeys, condition, buildSide, left, right)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.hashJoinMetrics(sparkContext)
}

object CometSortMergeJoinExec extends CometOperatorSerde[SortMergeJoinExec] {
  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED)

  override def convert(
      join: SortMergeJoinExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // `requiredOrders` and `getKeyOrdering` are copied from Spark's SortMergeJoinExec.
    def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
      keys.map(SortOrder(_, Ascending))
    }

    def getKeyOrdering(
        keys: Seq[Expression],
        childOutputOrdering: Seq[SortOrder]): Seq[SortOrder] = {
      val requiredOrdering = requiredOrders(keys)
      if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
        keys.zip(childOutputOrdering).map { case (key, childOrder) =>
          val sameOrderExpressionsSet = ExpressionSet(childOrder.children) - key
          SortOrder(key, Ascending, sameOrderExpressionsSet.toSeq)
        }
      } else {
        requiredOrdering
      }
    }

    if (join.condition.isDefined &&
      !CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED
        .get(join.conf)) {
      withInfo(
        join,
        s"${CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key} is not enabled",
        join.condition.get)
      return None
    }

    val condition = join.condition.map { cond =>
      val condProto = exprToProto(cond, join.left.output ++ join.right.output)
      if (condProto.isEmpty) {
        withInfo(join, cond)
        return None
      }
      condProto.get
    }

    val joinType = {
      import OperatorOuterClass.JoinType
      join.joinType match {
        case Inner => JoinType.Inner
        case LeftOuter => JoinType.LeftOuter
        case RightOuter => JoinType.RightOuter
        case FullOuter => JoinType.FullOuter
        case LeftSemi => JoinType.LeftSemi
        case LeftAnti => JoinType.LeftAnti
        case _ =>
          // Spark doesn't support other join types
          withInfo(join, s"Unsupported join type ${join.joinType}")
          return None
      }
    }

    // Checks if the join keys are supported by DataFusion SortMergeJoin.
    val errorMsgs = join.leftKeys.flatMap { key =>
      if (!supportedSortMergeJoinEqualType(key.dataType)) {
        Some(s"Unsupported join key type ${key.dataType} on key: ${key.sql}")
      } else {
        None
      }
    }

    if (errorMsgs.nonEmpty) {
      withInfo(join, errorMsgs.flatten.mkString("\n"))
      return None
    }

    val leftKeys = join.leftKeys.map(exprToProto(_, join.left.output))
    val rightKeys = join.rightKeys.map(exprToProto(_, join.right.output))

    val sortOptions = getKeyOrdering(join.leftKeys, join.left.outputOrdering)
      .map(exprToProto(_, join.left.output))

    if (sortOptions.forall(_.isDefined) &&
      leftKeys.forall(_.isDefined) &&
      rightKeys.forall(_.isDefined) &&
      childOp.nonEmpty) {
      val joinBuilder = OperatorOuterClass.SortMergeJoin
        .newBuilder()
        .setJoinType(joinType)
        .addAllSortOptions(sortOptions.map(_.get).asJava)
        .addAllLeftJoinKeys(leftKeys.map(_.get).asJava)
        .addAllRightJoinKeys(rightKeys.map(_.get).asJava)
      condition.map(joinBuilder.setCondition)
      Some(builder.setSortMergeJoin(joinBuilder).build())
    } else {
      val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
      withInfo(join, allExprs: _*)
      None
    }
  }

  override def createExec(nativeOp: Operator, op: SortMergeJoinExec): CometNativeExec = {
    CometSortMergeJoinExec(
      nativeOp,
      op,
      op.output,
      op.outputOrdering,
      op.leftKeys,
      op.rightKeys,
      op.joinType,
      op.condition,
      op.left,
      op.right,
      SerializedPlan(None))
  }

  /**
   * Returns true if given datatype is supported as a key in DataFusion sort merge join.
   */
  private def supportedSortMergeJoinEqualType(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: DateType | _: DecimalType | _: BooleanType =>
      true
    case TimestampNTZType => true
    case _ => false
  }

}

case class CometSortMergeJoinExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    override val outputOrdering: Seq[SortOrder],
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometBinaryExec {

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(s"ShuffledJoin should not take $x as the JoinType")
  }

  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    this.copy(left = newLeft, right = newRight)

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, condition, left, right)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometSortMergeJoinExec =>
        this.output == other.output &&
        this.leftKeys == other.leftKeys &&
        this.rightKeys == other.rightKeys &&
        this.condition == other.condition &&
        this.left == other.left &&
        this.right == other.right &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(output, leftKeys, rightKeys, condition, left, right)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.sortMergeJoinMetrics(sparkContext)
}

case class CometScanWrapper(override val nativeOp: Operator, override val originalPlan: SparkPlan)
    extends CometNativeExec
    with LeafExecNode {
  override val serializedPlanOpt: SerializedPlan = SerializedPlan(None)

  override def stringArgs: Iterator[Any] = Iterator(originalPlan.output, originalPlan)
}

/**
 * A pseudo Comet physical scan node after Comet operators. This node is used to be a placeholder
 * for chaining with following Comet native operators after previous Comet operators. This node
 * will be removed after `CometExecRule`.
 *
 * This is very similar to `CometScanWrapper` above except it has child.
 */
case class CometSinkPlaceHolder(
    override val nativeOp: Operator, // Must be a Scan
    override val originalPlan: SparkPlan,
    child: SparkPlan)
    extends CometUnaryExec {
  override val serializedPlanOpt: SerializedPlan = SerializedPlan(None)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    this.copy(child = newChild)
  }

  override def stringArgs: Iterator[Any] = Iterator(originalPlan.output, child)
}
