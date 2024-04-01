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

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.execution.shuffle.{ArrowReaderIterator, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{BinaryExecNode, ColumnarToRowExec, ExecSubqueryExpression, ExplainUtils, LeafExecNode, ScalarSubquery, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import com.google.common.base.Objects

import org.apache.comet.{CometConf, CometExecIterator, CometRuntimeException}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.vector.NativeUtil

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

  override def outputPartitioning: Partitioning = originalPlan.outputPartitioning

  /**
   * Executes this Comet operator and serialized output ColumnarBatch into bytes.
   */
  def getByteArrayRdd(): RDD[(Long, ChunkedByteBuffer)] = {
    executeColumnar().mapPartitionsInternal { iter =>
      serializeBatches(iter)
    }
  }

  /**
   * Serializes a list of `ColumnarBatch` into an output stream.
   *
   * @param batches
   *   the output batches, each batch is a list of Arrow vectors wrapped in `CometVector`
   * @param out
   *   the output stream
   */
  def serializeBatches(batches: Iterator[ColumnarBatch]): Iterator[(Long, ChunkedByteBuffer)] = {
    val nativeUtil = new NativeUtil()

    batches.map { batch =>
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val cbbos = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
      val out = new DataOutputStream(codec.compressedOutputStream(cbbos))

      val (fieldVectors, batchProviderOpt) = nativeUtil.getBatchFieldVectors(batch)
      val root = new VectorSchemaRoot(fieldVectors.asJava)
      val provider = batchProviderOpt.getOrElse(nativeUtil.getDictionaryProvider)

      val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(out))
      writer.start()
      writer.writeBatch()

      root.clear()
      writer.end()

      out.flush()
      out.close()

      if (out.size() > 0) {
        (batch.numRows(), cbbos.toChunkedByteBuffer)
      } else {
        (batch.numRows(), new ChunkedByteBuffer(Array.empty[ByteBuffer]))
      }
    }
  }

  /**
   * Executes the Comet operator and returns the result as an iterator of ColumnarBatch.
   */
  def executeColumnarCollectIterator(): (Long, Iterator[ColumnarBatch]) = {
    val countsAndBytes = getByteArrayRdd().collect()
    val total = countsAndBytes.map(_._1).sum
    val rows = countsAndBytes.iterator
      .flatMap(countAndBytes => CometExec.decodeBatches(countAndBytes._2))
    (total, rows)
  }
}

object CometExec {
  // An unique id for each CometExecIterator, used to identify the native query execution.
  private val curId = new java.util.concurrent.atomic.AtomicLong()

  def newIterId: Long = curId.getAndIncrement()

  def getCometIterator(
      inputs: Seq[Iterator[ColumnarBatch]],
      nativePlan: Operator): CometExecIterator = {
    getCometIterator(inputs, nativePlan, CometMetricNode(Map.empty))
  }

  def getCometIterator(
      inputs: Seq[Iterator[ColumnarBatch]],
      nativePlan: Operator,
      nativeMetrics: CometMetricNode): CometExecIterator = {
    val outputStream = new ByteArrayOutputStream()
    nativePlan.writeTo(outputStream)
    outputStream.close()
    val bytes = outputStream.toByteArray
    new CometExecIterator(newIterId, inputs, bytes, nativeMetrics)
  }

  /**
   * Decodes the byte arrays back to ColumnarBatchs and put them into buffer.
   */
  def decodeBatches(bytes: ChunkedByteBuffer): Iterator[ColumnarBatch] = {
    if (bytes.size == 0) {
      return Iterator.empty
    }

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val cbbis = bytes.toInputStream()
    val ins = new DataInputStream(codec.compressedInputStream(cbbis))

    new ArrowReaderIterator(Channels.newChannel(ins))
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

  override protected def doPrepare(): Unit = prepareSubqueries(originalPlan)

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

  private def setSubqueries(planId: Long, sparkPlan: SparkPlan): Unit = {
    sparkPlan.children.foreach(setSubqueries(planId, _))

    sparkPlan.expressions.foreach {
      _.collect { case sub: ScalarSubquery =>
        CometScalarSubquery.setSubquery(planId, sub)
      }
    }
  }

  private def cleanSubqueries(planId: Long, sparkPlan: SparkPlan): Unit = {
    sparkPlan.children.foreach(cleanSubqueries(planId, _))

    sparkPlan.expressions.foreach {
      _.collect { case sub: ScalarSubquery =>
        CometScalarSubquery.removeSubquery(planId, sub)
      }
    }
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

        def createCometExecIter(inputs: Seq[Iterator[ColumnarBatch]]): CometExecIterator = {
          val it =
            new CometExecIterator(CometExec.newIterId, inputs, serializedPlanCopy, nativeMetrics)

          setSubqueries(it.id, originalPlan)

          Option(TaskContext.get()).foreach { context =>
            context.addTaskCompletionListener[Unit] { _ =>
              it.close()
              cleanSubqueries(it.id, originalPlan)
            }
          }

          it
        }

        // Collect the input ColumnarBatches from the child operators and create a CometExecIterator
        // to execute the native plan.
        val inputs = ArrayBuffer.empty[RDD[ColumnarBatch]]

        foreachUntilCometInput(this)(inputs += _.executeColumnar())

        if (inputs.isEmpty) {
          throw new CometRuntimeException(s"No input for CometNativeExec: $this")
        }

        ZippedPartitionsRDD(sparkContext, inputs.toSeq)(createCometExecIter(_))
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
      case _: CometScanExec | _: CometBatchScanExec | _: ShuffleQueryStageExec |
          _: AQEShuffleReadExec | _: CometShuffleExchangeExec | _: CometUnionExec |
          _: CometTakeOrderedAndProjectExec | _: CometCoalesceExec | _: ReusedExchangeExec |
          _: CometBroadcastExchangeExec | _: BroadcastQueryStageExec =>
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
    val canonicalizedPlan = super.doCanonicalize().asInstanceOf[CometNativeExec]
    if (serializedPlanOpt.isDefined) {
      // If the plan is a boundary node, we should remove the serialized plan.
      canonicalizedPlan.cleanBlock()
    } else {
      canonicalizedPlan
    }
  }
}

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

case class CometProjectExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    projectList: Seq[NamedExpression],
    override val output: Seq[Attribute],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override def producedAttributes: AttributeSet = outputSet
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(output, projectList, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometProjectExec =>
        this.projectList == other.projectList &&
        this.output == other.output && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(projectList, output, child)
}

case class CometFilterExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    condition: Expression,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(originalPlan.output, condition, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometFilterExec =>
        this.condition == other.condition && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(condition, child)

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |Condition : ${condition}
       |""".stripMargin
  }
}

case class CometSortExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    sortOrder: Seq[SortOrder],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(originalPlan.output, sortOrder, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometSortExec =>
        this.sortOrder == other.sortOrder && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(sortOrder, child)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.baselineMetrics(sparkContext) ++
      Map(
        "spill_count" -> SQLMetrics.createMetric(sparkContext, "number of spills"),
        "spilled_bytes" -> SQLMetrics.createNanoTimingMetric(sparkContext, "total spilled bytes"))
}

case class CometLocalLimitExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    limit: Int,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(limit, child)

  override lazy val metrics: Map[String, SQLMetric] = Map.empty

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometLocalLimitExec =>
        this.limit == other.limit && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(limit: java.lang.Integer, child)
}

case class CometGlobalLimitExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    limit: Int,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(limit, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometGlobalLimitExec =>
        this.limit == other.limit && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(limit: java.lang.Integer, child)
}

case class CometExpandExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    projections: Seq[Seq[Expression]],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override def producedAttributes: AttributeSet = outputSet

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(projections, output, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometExpandExec =>
        this.projections == other.projections && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(projections, child)

  // TODO: support native Expand metrics
  override lazy val metrics: Map[String, SQLMetric] = Map.empty
}

case class CometUnionExec(override val originalPlan: SparkPlan, children: Seq[SparkPlan])
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
      case other: CometUnionExec => this.children == other.children
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(children)
}

case class CometHashAggregateExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    input: Seq[Attribute],
    mode: Option[AggregateMode],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
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
    Objects.hashCode(groupingExpressions, aggregateExpressions, input, mode, child)
}

case class CometHashJoinExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    buildSide: BuildSide,
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometBinaryExec {
  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    this.copy(left = newLeft, right = newRight)

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, condition, left, right)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometHashJoinExec =>
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
    Objects.hashCode(leftKeys, rightKeys, condition, buildSide, left, right)
}

case class CometBroadcastHashJoinExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    buildSide: BuildSide,
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometBinaryExec {
  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    this.copy(left = newLeft, right = newRight)

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, condition, left, right)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometBroadcastHashJoinExec =>
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
    Objects.hashCode(leftKeys, rightKeys, condition, buildSide, left, right)
}

case class CometSortMergeJoinExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometBinaryExec {
  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    this.copy(left = newLeft, right = newRight)

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, condition, left, right)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometSortMergeJoinExec =>
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
    Objects.hashCode(leftKeys, rightKeys, condition, left, right)
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
