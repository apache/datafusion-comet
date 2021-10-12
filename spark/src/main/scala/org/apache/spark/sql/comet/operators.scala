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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarToRowExec, ExecSubqueryExpression, ExplainUtils, LeafExecNode, ScalarSubquery, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.{CometConf, CometExecIterator, CometRuntimeException, CometSparkSessionExtensions}
import org.apache.comet.CometConf.{COMET_BATCH_SIZE, COMET_DEBUG_ENABLED, COMET_EXEC_MEMORY_FRACTION}
import org.apache.comet.serde.OperatorOuterClass.Operator

import com.google.common.base.Objects

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

    val configs = new java.util.HashMap[String, String]()

    val maxMemory =
      CometSparkSessionExtensions.getCometMemoryOverhead(SparkEnv.get.conf)
    configs.put("memory_limit", String.valueOf(maxMemory))
    configs.put("memory_fraction", String.valueOf(COMET_EXEC_MEMORY_FRACTION.get()))
    configs.put("batch_size", String.valueOf(COMET_BATCH_SIZE.get()))
    configs.put("debug_native", String.valueOf(COMET_DEBUG_ENABLED.get()))

    new CometExecIterator(newIterId, inputs, bytes, configs, nativeMetrics)
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
  private var serializedPlanOpt: Option[Array[Byte]] = None

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
    serializedPlanOpt match {
      case None =>
        assert(children.length == 1) // TODO: fix this!
        children.head.executeColumnar()
      case Some(serializedPlan) =>
        // Switch to use Decimal128 regardless of precision, since Arrow native execution
        // doesn't support Decimal32 and Decimal64 yet.
        conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")

        // Populate native configurations
        val configs = new java.util.HashMap[String, String]()
        val maxMemory = CometSparkSessionExtensions.getCometMemoryOverhead(sparkContext.getConf)
        configs.put("memory_limit", String.valueOf(maxMemory))
        configs.put("memory_fraction", String.valueOf(COMET_EXEC_MEMORY_FRACTION.get()))
        configs.put("batch_size", String.valueOf(COMET_BATCH_SIZE.get()))
        configs.put("debug_native", String.valueOf(COMET_DEBUG_ENABLED.get()))

        // Strip mandatory prefix spark. which is not required for datafusion session params
        session.conf.getAll.foreach {
          case (k, v) if k.startsWith("spark.datafusion") =>
            configs.put(k.replaceFirst("spark\\.", ""), v)
          case _ =>
        }
        val serializedPlanCopy = serializedPlan
        // TODO: support native metrics for all operators.
        val nativeMetrics = CometMetricNode.fromCometPlan(this)

        def createCometExecIter(inputs: Seq[Iterator[ColumnarBatch]]): CometExecIterator = {
          val it = new CometExecIterator(
            CometExec.newIterId,
            inputs,
            serializedPlanCopy,
            configs,
            nativeMetrics)

          setSubqueries(it.id, originalPlan)

          Option(TaskContext.get()).foreach { context =>
            context.addTaskCompletionListener[Unit] { _ =>
              it.close()
              cleanSubqueries(it.id, originalPlan)
            }
          }

          it
        }

        children.map(_.executeColumnar) match {
          case Seq(child) =>
            child.mapPartitionsInternal(iter => createCometExecIter(Seq(iter)))
          case Seq(first, second) =>
            first.zipPartitions(second) { (iter1, iter2) =>
              createCometExecIter(Seq(iter1, iter2))
            }
          case _ =>
            throw new CometRuntimeException(
              s"Expected only two children but got s${children.size}")
        }
    }
  }

  /**
   * Converts this native Comet operator and its children into a native block which can be
   * executed as a whole (i.e., in a single JNI call) from the native side.
   */
  def convertBlock(): Unit = {
    val out = new ByteArrayOutputStream()
    nativeOp.writeTo(out)
    out.close()
    serializedPlanOpt = Some(out.toByteArray)
  }
}

abstract class CometUnaryExec extends CometNativeExec with UnaryExecNode

case class CometProjectExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    projectList: Seq[NamedExpression],
    override val output: Seq[Attribute],
    child: SparkPlan)
    extends CometUnaryExec {
  override def producedAttributes: AttributeSet = outputSet
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(output, projectList, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometProjectExec =>
        this.projectList == other.projectList &&
        this.output == other.output && this.child == other.child
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
    child: SparkPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(originalPlan.output, condition, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometFilterExec =>
        this.condition == other.condition && this.child == other.child
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
    child: SparkPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(originalPlan.output, sortOrder, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometSortExec =>
        this.sortOrder == other.sortOrder && this.child == other.child
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
    child: SparkPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(limit, child)

  override lazy val metrics: Map[String, SQLMetric] = Map.empty
}

case class CometGlobalLimitExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    limit: Int,
    child: SparkPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(limit, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometGlobalLimitExec =>
        this.limit == other.limit && this.child == other.child
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
    child: SparkPlan)
    extends CometUnaryExec {
  override def producedAttributes: AttributeSet = outputSet

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(projections, output, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometExpandExec =>
        this.projections == other.projections && this.child == other.child
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
    mode: AggregateMode,
    child: SparkPlan)
    extends CometUnaryExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(input, mode, groupingExpressions, aggregateExpressions, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometHashAggregateExec =>
        this.groupingExpressions == other.groupingExpressions &&
        this.aggregateExpressions == other.aggregateExpressions &&
        this.input == other.input &&
        this.mode == other.mode &&
        this.child == other.child
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(groupingExpressions, aggregateExpressions, input, mode, child)
}

case class CometScanWrapper(override val nativeOp: Operator, override val originalPlan: SparkPlan)
    extends CometNativeExec
    with LeafExecNode {
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
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    this.copy(child = newChild)
  }

  override def stringArgs: Iterator[Any] = Iterator(originalPlan.output, child)
}
