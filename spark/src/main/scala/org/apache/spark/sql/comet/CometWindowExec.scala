package org.apache.spark.sql.comet

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType, windowExprToProto}

import scala.collection.JavaConverters.asJavaIterableConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression, SortOrder, WindowExpression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.CometWindowExec.getNativePlan
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Comet physical plan node for Spark `WindowsExec`.
 *
 * It is used to execute a `WindowsExec` physical operator by using Comet native engine. It is not
 * like other physical plan nodes which are wrapped by `CometExec`, because it contains two native
 * executions separated by a Comet shuffle exchange.
 */
case class CometWindowExec(
                            override val originalPlan: SparkPlan,
                            windowExpression: Seq[NamedExpression],
                            partitionSpec: Seq[Expression],
                            orderSpec: Seq[SortOrder],
                            child: SparkPlan)
  extends CometExec
    with UnaryExecNode {

  override def nodeName: String = "CometWindowExec"

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "shuffleReadElapsedCompute" ->
      SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle read elapsed compute at native"),
    "numPartitions" -> SQLMetrics.createMetric(
      sparkContext,
      "number of partitions")) ++ readMetrics ++ writeMetrics

  override def supportsColumnar: Boolean = true

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()

    childRDD.mapPartitionsInternal { iter =>
      CometExec.getCometIterator(
        Seq(iter),
        getNativePlan(output, windowExpression, partitionSpec, orderSpec, child).get)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

}

object CometWindowExec {
  def getNativePlan(
                     outputAttributes: Seq[Attribute],
                     windowExpression: Seq[NamedExpression],
                     partitionSpec: Seq[Expression],
                     orderSpec: Seq[SortOrder],
                     child: SparkPlan): Option[Operator] = {

    val orderSpecs = orderSpec.map(exprToProto(_, child.output))
    val partitionSpecs = partitionSpec.map(exprToProto(_, child.output))
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val scanOpBuilder = OperatorOuterClass.Operator.newBuilder()

    val scanTypes = outputAttributes.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    val windowExprs = windowExpression.map(w =>
      windowExprToProto(w.asInstanceOf[Alias].child.asInstanceOf[WindowExpression], child.output))

    val windowBuilder = OperatorOuterClass.Window
      .newBuilder()

    if (windowExprs.forall(_.isDefined)) {
      windowBuilder
        .addAllWindowExpr(windowExprs.map(_.get).asJava)

      if (orderSpecs.forall(_.isDefined)) {
        windowBuilder.addAllOrderByList(orderSpecs.map(_.get).asJava)
      }

      if (partitionSpecs.forall(_.isDefined)) {
        windowBuilder.addAllPartitionByList(partitionSpecs.map(_.get).asJava)
      }

      scanBuilder.addAllFields(scanTypes.asJava)

      val opBuilder = OperatorOuterClass.Operator
        .newBuilder()
        .addChildren(scanOpBuilder.setScan(scanBuilder))

      Some(opBuilder.setWindow(windowBuilder).build())
    } else None
  }
}