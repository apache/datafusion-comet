package org.apache.spark.sql.comet
import org.apache.spark.sql.execution.SparkPlan

case class CometCsvNativeScanExec() extends CometLeafExec {
  override def serializedPlanOpt: SerializedPlan = ???

  override def nativeOp = ???

  override def originalPlan: SparkPlan = ???
}
