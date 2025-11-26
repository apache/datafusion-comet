package org.apache.comet.serde.operator

import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeExec}

import org.apache.comet.ConfigEntry
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometCsvNativeScan extends CometOperatorSerde[CometBatchScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = ???

  override def convert(
      op: CometBatchScanExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = ???

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: CometBatchScanExec): CometNativeExec = ???
}
