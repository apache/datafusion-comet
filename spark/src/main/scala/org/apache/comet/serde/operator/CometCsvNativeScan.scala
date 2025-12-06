package org.apache.comet.serde.operator

import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeExec}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, Compatible, SupportLevel}

object CometCsvNativeScan extends CometOperatorSerde[CometBatchScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_CSV_V2_NATIVE_ENABLED)

  override def getSupportLevel(operator: CometBatchScanExec): SupportLevel = {
    Compatible(None)
  }

  override def convert(op: CometBatchScanExec, builder: Any, childOp: Any*): Option[Any] = ???

  override def createExec(nativeOp: Any, op: CometBatchScanExec): CometNativeExec = ???
}
