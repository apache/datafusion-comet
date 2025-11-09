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

package org.apache.comet.serde.operator

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.LocalTableScanExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

object CometLocalTableScan extends CometOperatorSerde[LocalTableScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED)

  override def convert(
      op: LocalTableScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val scanTypes = op.output.flatten(attr => serializeDataType(attr.dataType))
    val scanBuilder = OperatorOuterClass.Scan
      .newBuilder()
      .setSource(op.getClass.getSimpleName)
      .addAllFields(scanTypes.asJava)
      .setArrowFfiSafe(false)
    Some(builder.setScan(scanBuilder).build())
  }
}
