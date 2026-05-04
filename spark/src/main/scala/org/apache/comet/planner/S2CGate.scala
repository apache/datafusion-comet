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

package org.apache.comet.planner

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.CometSparkToColumnarExec
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometConf._

/**
 * Decides whether a non-Comet leaf should be wrapped in `CometSparkToColumnarExec` to bridge
 * row-at-a-time data into a Comet-consuming parent. Ports
 * `CometExecRule.shouldApplySparkToColumnar` and `isSparkToArrowEnabled` into a single entry
 * point used by Phase 2. Demand gating (parent must be LIKELY_COMET) is applied by Phase 2
 * itself, not here.
 */
object S2CGate extends Logging {

  def shouldApply(op: SparkPlan, conf: SQLConf): Boolean = {
    val fallbackReasons = new ListBuffer[String]()
    if (!CometSparkToColumnarExec.isSchemaSupported(op.schema, fallbackReasons)) {
      logDebug(
        s"S2CGate reject schemaUnsupported node=${op.getClass.getSimpleName} " +
          s"id=${op.id} reasons=${fallbackReasons.mkString(" | ")}")
      return false
    }

    op match {
      case scan: FileSourceScanExec =>
        scan.relation.fileFormat match {
          case _: CSVFileFormat => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
          case _: JsonFileFormat => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
          case _: ParquetFileFormat => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
          case _ => isSparkToArrowEnabled(op, conf)
        }
      case scan: BatchScanExec =>
        scan.scan match {
          case _: CSVScan => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
          case _: JsonScan => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
          case _: ParquetScan => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
          case _ => isSparkToArrowEnabled(op, conf)
        }
      case _: LeafExecNode =>
        isSparkToArrowEnabled(op, conf)
      case _ =>
        // Matches the old rule's conservative behavior. Non-leaf intermediate operators are
        // not wrapped in CometSparkToColumnarExec today.
        false
    }
  }

  private def isSparkToArrowEnabled(op: SparkPlan, conf: SQLConf): Boolean = {
    COMET_SPARK_TO_ARROW_ENABLED.get(conf) && {
      // Derive the operator name from the class name, not op.nodeName. Some operators
      // override nodeName (e.g. `InMemoryTableScanExec` returns `"Scan <cachedName>"`),
      // which would never match the "InMemoryTableScan" entry in the allowlist.
      val derivedName = op.getClass.getSimpleName.replaceAll("Exec$", "")
      COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.get(conf).contains(derivedName)
    }
  }
}
