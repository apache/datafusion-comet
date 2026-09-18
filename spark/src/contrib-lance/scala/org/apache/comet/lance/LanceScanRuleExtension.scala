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

package org.apache.comet.lance

import java.util.{Optional => JOptional}

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.rules.CometScanContrib

/**
 * `CometScanContrib` implementation for Lance V2 scans.
 *
 * The class is loaded through `ServiceLoader` from the optional contrib-lance profile. It keeps
 * Lance Spark references reflective so building this contrib does not require a lance-spark
 * compile-time dependency.
 */
class LanceScanRuleExtension extends CometScanContrib with Logging {

  private val LanceScanClassName = "org.lance.spark.read.LanceScan"
  private val NativeScanPlanMethod = "nativeScanPlan"

  override def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] = {
    if (scanExec.scan.getClass.getName != LanceScanClassName) {
      return None
    }

    Some(tryCreateNativeScan(scanExec).getOrElse(scanExec))
  }

  private def nativeScanPlan(scan: AnyRef): Option[AnyRef] = {
    try {
      val result = scan.getClass
        .getMethod(NativeScanPlanMethod)
        .invoke(scan)
        .asInstanceOf[JOptional[_]]
      if (result.isPresent) Some(result.get().asInstanceOf[AnyRef]) else None
    } catch {
      case NonFatal(e) =>
        logWarning(s"Native Lance scan disabled because $NativeScanPlanMethod() failed", e)
        None
    }
  }

  private def tryCreateNativeScan(scanExec: BatchScanExec): Option[SparkPlan] = {
    if (!CometConf.COMET_LANCE_NATIVE_ENABLED.get(scanExec.conf)) {
      withFallbackReason(
        scanExec,
        s"Native Lance scan disabled because ${CometConf.COMET_LANCE_NATIVE_ENABLED.key} " +
          "is not enabled")
      return None
    }

    if (!CometConf.COMET_EXEC_ENABLED.get(scanExec.conf)) {
      withFallbackReason(
        scanExec,
        s"Native Lance scan disabled because ${CometConf.COMET_EXEC_ENABLED.key} is not enabled")
      return None
    }

    val nativePlan = nativeScanPlan(scanExec.scan.asInstanceOf[AnyRef]) match {
      case Some(plan) => plan
      case None =>
        withFallbackReason(
          scanExec,
          s"Native Lance scan disabled because $LanceScanClassName.$NativeScanPlanMethod() " +
            "is not available")
        return None
    }

    CometLanceSupport.tryTransform(scanExec, nativePlan)
  }
}
