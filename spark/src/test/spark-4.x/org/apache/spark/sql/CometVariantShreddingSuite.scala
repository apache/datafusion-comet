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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/** Run Spark's unchanged reconstruction assertions in Comet's regular Spark 4 CI jobs. */
class CometVariantShreddingSuite extends VariantShreddingSuite {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    .set(CometConf.COMET_ENABLED.key, "true")
    .set(CometConf.COMET_EXEC_ENABLED.key, "true")
    .set(CometConf.COMET_ONHEAP_ENABLED.key, "true")
    .set(CometConf.COMET_SHUFFLE_ENABLED.key, "false")

  override protected def createSparkSession: TestSparkSession = {
    val session = super.createSparkSession
    new CometSparkSessionExtensions().apply(session.extensions)
    session
  }

  override def checkExpr(path: File, expr: String, expected: Any*): Unit = {
    super.checkExpr(path, expr, expected: _*)
    if (expr == "v" && !isPushEnabled) {
      val plan = read(path).queryExecution.executedPlan
      assert(plan.collect { case scan: CometNativeScanExec => scan }.nonEmpty, plan.toString)
    }
  }
}
