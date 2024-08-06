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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.sql.comet.shims.ShimCometTPCDSQuerySuite

import org.apache.comet.CometConf

class CometTPCDSQuerySuite
    extends {
      val tpcdsAllQueries: Seq[String] = Seq("q97")

      val tpcdsAllQueriesV2_7_0: Seq[String] = Seq()

      override val tpcdsQueries: Seq[String] = tpcdsAllQueries

      override val tpcdsQueriesV2_7_0: Seq[String] = tpcdsAllQueriesV2_7_0
    }
    with CometTPCDSQueryTestSuite
    with ShimCometTPCDSQuerySuite {
  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_DEBUG_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "15g")
    conf.set(CometConf.COMET_SHUFFLE_ENFORCE_MODE_ENABLED.key, "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "15g")
    conf
  }

  override protected val baseResourcePath: String = {
    getWorkspaceFilePath(
      "spark",
      "src",
      "test",
      "resources",
      "tpcds-query-results").toFile.getAbsolutePath
  }
}
