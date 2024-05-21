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

import org.apache.comet.CometConf

class CometTPCDSQuerySuite
    extends {
      // This is private in `TPCDSBase`.
      val excludedTpcdsQueries: Seq[String] = Seq()

      // This is private in `TPCDSBase` and `excludedTpcdsQueries` is private too.
      // So we cannot override `excludedTpcdsQueries` to exclude the queries.
      val tpcdsAllQueries: Seq[String] = Seq(
        "q16"
        // TODO: https://github.com/apache/datafusion-comet/issues/392
        //  comment out 39a and 39b for now because the expected result for stddev failed:
        //  expected: 1.5242630430075292, actual: 1.524263043007529.
        //  Will change the comparison logic to detect floating-point numbers and compare
        //  with epsilon
        // "q39a",
        // "q39b",
      )

      // TODO: enable the 3 queries after fixing the issues #1358.
      override val tpcdsQueries: Seq[String] =
        tpcdsAllQueries.filterNot(excludedTpcdsQueries.contains)
    }
    with TPCDSQueryTestSuite {
  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "20g")
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "20g")
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
