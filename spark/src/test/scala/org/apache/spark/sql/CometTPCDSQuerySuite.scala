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
      val tpcdsAllQueries: Seq[String] = Seq(
        "q1",
        "q2",
        "q3",
        "q4",
        "q5",
        "q6",
        "q7",
        "q8",
        "q9",
        "q10",
        "q11",
        "q12",
        "q13",
        "q14a",
        "q14b",
        "q15",
        "q16",
        "q17",
        "q18",
        "q19",
        "q20",
        "q21",
        "q22",
        "q23a",
        "q23b",
        "q24a",
        "q24b",
        "q25",
        "q26",
        "q27",
        "q28",
        "q29",
        "q30",
        "q31",
        "q32",
        "q33",
        "q34",
        "q35",
        "q36",
        "q37",
        "q38",
        // TODO: https://github.com/apache/datafusion-comet/issues/392
        //  comment out 39a and 39b for now because the expected result for stddev failed:
        //  expected: 1.5242630430075292, actual: 1.524263043007529.
        //  Will change the comparison logic to detect floating-point numbers and compare
        //  with epsilon
        // "q39a",
        // "q39b",
        "q40",
        "q41",
        "q42",
        "q43",
        "q44",
        "q45",
        "q46",
        "q47",
        "q48",
        "q49",
        "q50",
        "q51",
        "q52",
        "q53",
        "q54",
        "q55",
        "q56",
        "q57",
        "q58",
        "q59",
        "q60",
        "q61",
        "q62",
        "q63",
        "q64",
        "q65",
        "q66",
        "q67",
        "q68",
        "q69",
        "q70",
        "q71",
        "q72",
        "q73",
        "q74",
        "q75",
        "q76",
        "q77",
        "q78",
        "q79",
        "q80",
        "q81",
        "q82",
        "q83",
        "q84",
        "q85",
        "q86",
        "q87",
        "q88",
        "q89",
        "q90",
        "q91",
        "q92",
        "q93",
        "q94",
        "q95",
        "q96",
        "q97",
        "q98",
        "q99")

      val tpcdsAllQueriesV2_7_0: Seq[String] = Seq(
        "q5a",
        "q6",
        "q10a",
        "q11",
        "q12",
        "q14",
        "q14a",
        "q18a",
        "q20",
        "q22",
        "q22a",
        "q24",
        "q27a",
        "q34",
        "q35",
        "q35a",
        "q36a",
        "q47",
        "q49",
        "q51a",
        "q57",
        "q64",
        "q67a",
        "q70a",
        "q72",
        "q74",
        "q75",
        "q77a",
        "q78",
        "q80a",
        "q86a",
        "q98")

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
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "15g")
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
