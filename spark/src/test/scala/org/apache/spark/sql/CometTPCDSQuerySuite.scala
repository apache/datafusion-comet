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
      val excludedTpcdsQueries: Seq[String] =
        Seq("q66", "q71", "q88", "q90", "q96")

      // This is private in `TPCDSBase` and `excludedTpcdsQueries` is private too.
      // So we cannot override `excludedTpcdsQueries` to exclude the queries.
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
        "q39a",
        "q39b",
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

      // TODO: enable the 3 queries after fixing the issues #1358.
      override val tpcdsQueries: Seq[String] = Seq("q4")
      // tpcdsAllQueries.filterNot(excludedTpcdsQueries.contains)
      // Seq("q1", "q2", "q3", "q4")
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
    conf.set(CometConf.COMET_EXEC_ALL_EXPR_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "2g")
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
