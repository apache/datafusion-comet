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

package org.apache.comet

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometProjectExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CometRegExpJvmSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.comet.exec.regexp.useJVM", "true")

  test("rlike: Java regex semantics, with flag on") {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      sql("INSERT INTO t VALUES ('abc123'), ('no digits'), (NULL), ('mixed_42_data')")
      val df = sql("SELECT s, s rlike '\\\\d+' AS m FROM t")
      val rows = df.collect().map { r =>
        val matched: Any = if (r.isNullAt(1)) null else r.getBoolean(1)
        (Option(r.getString(0)), matched)
      }
      assert(
        rows.toSet === Set(
          (Some("abc123"), true),
          (Some("no digits"), false),
          (None, null),
          (Some("mixed_42_data"), true)))
    }
  }

  test("rlike: plan contains CometProjectExec when flag is on") {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      sql("INSERT INTO t VALUES ('a1')")
      val df = sql("SELECT s rlike '[a-z]\\\\d' FROM t")
      df.collect()
      val cometProjects = collect(df.queryExecution.executedPlan) { case p: CometProjectExec =>
        p
      }
      assert(
        cometProjects.nonEmpty,
        s"Expected at least one CometProjectExec in:\n${df.queryExecution.executedPlan}")
    }
  }

  test("rlike: result matches Spark for arbitrary patterns") {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      sql("INSERT INTO t VALUES ('123'), ('abc'), ('mix3d')")
      checkSparkAnswer(sql("SELECT s, s rlike '\\\\d+' FROM t"))
    }
  }
}
