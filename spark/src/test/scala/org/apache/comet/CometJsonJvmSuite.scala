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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CometJsonJvmSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  // No per-expression `allowIncompatible` is set, so all JSON expressions run through the
  // codegen dispatcher (Spark's own code) rather than the native rust path.
  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  private val rows = Seq(
    """{"a":1,"b":"x","arr":[1,2,3]}""",
    """{"a":2,"b":"y","arr":[]}""",
    """{"a":null,"b":"z","arr":[10]}""",
    null,
    """not json""")

  private def withJsonTable(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (j STRING) USING parquet")
      val sqlRows = rows
        .map(v => if (v == null) "(NULL)" else s"('${v.replace("'", "''")}')")
        .mkString(", ")
      sql(s"INSERT INTO t VALUES $sqlRows")
      f
    }
  }

  test("get_json_object via JVM engine") {
    withJsonTable {
      checkSparkAnswerAndOperator(sql("SELECT get_json_object(j, '$.a') FROM t"))
      checkSparkAnswerAndOperator(sql("SELECT get_json_object(j, '$.b') FROM t"))
    }
  }

  test("from_json with explicit schema via JVM engine") {
    withJsonTable {
      checkSparkAnswerAndOperator(sql("SELECT from_json(j, 'a INT, b STRING') FROM t"))
    }
  }

  test("to_json round-trip via JVM engine") {
    withJsonTable {
      checkSparkAnswerAndOperator(sql("SELECT to_json(from_json(j, 'a INT, b STRING')) FROM t"))
    }
  }
}
