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
import org.apache.spark.sql.internal.SQLConf

class CometDatetimeJvmSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(CometConf.COMET_DATETIME_ENGINE.key, CometConf.DATETIME_ENGINE_JAVA)

  private val crossTimezones = Seq("UTC", "America/Los_Angeles", "Asia/Tokyo")

  test("hour: TimestampNTZ produces Spark-compatible results in all session timezones") {
    withTable("t") {
      sql("CREATE TABLE t (ts_ntz TIMESTAMP_NTZ) USING parquet")
      sql("""INSERT INTO t VALUES
          | TIMESTAMP_NTZ'2024-06-15 12:34:56',
          | TIMESTAMP_NTZ'2024-01-01 00:00:00',
          | TIMESTAMP_NTZ'2024-12-31 23:59:59',
          | (NULL)""".stripMargin)
      for (tz <- crossTimezones) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
          checkSparkAnswerAndOperator("SELECT hour(ts_ntz) FROM t ORDER BY ts_ntz")
        }
      }
    }
  }

  test("hour: TimestampType produces Spark-compatible results in all session timezones") {
    withTable("t") {
      sql("CREATE TABLE t (ts TIMESTAMP) USING parquet")
      sql("""INSERT INTO t VALUES
          | TIMESTAMP'2024-06-15 12:34:56 UTC',
          | TIMESTAMP'2024-01-01 00:00:00 UTC',
          | TIMESTAMP'2024-12-31 23:59:59 UTC',
          | (NULL)""".stripMargin)
      for (tz <- crossTimezones) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
          checkSparkAnswerAndOperator("SELECT hour(ts) FROM t ORDER BY ts")
        }
      }
    }
  }

  test("hour: engine=java causes the plan to use JvmScalarUdf path") {
    withTable("t") {
      sql("CREATE TABLE t (ts_ntz TIMESTAMP_NTZ) USING parquet")
      sql("INSERT INTO t VALUES TIMESTAMP_NTZ'2024-06-15 12:34:56'")
      val df = sql("SELECT hour(ts_ntz) FROM t")
      checkSparkAnswerAndOperator(df)
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case p: CometProjectExec => p }.nonEmpty,
        s"Expected CometProjectExec (native execution via JVM UDF) in:\n$plan")
    }
  }

  test("minute: TimestampNTZ produces Spark-compatible results in all session timezones") {
    withTable("t") {
      sql("CREATE TABLE t (ts_ntz TIMESTAMP_NTZ) USING parquet")
      sql("""INSERT INTO t VALUES
          | TIMESTAMP_NTZ'2024-06-15 12:34:56',
          | TIMESTAMP_NTZ'2024-01-01 00:00:00',
          | TIMESTAMP_NTZ'2024-12-31 23:59:59',
          | (NULL)""".stripMargin)
      for (tz <- crossTimezones) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
          checkSparkAnswerAndOperator("SELECT minute(ts_ntz) FROM t ORDER BY ts_ntz")
        }
      }
    }
  }

  test("minute: TimestampType produces Spark-compatible results in all session timezones") {
    withTable("t") {
      sql("CREATE TABLE t (ts TIMESTAMP) USING parquet")
      sql("""INSERT INTO t VALUES
          | TIMESTAMP'2024-06-15 12:34:56 UTC',
          | (NULL)""".stripMargin)
      for (tz <- crossTimezones) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
          checkSparkAnswerAndOperator("SELECT minute(ts) FROM t ORDER BY ts")
        }
      }
    }
  }

}
