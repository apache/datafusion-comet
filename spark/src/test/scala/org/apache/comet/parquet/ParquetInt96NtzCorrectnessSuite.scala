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

package org.apache.comet.parquet

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.SparkException
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Demonstrates the correctness issue tracked in
 * https://github.com/apache/datafusion-comet/issues/3720: when a Parquet file stores timestamps
 * as INT96 (Spark's TimestampType, UTC-adjusted local-time semantics) and the read schema
 * requests TimestampNTZ, the `native_datafusion` scan silently returns wall-clock values that
 * disagree with what was written. Spark itself raises (SPARK-36182) to prevent the silent
 * reinterpretation.
 */
class ParquetInt96NtzCorrectnessSuite extends CometTestBase {
  import testImplicits._

  test("INT96 TimestampType read as TimestampNTZ silently returns wrong values") {
    val sessionTz = "America/Los_Angeles"
    val written = "2020-01-01 12:00:00"

    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionTz,
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96",
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        // Write "2020-01-01 12:00:00" America/Los_Angeles as INT96. The bits encode
        // the UTC instant 2020-01-01 20:00:00; reading back as TimestampType applies
        // session-TZ adjustment to recover the original local wall-clock value.
        Seq(Timestamp.valueOf(written)).toDF("ts").write.parquet(path)

        // Reference behavior: Spark refuses to read INT96 as TimestampNTZ
        // (SPARK-36182) because it cannot safely reinterpret an LTZ instant as NTZ.
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          intercept[SparkException] {
            spark.read.schema("ts timestamp_ntz").parquet(path).collect()
          }
        }

        // native_datafusion does not refuse; it silently returns a value that
        // disagrees with the wall-clock value originally written. This is the
        // correctness issue the safety-check fallback is intended to prevent.
        withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION) {
          val rows = spark.read.schema("ts timestamp_ntz").parquet(path).collect()
          assert(rows.length == 1)
          val actual = rows.head.getAs[LocalDateTime](0)
          assert(
            actual != LocalDateTime.parse("2020-01-01T12:00:00"),
            s"native_datafusion returned the original wall-clock value $actual; " +
              s"expected a silently-shifted value demonstrating the LTZ->NTZ " +
              s"correctness divergence (issue #3720 / SPARK-36182).")
        }
      }
    }
  }
}
