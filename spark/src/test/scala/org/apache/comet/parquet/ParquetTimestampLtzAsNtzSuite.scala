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

import org.apache.spark.SparkException
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

/**
 * Tests for reading Parquet TimestampLTZ columns as TimestampNTZ.
 *
 * Prior to Spark 4.0, Spark raises an error (SPARK-36182) when asked to read TimestampLTZ as
 * TimestampNTZ. Comet should match this behavior. In Spark 4.0+, this read is permitted
 * (SPARK-47447) and Comet should produce matching results.
 *
 * See https://github.com/apache/datafusion-comet/issues/4219
 */
class ParquetTimestampLtzAsNtzSuite extends CometTestBase {
  import testImplicits._

  private val tsTypes = Seq("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS")

  private val scanImpls =
    Seq(CometConf.SCAN_NATIVE_ICEBERG_COMPAT, CometConf.SCAN_NATIVE_DATAFUSION)

  for {
    tsType <- tsTypes
    scanImpl <- scanImpls
  } {
    test(s"read TimestampLTZ ($tsType) as TimestampNTZ throws pre-Spark 4 ($scanImpl)") {
      assume(!isSpark40Plus, "Spark 4.0+ allows reading TimestampLTZ as TimestampNTZ")
      // INT96 cannot be detected on the native_datafusion path: DataFusion's coerce_int96
      // strips the timezone, so by the time Comet's schema adapter runs, an INT96 column is
      // indistinguishable from a TIMESTAMP_NTZ_MICROS column. Tracked separately under #4219.
      assume(
        !(tsType == "INT96" && scanImpl == CometConf.SCAN_NATIVE_DATAFUSION),
        "https://github.com/apache/datafusion-comet/issues/4219 (INT96 + native_datafusion)")

      val sessionTz = "America/Los_Angeles"

      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionTz,
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> tsType,
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl) {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          Seq(Timestamp.valueOf("2020-01-01 12:00:00")).toDF("ts").write.parquet(path)

          // Spark refuses to read TimestampLTZ as TimestampNTZ (SPARK-36182)
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            intercept[SparkException] {
              spark.read.schema("ts timestamp_ntz").parquet(path).collect()
            }
          }

          // Comet should also refuse
          intercept[SparkException] {
            spark.read.schema("ts timestamp_ntz").parquet(path).collect()
          }
        }
      }
    }
  }

  tsTypes.foreach { tsType =>
    test(s"read TimestampLTZ ($tsType) as TimestampNTZ matches Spark") {
      assume(isSpark40Plus, "Spark 4.0+ allows reading TimestampLTZ as TimestampNTZ")
      val sessionTz = "America/Los_Angeles"

      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionTz,
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> tsType,
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          Seq(Timestamp.valueOf("2020-01-01 12:00:00")).toDF("ts").write.parquet(path)

          withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION) {
            checkSparkAnswerAndOperator(spark.read.schema("ts timestamp_ntz").parquet(path))
          }
        }
      }
    }
  }
}
