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

package org.apache.comet.csv

import org.apache.spark.sql.{CometTestBase, SaveMode}

import org.apache.comet.CometConf

class CometNativeCsvScanSuite extends CometTestBase {

  test("Native csv scan") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "false",
      CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "false",
      "spark.sql.sources.useV1SourceList" -> "") {
      spark.time {
        val df = spark.read
          .schema(Schemas.schema)
          .options(Map("header" -> "false", "delimiter" -> ","))
          .csv("src/test/resources/test-data/songs/")
        df.cache()
        df.count()
        df.explain(true)
      }
    }
    Thread.sleep(10000000)
  }
}
