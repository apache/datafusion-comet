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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

import org.apache.comet.CometConf

class CometCsvReadSuite extends CometTestBase {

  test("native csv read") {
    withSQLConf(
      CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("c", IntegerType)

      val df = spark.read
        .options(Map("header" -> "false", "delimiter" -> ","))
        .schema(schema)
        .csv("/Users/tendoo/Desktop/datafusion-comet/spark/src/test/resources/test-data/csv-test-1.csv")
      df.explain(true)
      df.show(false)
    }
  }
}
