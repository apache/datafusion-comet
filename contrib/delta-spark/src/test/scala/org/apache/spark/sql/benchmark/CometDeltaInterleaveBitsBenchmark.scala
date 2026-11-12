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

package org.apache.spark.sql.benchmark

import org.apache.spark.sql.delta.expressions.InterleaveBits
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/** Compares Delta's two JVM implementations with Comet over the same Parquet input. */
object CometDeltaInterleaveBitsBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    spark.sessionState.functionRegistry.createOrReplaceTempFunction(
      "delta_interleave_bits",
      children => InterleaveBits(children),
      "internal")
    runBenchmarkWithTable("Delta InterleaveBits", 1024 * 1024) { rows =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"SELECT cast(value as int) AS x, " +
              s"IF(value % 5 = 0, cast(NULL as int), cast(shiftright(value, 32) as int)) AS y FROM $tbl"))
          Seq(true, false).foreach { fast =>
            withSQLConf(DeltaSQLConf.FAST_INTERLEAVE_BITS_ENABLED.key -> fast.toString) {
              Seq("x", "x, y").foreach { inputs =>
                runExpressionBenchmark(
                  s"InterleaveBits($inputs), Delta fast=$fast",
                  rows.toLong,
                  s"SELECT delta_interleave_bits($inputs) FROM parquetV1Table")
              }
            }
          }
        }
      }
    }
  }
}
