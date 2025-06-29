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

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometMapExpressionSuite extends CometTestBase {

  test("read map[int, int] from parquet") {
    assume(usingDataSourceExec(conf))

    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Spark does not allow null as a key but does allow null as a
          // value, and the entire map be null
          .select(
            when(col("id") > 1, map(col("id"), when(col("id") > 2, col("id")))).alias("map1"))
        df.write.parquet(dir.toString())
      }

      Seq("", "parquet").foreach { v1List =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          val df = spark.read.parquet(dir.toString())
          if (v1List.isEmpty) {
            checkSparkAnswer(df.select("map1"))
          } else {
            checkSparkAnswerAndOperator(df.select("map1"))
          }
          // we fall back to Spark for map_keys and map_values
          checkSparkAnswer(df.select(map_keys(col("map1"))))
          checkSparkAnswer(df.select(map_values(col("map1"))))
        }
      }
    }
  }

  // repro for https://github.com/apache/datafusion-comet/issues/1754
  test("read map[struct, struct] from parquet") {
    assume(usingDataSourceExec(conf))

    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          .withColumn("id2", col("id"))
          .withColumn("id3", col("id"))
          // Spark does not allow null as a key but does allow null as a
          // value, and the entire map be null
          .select(
            when(
              col("id") > 1,
              map(
                struct(col("id"), col("id2"), col("id3")),
                when(col("id") > 2, struct(col("id"), col("id2"), col("id3"))))).alias("map1"))
        df.write.parquet(dir.toString())
      }

      Seq("", "parquet").foreach { v1List =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          val df = spark.read.parquet(dir.toString())
          df.createOrReplaceTempView("tbl")
          if (v1List.isEmpty) {
            checkSparkAnswer(df.select("map1"))
          } else {
            checkSparkAnswerAndOperator(df.select("map1"))
          }
          // we fall back to Spark for map_keys and map_values
          checkSparkAnswer(df.select(map_keys(col("map1"))))
          checkSparkAnswer(df.select(map_values(col("map1"))))
          checkSparkAnswer(spark.sql("SELECT map_keys(map1).id2 FROM tbl"))
          checkSparkAnswer(spark.sql("SELECT map_values(map1).id2 FROM tbl"))
        }
      }
    }
  }

  test("map_from_arrays") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val options = DataGenOptions(
          allowNull = false,
          generateNegativeZero = false,
          generateArray = true,
          generateStruct = false,
          generateMap = false)
        ParquetGenerator.makeParquetFile(random, spark, filename, 100, options)
      }
      spark.read.parquet(filename).createOrReplaceTempView("t1")
      val df = spark.sql("SELECT map_from_arrays(array(c12), array(c3)) FROM t1")
      checkSparkAnswerAndOperator(df)
    }
  }

}
