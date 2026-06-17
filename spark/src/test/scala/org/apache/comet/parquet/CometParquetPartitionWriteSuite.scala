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

import java.io.File

import org.apache.spark.sql.{CometTestBase, SaveMode}

import org.apache.comet.CometConf

class CometParquetPartitionWriteSuite extends CometTestBase {

  private val DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__"

  test("simple partition parquet write") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true") {
      withTempPath { dir =>
        val table = "test_write_partitions"
        val outputPath = new File(dir, "output.parquet").getAbsolutePath
        withTable(table) {
          sql(s"CREATE TABLE $table(col1 INT, col2 STRING) USING parquet")
          sql(s"INSERT INTO $table VALUES(1, 'a')")
          sql(s"INSERT INTO $table VALUES(2, 'b')")
          sql(s"INSERT INTO $table VALUES(3, 'c')")
          sql(s"SELECT * FROM $table").write
            .partitionBy("col1")
            .mode(SaveMode.Overwrite)
            .parquet(outputPath)
          Seq((1, "a"), (2, "b"), (3, "c")).foreach { case (col1, col2) =>
            val row = spark.read
              .parquet(s"$outputPath/col1=$col1")
              .collect()
              .headOption
            assert(row.isDefined)
            assert(row.get.getAs[String]("col2") == col2)
          }
        }
      }
    }
  }

  test("default hive partition parquet write") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true") {
      withTempPath { dir =>
        val table = "test_write_partitions"
        val outputPath = new File(dir, "output.parquet").getAbsolutePath
        withTable(table) {
          sql(s"CREATE TABLE $table(col1 INT, col2 STRING) USING parquet")
          sql(s"INSERT INTO $table VALUES(null, 'a')")
          sql(s"INSERT INTO $table VALUES(null, 'b')")
          sql(s"INSERT INTO $table VALUES(null, 'c')")
          sql(s"SELECT * FROM $table").write
            .partitionBy("col1")
            .mode(SaveMode.Overwrite)
            .parquet(outputPath)
          val rows = spark.read
            .parquet(s"$outputPath/col1=$DEFAULT_PARTITION_NAME")
            .collect()
          assert(rows.length == 3)
          assert(rows.map(_.getAs[String]("col2")).toSeq.sorted == Seq("a", "b", "c").sorted)
        }
      }
    }
  }
}
