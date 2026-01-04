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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.SparkPlan

import io.delta.tables.DeltaTable

/**
 * Test suite for native Delta scan using built-in native Parquet reader for simple reads
 */
class CometDeltaNativeSuite extends CometTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set(CometConf.COMET_NATIVE_SCAN_IMPL.key, "native_datafusion")
  }

  /** Collects all ComeNativeScanExec nodes from a plan */
  private def collectNativeScans(plan: SparkPlan): Seq[CometNativeScanExec] = {
    collect(plan) { case scan: CometNativeScanExec =>
      scan
    }
  }

  /**
   * Helper to verify query correctness and that exactly one CometNativeScanExec is used. This
   * ensures both correct results and that the native scan operator is being used.
   */
  private def checkNativeScan(query: String, native: Boolean): Unit = {
    val (_, plan) = checkSparkAnswer(query)
    val nativeScans = collectNativeScans(plan)

    if (native) {
      assert(
        nativeScans.length == 1,
        s"Expected exactly 1 CometNativeScanExec but found ${nativeScans.length}. Plan:\n$plan")
    } else {
      assert(
        nativeScans.length == 0,
        s"Expected no CometNativeScanExec but found ${nativeScans.length}. Plan:\n$plan")
    }
  }

  test("create and query simple Delta table") {
    import testImplicits._

    withTempDir { dir =>
      DeltaTable
        .create(spark)
        .tableName("test_table")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .addColumn("value", "DOUBLE")
        .location(dir.getAbsolutePath)
        .execute()

      Seq((1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.7))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .save(dir.getAbsolutePath)

      checkNativeScan("SELECT * FROM test_table ORDER BY id", true)

      spark.sql("DROP TABLE test_table")
    }
  }

  test("supported reader features") {
    import testImplicits._

    withTempDir { dir =>
      DeltaTable
        .create(spark)
        .tableName("test_table")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .addColumn("value", "DOUBLE")
        .property("delta.checkpointPolicy", "v2")
        .location(dir.getAbsolutePath)
        .execute()

      Seq((1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.7))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .save(dir.getAbsolutePath)

      checkNativeScan("SELECT * FROM test_table ORDER BY id", true)

      spark.sql("DROP TABLE test_table")
    }
  }

  test("deletion vectors not supported") {
    import testImplicits._

    withTempDir { dir =>
      DeltaTable
        .create(spark)
        .tableName("test_table")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .addColumn("value", "DOUBLE")
        .property("delta.enableDeletionVectors", "true")
        .location(dir.getAbsolutePath)
        .execute()

      Seq((1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.7))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .save(dir.getAbsolutePath)

      checkNativeScan("SELECT * FROM test_table ORDER BY id", false)

      spark.sql("DROP TABLE test_table")
    }
  }

  test("column mapping not supported") {
    import testImplicits._

    withTempDir { dir =>
      // Creating a table just with column mapping results in reader version 2
      val table = DeltaTable
        .create(spark)
        .tableName("test_table")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .addColumn("value", "DOUBLE")
        .property("delta.columnMapping.mode", "name")
        .location(dir.getAbsolutePath)
        .execute()

      assert(table.detail().select("minReaderVersion").first().getInt(0) == 2)

      Seq((1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.7))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .save(dir.getAbsolutePath)

      checkNativeScan("SELECT * FROM test_table ORDER BY id", false)

      // Now add a new feature that requires enable reader features
      table.addFeatureSupport("v2Checkpoint")

      assert(table.detail().select("minReaderVersion").first().getInt(0) == 3)

      checkNativeScan("SELECT * FROM test_table ORDER BY id", false)

      spark.sql("DROP TABLE test_table")
    }
  }

  test("complex Delta table") {

    withTempDir { dir =>
      val table = DeltaTable
        .create(spark)
        .tableName("test_table")
        .addColumn("nested", "struct<id: INT, value: DOUBLE>")
        .addColumn("arr", "array<struct<id: INT, value: DOUBLE>>")
        .location(dir.getAbsolutePath)
        .execute()

      spark
        .createDataFrame(
          List(
            Row((1, 10.5), Seq((1, 10.5), (2, 15.0))),
            Row((2, 20.3), Seq((2, 20.3), (3, 25.5)))).asJava,
          table.toDF.schema)
        .write
        .format("delta")
        .mode("append")
        .save(dir.getAbsolutePath)

      checkNativeScan("SELECT * FROM test_table ORDER BY nested.id", true)

      spark.sql("DROP TABLE test_table")
    }
  }
}
