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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.delta.DeltaLog

/**
 * Repro for Delta's own DeletionVectorsSuite expectation: DELETE on a DV-enabled table must WRITE
 * deletion vectors (not rewrite files) with Comet active. Mirrors "DELETE with DVs - on a table
 * with no prior DVs".
 */
class CometDeltaDmlReproSuite extends CometDeltaTestBase {

  test("DELETE writes DVs with useMetadataRowIndex=true (metadata row-index DML shape)") {
    withSQLConf(
      "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true",
      "spark.databricks.delta.deletionVectors.useMetadataRowIndex" -> "true") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark.range(0, 1000, 1, 500).write.format("delta").save(path)
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0 AND id < 200")

        val log = DeltaLog.forTable(spark, path)
        val withDvs = log.update().allFiles.collect().count(_.deletionVector != null)
        assert(withDvs == 100, s"expected 100 files with DVs, got $withDvs")
        assert(spark.read.format("delta").load(path).count() == 900)
      }
    }
  }

  test("DELETE writes DVs rather than rewriting files") {
    withSQLConf(
      "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true",
      "spark.databricks.delta.delete.deletionVectors.persistent" -> "true") {
      withTempDir { base =>
        // Mirror Delta's DeletionVectorsTestUtils: paths with spaces and a literal %2a.
        val dir = new java.io.File(base, "s p a r k %2a")
        val path = dir.getAbsolutePath
        spark.range(0, 1000, 1, 500).write.format("delta").save(path)
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0 AND id < 200")

        val log = DeltaLog.forTable(spark, path)
        val files = log.update().allFiles.collect()
        val withDvs = files.count(_.deletionVector != null)
        assert(files.length == 500, s"expected 500 files, got ${files.length}")
        assert(withDvs == 100, s"expected 100 files with DVs, got $withDvs")
        assert(spark.read.format("delta").load(path).count() == 900)
      }
    }
  }
}
