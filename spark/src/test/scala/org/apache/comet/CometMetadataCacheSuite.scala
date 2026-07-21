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

import org.apache.spark.sql.{CometTestBase, Row}

import org.apache.comet.serde.Config.ConfigMap

class CometMetadataCacheSuite extends CometTestBase {
  import testImplicits._

  test("metadata cache configs cross JNI with their default values") {
    val confs = ConfigMap
      .parseFrom(CometExecIterator.serializeCometSQLConfs())
      .getEntriesMap

    assert(confs.get(CometConf.COMET_METADATA_CACHE_ENABLED.key) == "true")
    assert(confs.get(CometConf.COMET_METADATA_CACHE_MEMORY_LIMIT.key) == "52428800")
  }

  // This exercises the shared-cache scan path end to end across an overwrite of the same
  // path within a single session: it fails if a stale cached footer or page index makes the
  // second read return the first read's data (or blow up on the first read's row group
  // layout). It does not, on its own, prove that (size, last_modified) validation is what
  // catches the change, since the row count change below also changes the file's size, and a
  // size change alone would invalidate a stale entry too.
  test("reading a table after it is overwritten returns the new data") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).toDF("id").write.parquet(path)
      checkAnswer(spark.read.parquet(path), Seq(Row(1), Row(2), Row(3)))

      Seq(4, 5).toDF("id").write.mode("overwrite").parquet(path)
      checkAnswer(spark.read.parquet(path), Seq(Row(4), Row(5)))
    }
  }

  // This is the only coverage of the per-task cache fallback in parquet_exec.rs, taken when
  // sharing is disabled.
  test("scan returns correct results with metadata cache sharing disabled") {
    withSQLConf(CometConf.COMET_METADATA_CACHE_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value").write.parquet(path)
        checkSparkAnswer(spark.read.parquet(path))
      }
    }
  }
}
