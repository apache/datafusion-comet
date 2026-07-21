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

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.spark.sql.CometTestBase

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

  // The repeated read of the same files is the part that matters here: it is the only
  // JVM-level coverage of a shared-cache hit across queries in one process, since the
  // registry outlives the task that populated it. The subsequent overwrite-and-read-again
  // step is a cheap guard on the whole scan path, but it does not test (size, last_modified)
  // invalidation: `write.mode("overwrite")` writes new file names, so the post-overwrite files
  // are distinct cache entries, not a stale hit on the pre-overwrite ones.
  test("reading a table after it is overwritten returns the new data") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      Seq(1, 2, 3).toDF("id").write.parquet(path)
      checkSparkAnswer(spark.read.parquet(path))
      checkSparkAnswer(spark.read.parquet(path))

      Seq(4, 5).toDF("id").write.mode("overwrite").parquet(path)
      checkSparkAnswer(spark.read.parquet(path))
    }
  }

  // Writes `numRows` rows whose pad column is `padWidth` characters, with dictionary encoding
  // off so that file size responds to the pad width. `extra` widens only the first row, which
  // gives finer control over the resulting file size than `padWidth` alone.
  private def writePaddedFile(dir: File, numRows: Int, padWidth: Int, extra: Int = 0): File = {
    (1 to numRows)
      .map(i => (i, "p" * (padWidth + (if (i == 1) extra else 0))))
      .toDF("id", "pad")
      .coalesce(1)
      .write
      .option("parquet.enable.dictionary", "false")
      .parquet(dir.getCanonicalPath)
    dir.listFiles().filter(_.getName.endsWith(".parquet")).head
  }

  // The shared cache outlives the task that populated it, so a file replaced in place must not
  // be read through the previous file's footer. Size alone does not catch this: the two files
  // here are byte-identical in length but hold different row counts, so the stale footer's row
  // group offsets do not describe the new file. Without the modification time reaching
  // `object_meta.last_modified`, this read fails outright with FAILED_READ_FILE.
  test("a same-size in-place replacement is not read through the stale footer") {
    withTempPath { root =>
      root.mkdirs()
      val target = new File(root, "target")
      target.mkdirs()

      val fileA = writePaddedFile(new File(root, "a"), numRows = 50, padWidth = 40)
      val sizeA = fileA.length()

      // Search for a 30-row file of exactly sizeA bytes. Encoding overhead means the size
      // change is not exactly the padding added, so iterate to a fixed point.
      var matched: Option[File] = None
      var padWidth = 20
      while (matched.isEmpty && padWidth < 60) {
        var extra =
          (sizeA - writePaddedFile(new File(root, s"b_${padWidth}_0"), 30, padWidth)
            .length()).toInt
        var attempt = 0
        while (matched.isEmpty && attempt < 12 && extra > 0 && extra < 6000) {
          val candidate =
            writePaddedFile(
              new File(root, s"b_${padWidth}_${attempt}_$extra"),
              30,
              padWidth,
              extra)
          val delta = (sizeA - candidate.length()).toInt
          if (delta == 0) {
            matched = Some(candidate)
          } else {
            extra += delta
          }
          attempt += 1
        }
        padWidth += 1
      }

      val fileB = matched.getOrElse(
        fail(s"could not construct a 30-row Parquet file of exactly $sizeA bytes"))

      val live = new File(target, "part-00000.parquet")
      Files.copy(fileA.toPath, live.toPath, StandardCopyOption.REPLACE_EXISTING)
      assert(spark.read.parquet(target.getCanonicalPath).count() == 50)

      Files.copy(fileB.toPath, live.toPath, StandardCopyOption.REPLACE_EXISTING)
      assert(live.length() == sizeA, "the replacement must not change the file size")

      // count(*) is answered from footer row counts, so a stale entry shows up here even
      // though no data page is touched.
      assert(spark.read.parquet(target.getCanonicalPath).count() == 30)
      assert(spark.read.parquet(target.getCanonicalPath).collect().length == 30)
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
