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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.File

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.util.Utils

import org.apache.comet.CometConf

/**
 * Runs a native shuffle in a session whose `spark.local.dir` is a directory named `local.data`.
 * The writer used to build its temp path with `replace(".data", ".data.tmp")`, which also rewrote
 * that directory name, so the native writer opened a file under a directory that did not exist.
 * See https://github.com/apache/datafusion-comet/issues/7594.
 */
class CometNativeShuffleLocalDirSuite extends CometTestBase {

  import testImplicits._

  private lazy val localDirParent: File = Utils.createTempDir()

  private lazy val localDataDir: File = {
    val dir = new File(localDirParent, "local.data")
    assert(dir.mkdir(), s"Could not create $dir")
    dir
  }

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.local.dir", localDataDir.getAbsolutePath)
      .set(CometConf.COMET_SHUFFLE_MODE.key, "native")

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      Utils.deleteRecursively(localDirParent)
    }
  }

  test("native shuffle in a local dir named local.data") {
    // The repro only holds if Spark really put its block manager dirs under local.data. An
    // environment override such as SPARK_LOCAL_DIRS would move them elsewhere.
    val blockManagerDirs = SparkEnv.get.blockManager.getLocalDiskDirs
    assert(blockManagerDirs.nonEmpty)
    blockManagerDirs.foreach { dir =>
      assert(
        new File(dir).getParentFile.getCanonicalFile == localDataDir.getCanonicalFile,
        s"$dir is not under $localDataDir")
    }

    withParquetTable((0 until 100).map(i => (i, s"v$i")), "tbl") {
      val df = sql("SELECT * FROM tbl").repartition(10, $"_1")
      checkCometExchange(df, 1, native = true)
      checkAnswer(df, (0 until 100).map(i => Row(i, s"v$i")))
    }
  }
}
