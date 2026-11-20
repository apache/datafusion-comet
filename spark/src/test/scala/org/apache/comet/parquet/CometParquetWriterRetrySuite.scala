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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf

import org.apache.comet.hadoop.fs.RecordingFileSystem

/**
 * Native Parquet writes whose tasks Spark retries. A suite of its own because a task gets more
 * than one attempt only under a `local[N,M]` master, and the failure tests elsewhere expect a
 * failed task to fail its job.
 */
class CometParquetWriterRetrySuite extends CometParquetWriterTestBase {

  override protected def sparkConf: SparkConf =
    // local[N,M] gives each task M attempts. spark.task.maxFailures does not override this part
    // of a local master URL.
    super.sparkConf.setMaster("local[5,2]")

  test("a task retried after its file was created through Hadoop writes each row once") {
    withRecordingFileSystem(rows = 1000, partitions = 1) { (df, outputPath) =>
      // Only the first attempt's file fails, after the attempt created it.
      RecordingFileSystem.failWrites = true
      RecordingFileSystem.failingFiles.set(1)
      val plan = captureWritePlan(p => df.write.parquet(p), outputPath)
      assertHasCometNativeWriteExec(plan)

      val created = RecordingFileSystem.created.asScala.toSeq
      assert(created.size == 2, s"Expected a file for each attempt, got $created")
      assert(RecordingFileSystem.aborted.asScala.toSeq == created.take(1))
      assert(RecordingFileSystem.closed.asScala.toSeq == created.drop(1))
      assert(RecordingFileSystemCommitProtocol.abortedTasks.get == 1)

      checkAnswer(spark.read.parquet(outputPath), df)
      val parts = new File(outputPath).listFiles().filter(_.getName.startsWith("part-"))
      assert(parts.length == 1, parts.mkString(", "))
      assert(!new File(outputPath, "_temporary").exists())
    }
  }
}
