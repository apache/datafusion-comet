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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object CometWriteTestHelpers {

  def hasTemporaryFolder(basePath: String)(implicit spark: SparkSession): Boolean = {
    try {
      val fs = new Path(basePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.exists(new Path(basePath, "_temporary"))
    } catch {
      case _: Exception => false
    }
  }

  def getTemporarySubfolders(basePath: String)(implicit spark: SparkSession): Seq[String] = {
    try {
      val fs = new Path(basePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val tempPath = new Path(basePath, "_temporary")
      if (!fs.exists(tempPath)) return Seq.empty

      fs.listStatus(tempPath).map(_.getPath.getName).toSeq
    } catch {
      case _: Exception => Seq.empty
    }
  }

  def countTemporaryFiles(basePath: String)(implicit spark: SparkSession): Int = {
    try {
      val fs = new Path(basePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val tempPath = new Path(basePath, "_temporary")
      if (!fs.exists(tempPath)) return 0

      def countRecursive(path: Path): Int = {
        val status = fs.listStatus(path)
        status.map { fileStatus =>
          if (fileStatus.isDirectory) {
            countRecursive(fileStatus.getPath)
          } else {
            1
          }
        }.sum
      }

      countRecursive(tempPath)
    } catch {
      case _: Exception => 0
    }
  }

  def waitForCondition(
      condition: => Boolean,
      timeoutMs: Long = 5000,
      intervalMs: Long = 100): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
      if (condition) return true
      Thread.sleep(intervalMs)
    }
    false
  }

  def listFiles(basePath: String)(implicit spark: SparkSession): Seq[String] = {
    try {
      val fs = new Path(basePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val path = new Path(basePath)
      if (!fs.exists(path)) return Seq.empty

      fs.listStatus(path)
        .filter(_.isFile)
        .map(_.getPath.getName)
        .toSeq
    } catch {
      case _: Exception => Seq.empty
    }
  }

  def listDirectories(basePath: String)(implicit spark: SparkSession): Seq[String] = {
    try {
      val fs = new Path(basePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val path = new Path(basePath)
      if (!fs.exists(path)) return Seq.empty

      fs.listStatus(path)
        .filter(_.isDirectory)
        .map(_.getPath.getName)
        .toSeq
    } catch {
      case _: Exception => Seq.empty
    }
  }
}
