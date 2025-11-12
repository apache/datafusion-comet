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

package org.apache.comet.fuzz

import java.io.File

import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import org.apache.spark.sql.{functions, SparkSession}

class ComparisonToolConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  object compareParquet extends Subcommand("compareParquet") {
    val inputSparkFolder: ScallopOption[String] =
      opt[String](required = true, descr = "Folder with Spark produced results in Parquet format")
    val inputCometFolder: ScallopOption[String] =
      opt[String](required = true, descr = "Folder with Comet produced results in Parquet format")
    val tolerance: ScallopOption[Double] =
      opt[Double](default = Some(0.000002), descr = "Tolerance for floating point comparisons")
  }
  addSubcommand(compareParquet)
  verify()
}

object ComparisonTool {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val conf = new ComparisonToolConf(args.toIndexedSeq)
    conf.subcommand match {
      case Some(conf.compareParquet) =>
        compareParquetFolders(
          spark,
          conf.compareParquet.inputSparkFolder(),
          conf.compareParquet.inputCometFolder(),
          conf.compareParquet.tolerance())

      case _ =>
        // scalastyle:off println
        println("Invalid subcommand")
        // scalastyle:on println
        sys.exit(-1)
    }
  }

  private def compareParquetFolders(
      spark: SparkSession,
      sparkFolderPath: String,
      cometFolderPath: String,
      tolerance: Double): Unit = {

    val output = QueryRunner.createOutputMdFile()

    try {
      val sparkFolder = new File(sparkFolderPath)
      val cometFolder = new File(cometFolderPath)

      if (!sparkFolder.exists() || !sparkFolder.isDirectory) {
        throw new IllegalArgumentException(
          s"Spark folder does not exist or is not a directory: $sparkFolderPath")
      }

      if (!cometFolder.exists() || !cometFolder.isDirectory) {
        throw new IllegalArgumentException(
          s"Comet folder does not exist or is not a directory: $cometFolderPath")
      }

      // Get all subdirectories from the Spark folder
      val sparkSubfolders = sparkFolder
        .listFiles()
        .filter(_.isDirectory)
        .map(_.getName)
        .sorted

      output.write("# Comparing Parquet Folders\n\n")
      output.write(s"Spark folder: $sparkFolderPath\n")
      output.write(s"Comet folder: $cometFolderPath\n")
      output.write(s"Found ${sparkSubfolders.length} subfolders to compare\n\n")

      // Compare each subfolder
      sparkSubfolders.foreach { subfolderName =>
        val sparkSubfolderPath = new File(sparkFolder, subfolderName)
        val cometSubfolderPath = new File(cometFolder, subfolderName)

        if (!cometSubfolderPath.exists() || !cometSubfolderPath.isDirectory) {
          output.write(s"## Subfolder: $subfolderName\n")
          output.write(
            s"[WARNING] Comet subfolder not found: ${cometSubfolderPath.getAbsolutePath}\n\n")
        } else {
          output.write(s"## Comparing subfolder: $subfolderName\n\n")

          try {
            // Read Spark parquet files
            spark.conf.set("spark.comet.enabled", "false")
            val sparkDf = spark.read.parquet(sparkSubfolderPath.getAbsolutePath)
            val sparkRows = sparkDf.orderBy(sparkDf.columns.map(functions.col): _*).collect()

            // Read Comet parquet files
            val cometDf = spark.read.parquet(cometSubfolderPath.getAbsolutePath)
            val cometRows = cometDf.orderBy(cometDf.columns.map(functions.col): _*).collect()

            // Compare the results
            if (QueryComparison.assertSameRows(sparkRows, cometRows, output, tolerance)) {
              output.write(s"Subfolder $subfolderName: ${sparkRows.length} rows matched\n\n")
            } else {
              // Output schema if dataframes are not equal
              QueryComparison.showSchema(
                output,
                sparkDf.schema.treeString,
                cometDf.schema.treeString)
            }
          } catch {
            case e: Exception =>
              output.write(
                s"[ERROR] Failed to compare subfolder $subfolderName: ${e.getMessage}\n")
              val sw = new java.io.StringWriter()
              val p = new java.io.PrintWriter(sw)
              e.printStackTrace(p)
              p.close()
              output.write(s"```\n${sw.toString}\n```\n\n")
          }
        }

        output.flush()
      }

      output.write("\n# Comparison Complete\n")
      output.write(s"Compared ${sparkSubfolders.length} subfolders\n")

    } finally {
      output.close()
    }
  }
}
