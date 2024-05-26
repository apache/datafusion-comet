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

import java.io.{BufferedWriter, FileWriter}

import scala.io.Source

import org.apache.spark.sql.SparkSession

object QueryRunner {

  def runQueries(
      spark: SparkSession,
      numFiles: Int,
      filename: String,
      showMatchingResults: Boolean,
      showFailedSparkQueries: Boolean = false): Unit = {

    val outputFilename = s"results-${System.currentTimeMillis()}.md"
    // scalastyle:off println
    println(s"Writing results to $outputFilename")
    // scalastyle:on println

    val w = new BufferedWriter(new FileWriter(outputFilename))

    // register input files
    for (i <- 0 until numFiles) {
      val table = spark.read.parquet(s"test$i.parquet")
      val tableName = s"test$i"
      table.createTempView(tableName)
      w.write(
        s"Created table $tableName with schema:\n\t" +
          s"${table.schema.fields.map(f => s"${f.name}: ${f.dataType}").mkString("\n\t")}\n\n")
    }

    val querySource = Source.fromFile(filename)
    try {
      querySource
        .getLines()
        .foreach(sql => {

          try {
            // execute with Spark
            spark.conf.set("spark.comet.enabled", "false")
            val df = spark.sql(sql)
            val sparkRows = df.collect()

            // TODO for now we sort the output to make this deterministic, but this means
            // that we are never testing Comet's sort for correctness
            val sparkRowsAsStrings = sparkRows.map(_.toString()).sorted
            val sparkResult = sparkRowsAsStrings.mkString("\n")

            val sparkPlan = df.queryExecution.executedPlan.toString

            w.write(s"## $sql\n\n")

            try {
              spark.conf.set("spark.comet.enabled", "true")
              val df = spark.sql(sql)
              val cometRows = df.collect()
              // TODO for now we sort the output to make this deterministic, but this means
              // that we are never testing Comet's sort for correctness
              val cometRowsAsStrings = cometRows.map(_.toString()).sorted
              val cometResult = cometRowsAsStrings.mkString("\n")
              val cometPlan = df.queryExecution.executedPlan.toString

              if (sparkResult == cometResult) {
                w.write(s"Spark and Comet produce the same results (${cometRows.length} rows).\n")
                if (showMatchingResults) {
                  w.write("### Spark Plan\n")
                  w.write(s"```\n$sparkPlan\n```\n")

                  w.write("### Comet Plan\n")
                  w.write(s"```\n$cometPlan\n```\n")

                  w.write("### Query Result\n")
                  w.write("```\n")
                  w.write(s"$cometResult\n")
                  w.write("```\n\n")
                }
              } else {
                w.write("[ERROR] Spark and Comet produced different results.\n")

                w.write("### Spark Plan\n")
                w.write(s"```\n$sparkPlan\n```\n")

                w.write("### Comet Plan\n")
                w.write(s"```\n$cometPlan\n```\n")

                w.write("### Results \n")

                w.write(
                  s"Spark produced ${sparkRows.length} rows and " +
                    s"Comet produced ${cometRows.length} rows.\n")

                if (sparkRows.length == cometRows.length) {
                  var i = 0
                  while (i < sparkRows.length) {
                    if (sparkRowsAsStrings(i) != cometRowsAsStrings(i)) {
                      w.write(s"First difference at row $i:\n")
                      w.write("Spark: `" + sparkRowsAsStrings(i) + "`\n")
                      w.write("Comet: `" + cometRowsAsStrings(i) + "`\n")
                      i = sparkRows.length
                    }
                    i += 1
                  }
                }
              }
            } catch {
              case e: Exception =>
                // the query worked in Spark but failed in Comet, so this is likely a bug in Comet
                w.write(s"Query failed in Comet: ${e.getMessage}\n")
            }

            // flush after every query so that results are saved in the event of the driver crashing
            w.flush()

          } catch {
            case e: Exception =>
              // we expect many generated queries to be invalid
              if (showFailedSparkQueries) {
                w.write(s"## $sql\n\n")
                w.write(s"Query failed in Spark: ${e.getMessage}\n")
              }
          }
        })

    } finally {
      w.close()
      querySource.close()
    }
  }

}
