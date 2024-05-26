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
            val sparkPlan = df.queryExecution.executedPlan.toString

            try {
              spark.conf.set("spark.comet.enabled", "true")
              val df = spark.sql(sql)
              val cometRows = df.collect()
              val cometPlan = df.queryExecution.executedPlan.toString

              if (sparkRows.length == cometRows.length) {
                var i = 0
                while (i < sparkRows.length) {
                  val l = sparkRows(i)
                  val r = cometRows(i)
                  assert(l.length == r.length)
                  for (j <- 0 until l.length) {
                    val same = (l(j), r(j)) match {
                      case (a: Float, b: Float) if a.isInfinity => b.isInfinity
                      case (a: Float, b: Float) if a.isNaN => b.isNaN
                      case (a: Float, b: Float) => (a - b).abs <= 0.000001f
                      case (a: Double, b: Double) if a.isInfinity => b.isInfinity
                      case (a: Double, b: Double) if a.isNaN => b.isNaN
                      case (a: Double, b: Double) => (a - b).abs <= 0.000001
                      case (a, b) => a == b
                    }
                    if (!same) {
                      w.write(s"## $sql\n\n")
                      showPlans(w, sparkPlan, cometPlan)
                      w.write(s"First difference at row $i:\n")
                      w.write("Spark: `" + l.mkString(",") + "`\n")
                      w.write("Comet: `" + r.mkString(",") + "`\n")
                      i = sparkRows.length
                    }
                  }
                  i += 1
                }
              } else {
                w.write(s"## $sql\n\n")
                showPlans(w, sparkPlan, cometPlan)
                w.write(
                  s"[ERROR] Spark produced ${sparkRows.length} rows and " +
                    s"Comet produced ${cometRows.length} rows.\n")
              }
            } catch {
              case e: Exception =>
                // the query worked in Spark but failed in Comet, so this is likely a bug in Comet
                w.write(s"## $sql\n\n")
                w.write(s"[ERROR] Query failed in Comet: ${e.getMessage}\n")
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

  private def showPlans(w: BufferedWriter, sparkPlan: String, cometPlan: String): Unit = {
    w.write("### Spark Plan\n")
    w.write(s"```\n$sparkPlan\n```\n")
    w.write("### Comet Plan\n")
    w.write(s"```\n$cometPlan\n```\n")
  }

}
