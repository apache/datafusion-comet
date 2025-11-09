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

import java.io.{BufferedWriter, FileWriter, PrintWriter, StringWriter}

import scala.collection.mutable
import scala.io.Source

import org.apache.spark.sql.{Row, SparkSession}

import org.apache.comet.fuzz.QueryComparison.showPlans

object QueryRunner {

  def createOutputMdFile(): BufferedWriter = {
    val outputFilename = s"results-${System.currentTimeMillis()}.md"
    // scalastyle:off println
    println(s"Writing results to $outputFilename")
    // scalastyle:on println

    new BufferedWriter(new FileWriter(outputFilename))
  }

  def runQueries(
      spark: SparkSession,
      numFiles: Int,
      filename: String,
      showFailedSparkQueries: Boolean = false): Unit = {

    var queryCount = 0
    var invalidQueryCount = 0
    var cometFailureCount = 0
    var cometSuccessCount = 0

    val w = createOutputMdFile()

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
          queryCount += 1
          try {
            // execute with Spark
            spark.conf.set("spark.comet.enabled", "false")
            val df = spark.sql(sql)
            val sparkRows = df.collect()
            val sparkPlan = df.queryExecution.executedPlan.toString

            // execute with Comet
            try {
              spark.conf.set("spark.comet.enabled", "true")
              val df = spark.sql(sql)
              val cometRows = df.collect()
              val cometPlan = df.queryExecution.executedPlan.toString

              var success = QueryComparison.assertSameRows(sparkRows, cometRows, output = w)

              // check that the plan contains Comet operators
              if (!cometPlan.contains("Comet")) {
                success = false
                w.write("[ERROR] Comet did not accelerate any part of the plan\n")
              }

              QueryComparison.showSQL(w, sql)

              if (success) {
                cometSuccessCount += 1
              } else {
                // show plans for failed queries
                showPlans(w, sparkPlan, cometPlan)
                cometFailureCount += 1
              }

            } catch {
              case e: Exception =>
                // the query worked in Spark but failed in Comet, so this is likely a bug in Comet
                cometFailureCount += 1
                QueryComparison.showSQL(w, sql)
                w.write("### Spark Plan\n")
                w.write(s"```\n$sparkPlan\n```\n")

                w.write(s"[ERROR] Query failed in Comet: ${e.getMessage}:\n")
                w.write("```\n")
                val sw = new StringWriter()
                val p = new PrintWriter(sw)
                e.printStackTrace(p)
                p.close()
                w.write(s"${sw.toString}\n")
                w.write("```\n")
            }

            // flush after every query so that results are saved in the event of the driver crashing
            w.flush()

          } catch {
            case e: Exception =>
              // we expect many generated queries to be invalid
              invalidQueryCount += 1
              if (showFailedSparkQueries) {
                QueryComparison.showSQL(w, sql)
                w.write(s"Query failed in Spark: ${e.getMessage}\n")
              }
          }
        })

      w.write("# Summary\n")
      w.write(
        s"Total queries: $queryCount; Invalid queries: $invalidQueryCount; " +
          s"Comet failed: $cometFailureCount; Comet succeeded: $cometSuccessCount\n")

    } finally {
      w.close()
      querySource.close()
    }
  }
}

object QueryComparison {
  def assertSameRows(
      sparkRows: Array[Row],
      cometRows: Array[Row],
      output: BufferedWriter,
      tolerance: Double = 0.000001): Boolean = {
    if (sparkRows.length == cometRows.length) {
      var i = 0
      while (i < sparkRows.length) {
        val l = sparkRows(i)
        val r = cometRows(i)

        // Check the schema is equal for first row only
        if (i == 0 && l.schema != r.schema) {
          output.write("[ERROR] Spark produced different schema than Comet.\n")

          return false
        }

        assert(l.length == r.length)
        for (j <- 0 until l.length) {
          if (!same(l(j), r(j), tolerance)) {
            output.write(s"First difference at row $i:\n")
            output.write("Spark: `" + formatRow(l) + "`\n")
            output.write("Comet: `" + formatRow(r) + "`\n")
            i = sparkRows.length

            return false
          }
        }
        i += 1
      }
    } else {
      output.write(
        s"[ERROR] Spark produced ${sparkRows.length} rows and " +
          s"Comet produced ${cometRows.length} rows.\n")

      return false
    }

    true
  }

  private def same(l: Any, r: Any, tolerance: Double): Boolean = {
    if (l == null || r == null) {
      return l == null && r == null
    }
    (l, r) match {
      case (a: Float, b: Float) if a.isPosInfinity => b.isPosInfinity
      case (a: Float, b: Float) if a.isNegInfinity => b.isNegInfinity
      case (a: Float, b: Float) if a.isInfinity => b.isInfinity
      case (a: Float, b: Float) if a.isNaN => b.isNaN
      case (a: Float, b: Float) => (a - b).abs <= tolerance
      case (a: Double, b: Double) if a.isPosInfinity => b.isPosInfinity
      case (a: Double, b: Double) if a.isNegInfinity => b.isNegInfinity
      case (a: Double, b: Double) if a.isInfinity => b.isInfinity
      case (a: Double, b: Double) if a.isNaN => b.isNaN
      case (a: Double, b: Double) => (a - b).abs <= tolerance
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall(x => same(x._1, x._2, tolerance))
      case (a: mutable.WrappedArray[_], b: mutable.WrappedArray[_]) =>
        a.length == b.length && a.zip(b).forall(x => same(x._1, x._2, tolerance))
      case (a: Row, b: Row) =>
        val aa = a.toSeq
        val bb = b.toSeq
        aa.length == bb.length && aa.zip(bb).forall(x => same(x._1, x._2, tolerance))
      case (a, b) => a == b
    }
  }

  private def format(value: Any): String = {
    value match {
      case null => "NULL"
      case v: mutable.WrappedArray[_] => s"[${v.map(format).mkString(",")}]"
      case v: Array[Byte] => s"[${v.mkString(",")}]"
      case r: Row => formatRow(r)
      case other => other.toString
    }
  }

  private def formatRow(row: Row): String = {
    row.toSeq.map(format).mkString(",")
  }

  def showSQL(w: BufferedWriter, sql: String, maxLength: Int = 120): Unit = {
    w.write("## SQL\n")
    w.write("```\n")
    val words = sql.split(" ")
    val currentLine = new StringBuilder
    for (word <- words) {
      if (currentLine.length + word.length + 1 > maxLength) {
        w.write(currentLine.toString.trim)
        w.write("\n")
        currentLine.setLength(0)
      }
      currentLine.append(word).append(" ")
    }
    if (currentLine.nonEmpty) {
      w.write(currentLine.toString.trim)
      w.write("\n")
    }
    w.write("```\n")
  }

  def showPlans(w: BufferedWriter, sparkPlan: String, cometPlan: String): Unit = {
    w.write("### Spark Plan\n")
    w.write(s"```\n$sparkPlan\n```\n")
    w.write("### Comet Plan\n")
    w.write(s"```\n$cometPlan\n```\n")
  }

  def showSchema(w: BufferedWriter, sparkSchema: String, cometSchema: String): Unit = {
    w.write("### Spark Schema\n")
    w.write(s"```\n$sparkSchema\n```\n")
    w.write("### Comet Schema\n")
    w.write(s"```\n$cometSchema\n```\n")
  }
}
