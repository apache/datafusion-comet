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

import scala.util.Random

import org.rogach.scallop.{ScallopConf, Subcommand}
import org.rogach.scallop.ScallopOption

import org.apache.spark.sql.SparkSession

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val generateData: generateData = new generateData
  class generateData extends Subcommand("data") {
    val numFiles: ScallopOption[Int] = opt[Int](required = true)
    val numRows: ScallopOption[Int] = opt[Int](required = true)
    val numColumns: ScallopOption[Int] = opt[Int](required = true)
  }
  val generateQueries: generateQueries = new generateQueries
  class generateQueries extends Subcommand("queries") {
    val numFiles: ScallopOption[Int] = opt[Int](required = false)
    val numQueries: ScallopOption[Int] = opt[Int](required = true)
  }
  val runQueries: runQueries = new runQueries
  class runQueries extends Subcommand("run") {
    val filename: ScallopOption[String] = opt[String](required = true)
    val numFiles: ScallopOption[Int] = opt[Int](required = false)
    val showMatchingResults: ScallopOption[Boolean] = opt[Boolean](required = false)
  }
  addSubcommand(generateData)
  addSubcommand(generateQueries)
  addSubcommand(runQueries)
  verify()
}

object Main {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val r = new Random(42)

    val conf = new Conf(args)
    conf.subcommand match {
      case Some(opt @ conf.generateData) =>
        DataGen.generateRandomFiles(
          r,
          spark,
          numFiles = opt.numFiles(),
          numRows = opt.numRows(),
          numColumns = opt.numColumns())
      case Some(opt @ conf.generateQueries) =>
        QueryGen.generateRandomQueries(r, spark, numFiles = opt.numFiles(), opt.numQueries())
      case Some(opt @ conf.runQueries) =>
        QueryRunner.runQueries(spark, opt.numFiles(), opt.filename(), opt.showMatchingResults())
      case _ =>
        // scalastyle:off println
        println("Invalid subcommand")
        // scalastyle:on println
        sys.exit(-1)
    }
  }
}
