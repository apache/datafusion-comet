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

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.SparkSession

object QueryGen {

  def generateRandomQueries(
      r: Random,
      spark: SparkSession,
      numFiles: Int,
      numQueries: Int): Unit = {
    for (i <- 0 until numFiles) {
      spark.read.parquet(s"test$i.parquet").createTempView(s"test$i")
    }

    val w = new BufferedWriter(new FileWriter("queries.sql"))

    val uniqueQueries = mutable.HashSet[String]()

    for (_ <- 0 until numQueries) {
      val sql = r.nextInt().abs % 3 match {
        case 0 => generateJoin(r, spark, numFiles)
        case 1 => generateAggregate(r, spark, numFiles)
        case 2 => generateScalar(r, spark, numFiles)
      }
      if (!uniqueQueries.contains(sql)) {
        uniqueQueries += sql
        w.write(sql + "\n")
      }
    }
    w.close()
  }

  val scalarFunc: Seq[Function] = Seq(
    // string
    Function("substring", 3),
    Function("coalesce", 1),
    Function("starts_with", 2),
    Function("ends_with", 2),
    Function("contains", 2),
    Function("ascii", 1),
    Function("bit_length", 1),
    Function("octet_length", 1),
    Function("upper", 1),
    Function("lower", 1),
    Function("chr", 1),
    Function("init_cap", 1),
    Function("trim", 1),
    Function("ltrim", 1),
    Function("rtrim", 1),
    Function("btrim", 1),
    Function("concat_ws", 2),
    Function("repeat", 2),
    Function("length", 1),
    Function("reverse", 1),
    Function("in_str", 2),
    Function("replace", 2),
    Function("translate", 2),
    // date
    Function("year", 1),
    Function("hour", 1),
    Function("minute", 1),
    Function("second", 1),
    // math
    Function("abs", 1),
    Function("acos", 1),
    Function("asin", 1),
    Function("atan", 1),
    Function("Atan2", 1),
    Function("Cos", 1),
    Function("Exp", 2),
    Function("Ln", 1),
    Function("Log10", 1),
    Function("Log2", 1),
    Function("Pow", 2),
    Function("Round", 1),
    Function("Signum", 1),
    Function("Sin", 1),
    Function("Sqrt", 1),
    Function("Tan", 1),
    Function("Ceil", 1),
    Function("Floor", 1))

  val aggFunc: Seq[Function] = Seq(
    Function("min", 1),
    Function("max", 1),
    Function("count", 1),
    Function("avg", 1),
    Function("sum", 1),
    Function("first", 1),
    Function("last", 1),
    Function("var_pop", 1),
    Function("var_samp", 1),
    Function("covar_pop", 1),
    Function("covar_samp", 1),
    Function("stddev_pop", 1),
    Function("stddev_samp", 1),
    Function("corr", 2))

  private def generateAggregate(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val func = Utils.randomChoice(aggFunc, r)
    val args = Range(0, func.num_args)
      .map(_ => Utils.randomChoice(table.columns, r))

    val groupingCols = Range(0, 2).map(_ => Utils.randomChoice(table.columns, r))

    if (groupingCols.isEmpty) {
      s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
        s"FROM $tableName " +
        s"ORDER BY ${args.mkString(", ")}"
    } else {
      s"SELECT ${groupingCols.mkString(", ")}, ${func.name}(${args.mkString(", ")}) " +
        s"FROM $tableName " +
        s"GROUP BY ${groupingCols.mkString(",")} " +
        s"ORDER BY ${groupingCols.mkString(", ")}"
    }
  }

  private def generateScalar(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val func = Utils.randomChoice(scalarFunc, r)
    val args = Range(0, func.num_args)
      .map(_ => Utils.randomChoice(table.columns, r))

    s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
      s"FROM $tableName " +
      s"ORDER BY ${args.mkString(", ")}"
  }

  private def generateJoin(r: Random, spark: SparkSession, numFiles: Int): String = {
    val leftTableName = s"test${r.nextInt(numFiles)}"
    val rightTableName = s"test${r.nextInt(numFiles)}"
    val leftTable = spark.table(leftTableName)
    val rightTable = spark.table(rightTableName)

    val leftCol = Utils.randomChoice(leftTable.columns, r)
    val rightCol = Utils.randomChoice(rightTable.columns, r)

    val joinTypes = Seq(("INNER", 0.4), ("LEFT", 0.3), ("RIGHT", 0.3))
    val joinType = Utils.randomWeightedChoice(joinTypes)

    val leftColProjection = leftTable.columns.map(c => s"l.$c").mkString(", ")
    val rightColProjection = rightTable.columns.map(c => s"r.$c").mkString(", ")
    "SELECT " +
      s"$leftColProjection, " +
      s"$rightColProjection " +
      s"FROM $leftTableName l " +
      s"$joinType JOIN $rightTableName r " +
      s"ON l.$leftCol = r.$rightCol " +
      "ORDER BY " +
      s"$leftColProjection, " +
      s"$rightColProjection;"
  }

}

case class Function(name: String, num_args: Int)
