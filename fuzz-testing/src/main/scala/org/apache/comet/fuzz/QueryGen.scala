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

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

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
      try {
        val sql = r.nextInt().abs % 8 match {
          case 0 => generateJoin(r, spark, numFiles)
          case 1 => generateAggregate(r, spark, numFiles)
          case 2 => generateScalar(r, spark, numFiles)
          case 3 => generateCast(r, spark, numFiles)
          case 4 => generateUnaryArithmetic(r, spark, numFiles)
          case 5 => generateBinaryArithmetic(r, spark, numFiles)
          case 6 => generateBinaryComparison(r, spark, numFiles)
          case _ => generateConditional(r, spark, numFiles)
        }
        if (!uniqueQueries.contains(sql)) {
          uniqueQueries += sql
          w.write(sql + "\n")
        }
      } catch {
        case e: Exception =>
          // scalastyle:off
          println(s"Failed to generate query: ${e.getMessage}")
      }
    }
    w.close()
  }

  private def generateAggregate(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val func = Utils.randomChoice(Meta.aggFunc, r)
    try {
      val signature = Utils.randomChoice(func.signatures, r)
      val args = signature.inputTypes.map(x => pickRandomColumn(r, table, x))

      val groupingCols = Range(0, 2).map(_ => Utils.randomChoice(table.columns, r))

      if (groupingCols.isEmpty) {
        s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
          s"FROM $tableName " +
          s"ORDER BY ${args.mkString(", ")};"
      } else {
        s"SELECT ${groupingCols.mkString(", ")}, ${func.name}(${args.mkString(", ")}) " +
          s"FROM $tableName " +
          s"GROUP BY ${groupingCols.mkString(",")} " +
          s"ORDER BY ${groupingCols.mkString(", ")};"
      }
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to generate SQL for aggregate function ${func.name}",
          e)
    }
  }

  private def generateScalar(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val func = Utils.randomChoice(Meta.scalarFunc, r)
    try {
      val signature = Utils.randomChoice(func.signatures, r)
      val args =
        if (signature.varArgs) {
          pickRandomColumns(r, table, signature.inputTypes.head)
        } else {
          signature.inputTypes.map(x => pickRandomColumn(r, table, x))
        }

      // Example SELECT c0, log(c0) as x FROM test0
      s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
        s"FROM $tableName " +
        s"ORDER BY ${args.mkString(", ")};"
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to generate SQL for scalar function ${func.name}",
          e)
    }
  }

  @tailrec
  private def pickRandomColumns(r: Random, df: DataFrame, targetType: SparkType): Seq[String] = {
    targetType match {
      case SparkTypeOneOf(choices) =>
        val chosenType = Utils.randomChoice(choices, r)
        pickRandomColumns(r, df, chosenType)
      case _ =>
        var columns = Set.empty[String]
        for (_ <- 0 to r.nextInt(df.columns.length)) {
          columns += pickRandomColumn(r, df, targetType)
        }
        columns.toSeq
    }
  }

  private def pickRandomColumn(r: Random, df: DataFrame, targetType: SparkType): String = {
    targetType match {
      case SparkAnyType =>
        Utils.randomChoice(df.schema.fields, r).name
      case SparkBooleanType =>
        select(r, df, _.dataType == BooleanType)
      case SparkByteType =>
        select(r, df, _.dataType == ByteType)
      case SparkShortType =>
        select(r, df, _.dataType == ShortType)
      case SparkIntType =>
        select(r, df, _.dataType == IntegerType)
      case SparkLongType =>
        select(r, df, _.dataType == LongType)
      case SparkFloatType =>
        select(r, df, _.dataType == FloatType)
      case SparkDoubleType =>
        select(r, df, _.dataType == DoubleType)
      case SparkDecimalType(_, _) =>
        select(r, df, _.dataType.isInstanceOf[DecimalType])
      case SparkIntegralType =>
        select(
          r,
          df,
          f =>
            f.dataType == ByteType || f.dataType == ShortType ||
              f.dataType == IntegerType || f.dataType == LongType)
      case SparkNumericType =>
        select(r, df, f => isNumeric(f.dataType))
      case SparkStringType =>
        select(r, df, _.dataType == StringType)
      case SparkBinaryType =>
        select(r, df, _.dataType == BinaryType)
      case SparkDateType =>
        select(r, df, _.dataType == DateType)
      case SparkTimestampType =>
        select(r, df, _.dataType == TimestampType)
      case SparkDateOrTimestampType =>
        select(r, df, f => f.dataType == DateType || f.dataType == TimestampType)
      case SparkTypeOneOf(choices) =>
        pickRandomColumn(r, df, Utils.randomChoice(choices, r))
      case SparkArrayType(elementType) =>
        select(
          r,
          df,
          _.dataType match {
            case ArrayType(x, _) if typeMatch(elementType, x) => true
            case _ => false
          })
      case SparkMapType(keyType, valueType) =>
        select(
          r,
          df,
          _.dataType match {
            case MapType(k, v, _) if typeMatch(keyType, k) && typeMatch(valueType, v) => true
            case _ => false
          })
      case SparkStructType(fields) =>
        select(
          r,
          df,
          _.dataType match {
            case StructType(structFields) if structFields.length == fields.length => true
            case _ => false
          })
      case _ =>
        throw new IllegalStateException(targetType.toString)
    }
  }

  def pickTwoRandomColumns(r: Random, df: DataFrame, targetType: SparkType): (String, String) = {
    val a = pickRandomColumn(r, df, targetType)
    val df2 = df.drop(a)
    val b = pickRandomColumn(r, df2, targetType)
    (a, b)
  }

  /** Select a random field that matches a predicate */
  private def select(r: Random, df: DataFrame, predicate: StructField => Boolean): String = {
    val candidates = df.schema.fields.filter(predicate)
    if (candidates.isEmpty) {
      throw new IllegalStateException("Failed to find suitable column")
    }
    Utils.randomChoice(candidates, r).name
  }

  private def isNumeric(d: DataType): Boolean = {
    d match {
      case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
          _: DoubleType | _: DecimalType =>
        true
      case _ => false
    }
  }

  private def typeMatch(s: SparkType, d: DataType): Boolean = {
    (s, d) match {
      case (SparkAnyType, _) => true
      case (SparkBooleanType, BooleanType) => true
      case (SparkByteType, ByteType) => true
      case (SparkShortType, ShortType) => true
      case (SparkIntType, IntegerType) => true
      case (SparkLongType, LongType) => true
      case (SparkFloatType, FloatType) => true
      case (SparkDoubleType, DoubleType) => true
      case (SparkDecimalType(_, _), _: DecimalType) => true
      case (SparkIntegralType, ByteType | ShortType | IntegerType | LongType) => true
      case (SparkNumericType, _) if isNumeric(d) => true
      case (SparkStringType, StringType) => true
      case (SparkBinaryType, BinaryType) => true
      case (SparkDateType, DateType) => true
      case (SparkTimestampType, TimestampType | TimestampNTZType) => true
      case (SparkDateOrTimestampType, DateType | TimestampType | TimestampNTZType) => true
      case (SparkArrayType(elementType), ArrayType(elementDataType, _)) =>
        typeMatch(elementType, elementDataType)
      case (SparkMapType(keyType, valueType), MapType(keyDataType, valueDataType, _)) =>
        typeMatch(keyType, keyDataType) && typeMatch(valueType, valueDataType)
      case (SparkStructType(fields), StructType(structFields)) =>
        fields.length == structFields.length &&
        fields.zip(structFields.map(_.dataType)).forall { case (sparkType, dataType) =>
          typeMatch(sparkType, dataType)
        }
      case (SparkTypeOneOf(choices), _) =>
        choices.exists(choice => typeMatch(choice, d))
      case _ => false
    }
  }

  private def generateUnaryArithmetic(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.unaryArithmeticOps, r)
    val a = pickRandomColumn(r, table, SparkNumericType)

    // Example SELECT a, -a FROM test0
    s"SELECT $a, $op$a " +
      s"FROM $tableName " +
      s"ORDER BY $a;"
  }

  private def generateBinaryArithmetic(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.binaryArithmeticOps, r)
    val (a, b) = pickTwoRandomColumns(r, table, SparkNumericType)

    // Example SELECT a, b, a+b FROM test0
    s"SELECT $a, $b, $a $op $b " +
      s"FROM $tableName " +
      s"ORDER BY $a, $b;"
  }

  private def generateBinaryComparison(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.comparisonOps, r)

    // pick two columns with the same type
    val opType = Utils.randomChoice(Meta.comparisonTypes, r)
    val (a, b) = pickTwoRandomColumns(r, table, opType)

    // Example SELECT a, b, a <=> b FROM test0
    s"SELECT $a, $b, $a $op $b " +
      s"FROM $tableName " +
      s"ORDER BY $a, $b;"
  }

  private def generateConditional(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.comparisonOps, r)

    // pick two columns with the same type
    val opType = Utils.randomChoice(Meta.comparisonTypes, r)
    val (a, b) = pickTwoRandomColumns(r, table, opType)

    // Example SELECT a, b, IF(a <=> b, 1, 2), CASE WHEN a <=> b THEN 1 ELSE 2 END FROM test0
    s"SELECT $a, $b, $a $op $b, IF($a $op $b, 1, 2), CASE WHEN $a $op $b THEN 1 ELSE 2 END " +
      s"FROM $tableName " +
      s"ORDER BY $a, $b;"
  }

  private def generateCast(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val toType = Utils.randomWeightedChoice(Meta.dataTypes, r).sql
    val arg = Utils.randomChoice(table.columns, r)

    // We test both `cast` and `try_cast` to cover LEGACY and TRY eval modes. It is not
    // recommended to run Comet Fuzz with ANSI enabled currently.
    // Example SELECT c0, cast(c0 as float), try_cast(c0 as float) FROM test0
    s"SELECT $arg, cast($arg as $toType), try_cast($arg as $toType) " +
      s"FROM $tableName " +
      s"ORDER BY $arg;"
  }

  private def generateJoin(r: Random, spark: SparkSession, numFiles: Int): String = {
    val leftTableName = s"test${r.nextInt(numFiles)}"
    val rightTableName = s"test${r.nextInt(numFiles)}"
    val leftTable = spark.table(leftTableName)
    val rightTable = spark.table(rightTableName)

    val leftCol = Utils.randomChoice(leftTable.columns, r)
    val rightCol = Utils.randomChoice(rightTable.columns, r)

    val joinTypes = Seq(("INNER", 0.4), ("LEFT", 0.3), ("RIGHT", 0.3))
    val joinType = Utils.randomWeightedChoice(joinTypes, r)

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
