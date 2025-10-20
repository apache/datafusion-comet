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

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes

sealed trait InputType
case class SparkType(dataType: DataType) extends InputType
case class SparkTypeOneOf(dataTypes: Seq[InputType]) extends InputType
case object SparkBinaryType extends InputType
case object SparkStringType extends InputType
case object SparkIntegralType extends InputType
case object SparkByteType extends InputType
case object SparkShortType extends InputType
case object SparkIntType extends InputType
case object SparkLongType extends InputType
case object SparkFloatType extends InputType
case object SparkDoubleType extends InputType
case class SparkDecimalType(p: Int, s: Int) extends InputType
case object SparkNumericType extends InputType
case object SparkAnyType extends InputType

case class FunctionSignature(inputTypes: Seq[InputType])

sealed trait Function {

  def name: String

  // query generator should generate types based on signature not just on arg count
  @deprecated
  def numArgs: Int
}

@deprecated
case class FunctionWithArgCount(name: String, argCount: Int) extends Function {
  // query generator should generate types based on signature not just on arg count
  override def numArgs: Int = argCount
}

case class FunctionWithSignature(name: String, signatures: Seq[FunctionSignature])
    extends Function {
  // query generator should generate types based on signature not just on arg count
  override def numArgs: Int = signatures.head.inputTypes.length
}

object Meta {

  val dataTypes: Seq[(DataType, Double)] = Seq(
    (DataTypes.BooleanType, 0.1),
    (DataTypes.ByteType, 0.2),
    (DataTypes.ShortType, 0.2),
    (DataTypes.IntegerType, 0.2),
    (DataTypes.LongType, 0.2),
    (DataTypes.FloatType, 0.2),
    (DataTypes.DoubleType, 0.2),
    (DataTypes.createDecimalType(10, 2), 0.2),
    (DataTypes.DateType, 0.2),
    (DataTypes.TimestampType, 0.2),
    (DataTypes.TimestampNTZType, 0.2),
    (DataTypes.StringType, 0.2),
    (DataTypes.BinaryType, 0.1))

  @deprecated
  private def createFunction(name: String, argCount: Int): FunctionWithArgCount = {
    FunctionWithArgCount(name, argCount)
  }

  private def createFunctionWithInputs(
      name: String,
      inputs: Seq[InputType]): FunctionWithSignature = {
    FunctionWithSignature(name, Seq(FunctionSignature(inputs)))
  }

  private def createFunctionWithSignatures(
      name: String,
      signatures: Seq[FunctionSignature]): FunctionWithSignature = {
    FunctionWithSignature(name, signatures)
  }

  private def createUnaryStringFunction(name: String): FunctionWithSignature = {
    createFunctionWithInputs(name, Seq(SparkStringType))
  }

  val stringScalarFunc: Seq[Function] = Seq(
    createFunction("substring", 3),
    createUnaryStringFunction("coalesce"),
    createFunctionWithInputs("starts_with", Seq(SparkStringType, SparkStringType)),
    createFunction("ends_with", 2),
    createFunction("contains", 2),
    createUnaryStringFunction("ascii"),
    createUnaryStringFunction("bit_length"),
    createUnaryStringFunction("octet_length"),
    createUnaryStringFunction("upper"),
    createUnaryStringFunction("lower"),
    createUnaryStringFunction("chr"),
    createUnaryStringFunction("init_cap"),
    createUnaryStringFunction("trim"),
    createUnaryStringFunction("ltrim"),
    createUnaryStringFunction("rtrim"),
    createFunction("string_space", 1),
    createFunctionWithSignatures(
      "rpad",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkIntegralType)),
        FunctionSignature(Seq(SparkStringType, SparkIntegralType, SparkStringType)))),
    createFunctionWithInputs(
      "hex",
      Seq(SparkTypeOneOf(Seq(SparkStringType, SparkBinaryType, SparkIntType, SparkLongType)))),
    createFunction("unhex", 1),
    createFunction("xxhash64", 1),
    createFunction("sha1", 1),
    // createFunction("sha2", 1), -- needs a second argument for number of bits
    createFunction("substring", 3),
    createFunction("btrim", 1),
    createFunction("concat_ws", 2),
    createFunction("repeat", 2),
    createFunction("length", 1),
    createFunction("reverse", 1),
    createFunction("instr", 2),
    createFunction("replace", 2),
    createFunction("translate", 2))

  val dateScalarFunc: Seq[Function] =
    Seq(
      createFunction("year", 1),
      createFunction("hour", 1),
      createFunction("minute", 1),
      createFunction("second", 1))

  val mathScalarFunc: Seq[Function] = Seq(
    createFunction("abs", 1),
    createFunction("acos", 1),
    createFunction("asin", 1),
    createFunction("atan", 1),
    createFunction("Atan2", 1),
    createFunction("Cos", 1),
    createFunction("Exp", 2),
    createFunction("Ln", 1),
    createFunction("Log10", 1),
    createFunction("Log2", 1),
    createFunction("Pow", 2),
    createFunction("Round", 1),
    createFunction("Signum", 1),
    createFunction("Sin", 1),
    createFunction("Sqrt", 1),
    createFunction("Tan", 1),
    createFunction("Ceil", 1),
    createFunction("Floor", 1),
    createFunction("bool_and", 1),
    createFunction("bool_or", 1),
    createFunction("bitwise_not", 1))

  val miscScalarFunc: Seq[Function] =
    Seq(createFunction("isnan", 1), createFunction("isnull", 1), createFunction("isnotnull", 1))

  val arrayScalarFunc: Seq[Function] = Seq(
    createFunction("array", 2),
    createFunction("array_remove", 2),
    createFunction("array_insert", 2),
    createFunction("array_contains", 2),
    createFunction("array_intersect", 2),
    createFunction("array_append", 2))

  val scalarFunc: Seq[Function] = stringScalarFunc ++ dateScalarFunc ++
    mathScalarFunc ++ miscScalarFunc ++ arrayScalarFunc

  val aggFunc: Seq[Function] = Seq(
    createFunction("min", 1),
    createFunction("max", 1),
    createFunction("count", 1),
    createFunction("avg", 1),
    createFunction("sum", 1),
    createFunction("first", 1),
    createFunction("last", 1),
    createFunction("var_pop", 1),
    createFunction("var_samp", 1),
    createFunction("covar_pop", 1),
    createFunction("covar_samp", 1),
    createFunction("stddev_pop", 1),
    createFunction("stddev_samp", 1),
    createFunction("corr", 2))

  val unaryArithmeticOps: Seq[String] = Seq("+", "-")

  val binaryArithmeticOps: Seq[String] =
    Seq("+", "-", "*", "/", "%", "&", "|", "^", "<<", ">>", "div")

  val comparisonOps: Seq[String] = Seq("=", "<=>", ">", ">=", "<", "<=")

}
