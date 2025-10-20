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

sealed trait SparkType
case class SparkTypeOneOf(dataTypes: Seq[SparkType]) extends SparkType
case object SparkBinaryType extends SparkType
case object SparkStringType extends SparkType
case object SparkIntegralType extends SparkType
case object SparkByteType extends SparkType
case object SparkShortType extends SparkType
case object SparkIntType extends SparkType
case object SparkLongType extends SparkType
case object SparkFloatType extends SparkType
case object SparkDoubleType extends SparkType
case class SparkDecimalType(p: Int, s: Int) extends SparkType
case object SparkNumericType extends SparkType
case object SparkDateType extends SparkType
case object SparkTimestampType extends SparkType
case object SparkDateOrTimestampType extends SparkType
case class SparkArrayType(elementType: SparkType) extends SparkType
case object SparkAnyType extends SparkType

case class FunctionSignature(inputTypes: Seq[SparkType])

sealed trait Function {

  def name: String

  // query generator should choose inputs based on signature not just on arg count
  @deprecated
  def numArgs: Int
}

@deprecated
case class FunctionWithArgCount(name: String, argCount: Int) extends Function {
  // query generator should choose inputs based on signature not just on arg count
  override def numArgs: Int = argCount
}

case class FunctionWithSignature(name: String, signatures: Seq[FunctionSignature])
    extends Function {
  // query generator should choose inputs based on signature not just on arg count
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
      inputs: Seq[SparkType]): FunctionWithSignature = {
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

  private def createUnaryNumericFunction(name: String): FunctionWithSignature = {
    createFunctionWithInputs(name, Seq(SparkNumericType))
  }

  val stringScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs("substring", Seq(SparkStringType, SparkIntType, SparkIntType)),
    createUnaryStringFunction("coalesce"),
    createFunctionWithInputs("starts_with", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputs("ends_with", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputs("contains", Seq(SparkStringType, SparkStringType)),
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
    createUnaryStringFunction("unhex"),
    createFunctionWithInputs("xxhash64", Seq(SparkAnyType)), // TODO can take multiple columns
    createFunctionWithInputs("sha1", Seq(SparkAnyType)),
    // createFunction("sha2", 1), -- needs a second argument for number of bits
    createFunctionWithInputs("substring", Seq(SparkStringType, SparkIntType, SparkIntType)),
    createUnaryStringFunction("btrim"),
    createFunction("concat_ws", 2),
    createFunction("repeat", 2),
    createFunctionWithInputs(
      "length",
      Seq(SparkTypeOneOf(Seq(SparkStringType, SparkBinaryType)))),
    createFunctionWithSignatures(
      "reverse",
      Seq(
        FunctionSignature(Seq(SparkStringType)),
        FunctionSignature(Seq(SparkArrayType(SparkAnyType))))),
    createFunctionWithInputs("instr", Seq(SparkStringType, SparkStringType)),
    createFunctionWithSignatures(
      "replace",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkStringType)),
        FunctionSignature(Seq(SparkStringType, SparkStringType, SparkStringType)))),
    createFunctionWithInputs("translate", Seq(SparkStringType, SparkStringType)))

  val dateScalarFunc: Seq[Function] =
    Seq(
      createFunctionWithInputs("year", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("hour", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("minute", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("second", Seq(SparkDateOrTimestampType)))

  val mathScalarFunc: Seq[Function] = Seq(
    createUnaryNumericFunction("abs"),
    createUnaryNumericFunction("acos"),
    createUnaryNumericFunction("asin"),
    createUnaryNumericFunction("atan"),
    createUnaryNumericFunction("Atan2"),
    createUnaryNumericFunction("Cos"),
    createFunctionWithInputs("Exp", Seq(SparkNumericType, SparkNumericType)),
    createUnaryNumericFunction("Ln"),
    createUnaryNumericFunction("Log10"),
    createUnaryNumericFunction("Log2"),
    createFunctionWithInputs("Pow", Seq(SparkNumericType, SparkNumericType)),
    createUnaryNumericFunction("Round"),
    createUnaryNumericFunction("Signum"),
    createUnaryNumericFunction("Sin"),
    createUnaryNumericFunction("Sqrt"),
    createUnaryNumericFunction("Tan"),
    createUnaryNumericFunction("Ceil"),
    createUnaryNumericFunction("Floor"),
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
    createUnaryNumericFunction("avg"),
    createUnaryNumericFunction("sum"),
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
