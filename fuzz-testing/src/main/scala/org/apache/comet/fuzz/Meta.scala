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
    createFunctionWithInputs("string_space", Seq(SparkIntType)),
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
    createFunctionWithInputs("concat_ws", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputs("repeat", Seq(SparkStringType, SparkIntType)),
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
    createFunctionWithInputs("bool_and", Seq(SparkAnyType)),
    createFunctionWithInputs("bool_or", Seq(SparkAnyType)),
    createFunctionWithInputs("bitwise_not", Seq(SparkIntegralType)))

  val miscScalarFunc: Seq[Function] =
    Seq(
      createFunctionWithInputs("isnan", Seq(SparkNumericType)),
      createFunctionWithInputs("isnull", Seq(SparkAnyType)),
      createFunctionWithInputs("isnotnull", Seq(SparkAnyType)))

  val arrayScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs("array", Seq(SparkAnyType, SparkAnyType)),
    createFunctionWithInputs("array_remove", Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputs("array_insert", Seq(SparkArrayType(SparkAnyType), SparkIntType)),
    createFunctionWithInputs("array_contains", Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputs(
      "array_intersect",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctionWithInputs("array_append", Seq(SparkArrayType(SparkAnyType), SparkAnyType)))

  val scalarFunc: Seq[Function] = stringScalarFunc ++ dateScalarFunc ++
    mathScalarFunc ++ miscScalarFunc ++ arrayScalarFunc

  val aggFunc: Seq[Function] = Seq(
    createFunctionWithInputs("min", Seq(SparkAnyType)),
    createFunctionWithInputs("max", Seq(SparkAnyType)),
    createFunctionWithInputs("count", Seq(SparkAnyType)),
    createUnaryNumericFunction("avg"),
    createUnaryNumericFunction("sum"),
    createFunctionWithInputs("first", Seq(SparkAnyType)),
    createFunctionWithInputs("last", Seq(SparkAnyType)),
    createFunctionWithInputs("var_pop", Seq(SparkNumericType)),
    createFunctionWithInputs("var_samp", Seq(SparkNumericType)),
    createFunctionWithInputs("covar_pop", Seq(SparkNumericType)),
    createFunctionWithInputs("covar_samp", Seq(SparkNumericType)),
    createFunctionWithInputs("stddev_pop", Seq(SparkNumericType)),
    createFunctionWithInputs("stddev_samp", Seq(SparkNumericType)),
    createFunctionWithInputs("corr", Seq(SparkNumericType, SparkNumericType)))

  val unaryArithmeticOps: Seq[String] = Seq("+", "-")

  val binaryArithmeticOps: Seq[String] =
    Seq("+", "-", "*", "/", "%", "&", "|", "^", "<<", ">>", "div")

  val comparisonOps: Seq[String] = Seq("=", "<=>", ">", ">=", "<", "<=")

}
