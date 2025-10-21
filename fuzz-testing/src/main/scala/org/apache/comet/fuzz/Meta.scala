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
case object SparkBooleanType extends SparkType
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
case class SparkMapType(keyType: SparkType, valueType: SparkType) extends SparkType
case class SparkStructType(fields: Seq[SparkType]) extends SparkType
case object SparkAnyType extends SparkType

case class FunctionSignature(inputTypes: Seq[SparkType])

case class Function(name: String, signatures: Seq[FunctionSignature]) {
  // query generator should choose inputs based on signature not just on arg count
  @deprecated
  def numArgs: Int = signatures.head.inputTypes.length
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

  private def createFunctionWithInputs(name: String, inputs: Seq[SparkType]): Function = {
    Function(name, Seq(FunctionSignature(inputs)))
  }

  private def createFunctions(name: String, signatures: Seq[FunctionSignature]): Function = {
    Function(name, signatures)
  }

  private def createUnaryStringFunction(name: String): Function = {
    createFunctionWithInputs(name, Seq(SparkStringType))
  }

  private def createUnaryNumericFunction(name: String): Function = {
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
    createFunctions(
      "rpad",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkIntegralType)),
        FunctionSignature(Seq(SparkStringType, SparkIntegralType, SparkStringType)))),
    createFunctions(
      "lpad",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkIntegralType)),
        FunctionSignature(Seq(SparkStringType, SparkIntegralType, SparkStringType)))),
    createFunctionWithInputs(
      "hex",
      Seq(SparkTypeOneOf(Seq(SparkStringType, SparkBinaryType, SparkIntType, SparkLongType)))),
    createUnaryStringFunction("unhex"),
    createFunctionWithInputs("xxhash64", Seq(SparkAnyType)), // TODO can take multiple columns
    createFunctionWithInputs("sha1", Seq(SparkAnyType)),
    createFunctionWithInputs("substring", Seq(SparkStringType, SparkIntType, SparkIntType)),
    createUnaryStringFunction("btrim"),
    createFunctionWithInputs("concat_ws", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputs("repeat", Seq(SparkStringType, SparkIntType)),
    createFunctionWithInputs(
      "length",
      Seq(SparkTypeOneOf(Seq(SparkStringType, SparkBinaryType)))),
    createFunctions(
      "reverse",
      Seq(
        FunctionSignature(Seq(SparkStringType)),
        FunctionSignature(Seq(SparkArrayType(SparkAnyType))))),
    createFunctionWithInputs("instr", Seq(SparkStringType, SparkStringType)),
    createFunctions(
      "replace",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkStringType)),
        FunctionSignature(Seq(SparkStringType, SparkStringType, SparkStringType)))),
    createFunctionWithInputs("translate", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputs("like", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputs("rlike", Seq(SparkStringType, SparkStringType)),
    createFunctions(
      "regexp_replace",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkStringType, SparkStringType)),
        FunctionSignature(Seq(SparkStringType, SparkStringType, SparkStringType, SparkIntType)))))

  val dateScalarFunc: Seq[Function] =
    Seq(
      createFunctionWithInputs("year", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("month", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("day", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("dayofmonth", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("dayofweek", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("dayofyear", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("weekday", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("weekofyear", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("quarter", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("hour", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("minute", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("second", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputs("date_add", Seq(SparkDateType, SparkIntType)),
      createFunctionWithInputs("date_sub", Seq(SparkDateType, SparkIntType)),
      createFunctionWithInputs("trunc", Seq(SparkDateOrTimestampType, SparkStringType)),
      createFunctions(
        "from_unixtime",
        Seq(
          FunctionSignature(Seq(SparkLongType)),
          FunctionSignature(Seq(SparkLongType, SparkStringType)))))

  val mathScalarFunc: Seq[Function] = Seq(
    createUnaryNumericFunction("abs"),
    createUnaryNumericFunction("acos"),
    createUnaryNumericFunction("asin"),
    createUnaryNumericFunction("atan"),
    createFunctionWithInputs("atan2", Seq(SparkNumericType, SparkNumericType)),
    createUnaryNumericFunction("cos"),
    createUnaryNumericFunction("exp"),
    createUnaryNumericFunction("expm1"),
    createFunctionWithInputs("log", Seq(SparkNumericType, SparkNumericType)),
    createUnaryNumericFunction("log10"),
    createUnaryNumericFunction("log2"),
    createFunctionWithInputs("pow", Seq(SparkNumericType, SparkNumericType)),
    createFunctions(
      "round",
      Seq(
        FunctionSignature(Seq(SparkNumericType)),
        FunctionSignature(Seq(SparkNumericType, SparkIntType)))),
    createUnaryNumericFunction("signum"),
    createUnaryNumericFunction("sin"),
    createUnaryNumericFunction("sqrt"),
    createUnaryNumericFunction("tan"),
    createUnaryNumericFunction("ceil"),
    createUnaryNumericFunction("floor"),
    createFunctionWithInputs("remainder", Seq(SparkNumericType, SparkNumericType)),
    createFunctionWithInputs("unary_minus", Seq(SparkNumericType)))

  val hashScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs("md5", Seq(SparkAnyType)),
    createFunctionWithInputs("murmur3_hash", Seq(SparkAnyType)), // TODO can take multiple columns
    createFunctionWithInputs("sha2", Seq(SparkAnyType, SparkIntType)))

  val bitwiseScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs("bitwise_and", Seq(SparkIntegralType, SparkIntegralType)),
    createFunctionWithInputs("bitwise_or", Seq(SparkIntegralType, SparkIntegralType)),
    createFunctionWithInputs("bitwise_xor", Seq(SparkIntegralType, SparkIntegralType)),
    createFunctionWithInputs("bitwise_not", Seq(SparkIntegralType)),
    createFunctionWithInputs("bitwise_count", Seq(SparkIntegralType)),
    createFunctionWithInputs("bitwise_get", Seq(SparkIntegralType, SparkIntType)),
    createFunctionWithInputs("shift_left", Seq(SparkIntegralType, SparkIntType)),
    createFunctionWithInputs("shift_right", Seq(SparkIntegralType, SparkIntType)))

  val predicateScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs("and", Seq(SparkBooleanType, SparkBooleanType)),
    createFunctionWithInputs("or", Seq(SparkBooleanType, SparkBooleanType)),
    createFunctionWithInputs("not", Seq(SparkBooleanType)),
    createFunctionWithInputs("in", Seq(SparkAnyType, SparkAnyType))
  ) // TODO: variadic

  val conditionalScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs("if", Seq(SparkBooleanType, SparkAnyType, SparkAnyType)))

  val miscScalarFunc: Seq[Function] =
    Seq(
      createFunctionWithInputs("isnan", Seq(SparkNumericType)),
      createFunctionWithInputs("isnull", Seq(SparkAnyType)),
      createFunctionWithInputs("isnotnull", Seq(SparkAnyType)),
      createFunctionWithInputs("coalesce", Seq(SparkAnyType, SparkAnyType))
    ) // TODO: variadic

  val arrayScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs("array", Seq(SparkAnyType, SparkAnyType)), // TODO: variadic
    createFunctionWithInputs("array_append", Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputs("array_compact", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputs("array_contains", Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputs("array_distinct", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputs(
      "array_except",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctionWithInputs(
      "array_insert",
      Seq(SparkArrayType(SparkAnyType), SparkIntType, SparkAnyType)),
    createFunctionWithInputs(
      "array_intersect",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctions(
      "array_join",
      Seq(
        FunctionSignature(Seq(SparkArrayType(SparkAnyType), SparkStringType)),
        FunctionSignature(Seq(SparkArrayType(SparkAnyType), SparkStringType, SparkStringType)))),
    createFunctionWithInputs("array_max", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputs("array_min", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputs("array_remove", Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputs("array_repeat", Seq(SparkAnyType, SparkIntType)),
    createFunctionWithInputs(
      "arrays_overlap",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctionWithInputs(
      "array_union",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctionWithInputs(
      "element_at",
      Seq(
        SparkTypeOneOf(
          Seq(SparkArrayType(SparkAnyType), SparkMapType(SparkAnyType, SparkAnyType))),
        SparkAnyType)),
    createFunctionWithInputs("flatten", Seq(SparkArrayType(SparkArrayType(SparkAnyType)))),
    createFunctionWithInputs("get_array_item", Seq(SparkArrayType(SparkAnyType), SparkIntType)))

  val mapScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs(
      "map_extract",
      Seq(SparkMapType(SparkAnyType, SparkAnyType), SparkAnyType)),
    createFunctionWithInputs("map_keys", Seq(SparkMapType(SparkAnyType, SparkAnyType))),
    createFunctionWithInputs("map_values", Seq(SparkMapType(SparkAnyType, SparkAnyType))),
    createFunctionWithInputs("map_entries", Seq(SparkMapType(SparkAnyType, SparkAnyType))),
    createFunctionWithInputs(
      "map_from_arrays",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))))

  val structScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputs(
      "create_named_struct",
      Seq(SparkStringType, SparkAnyType)
    ), // TODO: variadic name/value pairs
    createFunctionWithInputs(
      "get_struct_field",
      Seq(SparkStructType(Seq(SparkAnyType)), SparkStringType)))

  val scalarFunc: Seq[Function] = stringScalarFunc ++ dateScalarFunc ++
    mathScalarFunc ++ hashScalarFunc ++ bitwiseScalarFunc ++ predicateScalarFunc ++
    conditionalScalarFunc ++ miscScalarFunc ++ arrayScalarFunc ++ mapScalarFunc ++ structScalarFunc

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
    createFunctionWithInputs("covar_pop", Seq(SparkNumericType, SparkNumericType)),
    createFunctionWithInputs("covar_samp", Seq(SparkNumericType, SparkNumericType)),
    createFunctionWithInputs("stddev_pop", Seq(SparkNumericType)),
    createFunctionWithInputs("stddev_samp", Seq(SparkNumericType)),
    createFunctionWithInputs("corr", Seq(SparkNumericType, SparkNumericType)),
    createFunctionWithInputs("bit_and", Seq(SparkIntegralType)),
    createFunctionWithInputs("bit_or", Seq(SparkIntegralType)),
    createFunctionWithInputs("bit_xor", Seq(SparkIntegralType)))

  val unaryArithmeticOps: Seq[String] = Seq("+", "-")

  val binaryArithmeticOps: Seq[String] =
    Seq("+", "-", "*", "/", "%", "&", "|", "^", "<<", ">>", "div")

  val comparisonOps: Seq[String] = Seq("=", "<=>", ">", ">=", "<", "<=")

}
