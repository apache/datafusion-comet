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

case class FunctionSignature(inputTypes: Seq[SparkType], varArgs: Boolean = false)

case class Function(name: String, signatures: Seq[FunctionSignature])

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

  private def createFunctionWithInputTypes(
      name: String,
      inputs: Seq[SparkType],
      varArgs: Boolean = false): Function = {
    Function(name, Seq(FunctionSignature(inputs, varArgs)))
    createFunctions(name, Seq(FunctionSignature(inputs, varArgs)))
  }

  private def createFunctions(name: String, signatures: Seq[FunctionSignature]): Function = {
    signatures.foreach { s =>
      assert(
        !s.varArgs || s.inputTypes.length == 1,
        s"Variadic function $s must have exactly one input type")
    }
    Function(name, signatures)
  }

  private def createUnaryStringFunction(name: String): Function = {
    createFunctionWithInputTypes(name, Seq(SparkStringType))
  }

  private def createUnaryNumericFunction(name: String): Function = {
    createFunctionWithInputTypes(name, Seq(SparkNumericType))
  }

  // Math expressions (corresponds to mathExpressions in QueryPlanSerde)
  val mathScalarFunc: Seq[Function] = Seq(
    createUnaryNumericFunction("abs"),
    createUnaryNumericFunction("acos"),
    createUnaryNumericFunction("asin"),
    createUnaryNumericFunction("atan"),
    createFunctionWithInputTypes("atan2", Seq(SparkNumericType, SparkNumericType)),
    createUnaryNumericFunction("cos"),
    createUnaryNumericFunction("exp"),
    createUnaryNumericFunction("expm1"),
    createFunctionWithInputTypes("log", Seq(SparkNumericType, SparkNumericType)),
    createUnaryNumericFunction("log10"),
    createUnaryNumericFunction("log2"),
    createFunctionWithInputTypes("pow", Seq(SparkNumericType, SparkNumericType)),
    createFunctionWithInputTypes("remainder", Seq(SparkNumericType, SparkNumericType)),
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
    createFunctionWithInputTypes("unary_minus", Seq(SparkNumericType)))

  // Hash expressions (corresponds to hashExpressions in QueryPlanSerde)
  val hashScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes("md5", Seq(SparkAnyType)),
    createFunctionWithInputTypes("murmur3_hash", Seq(SparkAnyType)), // TODO variadic
    createFunctionWithInputTypes("sha2", Seq(SparkAnyType, SparkIntType)))

  // String expressions (corresponds to stringExpressions in QueryPlanSerde)
  val stringScalarFunc: Seq[Function] = Seq(
    createUnaryStringFunction("ascii"),
    createUnaryStringFunction("bit_length"),
    createUnaryStringFunction("chr"),
    createFunctionWithInputTypes(
      "concat",
      Seq(
        SparkTypeOneOf(
          Seq(
            SparkStringType,
            SparkBinaryType,
            SparkArrayType(SparkStringType),
            SparkArrayType(SparkNumericType),
            SparkArrayType(SparkBinaryType)))),
      varArgs = true),
    createFunctionWithInputTypes("concat_ws", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputTypes("contains", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputTypes("ends_with", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputTypes(
      "hex",
      Seq(SparkTypeOneOf(Seq(SparkStringType, SparkBinaryType, SparkIntType, SparkLongType)))),
    createUnaryStringFunction("init_cap"),
    createFunctionWithInputTypes("instr", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputTypes(
      "length",
      Seq(SparkTypeOneOf(Seq(SparkStringType, SparkBinaryType)))),
    createFunctionWithInputTypes("like", Seq(SparkStringType, SparkStringType)),
    createUnaryStringFunction("lower"),
    createFunctions(
      "lpad",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkIntegralType)),
        FunctionSignature(Seq(SparkStringType, SparkIntegralType, SparkStringType)))),
    createUnaryStringFunction("ltrim"),
    createUnaryStringFunction("octet_length"),
    createFunctions(
      "regexp_replace",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkStringType, SparkStringType)),
        FunctionSignature(Seq(SparkStringType, SparkStringType, SparkStringType, SparkIntType)))),
    createFunctionWithInputTypes("repeat", Seq(SparkStringType, SparkIntType)),
    createFunctions(
      "replace",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkStringType)),
        FunctionSignature(Seq(SparkStringType, SparkStringType, SparkStringType)))),
    createFunctions(
      "reverse",
      Seq(
        FunctionSignature(Seq(SparkStringType)),
        FunctionSignature(Seq(SparkArrayType(SparkAnyType))))),
    createFunctionWithInputTypes("rlike", Seq(SparkStringType, SparkStringType)),
    createFunctions(
      "rpad",
      Seq(
        FunctionSignature(Seq(SparkStringType, SparkIntegralType)),
        FunctionSignature(Seq(SparkStringType, SparkIntegralType, SparkStringType)))),
    createUnaryStringFunction("rtrim"),
    createFunctionWithInputTypes("starts_with", Seq(SparkStringType, SparkStringType)),
    createFunctionWithInputTypes("string_space", Seq(SparkIntType)),
    createFunctionWithInputTypes("substring", Seq(SparkStringType, SparkIntType, SparkIntType)),
    createFunctionWithInputTypes("translate", Seq(SparkStringType, SparkStringType)),
    createUnaryStringFunction("trim"),
    createUnaryStringFunction("btrim"),
    createUnaryStringFunction("unhex"),
    createUnaryStringFunction("upper"),
    createFunctionWithInputTypes("xxhash64", Seq(SparkAnyType)), // TODO variadic
    createFunctionWithInputTypes("sha1", Seq(SparkAnyType)))

  // Conditional expressions (corresponds to conditionalExpressions in QueryPlanSerde)
  val conditionalScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes("if", Seq(SparkBooleanType, SparkAnyType, SparkAnyType)))

  // Map expressions (corresponds to mapExpressions in QueryPlanSerde)
  val mapScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes(
      "map_extract",
      Seq(SparkMapType(SparkAnyType, SparkAnyType), SparkAnyType)),
    createFunctionWithInputTypes("map_keys", Seq(SparkMapType(SparkAnyType, SparkAnyType))),
    createFunctionWithInputTypes("map_entries", Seq(SparkMapType(SparkAnyType, SparkAnyType))),
    createFunctionWithInputTypes("map_values", Seq(SparkMapType(SparkAnyType, SparkAnyType))),
    createFunctionWithInputTypes(
      "map_from_arrays",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))))

  // Predicate expressions (corresponds to predicateExpressions in QueryPlanSerde)
  val predicateScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes("and", Seq(SparkBooleanType, SparkBooleanType)),
    createFunctionWithInputTypes("or", Seq(SparkBooleanType, SparkBooleanType)),
    createFunctionWithInputTypes("not", Seq(SparkBooleanType)),
    createFunctionWithInputTypes("in", Seq(SparkAnyType, SparkAnyType))
  ) // TODO: variadic

  // Struct expressions (corresponds to structExpressions in QueryPlanSerde)
  val structScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes(
      "create_named_struct",
      Seq(SparkStringType, SparkAnyType)
    ), // TODO: variadic name/value pairs
    createFunctionWithInputTypes(
      "get_struct_field",
      Seq(SparkStructType(Seq(SparkAnyType)), SparkStringType)))

  // Bitwise expressions (corresponds to bitwiseExpressions in QueryPlanSerde)
  val bitwiseScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes("bitwise_and", Seq(SparkIntegralType, SparkIntegralType)),
    createFunctionWithInputTypes("bitwise_count", Seq(SparkIntegralType)),
    createFunctionWithInputTypes("bitwise_get", Seq(SparkIntegralType, SparkIntType)),
    createFunctionWithInputTypes("bitwise_or", Seq(SparkIntegralType, SparkIntegralType)),
    createFunctionWithInputTypes("bitwise_not", Seq(SparkIntegralType)),
    createFunctionWithInputTypes("bitwise_xor", Seq(SparkIntegralType, SparkIntegralType)),
    createFunctionWithInputTypes("shift_left", Seq(SparkIntegralType, SparkIntType)),
    createFunctionWithInputTypes("shift_right", Seq(SparkIntegralType, SparkIntType)))

  // Misc expressions (corresponds to miscExpressions in QueryPlanSerde)
  val miscScalarFunc: Seq[Function] =
    Seq(
      createFunctionWithInputTypes("isnan", Seq(SparkNumericType)),
      createFunctionWithInputTypes("isnull", Seq(SparkAnyType)),
      createFunctionWithInputTypes("isnotnull", Seq(SparkAnyType)),
      createFunctionWithInputTypes("coalesce", Seq(SparkAnyType, SparkAnyType))
    ) // TODO: variadic

  // Array expressions (corresponds to arrayExpressions in QueryPlanSerde)
  val arrayScalarFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes("array_append", Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputTypes("array_compact", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputTypes(
      "array_contains",
      Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputTypes("array_distinct", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputTypes(
      "array_except",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctionWithInputTypes(
      "array_insert",
      Seq(SparkArrayType(SparkAnyType), SparkIntType, SparkAnyType)),
    createFunctionWithInputTypes(
      "array_intersect",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctions(
      "array_join",
      Seq(
        FunctionSignature(Seq(SparkArrayType(SparkAnyType), SparkStringType)),
        FunctionSignature(Seq(SparkArrayType(SparkAnyType), SparkStringType, SparkStringType)))),
    createFunctionWithInputTypes("array_max", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputTypes("array_min", Seq(SparkArrayType(SparkAnyType))),
    createFunctionWithInputTypes("array_remove", Seq(SparkArrayType(SparkAnyType), SparkAnyType)),
    createFunctionWithInputTypes("array_repeat", Seq(SparkAnyType, SparkIntType)),
    createFunctionWithInputTypes(
      "arrays_overlap",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctionWithInputTypes(
      "array_union",
      Seq(SparkArrayType(SparkAnyType), SparkArrayType(SparkAnyType))),
    createFunctionWithInputTypes("array", Seq(SparkAnyType, SparkAnyType)), // TODO: variadic
    createFunctionWithInputTypes(
      "element_at",
      Seq(
        SparkTypeOneOf(
          Seq(SparkArrayType(SparkAnyType), SparkMapType(SparkAnyType, SparkAnyType))),
        SparkAnyType)),
    createFunctionWithInputTypes("flatten", Seq(SparkArrayType(SparkArrayType(SparkAnyType)))),
    createFunctionWithInputTypes(
      "get_array_item",
      Seq(SparkArrayType(SparkAnyType), SparkIntType)))

  // Temporal expressions (corresponds to temporalExpressions in QueryPlanSerde)
  val temporalScalarFunc: Seq[Function] =
    Seq(
      createFunctionWithInputTypes("date_add", Seq(SparkDateType, SparkIntType)),
      createFunctionWithInputTypes("date_sub", Seq(SparkDateType, SparkIntType)),
      createFunctions(
        "from_unixtime",
        Seq(
          FunctionSignature(Seq(SparkLongType)),
          FunctionSignature(Seq(SparkLongType, SparkStringType)))),
      createFunctionWithInputTypes("hour", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("minute", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("second", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("trunc", Seq(SparkDateOrTimestampType, SparkStringType)),
      createFunctionWithInputTypes("year", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("month", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("day", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("dayofmonth", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("dayofweek", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("weekday", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("dayofyear", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("weekofyear", Seq(SparkDateOrTimestampType)),
      createFunctionWithInputTypes("quarter", Seq(SparkDateOrTimestampType)))

  // Combined in same order as exprSerdeMap in QueryPlanSerde
  val scalarFunc: Seq[Function] = mathScalarFunc ++ hashScalarFunc ++ stringScalarFunc ++
    conditionalScalarFunc ++ mapScalarFunc ++ predicateScalarFunc ++
    structScalarFunc ++ bitwiseScalarFunc ++ miscScalarFunc ++ arrayScalarFunc ++
    temporalScalarFunc

  val aggFunc: Seq[Function] = Seq(
    createFunctionWithInputTypes("min", Seq(SparkAnyType)),
    createFunctionWithInputTypes("max", Seq(SparkAnyType)),
    createFunctionWithInputTypes("count", Seq(SparkAnyType)),
    createUnaryNumericFunction("avg"),
    createUnaryNumericFunction("sum"),
    // first/last are non-deterministic and known to be incompatible with Spark
//    createFunctionWithInputTypes("first", Seq(SparkAnyType)),
//    createFunctionWithInputTypes("last", Seq(SparkAnyType)),
    createUnaryNumericFunction("var_pop"),
    createUnaryNumericFunction("var_samp"),
    createFunctionWithInputTypes("covar_pop", Seq(SparkNumericType, SparkNumericType)),
    createFunctionWithInputTypes("covar_samp", Seq(SparkNumericType, SparkNumericType)),
    createUnaryNumericFunction("stddev_pop"),
    createUnaryNumericFunction("stddev_samp"),
    createFunctionWithInputTypes("corr", Seq(SparkNumericType, SparkNumericType)),
    createFunctionWithInputTypes("bit_and", Seq(SparkIntegralType)),
    createFunctionWithInputTypes("bit_or", Seq(SparkIntegralType)),
    createFunctionWithInputTypes("bit_xor", Seq(SparkIntegralType)))

  val unaryArithmeticOps: Seq[String] = Seq("+", "-")

  val binaryArithmeticOps: Seq[String] =
    Seq("+", "-", "*", "/", "%", "&", "|", "^", "<<", ">>", "div")

  val comparisonOps: Seq[String] = Seq("=", "<=>", ">", ">=", "<", "<=")

  // TODO make this more comprehensive
  val comparisonTypes: Seq[SparkType] = Seq(
    SparkStringType,
    SparkBinaryType,
    SparkNumericType,
    SparkDateType,
    SparkTimestampType,
    SparkArrayType(SparkTypeOneOf(Seq(SparkStringType, SparkNumericType, SparkDateType))))

}
