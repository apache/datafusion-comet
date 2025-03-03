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
    // TimestampNTZType only in Spark 3.4+
    // (DataTypes.TimestampNTZType, 0.2),
    (DataTypes.StringType, 0.2),
    (DataTypes.BinaryType, 0.1))

  val stringScalarFunc: Seq[Function] = Seq(
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
    Function("string_space", 1),
    Function("rpad", 2),
    Function("rpad", 3), // rpad can have 2 or 3 arguments
    Function("hex", 1),
    Function("unhex", 1),
    Function("xxhash64", 1),
    Function("sha1", 1),
    // Function("sha2", 1), -- needs a second argument for number of bits
    Function("substring", 3),
    Function("btrim", 1),
    Function("concat_ws", 2),
    Function("repeat", 2),
    Function("length", 1),
    Function("reverse", 1),
    Function("in_str", 2),
    Function("replace", 2),
    Function("translate", 2))

  val dateScalarFunc: Seq[Function] =
    Seq(Function("year", 1), Function("hour", 1), Function("minute", 1), Function("second", 1))

  val mathScalarFunc: Seq[Function] = Seq(
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
    Function("Floor", 1),
    Function("bool_and", 1),
    Function("bool_or", 1),
    Function("bitwise_not", 1))

  val miscScalarFunc: Seq[Function] =
    Seq(Function("isnan", 1), Function("isnull", 1), Function("isnotnull", 1))

  val arrayScalarFunc: Seq[Function] = Seq(
    Function("array", 2),
    Function("array_remove", 2),
    Function("array_insert", 2),
    Function("array_contains", 2),
    Function("array_intersect", 2),
    Function("array_append", 2))

  val scalarFunc: Seq[Function] = stringScalarFunc ++ dateScalarFunc ++
    mathScalarFunc ++ miscScalarFunc ++ arrayScalarFunc

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

  val unaryArithmeticOps: Seq[String] = Seq("+", "-")

  val binaryArithmeticOps: Seq[String] = Seq("+", "-", "*", "/", "%", "&", "|", "^", "<<", ">>", "div")

  val comparisonOps: Seq[String] = Seq("=", "<=>", ">", ">=", "<", "<=")

}
