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

package org.apache.comet

import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}

class CometFuzzMathSuite extends CometFuzzTestBase {

  for (op <- Seq("+", "-", "*", "/", "div")) {
    test(s"integer math: $op") {
      val df = spark.read.parquet(filename)
      val cols = df.schema.fields
        .filter(_.dataType match {
          case _: IntegerType => true
          case _: LongType => true
          case _ => false
        })
        .map(_.name)
      df.createOrReplaceTempView("t1")
      val sql =
        s"SELECT ${cols(0)} $op ${cols(1)} FROM t1 ORDER BY ${cols(0)}, ${cols(1)} LIMIT 500"
      if (op == "div") {
        // cast(cast(c3#1975 as bigint) as decimal(19,0)) is not fully compatible with Spark (No overflow check)
        checkSparkAnswer(sql)
      } else {
        checkSparkAnswerAndOperator(sql)
      }
    }
  }

  for (op <- Seq("+", "-", "*", "/", "div")) {
    test(s"decimal math: $op") {
      val df = spark.read.parquet(filename)
      val cols = df.schema.fields.filter(_.dataType.isInstanceOf[DecimalType]).map(_.name)
      df.createOrReplaceTempView("t1")
      val sql =
        s"SELECT ${cols(0)} $op ${cols(1)} FROM t1 ORDER BY ${cols(0)}, ${cols(1)} LIMIT 500"
      checkSparkAnswerAndOperator(sql)
    }
  }

}
