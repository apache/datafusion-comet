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

package org.apache.comet.serde

import org.apache.spark.sql.catalyst.expressions.{Expression, XxHash64}
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.supportedDataType
import org.apache.comet.shims.CometExprShim

/** Type checks for XxHash64 expression */
object CometXxHash64 extends CometExpression with CometExprShim {
  override def checkSupport(expr: Expression): Boolean = {
    val hash = expr.asInstanceOf[XxHash64]
    for (child <- hash.children) {
      child.dataType match {
        case dt: DecimalType if dt.precision > 18 =>
          // Spark converts decimals with precision > 18 into
          // Java BigDecimal before hashing
          withInfo(expr, s"Unsupported datatype: $dt (precision > 18)")
          return false
        case dt if !supportedDataType(dt) =>
          withInfo(expr, s"Unsupported datatype $dt")
          return false
        case _ =>
      }
    }
    true
  }
}
