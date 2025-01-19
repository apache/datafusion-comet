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

import org.apache.spark.sql.catalyst.expressions.{ArrayRemove, Expression}
import org.apache.spark.sql.types.{DataType, DataTypes}

import org.apache.comet.CometSparkSessionExtensions.withInfo

trait CometExpression {
  def checkSupport(expr: Expression): Boolean
  def convert(expr: Expression)
}

object CometArrayRemove extends CometExpression {

  def isTypeSupported(dt: DataType): Boolean = {
//    dt match {
//      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
    //      DataTypes.LongType => true
//      case _ => false
//    }
    true
  }

  override def checkSupport(expr: Expression): Boolean = {
    expr match {
      case ArrayRemove(l, r) =>
        if (!isTypeSupported(l.dataType)) {
          withInfo(expr, s"data type not supported: ${l.dataType}")
          return false
        }
        if (!isTypeSupported(r.dataType)) {
          withInfo(expr, s"data type not supported: ${r.dataType}")
          return false
        }
        true
      case _ =>
        false
    }
  }

  override def convert(expr: Expression): Unit = {
    // TODO
  }
}
