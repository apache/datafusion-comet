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

package org.apache.spark.sql.comet

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, CheckOverflow, EvalMode, NumericEvalContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

class CometDecimalArithmeticViewSuite extends CometTestBase {

  // Spark 4.1.1 (SPARK-53968) stores `spark.sql.decimalOperations.allowPrecisionLoss` per
  // arithmetic expression so a view's analyzed plan keeps a stable result type across config
  // changes. Comet's DecimalPrecision rule used to recompute the result type from the current
  // SQLConf, producing a CheckOverflow target that disagreed with the stored Add.dataType and
  // re-labelling the Decimal128 buffer at the wrong scale (issue #4124). The repro requires a
  // mismatch between the stored evalContext and the live SQLConf, which is why we construct
  // Add directly rather than going through SQL parsing.
  test("issue #4124: DecimalPrecision.promote honours per-expression allowPrecisionLoss") {
    val left = AttributeReference("a", DecimalType(38, 18))()
    val right = AttributeReference("b", DecimalType(38, 18))()
    val storedTrue =
      Add(left, right, NumericEvalContext(EvalMode.LEGACY, allowDecimalPrecisionLoss = true))
    val storedFalse =
      Add(left, right, NumericEvalContext(EvalMode.LEGACY, allowDecimalPrecisionLoss = false))
    assert(storedTrue.dataType === DecimalType(38, 17))
    assert(storedFalse.dataType === DecimalType(38, 18))

    // Current SQLConf disagrees with the stored evalContext on each Add. The promoted
    // CheckOverflow's target type must come from Add.dataType (which honours the stored
    // evalContext), not from the current SQLConf. Otherwise the native CheckOverflow
    // re-labels the Decimal128 buffer at the wrong scale and values come out 10x off.
    Seq((true, storedFalse), (false, storedTrue)).foreach { case (currentConf, add) =>
      withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> currentConf.toString) {
        val promoted = org.apache.spark.sql.comet.DecimalPrecision
          .promote(add, nullOnOverflow = true)
        promoted match {
          case CheckOverflow(_, dt, _) =>
            assert(
              dt === add.dataType,
              s"CheckOverflow target $dt must match Add.dataType ${add.dataType}; mismatch " +
                s"causes the decimal buffer to be re-labelled at the wrong scale.")
          case other =>
            fail(s"Expected DecimalPrecision.promote to wrap Add in CheckOverflow, got: $other")
        }
      }
    }
  }
}
