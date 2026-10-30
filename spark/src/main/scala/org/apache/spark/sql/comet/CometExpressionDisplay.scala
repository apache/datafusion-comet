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

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, Literal, PrettyAttribute}
import org.apache.spark.sql.types.BinaryType

/** Display-only rewrites shared by operators that render expressions. */
private[comet] object CometExpressionDisplay {
  // Binary Literal.toString hex-encodes the entire value. Replace Bloom bytes before rendering,
  // without evaluating subqueries or changing executable expressions. These placeholders must
  // never reach native serialization.
  def summarizeBloomLiterals(expression: Expression): Expression = expression.transform {
    case bloom: BloomFilterMightContain =>
      bloom.copy(bloomFilterExpression = bloom.bloomFilterExpression.transform {
        case Literal(bytes: Array[Byte], BinaryType) =>
          PrettyAttribute(s"<bloom: ${bytes.length} bytes>", BinaryType)
      })
  }
}
