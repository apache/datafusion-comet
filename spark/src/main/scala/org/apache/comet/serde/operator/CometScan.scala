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

package org.apache.comet.serde.operator

import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * CometScan is the base trait for scan operators that read data from various sources (files,
 * tables, etc.) and produce native Comet execution plans.
 *
 * Scan operators are leaf nodes in the execution tree (no children) and serve as the entry point
 * for native execution chains.
 */
trait CometScan[T <: SparkPlan] extends CometOperatorSerde[T] {

  /**
   * Scan operators are leaf nodes and should not have children in the operator tree.
   */
  override def convert(
      op: T,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // Scans don't have children, so clear any children that might have been added
    builder.clearChildren()
    convertScan(op, builder)
  }

  /**
   * Convert a scan operator to its protobuf representation.
   *
   * @param op
   *   The Spark scan operator to convert
   * @param builder
   *   The protobuf builder for the operator
   * @return
   *   The converted Comet native operator, or None if conversion fails
   */
  protected def convertScan(op: T, builder: Operator.Builder): Option[Operator]
}
