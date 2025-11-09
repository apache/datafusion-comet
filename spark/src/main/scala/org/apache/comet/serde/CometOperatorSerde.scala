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

import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.ConfigEntry
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Trait for providing serialization logic for operators.
 */
trait CometOperatorSerde[T <: SparkPlan] {

  /**
   * Convert a Spark operator into a protocol buffer representation that can be passed into native
   * code.
   *
   * @param op
   *   The Spark operator.
   * @param builder
   *   The protobuf builder for the operator.
   * @param childOp
   *   Child operators that have already been converted to Comet.
   * @return
   *   Protocol buffer representation, or None if the operator could not be converted. In this
   *   case it is expected that the input operator will have been tagged with reasons why it could
   *   not be converted.
   */
  def convert(
      op: T,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator]

  /**
   * Get the optional Comet configuration entry that is used to enable or disable native support
   * for this operator.
   */
  def enabledConfig: Option[ConfigEntry[Boolean]]
}
