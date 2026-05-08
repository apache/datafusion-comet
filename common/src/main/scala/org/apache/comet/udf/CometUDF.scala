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

package org.apache.comet.udf

import org.apache.arrow.vector.ValueVector
import org.apache.spark.sql.types.DataType

/**
 * Scalar UDF invoked from native execution via JNI. Receives Arrow vectors as input and returns
 * an Arrow vector.
 *
 *   - Vector arguments arrive at the row count of the current batch.
 *   - Scalar (literal-folded) arguments arrive as length-1 vectors and must be read at index 0.
 *   - The returned vector's length must match the longest input.
 *
 * Implementations must have a public no-arg constructor and must be stateless: a single instance
 * per class is cached and shared across native worker threads for the lifetime of the JVM.
 */
trait CometUDF {

  /** UDF name as invoked from SQL or DataFrame. Must match the name registered with Spark. */
  def name: String

  /** Output Arrow vector type. */
  def returnType: DataType

  /** Whether the result vector may contain nulls. */
  def nullable: Boolean = true

  /**
   * Input data types. Required only for columnar-only registration via
   * [[CometUdfRegistry.registerColumnarOnly]]; ignored when a row-based Spark UDF is also
   * registered (Spark uses its own input schema in that case).
   */
  def inputTypes: Seq[DataType] = Seq.empty

  def evaluate(inputs: Array[ValueVector]): ValueVector
}
