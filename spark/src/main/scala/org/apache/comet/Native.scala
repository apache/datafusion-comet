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

import java.util.Map

import org.apache.spark.sql.comet.CometMetricNode

class Native extends NativeBase {

  /**
   * Create a native query plan from execution SparkPlan serialized in bytes.
   * @param id
   *   The id of the query plan.
   * @param configMap
   *   The Java Map object for the configs of native engine.
   * @param plan
   *   the bytes of serialized SparkPlan.
   * @param metrics
   *   the native metrics of SparkPlan.
   * @return
   *   the address to native query plan.
   */
  @native def createPlan(
      id: Long,
      configMap: Map[String, String],
      plan: Array[Byte],
      metrics: CometMetricNode): Long

  /**
   * Return the native query plan string for the given address of native query plan. For debugging
   * purpose.
   *
   * @param plan
   *   the address to native query plan.
   * @return
   *   the string of native query plan.
   */
  @native def getPlanString(plan: Long): String

  /**
   * Execute a native query plan based on given input Arrow arrays.
   *
   * @param plan
   *   the address to native query plan.
   * @param addresses
   *   the array of addresses of input Arrow arrays. The addresses are exported from Arrow Arrays
   *   so the number of addresses is always even number in the sequence like [array_address1,
   *   schema_address1, array_address2, schema_address2, ...]. Note that we can pass empty
   *   addresses to this API. In this case, it indicates there are no more input arrays to the
   *   native query plan, but the query plan possibly can still execute to produce output batch
   *   because it might contain blocking operators such as Sort, Aggregate. When this API returns
   *   an empty array back, it means the native query plan is finished.
   * @param finishes
   *   whether the end of input arrays is reached for each input. If this is set to true, the
   *   native library will know there is no more inputs. But it doesn't mean the execution is
   *   finished immediately. For some blocking operators native execution will continue to output.
   * @param numRows
   *   the number of rows in the batch.
   * @return
   *   an array containing: 1) the status flag (0 for pending, 1 for normal returned arrays,
   * -1 for end of output), 2) (optional) the number of rows if returned flag is 1 3) the
   * addresses of output Arrow arrays
   */
  @native def executePlan(
      plan: Long,
      addresses: Array[Array[Long]],
      finishes: Array[Boolean],
      numRows: Int): Array[Long]

  /**
   * Peeks the next batch of output Arrow arrays from the native query plan without pulling any
   * input batches.
   *
   * @param plan
   *   the address to native query plan.
   * @return
   *   an array containing: 1) the status flag (0 for pending, 1 for normal returned arrays, 2)
   *   (optional) the number of rows if returned flag is 1 3) the addresses of output Arrow arrays
   */
  @native def peekNext(plan: Long): Array[Long]

  /**
   * Release and drop the native query plan object and context object.
   *
   * @param plan
   *   the address to native query plan.
   */
  @native def releasePlan(plan: Long): Unit
}
