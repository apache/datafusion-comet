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

package org.apache.comet.ballista

/**
 * JNI binding to the native driver-side Ballista submission entry, implemented in the
 * `datafusion-comet-ballista` crate (`libdatafusion_comet_ballista`).
 *
 * SPIKE: kept in the test tree while the offload-to-Ballista mode is being proven out.
 */
class NativeBallista {

  /**
   * Build the fixed spike test proto (a single `NativeScan` over a Parquet file with one int32
   * column `a` = [1..5]) native-side and return its serialized bytes. Lets the JVM test exercise
   * the proto boundary without depending on the generated proto Java classes.
   */
  @native def buildTestProto(): Array[Byte]

  /**
   * Run a serialized Comet `Operator` proto on an in-process standalone Ballista engine (no Spark
   * executors) and export the single result batch into the caller-allocated Arrow C Data structs.
   *
   * @param proto
   *   serialized Comet `Operator` proto
   * @param arrayAddrs
   *   memory addresses of one `ArrowArray` struct per output column
   * @param schemaAddrs
   *   memory addresses of one `ArrowSchema` struct per output column
   * @return
   *   the number of rows exported
   */
  @native def executeQuery(
      proto: Array[Byte],
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long]): Long
}
