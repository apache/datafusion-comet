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

/** Reasons and notes shared by the HLL sketch serdes. */
private[serde] object CometHllReasons {

  val sketchBytesReason: String =
    "Sketches with few distinct values (LIST or SET mode) are written in the compact layout " +
      "where Spark writes the updatable one, so the raw sketch bytes differ from Spark even " +
      "though both engines read either layout " +
      "(https://github.com/apache/datafusion-comet/issues/7548)."

  private val invalidBufferError =
    "`HLL_INVALID_INPUT_SKETCH_BUFFER` (SQLSTATE 22546)"

  private def errorNote(conditions: String): String =
    s"Errors raise a Comet execution error instead of Spark's $conditions error, so the error " +
      "class and message differ even though both engines fail on the same inputs " +
      "(https://github.com/apache/datafusion-comet/issues/7549)."

  val estimateErrorNote: String = errorNote(invalidBufferError)

  val unionErrorNote: String =
    errorNote(s"$invalidBufferError or `HLL_UNION_DIFFERENT_LG_K` (SQLSTATE 22000)")
}
