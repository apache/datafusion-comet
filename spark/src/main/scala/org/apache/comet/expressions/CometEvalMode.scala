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

package org.apache.comet.expressions

/**
 * We cannot reference Spark's EvalMode directly because the package is different between Spark
 * versions, so we copy it here.
 *
 * Expression evaluation modes.
 *   - LEGACY: the default evaluation mode, which is compliant to Hive SQL.
 *   - ANSI: a evaluation mode which is compliant to ANSI SQL standard.
 *   - TRY: a evaluation mode for `try_*` functions. It is identical to ANSI evaluation mode
 *     except for returning null result on errors.
 */
object CometEvalMode extends Enumeration {
  val LEGACY, ANSI, TRY = Value

  def fromBoolean(ansiEnabled: Boolean): Value = if (ansiEnabled) {
    ANSI
  } else {
    LEGACY
  }

  def fromString(str: String): CometEvalMode.Value = CometEvalMode.withName(str)
}
