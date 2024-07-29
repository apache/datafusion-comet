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

object RegExp {

  /** Determine whether the regexp pattern is supported natively and compatible with Spark */
  def isSupportedPattern(pattern: String): Boolean = {
    // this is a placeholder for implementing logic to determine if the pattern
    // is known to be compatible with Spark, so that we can enable regexp automatically
    // for common cases and fallback to Spark for more complex cases
    false
  }

  def escape(pattern: String): String = pattern.map {
    case '\t' => "\\t"
    case '\r' => "\\r"
    case '\n' => "\\n"
    case other => other
  }.mkString

}
