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

package org.apache.comet.shims

import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}

trait ShimSQLConf {
  protected val LEGACY = LegacyBehaviorPolicy.LEGACY
  protected val CORRECTED = LegacyBehaviorPolicy.CORRECTED

  /**
   * Reads `spark.sql.execution.arrow.useLargeVarTypes`. Spark 4.x exposes a typed accessor; 3.4
   * lacks it (a 3.5 backport added it, but Comet's 3.x shim collapses both into a single string
   * fallback). Forward to the accessor here so callers do not depend on which version they're
   * compiled against.
   */
  protected def arrowUseLargeVarTypes(conf: SQLConf): Boolean = conf.arrowUseLargeVarTypes
}
