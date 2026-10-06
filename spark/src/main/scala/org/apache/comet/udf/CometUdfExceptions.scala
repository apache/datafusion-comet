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

import org.apache.comet.CometRuntimeException

/**
 * Thrown when Spark itself evaluates a call to a UDF registered with Comet, which means Comet did
 * not take the operator holding it.
 */
class CometUdfNotEvaluatedException(name: String)
    extends CometRuntimeException(
      s"UDF '$name' is registered with Comet and runs only inside Comet's native execution, but " +
        "Spark evaluated it, which means Comet did not take the operator holding the call. The " +
        "query's extended explain output gives the reason that operator fell back to Spark.")
