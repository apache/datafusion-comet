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

package org.apache.spark.sql.comet.shims

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.errors.QueryExecutionErrors.toSQLConf
import org.apache.spark.sql.internal.SQLConf

// TODO: Only the Spark 3.3 version of this class is different from the others.
//       Remove this class after dropping Spark 3.3 support.
class ShimCastOverflowException(t: String, from: String, to: String)
  extends SparkArithmeticException(
    "CAST_OVERFLOW",
    Map(
      "value" -> t,
      "sourceType" -> from,
      "targetType" -> to,
      "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
    Array.empty,
    "") {}
