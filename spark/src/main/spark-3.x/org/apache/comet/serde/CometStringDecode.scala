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

import org.apache.spark.sql.catalyst.expressions.StringDecode

/**
 * `decode(bin, charset)` runs Spark's own `doGenCode` through the codegen dispatcher rather than
 * a native lowering. The previous lowering to `Cast(bin, StringType, TRY)` produced wrong results
 * on invalid byte sequences (Spark substitutes the Unicode replacement character on Spark 3.x;
 * Spark 4.0 also does so when `legacyErrorAction = true`, and otherwise raises
 * `MALFORMED_CHARACTER_CODING`). The codegen dispatcher invokes Spark's exact decoder so the
 * result matches Spark for valid inputs, replacement-character substitution, and the strict-mode
 * error.
 */
object CometStringDecode extends CometCodegenDispatch[StringDecode]
